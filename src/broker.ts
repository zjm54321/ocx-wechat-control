import type { WeChatAdapter } from "./adapter"
import { CONTROL_OFF_TEXT, HELP_TEXT, OFFLINE_TEXT, PERMISSION_DENIED_TEXT, SerialQueue, Store, formatOutbound, isPlainText, parseInboundText, type Binding, type WeixinInbound } from "./core"

type Fetch = typeof fetch
export const DEFAULT_CALLBACK_TIMEOUT_MS = 10 * 60_000
export const MIN_CALLBACK_TIMEOUT_MS = 30_000
export const MAX_CALLBACK_TIMEOUT_MS = 10 * 60_000
export function clampCallbackTimeout(value?: number): number { return Math.min(MAX_CALLBACK_TIMEOUT_MS, Math.max(MIN_CALLBACK_TIMEOUT_MS, value ?? DEFAULT_CALLBACK_TIMEOUT_MS)) }

export interface BrokerOptions { callbackTimeoutMs?: number }

export class BrokerService {
	private server?: ReturnType<typeof Bun.serve>
	private readonly queue = new SerialQueue()
	private readonly background = new Set<Promise<void>>()
	private readonly callbackTimeoutMs: number
	constructor(readonly store: Store, readonly adapter: WeChatAdapter, readonly sharedSecret: string, readonly workerToken: string, private readonly fetcher: Fetch = fetch, options: BrokerOptions = {}) { this.callbackTimeoutMs = clampCallbackTimeout(options.callbackTimeoutMs) }
	start(): string {
		if (this.server) return this.server.url.toString()
		this.server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: (request) => this.handleRequest(request) })
		return this.server.url.toString()
	}
	stop(): void { this.server?.stop(); this.server = undefined; this.adapter.stop() }
	async startAdapter(): Promise<void> { await this.adapter.start((message) => this.handleInbound(message)) }
	async drainBackground(): Promise<void> { await Promise.all([...this.background]) }
	private defer(action: Promise<void>): void {
		let guarded: Promise<void>
		guarded = action.catch(() => { try { this.store.audit("background-action-failed") } catch {} }).finally(() => this.background.delete(guarded))
		this.background.add(guarded); void guarded
	}
	async handleRequest(request: Request): Promise<Response> {
		if (request.method !== "POST") return Response.json({ error: "method" }, { status: 405 })
		if (request.headers.get("x-wechat-control-key") !== this.sharedSecret) return Response.json({ error: "unauthorized" }, { status: 401 })
		let body: any
		try { body = await request.json() } catch { return Response.json({ error: "bad-request" }, { status: 400 }) }
		if (body.method === "health") return body.challenge === this.workerToken ? Response.json({ ok: true, challenge: this.workerToken, adapter: this.adapter.status() }) : Response.json({ error: "challenge" }, { status: 401 })
		if (body.method === "register") {
			if (!validId(body.instanceId) || !validId(body.instanceToken) || !isLoopbackEndpoint(body.endpoint)) return Response.json({ error: "invalid-register" }, { status: 400 })
			if (!(await this.callbackHealth(body.endpoint, body.instanceToken))) return Response.json({ error: "callback-unreachable" }, { status: 409 })
			this.store.register(body.instanceId, body.instanceToken, body.endpoint)
			return Response.json({ ok: true })
		}
		if (!this.store.authenticate(body.instanceId, body.instanceToken)) return Response.json({ error: "instance-unauthorized" }, { status: 403 })
		if (body.method === "heartbeat") return Response.json({ ok: this.store.touch(body.instanceId, body.instanceToken) })
		if (body.method === "unregister") return Response.json({ ok: this.store.unregister(body.instanceId, body.instanceToken) })
		if (body.method === "bind-current") {
			if (this.adapter.status() !== "Ready") return Response.json({ error: "adapter-not-ready", adapter: this.adapter.status() }, { status: 503 })
			if (!validId(body.rootSessionId) || typeof body.directory !== "string" || !validId(body.conversationId) || (body.alias !== undefined && (!Number.isSafeInteger(body.alias) || body.alias < 1))) return Response.json({ error: "invalid-binding" }, { status: 400 })
			if (!(await this.callbackHealth((this.store.instance(body.instanceId))!.endpoint, body.instanceToken, body.rootSessionId))) return Response.json({ error: "callback-session-unreachable" }, { status: 409 })
			try { return Response.json({ ok: true, binding: this.store.bind({ alias: body.alias, rootSessionId: body.rootSessionId, directory: body.directory, ownerInstance: body.instanceId, conversationId: body.conversationId }) }) }
			catch { return Response.json({ error: "binding-conflict" }, { status: 409 }) }
		}
		if (body.method === "status") return Response.json({ ok: true, adapter: this.adapter.status(), manualReplay: "not-implemented-controlled" })
		if (body.method === "control-get") {
			const control = this.store.control(), binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
			return Response.json({ ok: true, ...control, adapter: this.adapter.status(), routable: Boolean(binding && binding.ownerInstance === body.instanceId && binding.contextToken) })
		}
		if (body.method === "control-set") {
			if (typeof body.enabled !== "boolean") return Response.json({ error: "invalid-control" }, { status: 400 })
			return Response.json({ ok: true, ...this.store.setControl(body.enabled) })
		}
		if (body.method === "request-input") return this.requestInput(body)
		if (body.method === "observe-status") {
			if (!this.store.control().enabled || !validId(body.rootSessionId) || (body.status !== "busy" && body.status !== "idle") || this.store.bindingForRoot(body.rootSessionId)?.ownerInstance !== body.instanceId) return Response.json({ ok: true, observed: false })
			this.store.observeStatus(body.rootSessionId, body.instanceId, body.status); await this.tryCompletion(body.rootSessionId, body.instanceId); return Response.json({ ok: true, observed: true })
		}
		if (body.method === "observe-assistant") {
			if (!this.store.control().enabled || !validId(body.rootSessionId) || !validId(body.assistantMessageId) || typeof body.failed !== "boolean" || this.store.bindingForRoot(body.rootSessionId)?.ownerInstance !== body.instanceId) return Response.json({ ok: true, observed: false })
			this.store.observeAssistant(body.rootSessionId, body.instanceId, body.assistantMessageId, body.failed); await this.tryCompletion(body.rootSessionId, body.instanceId); return Response.json({ ok: true, observed: true })
		}
		if (body.method === "permission-denied-notice") {
			const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
			if (!validId(body.permissionId) || !this.store.control().enabled || !binding?.contextToken || binding.ownerInstance !== body.instanceId) return Response.json({ ok: true, handled: false })
			const dedupeKey = `permission:${body.permissionId}`, payload = formatOutbound(binding.alias, PERMISSION_DENIED_TEXT), claim = this.store.claimControlOutbound({ dedupeKey, root: binding.rootSessionId, kind: "permission", payload })
			if (claim) this.defer(this.dispatchControl(claim.outboundId, claim.binding, payload))
			return Response.json({ ok: true, handled: Boolean(claim || this.store.controlOutboundState(dedupeKey)) })
		}
		if (body.method === "manual-replay") return Response.json({ ok: false, reason: "manual replay is an explicit placeholder and never executes" }, { status: 501 })
		// Raw bind/send/inject endpoints are intentionally locked out.
		return Response.json({ error: "method-not-allowed" }, { status: 403 })
	}
	async handleInbound(message: WeixinInbound): Promise<{ ok: boolean; reason?: string }> {
		if (this.adapter.status() !== "Ready") return { ok: false, reason: "adapter-not-ready" }
		if (!this.store.beginInbound(message)) return { ok: false, reason: "duplicate-at-least-once-key" }
		if (this.store.matchesEcho(message.fromUserId, message.contextToken, message.text)) { this.store.markUnknown(message.id, "outbound-echo"); return { ok: true, reason: "outbound-echo" } }
		const parsed = parseInboundText(message.text)
		if (parsed.ok === false) { this.store.markUnknown(message.id, parsed.reason); return { ok: false, reason: parsed.reason } }
		if (parsed.kind === "help") { await this.safeSystemReply(message, HELP_TEXT); this.store.markUnknown(message.id, "help-only"); return { ok: true } }
		if (!this.store.control().enabled) { await this.safeSystemReply(message, CONTROL_OFF_TEXT); this.store.markUnknown(message.id, "control-disabled"); return { ok: false, reason: "control-disabled" } }
		const binding = this.store.bindingForAlias(parsed.alias)
		if (!binding || binding.conversationId !== message.fromUserId) { this.store.markUnknown(message.id, "unauthorized-conversation"); await this.safeSystemReply(message, "未授权的会话编号。"); return { ok: false, reason: "unauthorized" } }
		this.store.updateContext(binding.alias, message.contextToken)
		const instance = this.store.instance(binding.ownerInstance)
		if (!instance?.online) { this.store.markUnknown(message.id, "owner-offline"); await this.safeSystemReply(message, OFFLINE_TEXT); return { ok: false, reason: "owner-offline" } }
		return this.queue.run(binding.rootSessionId, async () => {
			const control = this.store.control(); if (!control.enabled) { this.store.markUnknown(message.id, "control-changed-before-injection"); return { ok: false, reason: "control-disabled" } }
			const checkpoint = this.store.openCheckpointFor(binding)
			if (checkpoint && !this.store.claimCheckpoint(checkpoint.checkpointId, message.id, binding)) { this.store.markUnknown(message.id, "checkpoint-consumed"); return { ok: false, reason: "checkpoint-consumed" } }
			this.store.beginPending(message.id, binding.rootSessionId, binding.alias, control.revision)
			this.store.markDirectPending(binding.rootSessionId, binding.ownerInstance, message.id, checkpoint?.checkpointId)
			try {
				const response = await this.fetcher(`${instance.endpoint}/inject`, { method: "POST", headers: callbackHeaders(this.sharedSecret, instance.instanceToken), body: JSON.stringify({ rootSessionId: binding.rootSessionId, directory: binding.directory, text: parsed.body, inboundId: message.id, envelope: checkpoint ? { kind: "checkpoint", checkpointId: checkpoint.checkpointId } : { kind: "inbound" } }), signal: AbortSignal.timeout(this.callbackTimeoutMs) })
				const result = await response.json() as any
				if (!response.ok || !validId(result.promptMessageId) || !validId(result.assistantMessageId) || typeof result.text !== "string" || !isPlainText(result.text)) throw new Error("callback rejected")
				if (checkpoint) this.store.checkpointAnswered(checkpoint.checkpointId)
				if (this.adapter.status() !== "Ready") throw new Error("adapter degraded before send")
				const claimed = this.store.completePendingAndClaim(binding.ownerInstance, binding.rootSessionId, message.id, result.promptMessageId, result.assistantMessageId, result.text, control.revision)
				if (!claimed || !this.store.controlMatches(control.revision)) { this.store.markUnknown(message.id, "control-changed-before-send"); return { ok: false, reason: "control-changed-before-send" } }
				this.store.markDirectAssistant(binding.rootSessionId, binding.ownerInstance, result.assistantMessageId)
				try { this.store.recordEcho(claimed.binding.conversationId, claimed.binding.contextToken!, claimed.payload); await this.adapter.send(claimed.binding.conversationId, claimed.payload, claimed.binding.contextToken!); this.store.finishReply(message.id, result.assistantMessageId, true); return { ok: true } }
				catch { this.store.finishReply(message.id, result.assistantMessageId, false); return { ok: false, reason: "unknown-no-replay" } }
			} catch { this.store.markUnknown(message.id, "callback-failed-or-timeout"); if (checkpoint) this.store.checkpointInjectionUnknown(checkpoint.checkpointId); return { ok: false, reason: "callback-failed-or-timeout" } }
		})
	}
	private async requestInput(body: any): Promise<Response> {
		const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
		if (!binding || binding.ownerInstance !== body.instanceId || !validId(body.requestKey)) return Response.json({ error: "request-input-unavailable" }, { status: 409 })
		const replay = this.store.checkpointForRequest(body.requestKey, binding.rootSessionId); if (replay) return Response.json({ ok: replay.state !== "UNKNOWN", replayed: true, checkpointId: replay.checkpointId, state: replay.state, ownerChanged: replay.ownerInstance !== body.instanceId, mode: "async-new-turn" })
		const control = this.store.control(), rawChoices = body.choices as unknown
		if (this.adapter.status() !== "Ready") return Response.json({ error: "adapter-not-ready" }, { status: 503 })
		if (!binding.contextToken || !control.enabled || typeof body.question !== "string" || !isPlainText(body.question) || !body.question.trim() || body.question.length > 1500 || !Array.isArray(rawChoices) || rawChoices.length > 8 || rawChoices.some((value: unknown) => typeof value !== "string" || !isPlainText(value) || !value.trim() || value.length > 120 || /[\r\n]/.test(value))) return Response.json({ error: "invalid-request-input" }, { status: 409 })
		const choices = rawChoices as string[]
		const choiceText = choices.length ? `\n${choices.map((choice, index) => `${index + 1}. ${choice}`).join("\n")}` : ""
		const bodyText = `${body.question}${choiceText}\n请回复 #${binding.alias} 后换行填写答案。`, payload = formatOutbound(binding.alias, bodyText)
		if (!isPlainText(bodyText) || payload.length > 4000) return Response.json({ error: "checkpoint-payload-too-large" }, { status: 409 })
		const checkpointId = crypto.randomUUID()
		if (!this.store.openCheckpoint({ checkpointId, requestKey: body.requestKey, root: binding.rootSessionId, owner: body.instanceId, conversationId: binding.conversationId, alias: binding.alias, question: body.question, choices: choices as string[], revision: control.revision })) return Response.json({ error: "checkpoint-already-active" }, { status: 409 })
		const sent = await this.sendControl(binding, `checkpoint:${body.requestKey}`, "checkpoint", payload, control.revision)
		if (!sent) { this.store.failCheckpoint(checkpointId); return Response.json({ ok: false, checkpointId, state: "UNKNOWN", unknown: true, mode: "async-new-turn" }) }
		if (!this.store.activateCheckpoint(checkpointId)) return Response.json({ error: "checkpoint-cancelled-after-send" }, { status: 409 })
		return Response.json({ ok: true, checkpointId, state: "OPEN", mode: "async-new-turn" })
	}
	private async tryCompletion(root: string, owner: string): Promise<void> {
		const claim = this.store.claimCompletion(root, owner); if (claim) this.defer(this.dispatchControl(claim.outboundId, claim.binding, claim.payload))
	}
	private async sendControl(binding: Binding, dedupeKey: string, kind: string, payload: string, revision?: number): Promise<boolean> {
		const claim = this.store.claimControlOutbound({ dedupeKey, root: binding.rootSessionId, kind, payload, revision }); if (!claim) return false
		await this.dispatchControl(claim.outboundId, claim.binding, payload); return this.store.controlOutboundState(dedupeKey) === "SENT"
	}
	private async dispatchControl(outboundId: string, binding: Binding, payload: string): Promise<void> { this.store.recordEcho(binding.conversationId, binding.contextToken!, payload); try { await this.adapter.send(binding.conversationId, payload, binding.contextToken!); this.store.finishControlOutbound(outboundId, true) } catch { this.store.finishControlOutbound(outboundId, false) } }
	private async callbackHealth(endpoint: string, token: string, rootSessionId?: string): Promise<boolean> {
		try { const response = await this.fetcher(`${endpoint}/health`, { method: "POST", headers: callbackHeaders(this.sharedSecret, token), body: JSON.stringify({ rootSessionId }), signal: AbortSignal.timeout(3000) }); return response.ok }
		catch { return false }
	}
	private async safeSystemReply(message: WeixinInbound, text: string): Promise<void> { const kind = text === HELP_TEXT ? "help" : text === CONTROL_OFF_TEXT ? "control-off" : text === OFFLINE_TEXT ? "offline" : "unauthorized", claim = this.store.claimSystemOutbound(message, kind, text); if (!claim) return; this.store.recordEcho(message.fromUserId, message.contextToken, text); try { await this.adapter.send(message.fromUserId, text, message.contextToken); this.store.finishControlOutbound(claim.outboundId, true) } catch { this.store.finishControlOutbound(claim.outboundId, false) } }
}

function validId(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 500 }
function isLoopbackEndpoint(value: unknown): value is string { if (typeof value !== "string") return false; try { const url = new URL(value); return url.protocol === "http:" && url.hostname === "127.0.0.1" } catch { return false } }
function callbackHeaders(secret: string, token: string): HeadersInit { return { "content-type": "application/json", "x-wechat-control-key": secret, "x-wechat-instance-token": token } }

export function makeRpcRequest(endpoint: string, secret: string, body: object, timeoutMs = 10_000): Promise<Response> {
	return fetch(endpoint, { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": secret }, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) })
}
