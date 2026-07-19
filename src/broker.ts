import type { WeChatAdapter } from "./adapter"
import type { TypingCoordinator } from "./typing"
import { CONTROL_OFF_TEXT, HELP_TEXT, OFFLINE_TEXT, PERMISSION_DENIED_TEXT, REQUEST_CODE_PATTERN, SerialQueue, Store, boundedJson, formatOutbound, formatRegistrationList, isPlainText, isValidRouteMetadata, parseInboundText, type Binding, type NativeRequest, type NativeRequestKind, type RoutedBinding, type WeixinInbound } from "./core"
import type { WorkerMetadata } from "./worker-protocol"

type Fetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>
type JsonObject = Record<string, unknown>
type RuntimeSnapshot = ReturnType<Store["activeRuntimeSnapshots"]>[number]
export const DEFAULT_CALLBACK_TIMEOUT_MS = 10 * 60_000
export const MIN_CALLBACK_TIMEOUT_MS = 30_000
export const MAX_CALLBACK_TIMEOUT_MS = 10 * 60_000
const MAX_NATIVE_QUESTIONS = 16
const MAX_NATIVE_OPTIONS = 32
const MAX_NATIVE_FIELD = 1000
const NATIVE_PROCESSING_TEXT = "该请求正在处理中，请勿重复提交。"
const NATIVE_UNKNOWN_TEXT = "该请求状态不确定，请在 OpenCode 本地界面处理。"
const NATIVE_INVALID_TEXT = "请求编号无效或已过期，请按当前会话的请求重新回复。"
const NATIVE_CROSS_ROOT_TEXT = "该请求属于另一个当前会话；请使用对应的会话编号和请求编号。"

export function clampCallbackTimeout(value?: number): number { return Math.min(MAX_CALLBACK_TIMEOUT_MS, Math.max(MIN_CALLBACK_TIMEOUT_MS, value ?? DEFAULT_CALLBACK_TIMEOUT_MS)) }
export interface BrokerOptions { callbackTimeoutMs?: number; typing?: TypingCoordinator; workerMetadata?: WorkerMetadata }

export interface NativeQuestion {
	question: string
	options: Array<{ label: string; description?: string }>
	multiple: boolean
	custom: boolean
}
export interface NativeQuestionPayload { sourceSessionId: string; questions: NativeQuestion[] }
export interface NativePermissionPayload { sourceSessionId: string; permission: string }

function object(value: unknown): JsonObject | undefined { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined }
function validId(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 500 && !/[\u0000-\u001f\u007f]/.test(value) }
function boundedText(value: unknown, max = MAX_NATIVE_FIELD): value is string { return typeof value === "string" && value.length > 0 && value.length <= max && isPlainText(value) }
function isLoopbackEndpoint(value: unknown): value is string { if (typeof value !== "string") return false; try { const url = new URL(value); return url.protocol === "http:" && url.hostname === "127.0.0.1" } catch { return false } }
function callbackHeaders(secret: string, token: string): HeadersInit { return { "content-type": "application/json", "x-wechat-control-key": secret, "x-wechat-instance-token": token } }

function parseQuestionPayload(value: unknown): NativeQuestionPayload | undefined {
	const payload = object(value)
	if (!payload || !validId(payload.sourceSessionId) || !Array.isArray(payload.questions) || payload.questions.length < 1 || payload.questions.length > MAX_NATIVE_QUESTIONS) return
	const questions: NativeQuestion[] = []
	for (const raw of payload.questions) {
		const question = object(raw)
		if (!question || !boundedText(question.question) || !Array.isArray(question.options) || question.options.length > MAX_NATIVE_OPTIONS || (question.multiple !== undefined && typeof question.multiple !== "boolean") || (question.custom !== undefined && typeof question.custom !== "boolean")) return
		const options: Array<{ label: string; description?: string }> = []
		for (const rawOption of question.options) {
			const option = object(rawOption)
			if (!option || !boundedText(option.label, 120) || /[\r\n,]/.test(option.label) || (option.description !== undefined && !boundedText(option.description, 500))) return
			options.push({ label: option.label, ...(typeof option.description === "string" ? { description: option.description } : {}) })
		}
		if (!options.length && question.custom === false) return
		questions.push({ question: question.question, options, multiple: question.multiple === true, custom: question.custom !== false })
	}
	return { sourceSessionId: payload.sourceSessionId, questions }
}

function parsePermissionPayload(value: unknown): NativePermissionPayload | undefined {
	const payload = object(value)
	return payload && validId(payload.sourceSessionId) && boundedText(payload.permission, 500) ? { sourceSessionId: payload.sourceSessionId, permission: payload.permission } : undefined
}

function parseQuestionToken(question: NativeQuestion, token: string): string[] | undefined {
	const parts = question.multiple ? token.split(",").map((item) => item.trim()) : [token]
	if (!parts.length || parts.some((item) => !item)) return
	if (!question.multiple && parts.length !== 1) return
	const answers: string[] = []
	for (const part of parts) {
		let answer: string | undefined
		if (/^[1-9][0-9]*$/.test(part)) { const index = Number(part); answer = question.options[index - 1]?.label }
		else if (part.startsWith("=")) { const custom = part.slice(1); if (question.custom && boundedText(custom, MAX_NATIVE_FIELD)) answer = custom }
		else answer = question.options.find((option) => option.label === part)?.label
		if (!answer || answers.includes(answer)) return
		answers.push(answer)
	}
	return answers
}

export function parseQuestionAnswers(payload: NativeQuestionPayload, body: string): string[][] | undefined {
	if (!isPlainText(body)) return
	if (payload.questions.length === 1) { const answer = parseQuestionToken(payload.questions[0], body); return answer ? [answer] : undefined }
	const lines = body.replace(/\r\n?/g, "\n").split("\n")
	if (lines.length !== payload.questions.length) return
	const ordered: Array<string[] | undefined> = Array(payload.questions.length)
	for (const line of lines) {
		const match = /^([1-9][0-9]*):[ \t]*(.+)$/.exec(line)
		if (!match) return
		const index = Number(match[1]) - 1
		if (index < 0 || index >= payload.questions.length || ordered[index]) return
		ordered[index] = parseQuestionToken(payload.questions[index], match[2])
		if (!ordered[index]) return
	}
	return ordered.every(Boolean) ? ordered as string[][] : undefined
}

function requestCode(body: string): { code: string; answer: string } | undefined {
	const match = /^([QP][A-Z2-7]{6})(?:[ \t]+|\n|$)/.exec(body)
	if (!match) return
	return { code: match[1], answer: body.slice(match[0].length) }
}

function nativeAnswerUsage(request: NativeRequest, alias: number): string {
	if (request.kind === "PERMISSION") return `答案格式无效。请一次性回复 #${alias}\n${request.code} once 或 #${alias}\n${request.code} reject；仅接受 once 或 reject。`
	const payload = parseQuestionPayload(request.payload)
	if (!payload) return "答案格式无效，请按请求中的选项和编号重新回复。"
	if (payload.questions.length === 1) {
		const options = payload.questions[0].options.map((option, index) => `${index + 1}=${option.label}`).join("、")
		return `答案格式无效。请一次性回复 #${alias}\n${request.code} 1，或 #${alias}\n1。可选：${options || "允许自定义"}`
	}
	return `答案格式无效。请一次性回复 #${alias}\n${request.code} 后按“问题序号: 答案”逐行填写；不能拆成多条消息。`
}

function nativeRelayText(request: NativeRequest, alias: number): string | undefined {
	if (request.kind === "PERMISSION") {
		const payload = parsePermissionPayload(request.payload); if (!payload) return
		return `${request.code} 权限请求\n${payload.permission}\n请一次性回复 #${alias}\n${request.code} once 或 #${alias}\n${request.code} reject；仅接受 once 或 reject。`
	}
	const payload = parseQuestionPayload(request.payload); if (!payload) return
	const lines = [`${request.code} 问题请求`]
	payload.questions.forEach((question, index) => { if (index > 0) lines.push(""); lines.push(`${payload.questions.length > 1 ? `${index + 1}: ` : ""}${question.question}`); question.options.forEach((option, optionIndex) => lines.push(`${optionIndex + 1}. ${option.label}`)) })
	lines.push(payload.questions.length > 1 ? `请一次性回复 #${alias}\n${request.code} 后按“N: 答案”逐行填写。` : `请一次性回复 #${alias}\n${request.code} 1，或 #${alias}\n1。`)
	const text = lines.join("\n"); return isPlainText(text) ? text : undefined
}

export class BrokerService {
	private server?: ReturnType<typeof Bun.serve>
	private readonly queue = new SerialQueue()
	private readonly background = new Set<Promise<void>>()
	private readonly callbackTimeoutMs: number
	private readonly typing?: TypingCoordinator
	private readonly workerMetadata?: WorkerMetadata
	constructor(readonly store: Store, readonly adapter: WeChatAdapter, readonly sharedSecret: string, readonly workerToken: string, private readonly fetcher: Fetch = fetch, options: BrokerOptions = {}) { this.callbackTimeoutMs = clampCallbackTimeout(options.callbackTimeoutMs); this.typing = options.typing; this.workerMetadata = options.workerMetadata }
	start(): string { if (this.server) return this.server.url.toString(); this.server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: (request) => this.handleRequest(request) }); return this.server.url.toString() }
	stop(): void { this.server?.stop(); this.server = undefined; this.adapter.stop() }
	async startAdapter(): Promise<void> { await this.adapter.start(async (message) => { await this.handleInbound(message) }) }
	async drainBackground(): Promise<void> { await Promise.all([...this.background]) }
	async reconcileActiveRuntimes(now = Date.now(), leaseMs?: number): Promise<void> {
		const snapshots = this.store.activeRuntimeSnapshots(), groups = new Map<string, typeof snapshots>()
		for (const snapshot of snapshots) { const key = JSON.stringify([snapshot.ownerInstance, snapshot.directory, snapshot.endpoint, snapshot.instanceToken]); const group = groups.get(key) ?? []; group.push(snapshot); groups.set(key, group) }
		await Promise.all([...groups.values()].map(async (group) => {
			const resolved = await this.queryRuntimeStatuses(group, now, leaseMs)
			if (group.length > 1) await Promise.all(group.filter((snapshot) => !resolved.has(snapshot.rootSessionId)).map(async (snapshot) => { await this.queryRuntimeStatuses([snapshot], now, leaseMs) }))
		}))
		this.refreshTyping()
	}
	private runtimeSnapshotCurrent(snapshot: RuntimeSnapshot): boolean {
		const binding = this.store.bindingForRoot(snapshot.rootSessionId), instance = this.store.instance(snapshot.ownerInstance), runtime = this.store.runtime(snapshot.rootSessionId)
		return Boolean(binding?.active && binding.ownerInstance === snapshot.ownerInstance && binding.directory === snapshot.directory && instance?.endpoint === snapshot.endpoint && instance.instanceToken === snapshot.instanceToken && runtime?.ownerInstance === snapshot.ownerInstance && runtime.generation === snapshot.generation)
	}
	private async queryRuntimeStatuses(group: RuntimeSnapshot[], now: number, leaseMs?: number): Promise<Set<string>> {
		const resolved = new Set<string>(), first = group[0]
		try {
			const response = await this.fetcher(`${first.endpoint}/runtime-status`, { method: "POST", headers: callbackHeaders(this.sharedSecret, first.instanceToken), body: JSON.stringify({ rootSessionId: first.rootSessionId, rootSessionIds: group.map((item) => item.rootSessionId), directory: first.directory }), signal: AbortSignal.timeout(3000) })
			const value = object(await response.json())
			if (value?.error === "not-root") {
				if (group.length === 1 && this.runtimeSnapshotCurrent(first)) { this.store.deactivateBinding(first.rootSessionId, first.ownerInstance); resolved.add(first.rootSessionId) }
				return resolved
			}
			if (!response.ok || !Array.isArray(value?.statuses)) return resolved
			for (const raw of value.statuses) {
				const row = object(raw), snapshot = group.find((item) => item.rootSessionId === row?.rootSessionId)
				if (!snapshot || (row?.status !== "BUSY" && row?.status !== "RETRY" && row?.status !== "IDLE")) continue
				resolved.add(snapshot.rootSessionId)
				if (this.runtimeSnapshotCurrent(snapshot)) this.store.syncRuntimeAuthoritative(snapshot.rootSessionId, snapshot.ownerInstance, row.status, snapshot.generation, now, leaseMs)
			}
		} catch {}
		return resolved
	}
	private defer(action: Promise<void>): void { let guarded: Promise<void>; guarded = action.catch(() => { try { this.store.audit("background-action-failed") } catch {} }).finally(() => this.background.delete(guarded)); this.background.add(guarded); void guarded }

	async handleRequest(request: Request): Promise<Response> {
		if (request.method !== "POST") return Response.json({ error: "method" }, { status: 405 })
		if (request.headers.get("x-wechat-control-key") !== this.sharedSecret) return Response.json({ error: "unauthorized" }, { status: 401 })
		let body: JsonObject | undefined
		try { body = object(await request.json()) } catch {}
		if (!body) return Response.json({ error: "bad-request" }, { status: 400 })
		if (body.method === "health") return body.challenge === this.workerToken ? Response.json({ ok: true, challenge: this.workerToken, adapter: this.adapter.status(), ...this.workerMetadata }) : Response.json({ error: "challenge" }, { status: 401 })
		if (body.method === "register") {
			if (!validId(body.instanceId) || !validId(body.instanceToken) || !isLoopbackEndpoint(body.endpoint)) return Response.json({ error: "invalid-register" }, { status: 400 })
			if (!(await this.callbackHealth(body.endpoint, body.instanceToken))) return Response.json({ error: "callback-unreachable" }, { status: 409 })
			this.store.register(body.instanceId, body.instanceToken, body.endpoint); return Response.json({ ok: true })
		}
		if (!validId(body.instanceId) || !validId(body.instanceToken)) return Response.json({ error: "instance-unauthorized" }, { status: 403 })
		if (body.method === "unregister") { const ok = this.store.unregister(body.instanceId, body.instanceToken); if (ok) this.refreshTyping(); return Response.json({ ok }) }
		if (!this.store.authenticate(body.instanceId, body.instanceToken)) return Response.json({ error: "instance-unauthorized" }, { status: 403 })
		if (body.method === "heartbeat") return Response.json({ ok: this.store.touch(body.instanceId, body.instanceToken) })
		if (body.method === "bind-current") return Response.json({ error: "deprecated-manual-binding" }, { status: 410 })
		if (body.method === "leave-root") {
			if (!validId(body.rootSessionId) || typeof body.directory !== "string" || !body.directory || body.directory.length > 32_767 || (body.title !== null && body.title !== undefined && (typeof body.title !== "string" || body.title.length > 1000))) return Response.json({ error: "invalid-registration" }, { status: 400 })
			const instance = this.store.instance(body.instanceId), health = instance ? await this.callbackHealth(instance.endpoint, body.instanceToken, body.rootSessionId) : "unavailable"; if (health !== "ok") { if (health === "not-root") this.store.deactivateBinding(body.rootSessionId, body.instanceId); return Response.json({ error: "callback-session-unreachable" }, { status: 409 }) }
			try { const binding = this.store.bind({ rootSessionId: body.rootSessionId, directory: body.directory, ownerInstance: body.instanceId, title: typeof body.title === "string" ? body.title : null }); this.refreshTyping(); return Response.json({ ok: true, binding }) } catch (error) { return Response.json({ error: error instanceof Error && error.message === "owner-live" ? "owner-live" : "registration-conflict" }, { status: 409 }) }
		}
		if (body.method === "status") return Response.json({ ok: true, adapter: this.adapter.status(), manualReplay: "not-implemented-controlled" })
		if (body.method === "control-get") { const control = this.store.control(), binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined, route = this.store.route(); return Response.json({ ok: true, ...control, adapter: this.adapter.status(), registered: Boolean(binding?.active), alias: binding?.alias ?? null, routeReady: Boolean(route.conversationId && route.contextToken), routable: Boolean(binding?.active && binding.ownerInstance === body.instanceId && route.conversationId && route.contextToken) }) }
		if (body.method === "back-global") { const result = this.store.setControl(false); this.refreshTyping(); return Response.json({ ok: true, ...result }) }
		if (body.method === "control-set") return Response.json({ error: "deprecated-control" }, { status: 410 })
		if (body.method === "native-request-open") return this.nativeRequestOpen(body)
		if (body.method === "native-request-terminal") return this.nativeRequestTerminal(body)
		if (body.method === "wechat-reply") return this.wechatReply(body)
		if (body.method === "request-input") return this.requestInput(body)
		if (body.method === "observe-status") {
			const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
			if (!binding?.active || binding.ownerInstance !== body.instanceId || (body.status !== "busy" && body.status !== "retry" && body.status !== "idle")) return Response.json({ ok: true, observed: false })
			const instance = this.store.instance(binding.ownerInstance), before = this.store.runtime(binding.rootSessionId)
			if (!instance) return Response.json({ ok: true, observed: false })
			let status: "BUSY" | "RETRY" | "IDLE" | undefined
			try { const response = await this.fetcher(`${instance.endpoint}/runtime-status`, { method: "POST", headers: callbackHeaders(this.sharedSecret, body.instanceToken as string), body: JSON.stringify({ rootSessionId: binding.rootSessionId, rootSessionIds: [binding.rootSessionId], directory: binding.directory }), signal: AbortSignal.timeout(3000) }); const value = object(await response.json()); if (value?.error === "not-root") this.store.deactivateBinding(binding.rootSessionId, binding.ownerInstance); const rows = value?.statuses; const row = Array.isArray(rows) ? object(rows[0]) : undefined; if (response.ok && row?.rootSessionId === binding.rootSessionId && (row.status === "BUSY" || row.status === "RETRY" || row.status === "IDLE")) status = row.status } catch {}
			const current = this.store.runtime(binding.rootSessionId)
			if (!status || (before ? current?.generation !== before.generation : current !== undefined)) return Response.json({ ok: true, observed: false })
			if (before) this.store.syncRuntimeAuthoritative(binding.rootSessionId, binding.ownerInstance, status, before.generation)
			else this.store.reconcileRuntimeAuthoritative(binding.rootSessionId, binding.ownerInstance, status)
			this.refreshTyping()
			return Response.json({ ok: true, observed: true })
		}
		if (body.method === "observe-assistant") return Response.json({ ok: true, observed: false })
		if (body.method === "permission-denied-notice") {
			const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
			if (!validId(body.permissionId) || !this.store.control().enabled || !this.store.route().contextToken || !binding?.active || binding.ownerInstance !== body.instanceId) return Response.json({ ok: true, handled: false })
			const dedupeKey = `permission:${body.permissionId}`, payload = formatOutbound(binding.alias, PERMISSION_DENIED_TEXT), claim = this.store.claimControlOutbound({ dedupeKey, root: binding.rootSessionId, kind: "permission", payload })
			if (claim) this.defer(this.dispatchControl(claim.outboundId, claim.binding, payload)); return Response.json({ ok: true, handled: Boolean(claim || this.store.controlOutboundState(dedupeKey)) })
		}
		if (body.method === "manual-replay") return Response.json({ ok: false, reason: "manual replay is an explicit placeholder and never executes" }, { status: 501 })
		return Response.json({ error: "method-not-allowed" }, { status: 403 })
	}

	async handleInbound(message: WeixinInbound): Promise<{ ok: boolean; reason?: string }> {
		if (this.adapter.status() !== "Ready") return { ok: false, reason: "adapter-not-ready" }
		if (!isValidRouteMetadata(message.fromUserId, message.contextToken)) return { ok: false, reason: "invalid-route-metadata" }
		if (!this.store.beginInbound(message)) return { ok: false, reason: "duplicate-at-least-once-key" }
		if (this.store.matchesEcho(message.fromUserId, message.contextToken, message.text)) { this.store.markUnknown(message.id, "outbound-echo"); return { ok: true, reason: "outbound-echo" } }
		const parsed = parseInboundText(message.text)
		if (parsed.ok === false) { this.store.markUnknown(message.id, parsed.reason); return { ok: false, reason: parsed.reason } }
		const route = this.store.acceptInboundRoute(message.fromUserId, message.contextToken, parsed.kind === "list")
		if (route === "REJECTED") { this.store.markUnknown(message.id, "route-rejected"); return { ok: false, reason: "route-rejected" } }
		this.refreshTyping()
		if (parsed.kind === "help") { await this.safeSystemReply(message, "help", HELP_TEXT); this.store.markUnknown(message.id, "help-only"); return { ok: true } }
		if (parsed.kind === "list") { await this.safeSystemReply(message, "list", formatRegistrationList(this.store.bindings())); this.store.markUnknown(message.id, "list-only"); return { ok: true } }
		if (!this.store.control().enabled) { await this.safeSystemReply(message, "control-off", CONTROL_OFF_TEXT); this.store.markUnknown(message.id, "control-disabled"); return { ok: false, reason: "control-disabled" } }
		const binding = this.store.bindingForAlias(parsed.alias)
		if (!binding?.active) { this.store.markUnknown(message.id, "invalid-alias"); await this.safeSystemReply(message, "unauthorized", "未授权的会话编号。"); return { ok: false, reason: "unauthorized" } }
		const instance = this.store.instance(binding.ownerInstance)
		if (!instance?.online) { this.store.markUnknown(message.id, "owner-offline"); await this.safeSystemReply(message, "offline", OFFLINE_TEXT); return { ok: false, reason: "owner-offline" } }

		const explicit = requestCode(parsed.body)
		if (explicit) {
			const native = this.store.nativeRequest(explicit.code)
			if (!native) return this.nativeNotice(message, "native-invalid", NATIVE_INVALID_TEXT, "native-code-invalid")
			if (native.rootSessionId !== binding.rootSessionId) return this.nativeNotice(message, "native-cross-root", NATIVE_CROSS_ROOT_TEXT, "native-code-cross-root")
			if (!this.store.activeNativeRequests(binding.rootSessionId).some((item) => item.requestId === native.requestId)) return this.nativeNotice(message, "native-invalid", NATIVE_INVALID_TEXT, "native-code-invalid")
			if (!explicit.answer) this.store.recordNativeAnswerGuard(binding.rootSessionId, native.requestId)
			return this.resolveNative(message, binding, instance, native, explicit.answer)
		}
		const query = this.store.nativeQuery(binding.rootSessionId)
		if (query.kind === "ONE") {
			if (this.store.consumeNativeAnswerGuard(binding.rootSessionId, query.request.requestId)) return this.nativeNotice(message, "native-usage", nativeAnswerUsage(query.request, binding.alias), "native-answer-invalid")
			return this.resolveNative(message, binding, instance, query.request, parsed.body)
		}
		if (query.kind === "MULTIPLE") return this.nativeNotice(message, "native-choices", `存在多个待处理请求，请一次性回复 #${binding.alias}\n请求编号和答案：\n${query.requests.map((item) => item.code).join("\n")}`, "native-code-required")
		return this.admitPrompt(message, binding, instance, parsed.body)
	}

	private async admitPrompt(message: WeixinInbound, binding: Binding, instance: { endpoint: string; instanceToken: string }, text: string): Promise<{ ok: boolean; reason?: string }> {
		return this.queue.run(binding.rootSessionId, async () => {
			const control = this.store.control()
			const currentBinding = this.store.bindingForRoot(binding.rootSessionId)
			if (!control.enabled || !currentBinding?.active || currentBinding.ownerInstance !== binding.ownerInstance) { this.store.markUnknown(message.id, "control-changed-before-admission"); return { ok: false, reason: "control-disabled" } }
			const currentInstance = this.store.instance(currentBinding.ownerInstance)
			if (!currentInstance?.online) { this.store.markUnknown(message.id, "owner-offline-before-admission"); return { ok: false, reason: "owner-offline" } }
			const submission = this.store.claimPromptSubmission({ submissionId: message.id, inboundId: message.id, root: currentBinding.rootSessionId, owner: currentBinding.ownerInstance, alias: currentBinding.alias, body: text, revision: control.revision })
			if (!submission || submission.state !== "SUBMITTING" || this.store.beginRuntimeAdmission(message.id, binding.rootSessionId, binding.ownerInstance) === undefined) { this.store.markUnknown(message.id, "prompt-claim-failed"); return { ok: false, reason: "prompt-claim-failed" } }
			const messageId = submission.messageId
			this.refreshTyping()
			if (!this.store.markPromptCallStarted(message.id)) { this.store.finishRuntimeAdmission(message.id, binding.rootSessionId, binding.ownerInstance); this.store.markUnknown(message.id, "prompt-call-cancelled"); return { ok: false, reason: "prompt-call-cancelled" } }
			try {
				const response = await this.fetcher(`${currentInstance.endpoint}/submit-prompt`, { method: "POST", headers: callbackHeaders(this.sharedSecret, currentInstance.instanceToken), body: JSON.stringify({ rootSessionId: currentBinding.rootSessionId, directory: currentBinding.directory, inboundId: message.id, messageId, text }), signal: AbortSignal.timeout(this.callbackTimeoutMs) })
				let result: JsonObject | undefined; try { result = object(await response.json()) } catch {}
				if (response.ok && result?.ok === true && result.accepted === true) { const afterBinding = this.store.bindingForRoot(binding.rootSessionId); if (afterBinding?.active && afterBinding.ownerInstance === binding.ownerInstance && this.store.finishPromptSubmission(message.id, "SUBMITTED", messageId) && this.store.controlMatches(control.revision)) { this.store.finishInboundAdmission(message.id, binding.rootSessionId, messageId); return { ok: true } }; this.store.markUnknown(message.id, "control-changed-after-admission"); return { ok: false, reason: "control-changed-after-admission" } }
				if (result?.certainty === "REJECTED") { this.store.rejectPromptSubmissionNoEffect(message.id, binding.rootSessionId, binding.ownerInstance, typeof result.error === "string" ? result.error : undefined); if (result.error === "not-root") this.store.deactivateBinding(binding.rootSessionId, binding.ownerInstance); this.store.markUnknown(message.id, "prompt-rejected-no-effect"); return { ok: false, reason: "prompt-rejected" } }
				this.store.finishPromptSubmission(message.id, "UNKNOWN"); this.store.markUnknown(message.id, "prompt-admission-uncertain"); return { ok: false, reason: "unknown-no-replay" }
			} catch { this.store.finishPromptSubmission(message.id, "UNKNOWN"); this.store.markUnknown(message.id, "prompt-admission-uncertain"); return { ok: false, reason: "unknown-no-replay" } } finally { this.store.finishRuntimeAdmission(message.id, binding.rootSessionId, binding.ownerInstance); this.refreshTyping() }
		})
	}

	private async resolveNative(message: WeixinInbound, binding: Binding, instance: { endpoint: string; instanceToken: string }, native: NativeRequest, answerBody: string): Promise<{ ok: boolean; reason?: string }> {
		const currentBinding = this.store.bindingForRoot(binding.rootSessionId)
		if (!currentBinding?.active || currentBinding.ownerInstance !== binding.ownerInstance) return this.nativeNotice(message, "native-inactive", "该会话已不再活动，请使用当前 id 列表。", "native-session-inactive")
		instance = this.store.instance(currentBinding.ownerInstance) ?? instance
		binding = currentBinding
		if (native.state === "RESOLVING" || native.state === "ANNOUNCING") return this.nativeNotice(message, "native-processing", NATIVE_PROCESSING_TEXT, "native-processing")
		if (native.state === "UNKNOWN") return this.nativeNotice(message, "native-local", NATIVE_UNKNOWN_TEXT, "native-unknown-local")
		if (native.state !== "OPEN") return this.nativeNotice(message, "native-invalid", NATIVE_INVALID_TEXT, "native-code-invalid")
		let callbackPath: string, callbackBody: JsonObject, resolution: unknown, terminal: "RESOLVED" | "REJECTED" = "RESOLVED"
		if (native.kind === "QUESTION") {
			const payload = parseQuestionPayload(native.payload), parsed = payload && parseQuestionAnswers(payload, answerBody)
			if (!payload || !parsed) return this.nativeNotice(message, "native-usage", nativeAnswerUsage(native, binding.alias), "native-answer-invalid")
			callbackPath = "/resolve-question"; resolution = parsed; callbackBody = { rootSessionId: binding.rootSessionId, sourceSessionId: payload.sourceSessionId, requestId: native.requestId, directory: binding.directory, answers: parsed }
		} else {
			const payload = parsePermissionPayload(native.payload)
			if (!payload || (answerBody !== "once" && answerBody !== "reject")) return this.nativeNotice(message, "native-usage", nativeAnswerUsage(native, binding.alias), "native-answer-invalid")
			callbackPath = "/resolve-permission"; resolution = answerBody; terminal = answerBody === "reject" ? "REJECTED" : "RESOLVED"; callbackBody = { rootSessionId: binding.rootSessionId, sourceSessionId: payload.sourceSessionId, requestId: native.requestId, directory: binding.directory, decision: answerBody }
		}
		if (!this.store.claimNativeResolution(native.requestId, message.id)) return this.nativeNotice(message, "native-race", NATIVE_PROCESSING_TEXT, "native-resolution-race")
		this.store.markUnknown(message.id, "native-resolution")
		try {
			const response = await this.fetcher(`${instance.endpoint}${callbackPath}`, { method: "POST", headers: callbackHeaders(this.sharedSecret, instance.instanceToken), body: JSON.stringify(callbackBody), signal: AbortSignal.timeout(this.callbackTimeoutMs) })
			let result: JsonObject | undefined; try { result = object(await response.json()) } catch {}
			if (response.ok && result?.ok === true && result.resolved === true) { this.store.finishNativeResolution(native.requestId, terminal, resolution); return { ok: true } }
			if (result?.certainty === "REJECTED") { this.store.releaseNativeResolution(native.requestId, message.id); return { ok: false, reason: "native-definite-rejection" } }
			this.store.finishNativeResolution(native.requestId, "UNKNOWN"); return { ok: false, reason: "unknown-no-replay" }
		} catch { this.store.finishNativeResolution(native.requestId, "UNKNOWN"); return { ok: false, reason: "unknown-no-replay" } }
	}

	private async nativeRequestOpen(body: JsonObject): Promise<Response> {
		const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined, kind = body.kind
		if (!binding?.active || binding.ownerInstance !== body.instanceId || !validId(body.requestId) || !validId(body.requestKey) || (kind !== "QUESTION" && kind !== "PERMISSION")) return Response.json({ error: "native-request-unavailable" }, { status: 409 })
		const control = this.store.control(), route = this.store.route(), payload = kind === "QUESTION" ? parseQuestionPayload(body.payload) : parsePermissionPayload(body.payload)
		if (!control.enabled || !route.conversationId || !route.contextToken || this.adapter.status() !== "Ready" || !payload) return Response.json({ error: "invalid-native-request" }, { status: 409 })
		try { boundedJson(payload) } catch { return Response.json({ error: "invalid-native-request" }, { status: 400 }) }
		const replay = this.store.nativeRequestReplay({ requestKey: body.requestKey, requestId: body.requestId, root: binding.rootSessionId, owner: binding.ownerInstance, kind: kind as NativeRequestKind, payload, controlRevision: control.revision })
		if (replay.result === "EXACT") return Response.json({ ok: true, replayed: true, request: replay.request })
		if (replay.result === "MISMATCH") return Response.json({ error: "native-request-conflict" }, { status: 409 })
		const opened = this.store.openNativeRequest({ requestId: body.requestId, requestKey: body.requestKey, root: binding.rootSessionId, owner: binding.ownerInstance, alias: binding.alias, kind: kind as NativeRequestKind, payload, revision: control.revision })
		if (!opened) return Response.json({ error: "native-request-conflict" }, { status: 409 })
		const currentBinding = this.store.bindingForRoot(binding.rootSessionId), text = currentBinding ? nativeRelayText(opened, currentBinding.alias) : undefined, formatted = text && currentBinding ? formatOutbound(currentBinding.alias, text) : undefined
		if (!formatted || formatted.length > 4000) { this.store.finishNativeAnnouncement(opened.requestId, false); return Response.json({ ok: false, state: "OPEN", relay: "UNKNOWN", request: this.store.nativeRequest(opened.requestId) }) }
		const sent = await this.sendControl(currentBinding!, `native-relay:${body.requestKey}`, "native-request", formatted, control.revision)
		this.store.finishNativeAnnouncement(opened.requestId, sent)
		return Response.json({ ok: true, state: "OPEN", relay: sent ? "SENT" : "UNKNOWN", request: this.store.nativeRequest(opened.requestId) })
	}

	private nativeRequestTerminal(body: JsonObject): Response {
		const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
		const control = this.store.control(), route = this.store.route()
		if (!binding?.active || binding.ownerInstance !== body.instanceId || !control.enabled || !route.conversationId || !route.contextToken || !validId(body.requestId) || (body.state !== "RESOLVED" && body.state !== "REJECTED")) return Response.json({ error: "invalid-native-terminal" }, { status: 400 })
		const native = this.store.nativeRequest(body.requestId)
		if (!native || native.rootSessionId !== binding.rootSessionId || native.ownerInstance !== body.instanceId) return Response.json({ ok: true, settled: false })
		if (body.resolution !== undefined) try { boundedJson(body.resolution) } catch { return Response.json({ error: "invalid-native-terminal" }, { status: 400 }) }
		return Response.json({ ok: true, settled: this.store.settleNativeTerminal(native.requestId, body.state, body.resolution) })
	}
	private async wechatReply(body: JsonObject): Promise<Response> {
		const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined, route = this.store.route()
		if (!binding?.active || binding.ownerInstance !== body.instanceId || !validId(body.callId) || !boundedText(body.text, 4000) || !this.store.control().enabled || !route.conversationId || !route.contextToken || this.adapter.status() !== "Ready") return Response.json({ error: "reply-unavailable" }, { status: 409 })
		const dedupeKey = `wechat-reply:${binding.rootSessionId}:${body.callId}`, payload = formatOutbound(binding.alias, body.text), claim = this.store.claimControlOutbound({ dedupeKey, root: binding.rootSessionId, kind: "wechat-reply", payload, logicalText: body.text, revision: this.store.control().revision })
		if (!claim) return Response.json({ error: "reply-unavailable" }, { status: 409 })
		if (claim.result === "REPLAY") { this.reassertTyping(); return Response.json({ ok: claim.state === "SENT", state: claim.state, replayed: true }) }
		if (claim.result === "CONFLICT") return Response.json({ error: "reply-call-conflict" }, { status: 409 })
		await this.dispatchControl(claim.outboundId, claim.binding, payload)
		const state = this.store.controlOutboundState(`wechat-reply:${binding.rootSessionId}:${body.callId}`)
		this.reassertTyping()
		return Response.json({ ok: state === "SENT", state: state ?? "UNKNOWN", replayed: false }, { status: state === "SENT" ? 200 : 409 })
	}

	private async nativeNotice(message: WeixinInbound, kind: string, text: string, reason: string): Promise<{ ok: boolean; reason?: string }> { this.store.markUnknown(message.id, reason); await this.safeSystemReply(message, kind, text); return { ok: false, reason } }

	private async requestInput(body: JsonObject): Promise<Response> {
		const binding = validId(body.rootSessionId) ? this.store.bindingForRoot(body.rootSessionId) : undefined
		if (!binding?.active || binding.ownerInstance !== body.instanceId || !validId(body.requestKey)) return Response.json({ error: "request-input-unavailable" }, { status: 409 })
		const replay = this.store.checkpointForRequest(body.requestKey, binding.rootSessionId); if (replay) return Response.json({ ok: replay.state !== "UNKNOWN", replayed: true, checkpointId: replay.checkpointId, state: replay.state, ownerChanged: replay.ownerInstance !== body.instanceId, mode: "async-new-turn" })
		const control = this.store.control(), rawChoices = body.choices, route = this.store.route()
		if (this.adapter.status() !== "Ready") return Response.json({ error: "adapter-not-ready" }, { status: 503 })
		if (!route.conversationId || !route.contextToken || !control.enabled || typeof body.question !== "string" || !isPlainText(body.question) || !body.question.trim() || body.question.length > 1500 || !Array.isArray(rawChoices) || rawChoices.length > 8 || rawChoices.some((value) => typeof value !== "string" || !isPlainText(value) || !value.trim() || value.length > 120 || /[\r\n]/.test(value))) return Response.json({ error: "invalid-request-input" }, { status: 409 })
		const choices = rawChoices as string[], choiceText = choices.length ? `\n${choices.map((choice, index) => `${index + 1}. ${choice}`).join("\n")}` : "", bodyText = `${body.question}${choiceText}\n请回复 #${binding.alias} 后换行填写答案。`, payload = formatOutbound(binding.alias, bodyText)
		if (!isPlainText(bodyText) || payload.length > 4000) return Response.json({ error: "checkpoint-payload-too-large" }, { status: 409 })
		const checkpointId = crypto.randomUUID(); if (!this.store.openCheckpoint({ checkpointId, requestKey: body.requestKey, root: binding.rootSessionId, owner: String(body.instanceId), alias: binding.alias, question: body.question, choices, revision: control.revision })) return Response.json({ error: "checkpoint-already-active" }, { status: 409 })
		const sent = await this.sendControl(binding, `checkpoint:${body.requestKey}`, "checkpoint", payload, control.revision); if (!sent) { this.store.failCheckpoint(checkpointId); return Response.json({ ok: false, checkpointId, state: "UNKNOWN", unknown: true, mode: "async-new-turn" }) }
		if (!this.store.activateCheckpoint(checkpointId)) return Response.json({ error: "checkpoint-cancelled-after-send" }, { status: 409 }); return Response.json({ ok: true, checkpointId, state: "OPEN", mode: "async-new-turn" })
	}

	private async sendControl(binding: Binding, dedupeKey: string, kind: string, payload: string, revision?: number): Promise<boolean> { const claim = this.store.claimControlOutbound({ dedupeKey, root: binding.rootSessionId, kind, payload, revision }); if (!claim) return false; await this.dispatchControl(claim.outboundId, claim.binding, payload); return this.store.controlOutboundState(dedupeKey) === "SENT" }
	private refreshTyping(): void { try { this.typing?.refresh() } catch {} }
	private reassertTyping(): void { try { this.typing?.reassert() } catch {} }
	private async dispatchControl(outboundId: string, binding: RoutedBinding, payload: string): Promise<void> { try { this.store.recordEcho(binding.conversationId, binding.contextToken, payload); await this.adapter.send(binding.conversationId, payload, binding.contextToken); this.store.finishControlOutbound(outboundId, true) } catch { this.store.finishControlOutbound(outboundId, false) } }
	private async callbackHealth(endpoint: string, token: string, rootSessionId?: string): Promise<"ok" | "not-root" | "unavailable"> { try { const response = await this.fetcher(`${endpoint}/health`, { method: "POST", headers: callbackHeaders(this.sharedSecret, token), body: JSON.stringify({ rootSessionId }), signal: AbortSignal.timeout(3000) }); if (response.ok) return "ok"; const body = object(await response.json()); return body?.error === "not-root" ? "not-root" : "unavailable" } catch { return "unavailable" } }
	private async safeSystemReply(message: WeixinInbound, kind: string, text: string): Promise<void> { const claim = this.store.claimSystemOutbound(message, kind, text); if (!claim) return; try { this.store.recordEcho(message.fromUserId, message.contextToken, text); await this.adapter.send(message.fromUserId, text, message.contextToken); this.store.finishControlOutbound(claim.outboundId, true) } catch { this.store.finishControlOutbound(claim.outboundId, false) } }
}

export function makeRpcRequest(endpoint: string, secret: string, body: object, timeoutMs = 10_000): Promise<Response> { return fetch(endpoint, { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": secret }, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) }) }
