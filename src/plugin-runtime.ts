import { tool, type Plugin } from "@opencode-ai/plugin"
import { ClientLifecycleRegistry, connectOrStartWorker, resolveRootSession, rpc, startV2CallbackServer, type CallbackServer, type ClientLifecycle } from "./client"
import { makeRpcRequest } from "./broker"
import { sanitizeTitle, SerialQueue, sha256 } from "./core"
import { createRequire } from "node:module"
import { HttpServerResponse } from "effect/unstable/http"

export function resolveWeixinCommand(value: unknown): string[] {
	if (value === undefined) return ["node", createRequire(import.meta.url).resolve("weixin-mcp/dist/cli.js")]
	if (!Array.isArray(value) || value.length !== 2 || value[0] !== "node" || typeof value[1] !== "string") throw new Error("wechat-control weixinCommand must be ['node', absolute-cli-path]")
	return [...value]
}
const LEAVE_SENTINEL = "__WECHAT_CONTROL_LEAVE_HANDLED__", BACK_SENTINEL = "__WECHAT_CONTROL_BACK_HANDLED__"
const CONTROL_COMMANDS = { leave: { template: LEAVE_SENTINEL, description: "启用受限微信接管" }, back: { template: BACK_SENTINEL, description: "关闭受限微信接管" } } as const
const LIFECYCLE_SYMBOL = Symbol.for("ocx-wechat-control.plugin-lifecycle.v1")
type LifecycleContainer = { version: 1; registry: ClientLifecycleRegistry; exitHandlersInstalled: boolean; exitHandlers?: Partial<Record<"beforeExit" | "SIGINT" | "SIGTERM", () => void>> }
const lifecycleGlobal = globalThis as typeof globalThis & Record<symbol, unknown>
const existingLifecycle = lifecycleGlobal[LIFECYCLE_SYMBOL] as Partial<LifecycleContainer> | undefined
const lifecycle: LifecycleContainer = existingLifecycle?.version === 1 && existingLifecycle.registry !== undefined
	? existingLifecycle as LifecycleContainer
	: { version: 1, registry: new ClientLifecycleRegistry(), exitHandlersInstalled: false }
lifecycleGlobal[LIFECYCLE_SYMBOL] = lifecycle
const registry = lifecycle.registry
async function stopAll(): Promise<void> { await registry.stopAll() }
function installExitHandlers(): void {
	if (lifecycle.exitHandlersInstalled) return
	lifecycle.exitHandlersInstalled = true
	const exitHandlers = { beforeExit: () => { void stopAll() }, SIGINT: () => { void stopAll() }, SIGTERM: () => { void stopAll() } }
	lifecycle.exitHandlers = exitHandlers
	process.once("beforeExit", exitHandlers.beforeExit)
	process.once("SIGINT", exitHandlers.SIGINT)
	process.once("SIGTERM", exitHandlers.SIGTERM)
}
export function lifecycleRegistrySize(): number { return registry.size() }
export const pluginLifecycleTestHooks = {
	registry,
	installExitHandlers,
	exitHandlersInstalled: () => lifecycle.exitHandlersInstalled,
	reset: async () => {
		await registry.stopAll()
		if (lifecycle.exitHandlers) for (const [event, handler] of Object.entries(lifecycle.exitHandlers)) process.removeListener(event, handler)
		lifecycle.exitHandlers = undefined
		lifecycle.exitHandlersInstalled = false
	},
}

export function startPluginCallbackServer(client: Parameters<typeof startV2CallbackServer>[0], _serverUrl: URL, directory: string, sharedSecret: string, instanceToken: string): CallbackServer {
	return startV2CallbackServer(client, directory, sharedSecret, instanceToken)
}

export function registerControlCommands(config: any): void {
	config.command ??= {}
	for (const name of ["leave", "back"] as const) {
		if (Object.hasOwn(config.command, name) && JSON.stringify(config.command[name]) !== JSON.stringify(CONTROL_COMMANDS[name])) throw new Error(`wechat-control command conflict: /${name}`)
		config.command[name] = { ...CONTROL_COMMANDS[name] }
	}
}

export function createControlCommandHook(rpcCall: typeof rpc, toast: (enabled: boolean, alias?: number) => Promise<void>, endpoint: string, secret: string, instanceId: string, instanceToken: string, sessionGet?: (sessionId: string) => Promise<any>, directory = "") {
	return async (input: { command: string; arguments: string; sessionID?: string }, output: { parts: unknown[] }) => {
		if (input.command !== "leave" && input.command !== "back") return
		output.parts.splice(0)
		if (input.arguments.trim()) throw HttpServerResponse.jsonUnsafe({ error: `/${input.command} does not accept arguments` }, { status: 400 })
		const enabled = input.command === "leave"
		let alias: number | undefined
		try {
			if (enabled) {
				if (!input.sessionID || !sessionGet) throw new Error("missing-session")
				const response = await sessionGet(input.sessionID), session = response?.data
				if (!session || session.parentID || session.id !== input.sessionID) throw new Error("not-root")
				const result = await rpcCall(endpoint, secret, { method: "leave-root", instanceId, instanceToken, rootSessionId: input.sessionID, directory, title: sanitizeTitle(session.title) })
				alias = result.binding.alias
			} else await rpcCall(endpoint, secret, { method: "back-global", instanceId, instanceToken })
		}
		catch { throw HttpServerResponse.jsonUnsafe({ error: "wechat control command failed before model admission" }, { status: 503 }) }
		try { await toast(enabled, alias) } catch {}
		// OpenCode 1.18.3 has no command-hook cancellation result. Its Effect HTTP
		// boundary treats this response as the completed request, before prompt()
		// can create a message or call a model.
		throw HttpServerResponse.empty({ status: 204, headers: { "x-ocx-command": enabled ? `leave:${alias}` : "back" } })
	}
}

export function createPermissionHook(resolveRoot: (sessionId: string) => Promise<string>, rpcCall: typeof rpc, endpoint: string, secret: string, instanceId: string, instanceToken: string) {
	return async (input: { sessionID: string; id: string }, output: { status: "ask" | "deny" | "allow" }) => {
		output.status = "ask"
		try { await resolveRoot(input.sessionID) } catch { /* retain native ask on validation failure */ }
	}
}

function validEventText(value: unknown, max = 4000): value is string { return typeof value === "string" && value.length > 0 && value.length <= max && !/[\u0000-\u001f\u007f]/.test(value) }
function nativeEvent(value: unknown): Record<string, unknown> | undefined { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined }

export function createControlEventHook(rpcCall: typeof rpc, endpoint: string, secret: string, instanceId: string, instanceToken: string, resolveRoot: (sessionId: string) => Promise<string> = async (sessionId) => sessionId) {
	const queue = new SerialQueue(), auth = { instanceId, instanceToken }
	return async ({ event }: any) => {
		const properties = nativeEvent(event?.properties), info = nativeEvent(properties?.info), sessionID = typeof (event.type === "message.updated" ? info?.sessionID : properties?.sessionID) === "string" ? (event.type === "message.updated" ? info?.sessionID : properties?.sessionID) as string : undefined
		if (typeof sessionID !== "string") return
		return queue.run(sessionID, async () => {
			try {
				const rootSessionId = await resolveRoot(sessionID)
				if (event.type === "message.updated") { if (info?.role !== "assistant" || info.summary || (!nativeEvent(info.time)?.completed && !info.finish && !info.error) || !validEventText(info.id as unknown, 500)) return; await rpcCall(endpoint, secret, { method: "observe-assistant", ...auth, rootSessionId, assistantMessageId: info.id, failed: Boolean(info.error) }) }
				else if (event.type === "session.status") { const status = event.properties?.status?.type; if (status !== "busy" && status !== "retry" && status !== "idle") return; await rpcCall(endpoint, secret, { method: "observe-status", ...auth, rootSessionId, status }) }
				else if (event.type === "session.idle") await rpcCall(endpoint, secret, { method: "observe-status", ...auth, rootSessionId, status: "idle" })
				else if (event.type === "question.asked" || event.type === "permission.asked") {
					const id = properties?.id, requestKey = `${event.type}:${id}`, payload = event.type === "question.asked" ? { sourceSessionId: sessionID, questions: properties?.questions } : { sourceSessionId: sessionID, permission: properties?.permission }
					if (!validEventText(id, 500) || (event.type === "question.asked" && !Array.isArray(properties?.questions)) || (event.type === "permission.asked" && !validEventText(properties?.permission, 500))) return
					await rpcCall(endpoint, secret, { method: "native-request-open", ...auth, rootSessionId, requestId: id, requestKey, kind: event.type === "question.asked" ? "QUESTION" : "PERMISSION", payload })
				} else if (event.type === "question.replied" || event.type === "question.rejected" || event.type === "permission.replied" || event.type === "permission.rejected") {
					const id = properties?.requestID ?? properties?.id, reply = properties?.reply
					if (event.type === "permission.replied" && reply !== "once" && reply !== "always" && reply !== "reject") return
					const terminal = event.type.endsWith("rejected") || (event.type === "permission.replied" && reply === "reject") ? "REJECTED" : "RESOLVED"
					if (!validEventText(id, 500)) return
					await rpcCall(endpoint, secret, { method: "native-request-terminal", ...auth, rootSessionId, requestId: id, state: terminal, resolution: properties?.answers ?? reply })
				}
			} catch {}
		})
	}
}

export function createPluginEventHook(resolveRoot: (sessionId: string) => Promise<string>, rpcCall: typeof rpc, endpoint: string, secret: string, instanceId: string, instanceToken: string) { return createControlEventHook(rpcCall, endpoint, secret, instanceId, instanceToken, resolveRoot) }

export async function requestInputToolOutcome(action: () => Promise<any>): Promise<string> {
	try { const result = await action(); if (result.unknown || result.state === "UNKNOWN") return "检查点发送结果未知。禁止重复发送；请立即结束本回合。" }
	catch { return "检查点未能确认。禁止重复发送；请立即结束本回合。" }
	return "检查点已异步发送。不要等待或猜测微信答案；请立即结束本回合。答案将作为一个新的用户 turn 注入。"
}

type RpcRequest = (endpoint: string, secret: string, body: object, timeoutMs?: number) => Promise<Response>
export async function wechatReplyRpc(endpoint: string, secret: string, body: object, request: RpcRequest = makeRpcRequest): Promise<any> {
	const response = await request(endpoint, secret, body)
	const value = await response.json() as any
	if (response.ok) return value
	const keys = value !== null && typeof value === "object" && !Array.isArray(value) ? Object.keys(value) : []
	if (response.status === 409 && keys.length === 3 && value.ok === false && value.state === "UNKNOWN" && value.replayed === false) return value
	throw new Error(value?.error || "broker RPC failed")
}

export function captureReplyCallID(input: any, output: any): void { if (input.tool === "wechat_reply" && typeof input.callID === "string" && output.args && typeof output.args === "object") output.args.__wechatCallID = input.callID }
export function requestInputRequestKey(args: any, context: any): string { return String(context.callID ?? args.__wechatRequestKey ?? `fallback:${context.messageID}:${sha256(JSON.stringify([args.question, args.choices ?? []]))}`) }
export function executeRequestInputTool(args: any, context: any, action: (requestKey: string) => Promise<any>): Promise<string> { const requestKey = requestInputRequestKey(args, context); return requestInputToolOutcome(() => action(requestKey)) }

export const WeChatControlPlugin: Plugin = async ({ client, directory, serverUrl }, options) => {
	const config = (options ?? {}) as { enabled?: unknown; weixinCommand?: unknown }
	if (config.enabled !== true) throw new Error("wechat-control requires enabled:true")
	const weixinCommand = resolveWeixinCommand(config.weixinCommand)
	installExitHandlers(); await registry.stop(directory)
	const state = await (await import("./core")).initializeState()
	const instanceId = crypto.randomUUID(), instanceToken = crypto.randomUUID()
	const callback = startPluginCallbackServer(client, serverUrl, directory, state.secret, instanceToken)
	let worker: { endpoint: string; secret: string; adapter: string }
	try { worker = await connectOrStartWorker({ enabled: true, weixinCommand }); await rpc(worker.endpoint, worker.secret, { method: "register", instanceId, instanceToken, endpoint: callback.endpoint }) }
	catch (error) { callback.stop(); throw error }
	const heartbeat = setInterval(() => { void rpc(worker.endpoint, worker.secret, { method: "heartbeat", instanceId, instanceToken }).catch(() => {}) }, 15_000)
	let stopped = false
	const lifecycle: ClientLifecycle = { stop: async () => { if (stopped) return; stopped = true; clearInterval(heartbeat); callback.stop(); await rpc(worker.endpoint, worker.secret, { method: "unregister", instanceId, instanceToken }).catch(() => {}); await registry.remove(directory, lifecycle) } }
	await registry.replace(directory, lifecycle)
	const auth = { instanceId, instanceToken }
	const root = (sessionID: string) => resolveRootSession(client, sessionID)
	const commandHook = createControlCommandHook(rpc, async (enabled, alias) => { await client.tui.showToast({ query: { directory }, body: { title: "WeChat Control", message: enabled ? `已登记 #${alias}，微信输入 id 可查看会话` : "受限微信接管已关闭", variant: enabled ? "success" : "info" } }) }, worker.endpoint, worker.secret, instanceId, instanceToken, (sessionID) => client.session.get({ path: { id: sessionID } }), directory)
	const permissionHook = createPermissionHook(root, rpc, worker.endpoint, worker.secret, instanceId, instanceToken)
	const eventHook = createPluginEventHook(root, rpc, worker.endpoint, worker.secret, instanceId, instanceToken)

	return {
		config: async (value: any) => { registerControlCommands(value) },
		"command.execute.before": commandHook,
		"tool.execute.before": async (input: any, output: any) => { captureReplyCallID(input, output) },
		tool: {
			wechat_reply: tool({ description: "Send text to the bound WeChat conversation. The call is durably deduplicated by tool call ID and never retries an unknown send.", args: { text: tool.schema.string().min(1).max(4000), __wechatCallID: tool.schema.string().max(500).optional() }, async execute(args, context) { const rootSessionId = await root(context.sessionID), callId = args.__wechatCallID ?? context.messageID; const result = await wechatReplyRpc(worker.endpoint, worker.secret, { method: "wechat-reply", ...auth, rootSessionId, callId, text: args.text }); return result.state === "UNKNOWN" ? "微信回复发送结果未知；禁止重复发送。" : result.ok ? "已发送。" : "微信回复未发送。" } }),
			wechat_send_text: tool({ description: "Restricted compatibility tool; arbitrary/manual sends are disabled.", args: { text: tool.schema.string().min(1).max(4000) }, async execute() { return "拒绝：仅允许受限接管状态机产生的固定或关联外发。" } }),
			wechat_control_status: tool({ description: "Report registration, global route and takeover state without sending or polling.", args: {}, async execute(_args, context) { const rootSessionId = await root(context.sessionID); const status = await rpc(worker.endpoint, worker.secret, { method: "control-get", ...auth, rootSessionId }); return `broker=Ready adapter=${status.adapter} takeover=${status.enabled ? "on" : "off"} registered=${status.registered} alias=${status.alias ?? "none"} route=${status.routeReady ? "ready" : "missing"}` } }),
		},
		"experimental.chat.system.transform": async (input: any, output: { system: string[] }) => {
			if (!input.sessionID) return
			try { const rootSessionId = await root(input.sessionID), status = await rpc(worker.endpoint, worker.secret, { method: "control-get", ...auth, rootSessionId }); if (status.enabled && status.routable) output.system.push("当前处于受限微信接管。仅在明确需要时调用 wechat_reply({text}) 向微信发送文字；不要自动复述助手内容。Question/Permission 请求会由系统转发到微信；微信答案必须使用当前 id 映射，并在一条消息中发送 #N\\nQCODE 1，单个问题也可发送 #N\\n1。") } catch {}
		},
		"permission.ask": permissionHook,
		event: eventHook,
	}
}

export default WeChatControlPlugin
