import { tool, type Plugin } from "@opencode-ai/plugin"
import { ClientLifecycleRegistry, connectOrStartWorker, requestInputRpc, resolveRootSession, rpc, startCallbackServer, type ClientLifecycle } from "./client"
import { sanitizeTitle, SerialQueue, sha256 } from "./core"
import { createRequire } from "node:module"

export function resolveWeixinCommand(value: unknown): string[] {
	if (value === undefined) return ["node", createRequire(import.meta.url).resolve("weixin-mcp/dist/cli.js")]
	if (!Array.isArray(value) || value.length !== 2 || value[0] !== "node" || typeof value[1] !== "string") throw new Error("wechat-control weixinCommand must be ['node', absolute-cli-path]")
	return [...value]
}
const LEAVE_SENTINEL = "__WECHAT_CONTROL_LEAVE_HANDLED__", BACK_SENTINEL = "__WECHAT_CONTROL_BACK_HANDLED__"
const CONTROL_COMMANDS = { leave: { template: LEAVE_SENTINEL, description: "启用受限微信接管" }, back: { template: BACK_SENTINEL, description: "关闭受限微信接管" } } as const
const registry = new ClientLifecycleRegistry()
let exitHandlersInstalled = false
async function stopAll(): Promise<void> { await registry.stopAll() }
function installExitHandlers(): void { if (exitHandlersInstalled) return; exitHandlersInstalled = true; process.once("beforeExit", () => { void stopAll() }); process.once("SIGINT", () => { void stopAll() }); process.once("SIGTERM", () => { void stopAll() }) }
export function lifecycleRegistrySize(): number { return registry.size() }

export class ControlCommandHandled extends Error { constructor(message: string) { super(message); this.name = "WechatControlCommandHandled" } }

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
		if (input.arguments.trim()) throw new ControlCommandHandled(`/${input.command} 不接受参数。`)
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
		catch { throw new ControlCommandHandled("微信接管状态更新失败；命令已拦截，未发送给模型。") }
		try { await toast(enabled, alias) } catch {}
		throw new ControlCommandHandled(enabled ? `已登记 #${alias}，微信输入 id 可查看会话。` : "受限微信接管已关闭。")
	}
}

export function createPermissionHook(resolveRoot: (sessionId: string) => Promise<string>, rpcCall: typeof rpc, endpoint: string, secret: string, instanceId: string, instanceToken: string) {
	return async (input: { sessionID: string; id: string }, output: { status: "ask" | "deny" | "allow" }) => {
		output.status = "ask"
		try {
			const rootSessionId = await resolveRoot(input.sessionID), status = await rpcCall(endpoint, secret, { method: "control-get", instanceId, instanceToken, rootSessionId }, 1500)
			if (!status.enabled || !status.routable) return
			output.status = "deny"
			void rpcCall(endpoint, secret, { method: "permission-denied-notice", instanceId, instanceToken, rootSessionId, permissionId: input.id }, 1500).catch(() => {})
		} catch { output.status = "ask" }
	}
}

export function createControlEventHook(rpcCall: typeof rpc, endpoint: string, secret: string, instanceId: string, instanceToken: string) {
	const queue = new SerialQueue(), auth = { instanceId, instanceToken }
	return async ({ event }: any) => {
		const sessionID = event.type === "message.updated" ? event.properties?.info?.sessionID : event.properties?.sessionID
		if (typeof sessionID !== "string") return
		return queue.run(sessionID, async () => {
			try {
				if (event.type === "message.updated") { const info = event.properties?.info; if (info?.role !== "assistant" || info.summary || (!info.time?.completed && !info.finish && !info.error)) return; await rpcCall(endpoint, secret, { method: "observe-assistant", ...auth, rootSessionId: sessionID, assistantMessageId: info.id, failed: Boolean(info.error) }) }
				else if (event.type === "session.status") { const status = event.properties?.status?.type; if (status !== "busy" && status !== "idle") return; await rpcCall(endpoint, secret, { method: "observe-status", ...auth, rootSessionId: sessionID, status }) }
				else if (event.type === "session.idle") await rpcCall(endpoint, secret, { method: "observe-status", ...auth, rootSessionId: sessionID, status: "idle" })
			} catch {}
		})
	}
}

export async function requestInputToolOutcome(action: () => Promise<any>): Promise<string> {
	try { const result = await action(); if (result.unknown || result.state === "UNKNOWN") return "检查点发送结果未知。禁止重复发送；请立即结束本回合。" }
	catch { return "检查点未能确认。禁止重复发送；请立即结束本回合。" }
	return "检查点已异步发送。不要等待或猜测微信答案；请立即结束本回合。答案将作为一个新的用户 turn 注入。"
}

export function captureRequestInputCallID(input: any, output: any): void { if (input.tool === "wechat_request_input" && typeof input.callID === "string" && output.args && typeof output.args === "object") output.args.__wechatRequestKey = input.callID }
export function requestInputRequestKey(args: any, context: any): string { return String(context.callID ?? args.__wechatRequestKey ?? `fallback:${context.messageID}:${sha256(JSON.stringify([args.question, args.choices ?? []]))}`) }
export function executeRequestInputTool(args: any, context: any, action: (requestKey: string) => Promise<any>): Promise<string> { const requestKey = requestInputRequestKey(args, context); return requestInputToolOutcome(() => action(requestKey)) }

export const WeChatControlPlugin: Plugin = async ({ client, directory }, options) => {
	const config = (options ?? {}) as { enabled?: unknown; weixinCommand?: unknown }
	if (config.enabled !== true) throw new Error("wechat-control requires enabled:true")
	const weixinCommand = resolveWeixinCommand(config.weixinCommand)
	installExitHandlers(); await registry.stop(directory)
	const state = await (await import("./core")).initializeState()
	const instanceId = crypto.randomUUID(), instanceToken = crypto.randomUUID()
	const callback = startCallbackServer(client, state.secret, instanceToken)
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
	const eventHook = createControlEventHook(rpc, worker.endpoint, worker.secret, instanceId, instanceToken)

	return {
		config: async (value: any) => { registerControlCommands(value) },
		"command.execute.before": commandHook,
		"tool.execute.before": async (input: any, output: any) => { captureRequestInputCallID(input, output) },
		tool: {
			wechat_request_input: tool({ description: "Asynchronously send one restricted takeover checkpoint to the bound WeChat conversation. The answer arrives as a later, new turn; call once and then end this turn.", args: { question: tool.schema.string().min(1).max(1500), choices: tool.schema.array(tool.schema.string().min(1).max(120)).max(8).optional() }, async execute(args, context) {
				return executeRequestInputTool(args, context, async (requestKey) => { const rootSessionId = await root(context.sessionID); return requestInputRpc(worker.endpoint, worker.secret, { method: "request-input", ...auth, rootSessionId, requestKey, question: args.question, choices: args.choices ?? [] }) })
			} }),
			wechat_send_text: tool({ description: "Restricted compatibility tool; arbitrary/manual sends are disabled.", args: { text: tool.schema.string().min(1).max(4000) }, async execute() { return "拒绝：仅允许受限接管状态机产生的固定或关联外发。" } }),
			wechat_control_status: tool({ description: "Report registration, global route and takeover state without sending or polling.", args: {}, async execute(_args, context) { const rootSessionId = await root(context.sessionID); const status = await rpc(worker.endpoint, worker.secret, { method: "control-get", ...auth, rootSessionId }); return `broker=Ready adapter=${status.adapter} takeover=${status.enabled ? "on" : "off"} registered=${status.registered} alias=${status.alias ?? "none"} route=${status.routeReady ? "ready" : "missing"}` } }),
		},
		"experimental.chat.system.transform": async (input: any, output: { system: string[] }) => {
			if (!input.sessionID) return
			try { const rootSessionId = await root(input.sessionID), status = await rpc(worker.endpoint, worker.secret, { method: "control-get", ...auth, rootSessionId }); if (status.enabled && status.routable) output.system.push("当前处于受限微信接管。不要调用原生 question：它不会被转发到微信。需要用户输入时，只调用一次 wechat_request_input，然后立即结束本回合。该工具仅异步发送检查点；微信回答会成为之后的新用户 turn。") } catch {}
		},
		"permission.ask": permissionHook,
		event: eventHook,
	}
}

export default WeChatControlPlugin
