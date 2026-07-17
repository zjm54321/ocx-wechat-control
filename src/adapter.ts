import { parsePollToolResult, type WeixinInbound } from "./core"
import { existsSync } from "node:fs"

export type AdapterStatus = "NotConfigured" | "Starting" | "Ready" | "Degraded" | "Stopped"
export type SendFailureClassification = "mcp-error" | "malformed-result" | "explicit-business-failure" | "ambiguous-result" | "unknown-result"

export class AdapterSendError extends Error {
	constructor(readonly classification: SendFailureClassification) { super(`weixin_send failed: ${classification}`); this.name = "AdapterSendError" }
}

export function assertWeixinSendSuccess(result: unknown): void {
	if (!result || typeof result !== "object") throw new AdapterSendError("malformed-result")
	if ((result as { isError?: unknown }).isError === true) throw new AdapterSendError("mcp-error")
	const content = (result as { content?: unknown }).content
	if (!Array.isArray(content) || content.length !== 1) throw new AdapterSendError("malformed-result")
	const item = content[0] as { type?: unknown; text?: unknown }
	if (item?.type !== "text" || typeof item.text !== "string") throw new AdapterSendError("malformed-result")
	let value: unknown
	try { value = JSON.parse(item.text) } catch { throw new AdapterSendError("malformed-result") }
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new AdapterSendError("malformed-result")
	const response = value as Record<string, unknown>
	const keys = Object.keys(response)
	// Pinned 1.7.7 passes the send API JSON through unchanged and its own CLI
	// treats an absent status as success. Accept only the structurally exact empty
	// response supported by that status-less contract; never arbitrary objects.
	if (keys.length === 0) return
	const hasRet = Object.hasOwn(response, "ret"), hasErrcode = Object.hasOwn(response, "errcode")
	if (hasRet && response.ret !== 0) throw new AdapterSendError("explicit-business-failure")
	if (hasErrcode && response.errcode !== 0) throw new AdapterSendError("explicit-business-failure")
	if (Object.hasOwn(response, "error") || Object.hasOwn(response, "errmsg")) throw new AdapterSendError("ambiguous-result")
	if ((hasRet && response.ret === 0) || (hasErrcode && response.errcode === 0)) return
	throw new AdapterSendError("unknown-result")
}

export interface McpClient {
	request(method: string, params?: unknown): Promise<any>
	notify(method: string, params?: unknown): Promise<void>
	close(): void
}

function spawnMcp(command: string[]) { return Bun.spawn(command, { stdin: "pipe", stdout: "pipe", stderr: "pipe", windowsHide: true } as const) }

type Pending = { resolve(value: unknown): void; reject(error: Error): void; timer: Timer }

export class JsonRpcPendingMap {
	private readonly pending = new Map<number, Pending>()
	private id = 0
	request(write: (message: object) => void, method: string, params: unknown): Promise<unknown> {
		const id = ++this.id
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`MCP request timeout: ${method}`)) }, 90_000)
			this.pending.set(id, { resolve, reject, timer })
			try { write({ jsonrpc: "2.0", id, method, params }) } catch (error) { clearTimeout(timer); this.pending.delete(id); reject(error instanceof Error ? error : new Error("MCP write failed")) }
		})
	}
	accept(message: any): void {
		if (message?.jsonrpc !== "2.0" || typeof message.id !== "number") return
		const pending = this.pending.get(message.id); if (!pending) return
		clearTimeout(pending.timer); this.pending.delete(message.id)
		if (message.error) pending.reject(new Error("MCP JSON-RPC error")); else pending.resolve(message.result)
	}
	fail(error: Error): void { for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(error) }; this.pending.clear() }
}

export class JsonRpcStdioClient implements McpClient {
	private readonly process: ReturnType<typeof spawnMcp>
	private readonly rpc = new JsonRpcPendingMap()
	private closed = false
	constructor(command: string[]) {
		if (!command.length) throw new Error("empty MCP command")
		this.process = spawnMcp(command)
		void this.readStdout()
		// stderr can contain account paths, tokens or QR material. Drain and discard it.
		void new Response(this.process.stderr).arrayBuffer().catch(() => {})
		void this.process.exited.then((code) => this.failAll(new Error(`MCP process exited (${code})`)))
	}
	request(method: string, params?: unknown): Promise<any> {
		if (this.closed) return Promise.reject(new Error("MCP client closed"))
		return this.rpc.request((message) => this.write(message), method, params ?? {})
	}
	async notify(method: string, params?: unknown): Promise<void> { this.write({ jsonrpc: "2.0", method, params: params ?? {} }) }
	close(): void { if (this.closed) return; this.closed = true; this.process.kill(); this.rpc.fail(new Error("MCP client closed")) }
	private write(message: object): void {
		const line = `${JSON.stringify(message)}\n`
		this.process.stdin.write(line)
		this.process.stdin.flush()
	}
	private async readStdout(): Promise<void> {
		const reader = this.process.stdout.getReader()
		const decoder = new TextDecoder()
		let buffer = ""
		try {
			while (true) {
				const { done, value } = await reader.read()
				if (done) break
				buffer += decoder.decode(value, { stream: true })
				if (buffer.length > 4_000_000) throw new Error("MCP stdout frame too large")
				let newline: number
				while ((newline = buffer.indexOf("\n")) >= 0) {
					const line = buffer.slice(0, newline).trim(); buffer = buffer.slice(newline + 1)
					if (line) this.acceptLine(line)
				}
			}
		} catch (error) { this.failAll(error instanceof Error ? error : new Error("MCP stdout failed")) }
	}
	private acceptLine(line: string): void {
		let message: any
		try { message = JSON.parse(line) } catch { this.failAll(new Error("non-JSON MCP stdout")); this.close(); return }
		this.rpc.accept(message)
	}
	private failAll(error: Error): void { this.rpc.fail(error) }
}

export interface WeChatAdapter {
	status(): AdapterStatus
	start(onInbound: (message: WeixinInbound) => Promise<void>): Promise<void>
	send(to: string, text: string, contextToken: string): Promise<void>
	stop(): void
}

export interface WeixinAdapterOptions {
	enabled: boolean
	command: string[]
	clientFactory?: (command: string[]) => McpClient
	retry?: boolean
}

export class WeixinMcpAdapter implements WeChatAdapter {
	private currentStatus: AdapterStatus = "NotConfigured"
	private client?: McpClient
	private stopping = false
	private attempts = 0
	private handler?: (message: WeixinInbound) => Promise<void>
	constructor(private readonly options: WeixinAdapterOptions) {}
	status(): AdapterStatus { return this.currentStatus }
	async start(onInbound: (message: WeixinInbound) => Promise<void>): Promise<void> {
		if (!this.options.enabled) { this.currentStatus = "NotConfigured"; return }
		if (this.client || this.currentStatus === "Starting" || this.currentStatus === "Ready") return
		this.handler = onInbound; this.currentStatus = "Starting"
		try {
			if (!this.options.clientFactory && (this.options.command[0] !== "node" || typeof this.options.command[1] !== "string" || !existsSync(this.options.command[1]))) throw new Error("fixed local adapter script missing")
			const factory = this.options.clientFactory ?? ((command) => new JsonRpcStdioClient(command))
			const client = factory([...this.options.command]); this.client = client
			await client.request("initialize", { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "opencode-wechat-control", version: "1.0.0" } })
			await client.notify("notifications/initialized")
			const listed = await client.request("tools/list", {})
			const names = new Set(Array.isArray(listed?.tools) ? listed.tools.map((tool: any) => tool?.name) : [])
			if (!names.has("weixin_poll") || !names.has("weixin_send")) throw new Error("required MCP tools unavailable")
			this.attempts = 0; this.currentStatus = "Ready"
			void this.pollLoop(client)
		} catch {
			this.degrade()
		}
	}
	async send(to: string, text: string, contextToken: string): Promise<void> {
		if (this.currentStatus !== "Ready" || !this.client) throw new Error("Weixin adapter degraded")
		if (!to || !text || !contextToken) throw new Error("recipient/context unavailable")
		const result = await this.client.request("tools/call", { name: "weixin_send", arguments: { to, text, context_token: contextToken } })
		assertWeixinSendSuccess(result)
	}
	stop(): void { this.stopping = true; this.currentStatus = "Stopped"; this.client?.close(); this.client = undefined }
	private async pollLoop(client: McpClient): Promise<void> {
		try {
			while (!this.stopping && this.client === client) {
				const result = await client.request("tools/call", { name: "weixin_poll", arguments: {} })
				for (const message of parsePollToolResult(result)) await this.handler!(message)
			}
		} catch { if (!this.stopping) this.degrade() }
	}
	private degrade(): void {
		this.client?.close(); this.client = undefined; this.currentStatus = "Degraded"
		if (this.stopping || this.options.retry === false || ++this.attempts > 5 || !this.handler) return
		const delay = Math.min(60_000, 1000 * 2 ** (this.attempts - 1))
		setTimeout(() => { if (!this.stopping && this.handler) void this.start(this.handler) }, delay)
	}
}

export class MockWeChatAdapter implements WeChatAdapter {
	readonly sent: Array<{ to: string; text: string; contextToken: string }> = []
	failSend = false
	statusValue: AdapterStatus = "Ready"
	private handler?: (message: WeixinInbound) => Promise<void>
	status(): AdapterStatus { return this.statusValue }
	async start(handler: (message: WeixinInbound) => Promise<void>): Promise<void> { this.handler = handler }
	async emit(message: WeixinInbound): Promise<void> { await this.handler?.(message) }
	async send(to: string, text: string, contextToken: string): Promise<void> { if (this.failSend) throw new Error("mock business failure"); this.sent.push({ to, text, contextToken }) }
	stop(): void {}
}
