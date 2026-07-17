import { readLock, authenticatedHealth, pidStatus } from "./worker-runtime"
import { initializeState, isPlainText, type Binding } from "./core"
import { makeRpcRequest } from "./broker"
import * as path from "node:path"
import { existsSync } from "node:fs"

export interface WorkerOptions { enabled: boolean; weixinCommand: string[] }
export interface CallbackServer { endpoint: string; token: string; stop(): void }
export interface ClientLifecycle { stop(): Promise<void> }
export class ClientLifecycleRegistry {
	private readonly entries = new Map<string, ClientLifecycle>()
	async stop(directory: string): Promise<void> { const current = this.entries.get(directory); if (current) { this.entries.delete(directory); await current.stop() } }
	async replace(directory: string, next: ClientLifecycle): Promise<void> { await this.entries.get(directory)?.stop(); this.entries.set(directory, next) }
	async remove(directory: string, expected: ClientLifecycle): Promise<void> { if (this.entries.get(directory) === expected) this.entries.delete(directory) }
	async stopAll(): Promise<void> { const values = [...this.entries.values()]; this.entries.clear(); await Promise.all(values.map((item) => item.stop())) }
	size(): number { return this.entries.size }
}

export function extractPromptAssistant(result: any): { promptMessageId: string; assistantMessageId: string; text: string } {
	if (result?.error || !result?.data || result.data.info?.role !== "assistant" || typeof result.data.info?.id !== "string" || typeof result.data.info?.parentID !== "string" || !result.data.info.parentID.startsWith("msg") || !Array.isArray(result.data.parts)) throw new Error("invalid synchronous prompt response")
	const text = result.data.parts.filter((part: any) => part?.type === "text" && typeof part.text === "string").map((part: any) => part.text).join("\n")
	if (!isPlainText(text)) throw new Error("synchronous prompt returned no final text")
	return { promptMessageId: result.data.info.parentID, assistantMessageId: result.data.info.id, text }
}

export async function resolveRootSession(client: any, sessionId: string): Promise<string> {
	if (!sessionId) throw new Error("missing session ID")
	const seen = new Set<string>(); let current = sessionId
	for (let depth = 0; depth < 32; depth++) {
		if (seen.has(current)) throw new Error("session parent cycle")
		seen.add(current)
		const response = await client.session.get({ path: { id: current } }), session = response.data
		if (!session || typeof session.id !== "string") throw new Error("session unavailable")
		if (!session.parentID) return session.id
		if (typeof session.parentID !== "string") throw new Error("invalid session parent")
		current = session.parentID
	}
	throw new Error("session parent depth exceeded")
}

export function createCallbackHandler(client: any, sharedSecret: string, instanceToken: string): (request: Request) => Promise<Response> {
	return async (request) => {
		if (request.method !== "POST") return Response.json({ error: "method" }, { status: 405 })
		if (request.headers.get("x-wechat-control-key") !== sharedSecret || request.headers.get("x-wechat-instance-token") !== instanceToken) return Response.json({ error: "unauthorized" }, { status: 401 })
		if (new URL(request.url).pathname === "/health") {
			try { const body = await request.json() as any; if (body.rootSessionId) { const response = await client.session.get({ path: { id: body.rootSessionId } }); if (!response.data || response.data.parentID) return Response.json({ error: "not-root" }, { status: 409 }) }; return Response.json({ ok: true }) } catch { return Response.json({ error: "session-unreachable" }, { status: 409 }) }
		}
		if (new URL(request.url).pathname !== "/inject") return Response.json({ error: "not-found" }, { status: 404 })
		let stage = "request"
		try {
			const body = await request.json() as any
			if (typeof body.rootSessionId !== "string" || typeof body.directory !== "string" || typeof body.text !== "string" || typeof body.inboundId !== "string" || !body.envelope || (body.envelope.kind !== "inbound" && (body.envelope.kind !== "checkpoint" || typeof body.envelope.checkpointId !== "string"))) return Response.json({ error: "bad-request" }, { status: 400 })
			stage = "session"
			const session = await client.session.get({ path: { id: body.rootSessionId } })
			if (!session.data || session.data.parentID) return Response.json({ error: "not-root" }, { status: 409 })
			stage = "prompt"
			const promptText = body.envelope.kind === "checkpoint" ? `[受限微信接管：异步检查点回答 ${body.envelope.checkpointId}]\n${body.text}` : body.text
			const result = await client.session.prompt({ path: { id: body.rootSessionId }, query: { directory: body.directory }, body: { parts: [{ type: "text", text: promptText }] }, signal: request.signal })
			stage = "response"
			return Response.json({ ok: true, ...extractPromptAssistant(result) })
		} catch { return Response.json({ error: "inject-failed", stage }, { status: 409 }) }
	}
}

export function startCallbackServer(client: any, sharedSecret: string, instanceToken: string): CallbackServer {
	const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: createCallbackHandler(client, sharedSecret, instanceToken) })
	return { endpoint: server.url.toString(), token: instanceToken, stop: () => server.stop() }
}

export async function rpc(endpoint: string, secret: string, body: object, timeoutMs = 10_000): Promise<any> {
	const response = await makeRpcRequest(endpoint, secret, body, timeoutMs)
	const value = await response.json() as any
	if (!response.ok) throw new Error(value?.error || "broker RPC failed")
	return value
}

export function requestInputRpc(endpoint: string, secret: string, body: object): Promise<any> { return rpc(endpoint, secret, body, 90_000) }

export async function connectOrStartWorker(options: WorkerOptions): Promise<{ endpoint: string; secret: string; adapter: string }> {
	const state = await initializeState()
	let lock = await readLock(state.directory)
	if (!lock || !(await authenticatedHealth(lock, state.secret))) {
		if (lock && pidStatus(lock.pid) !== "dead") throw new Error("Broker unavailable and existing PID is alive or unknown; takeover refused")
		const builtWorker = path.join(import.meta.dir, "worker.js"), workerPath = existsSync(builtWorker) ? builtWorker : path.join(import.meta.dir, "worker.ts")
		const bun = Bun.which("bun") ?? process.execPath
		const encoded = Buffer.from(JSON.stringify(options)).toString("base64url")
		Bun.spawn([bun, workerPath, encoded], { stdin: "ignore", stdout: "ignore", stderr: "ignore", detached: true })
		for (let attempt = 0; attempt < 50; attempt++) { await Bun.sleep(100); lock = await readLock(state.directory); if (lock && await authenticatedHealth(lock, state.secret)) break }
	}
	if (!lock || !(await authenticatedHealth(lock, state.secret))) throw new Error("broker did not become healthy")
	const health = await makeRpcRequest(lock.endpoint, state.secret, { method: "health", challenge: lock.workerToken }); const body = await health.json() as any
	return { endpoint: lock.endpoint, secret: state.secret, adapter: typeof body.adapter === "string" ? body.adapter : "unknown" }
}

export async function bindCurrent(endpoint: string, secret: string, instanceId: string, instanceToken: string, binding: Omit<Binding, "alias" | "contextToken" | "ownerInstance"> & { alias?: number }): Promise<any> {
	return rpc(endpoint, secret, { method: "bind-current", instanceId, instanceToken, ...binding })
}
