import { readLock, authenticatedHealth, pidStatus } from "./worker-runtime"
import { initializeState, isPlainText } from "./core"
import { makeRpcRequest } from "./broker"
import * as path from "node:path"
import { existsSync } from "node:fs"
import { createV2ControlClient, type ControlResult, type V2ControlClient } from "./control-client"

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

export function extractPromptAssistant(result: unknown): { promptMessageId: string; assistantMessageId: string; text: string } {
	const response = object(result), data = object(response?.data), info = object(data?.info), parts = data?.parts
	if (response?.error || !data || info?.role !== "assistant" || typeof info.id !== "string" || typeof info.parentID !== "string" || !info.parentID.startsWith("msg") || !Array.isArray(parts)) throw new Error("invalid synchronous prompt response")
	const text = parts.map(object).filter((part): part is JsonObject => Boolean(part && part.type === "text" && typeof part.text === "string")).map((part) => part.text).join("\n")
	if (!isPlainText(text)) throw new Error("synchronous prompt returned no final text")
	return { promptMessageId: info.parentID, assistantMessageId: info.id, text }
}

export async function resolveRootSession(client: { session: { get(input: unknown): Promise<unknown> } }, sessionId: string): Promise<string> {
	if (!sessionId) throw new Error("missing session ID")
	const seen = new Set<string>(); let current = sessionId
	for (let depth = 0; depth < 32; depth++) {
		if (seen.has(current)) throw new Error("session parent cycle")
		seen.add(current)
		const response = object(await client.session.get({ path: { id: current } })), session = object(response?.data)
		if (!session || typeof session.id !== "string") throw new Error("session unavailable")
		if (!session.parentID) return session.id
		if (typeof session.parentID !== "string") throw new Error("invalid session parent")
		current = session.parentID
	}
	throw new Error("session parent depth exceeded")
}

const MAX_CALLBACK_BODY_BYTES = 128 * 1024
const MAX_CALLBACK_ID_LENGTH = 500
const MAX_DIRECTORY_LENGTH = 2000
const MAX_ROOT_STATUS_COUNT = 64
const MAX_QUESTIONS = 32
const MAX_ANSWERS_PER_QUESTION = 32
const MAX_ANSWER_LENGTH = 1000

type JsonObject = Record<string, unknown>
type LegacyCallbackClient = { session: { get(input: unknown): Promise<unknown>; prompt(input: unknown): Promise<unknown> } }

function object(value: unknown): JsonObject | undefined { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined }
function boundedString(value: unknown, max = MAX_CALLBACK_ID_LENGTH): value is string { return typeof value === "string" && value.length > 0 && value.length <= max && !/[\u0000-\u001f\u007f]/.test(value) }
function resultData(value: unknown): unknown { return object(value)?.data }
function explicitSuccess(result: ControlResult, status: number): boolean { return result.status === status && result.error === undefined }
function rejected(error: string, status = 400): Response { return Response.json({ ok: false, certainty: "REJECTED", error }, { status }) }
function uncertain(error: string): Response { return Response.json({ ok: false, certainty: "UNKNOWN", error }, { status: 409 }) }

async function boundedBody(request: Request): Promise<JsonObject | undefined> {
	const declared = request.headers.get("content-length")
	if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > MAX_CALLBACK_BODY_BYTES)) return
	const text = await request.text()
	if (!text.length || new TextEncoder().encode(text).length > MAX_CALLBACK_BODY_BYTES) return
	try { return object(JSON.parse(text)) } catch { return }
}

function sessionIdentity(data: unknown): { id: string; parentID?: string } | undefined {
	const value = object(data)
	if (!value || !boundedString(value.id)) return
	if (value.parentID !== undefined && !boundedString(value.parentID)) return
	return { id: value.id, ...(typeof value.parentID === "string" ? { parentID: value.parentID } : {}) }
}

async function exactRoot(client: V2ControlClient, rootSessionId: string, directory: string): Promise<boolean> {
	const response = await client.sessionGet({ sessionID: rootSessionId, directory })
	const session = response.error === undefined ? sessionIdentity(response.data) : undefined
	return response.status === 200 && session?.id === rootSessionId && session.parentID === undefined
}

async function sourceBelongsToRoot(client: V2ControlClient, sourceSessionId: string, rootSessionId: string, directory: string): Promise<boolean> {
	const seen = new Set<string>(); let current = sourceSessionId
	for (let depth = 0; depth < 32; depth++) {
		if (seen.has(current)) return false
		seen.add(current)
		const response = await client.sessionGet({ sessionID: current, directory }), session = response.error === undefined ? sessionIdentity(response.data) : undefined
		if (response.status !== 200 || !session || session.id !== current) return false
		if (session.parentID === undefined) return session.id === rootSessionId
		current = session.parentID
	}
	return false
}

function answers(value: unknown): string[][] | undefined {
	if (!Array.isArray(value) || value.length === 0 || value.length > MAX_QUESTIONS) return
	const output: string[][] = []
	for (const answer of value) {
		if (!Array.isArray(answer) || answer.length > MAX_ANSWERS_PER_QUESTION) return
		const row: string[] = []
		for (const item of answer) { if (!boundedString(item, MAX_ANSWER_LENGTH)) return; row.push(item) }
		output.push(row)
	}
	return output
}

function pendingRequestMatches(data: unknown, requestId: string, sourceSessionId: string): boolean {
	if (!Array.isArray(data) || data.length > 1000) return false
	return data.some((item) => { const value = object(item); return value?.id === requestId && value.sessionID === sourceSessionId })
}

function normalizedStatus(value: unknown): "IDLE" | "BUSY" | "RETRY" | "UNKNOWN" {
	const type = object(value)?.type
	return type === "idle" ? "IDLE" : type === "busy" ? "BUSY" : type === "retry" ? "RETRY" : "UNKNOWN"
}

async function handleV2Callback(pathname: string, request: Request, client: V2ControlClient): Promise<Response> {
	const body = await boundedBody(request)
	if (!body) return rejected("bad-request")
	const rootSessionId = body.rootSessionId, directory = body.directory
	if (!boundedString(rootSessionId) || !boundedString(directory, MAX_DIRECTORY_LENGTH)) return rejected("bad-request")

	if (pathname === "/submit-prompt") {
		if (!boundedString(body.inboundId) || !boundedString(body.messageId) || typeof body.text !== "string" || !isPlainText(body.text)) return rejected("bad-request")
		try { if (!(await exactRoot(client, rootSessionId, directory))) return rejected("not-root", 409) } catch { return rejected("session-unreachable", 409) }
		try {
			const result = await client.sessionPromptAsync({ sessionID: rootSessionId, directory, messageID: body.messageId, parts: [{ type: "text", text: body.text }] }, request.signal)
			return explicitSuccess(result, 204) ? Response.json({ ok: true, accepted: true }) : uncertain("prompt-admission-uncertain")
		} catch { return uncertain("prompt-admission-uncertain") }
	}

	if (pathname === "/resolve-question") {
		const sourceSessionId = body.sourceSessionId, requestId = body.requestId, orderedAnswers = answers(body.answers)
		if (!boundedString(sourceSessionId) || !boundedString(requestId) || !orderedAnswers) return rejected("bad-request")
		try {
			if (!(await sourceBelongsToRoot(client, sourceSessionId, rootSessionId, directory))) return rejected("source-root-mismatch", 409)
			const listed = await client.questionList({ directory })
			if (!explicitSuccess(listed, 200) || !pendingRequestMatches(listed.data, requestId, sourceSessionId)) return rejected("request-unavailable", 409)
		} catch { return rejected("validation-unavailable", 409) }
		try { const result = await client.questionReply({ requestID: requestId, directory, answers: orderedAnswers }, request.signal); return explicitSuccess(result, 200) && result.data === true ? Response.json({ ok: true, resolved: true }) : uncertain("question-resolution-uncertain") } catch { return uncertain("question-resolution-uncertain") }
	}

	if (pathname === "/resolve-permission") {
		const sourceSessionId = body.sourceSessionId, requestId = body.requestId, decision = body.decision
		if (!boundedString(sourceSessionId) || !boundedString(requestId) || (decision !== "once" && decision !== "reject")) return rejected("bad-request")
		try {
			if (!(await sourceBelongsToRoot(client, sourceSessionId, rootSessionId, directory))) return rejected("source-root-mismatch", 409)
			const listed = await client.permissionList({ directory })
			if (!explicitSuccess(listed, 200) || !pendingRequestMatches(listed.data, requestId, sourceSessionId)) return rejected("request-unavailable", 409)
		} catch { return rejected("validation-unavailable", 409) }
		try { const result = await client.permissionReply({ requestID: requestId, directory, reply: decision }, request.signal); return explicitSuccess(result, 200) && result.data === true ? Response.json({ ok: true, resolved: true }) : uncertain("permission-resolution-uncertain") } catch { return uncertain("permission-resolution-uncertain") }
	}

	if (pathname === "/runtime-status") {
		if (!Array.isArray(body.rootSessionIds) || body.rootSessionIds.length === 0 || body.rootSessionIds.length > MAX_ROOT_STATUS_COUNT || !body.rootSessionIds.every((item) => boundedString(item)) || new Set(body.rootSessionIds).size !== body.rootSessionIds.length) return rejected("bad-request")
		const roots = body.rootSessionIds as string[]
		try {
			for (const root of roots) if (!(await exactRoot(client, root, directory))) return rejected("not-root", 409)
			const result = await client.sessionStatus({ directory }), values = object(result.data)
			if (!explicitSuccess(result, 200) || !values) return rejected("status-unavailable", 409)
			return Response.json({ ok: true, statuses: roots.map((root) => ({ rootSessionId: root, status: values[root] === undefined ? "IDLE" : normalizedStatus(values[root]) })) })
		} catch { return rejected("status-unavailable", 409) }
	}
	return Response.json({ error: "not-found" }, { status: 404 })
}

export function createCallbackHandler(client: LegacyCallbackClient, sharedSecret: string, instanceToken: string, controlClient?: V2ControlClient): (request: Request) => Promise<Response> {
	return async (request) => {
		if (request.method !== "POST") return Response.json({ error: "method" }, { status: 405 })
		if (request.headers.get("x-wechat-control-key") !== sharedSecret || request.headers.get("x-wechat-instance-token") !== instanceToken) return Response.json({ error: "unauthorized" }, { status: 401 })
		const pathname = new URL(request.url).pathname
		if (pathname === "/health") {
			try { const body = object(await request.json()); if (!body) return Response.json({ error: "bad-request" }, { status: 400 }); if (body.rootSessionId) { if (!boundedString(body.rootSessionId)) return Response.json({ error: "bad-request" }, { status: 400 }); const response = object(await client.session.get({ path: { id: body.rootSessionId } })), session = sessionIdentity(response?.data); if (!session || session.id !== body.rootSessionId || session.parentID) return Response.json({ error: "not-root" }, { status: 409 }) }; return Response.json({ ok: true }) } catch { return Response.json({ error: "session-unreachable" }, { status: 409 }) }
		}
		if (pathname !== "/inject") return controlClient ? handleV2Callback(pathname, request, controlClient) : Response.json({ error: "v2-client-unavailable" }, { status: 503 })
		let stage = "request"
		try {
			const body = object(await request.json()), envelope = object(body?.envelope)
			if (!body || typeof body.rootSessionId !== "string" || typeof body.directory !== "string" || typeof body.text !== "string" || typeof body.inboundId !== "string" || !envelope || (envelope.kind !== "inbound" && (envelope.kind !== "checkpoint" || typeof envelope.checkpointId !== "string"))) return Response.json({ error: "bad-request" }, { status: 400 })
			stage = "session"
			const session = object(await client.session.get({ path: { id: body.rootSessionId } })), identity = sessionIdentity(session?.data)
			if (!identity || identity.id !== body.rootSessionId || identity.parentID) return Response.json({ error: "not-root" }, { status: 409 })
			stage = "prompt"
			const promptText = envelope.kind === "checkpoint" ? `[受限微信接管：异步检查点回答 ${envelope.checkpointId}]\n${body.text}` : body.text
			const result = await client.session.prompt({ path: { id: body.rootSessionId }, query: { directory: body.directory }, body: { parts: [{ type: "text", text: promptText }] }, signal: request.signal })
			stage = "response"
			return Response.json({ ok: true, ...extractPromptAssistant(result) })
		} catch { return Response.json({ error: "inject-failed", stage }, { status: 409 }) }
	}
}

export function startCallbackServer(client: LegacyCallbackClient, sharedSecret: string, instanceToken: string, controlClient?: V2ControlClient): CallbackServer {
	const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: createCallbackHandler(client, sharedSecret, instanceToken, controlClient) })
	return { endpoint: server.url.toString(), token: instanceToken, stop: () => server.stop() }
}

export function startV2CallbackServer(client: LegacyCallbackClient, serverUrl: URL | string, directory: string, sharedSecret: string, instanceToken: string): CallbackServer { return startCallbackServer(client, sharedSecret, instanceToken, createV2ControlClient(serverUrl, directory)) }

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
		const spawnOptions = { stdin: "ignore", stdout: "ignore", stderr: "ignore", detached: true } as const
		Bun.spawn([bun, workerPath, encoded], spawnOptions)
		for (let attempt = 0; attempt < 50; attempt++) { await Bun.sleep(100); lock = await readLock(state.directory); if (lock && await authenticatedHealth(lock, state.secret)) break }
	}
	if (!lock || !(await authenticatedHealth(lock, state.secret))) throw new Error("broker did not become healthy")
	const health = await makeRpcRequest(lock.endpoint, state.secret, { method: "health", challenge: lock.workerToken }), body = object(await health.json())
	return { endpoint: lock.endpoint, secret: state.secret, adapter: typeof body?.adapter === "string" ? body.adapter : "unknown" }
}
