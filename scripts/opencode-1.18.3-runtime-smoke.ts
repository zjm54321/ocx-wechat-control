import { appendFile, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises"
import * as path from "node:path"
import { pathToFileURL } from "node:url"
import { Database } from "bun:sqlite"

type Json = Record<string, any>
type NativeRequestRow = { requestId: string; requestKey: string; code: string; rootSessionId: string; kind: string; state: string; payload: string }
const repo = path.resolve(import.meta.dir, ".."), shim = Bun.which("opencode")
if (!shim) throw new Error("opencode is not installed")
const executable = process.platform === "win32" ? path.join(path.dirname(shim), "node_modules", "opencode-ai", "bin", "opencode.exe") : shim
if (!(await Bun.file(executable).exists())) throw new Error(`OpenCode executable not found: ${executable}`)
const version = (await Bun.$`${executable} --version`.text()).trim()
if (version !== "1.18.3") throw new Error(`expected OpenCode 1.18.3, found ${version}`)

const root = path.join(process.env.TEMP ?? import.meta.dir, `ocx-runtime-smoke-${crypto.randomUUID()}`)
const local = path.join(root, "local"), configHome = path.join(root, "config"), project = path.join(root, "project")
const opencodeConfigDir = path.join(configHome, "opencode")
const accounts = path.join(root, "weixin-accounts"), inbox = path.join(root, "mcp-inbox"), evidenceFile = path.join(root, "mcp-evidence.jsonl")
const mcp = path.join(root, "fake", "node_modules", "weixin-mcp", "dist", "cli.js")
const tlsCert = path.join(import.meta.dir, "fixtures", "localhost-weixin-cert.pem"), tlsKey = path.join(import.meta.dir, "fixtures", "localhost-weixin-key.pem")
const state = path.join(local, "opencode", "wechat-control"), stateDb = path.join(state, "state.sqlite"), lockFile = path.join(state, "broker.lock", "owner.json")
await Promise.all([mkdir(path.dirname(mcp), { recursive: true }), mkdir(accounts, { recursive: true }), mkdir(inbox, { recursive: true }), mkdir(project, { recursive: true }), mkdir(opencodeConfigDir, { recursive: true })])
async function bootstrapPlugin(): Promise<void> {
	await writeFile(path.join(opencodeConfigDir, "package.json"), JSON.stringify({ private: true, dependencies: { "@opencode-ai/plugin": "1.18.3" } }, null, 2))
	const child = Bun.spawn([process.execPath, "install", "--ignore-scripts", "--no-progress"], { cwd: opencodeConfigDir, env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("OPENCODE_"))), stdout: "pipe", stderr: "pipe" })
	const output = await Promise.race([Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(async ([stdout, stderr]) => { await child.exited; return { stdout, stderr, timedOut: false } }), Bun.sleep(120_000).then(() => ({ stdout: "", stderr: "", timedOut: true }))])
	if (output.timedOut) { child.kill(9); await child.exited; throw new Error(`plugin bootstrap timed out after 120s in ${opencodeConfigDir}; command=${process.execPath} install --ignore-scripts --no-progress`) }
	if (child.exitCode !== 0) throw new Error(`plugin bootstrap failed (exit ${child.exitCode}) in ${opencodeConfigDir}\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`)
}
try { await bootstrapPlugin() } catch (error) { await rm(root, { recursive: true, force: true }); throw error }

const mcpSource = String.raw`const { appendFileSync, readdirSync, readFileSync, unlinkSync } = require("node:fs");
const inbox=process.env.OCX_SMOKE_MCP_INBOX, evidence=process.env.OCX_SMOKE_MCP_EVIDENCE;
const log=(kind,data={})=>appendFileSync(evidence,JSON.stringify({at:Date.now(),kind,...data})+"\n");
let buffer=""; process.stdin.setEncoding("utf8"); process.stdin.on("data",async chunk=>{buffer+=chunk; let nl; while((nl=buffer.indexOf("\n"))>=0){const line=buffer.slice(0,nl).trim(); buffer=buffer.slice(nl+1); if(!line)continue; const msg=JSON.parse(line); log("rpc",{method:msg.method}); if(msg.method==="notifications/initialized"){log("initialized");continue} if(msg.id===undefined)continue; let result;
if(msg.method==="initialize"){log("handshake",{params:msg.params});result={protocolVersion:"2025-03-26",capabilities:{tools:{}},serverInfo:{name:"fake-weixin-mcp",version:"1.0.0"}}}
else if(msg.method==="tools/list")result={tools:[{name:"weixin_poll",description:"poll",inputSchema:{type:"object",properties:{}}},{name:"weixin_send",description:"send",inputSchema:{type:"object",properties:{}}}]};
else if(msg.method==="tools/call"&&msg.params?.name==="weixin_send"){log("send",{arguments:msg.params.arguments});result={content:[{type:"text",text:"{}"}]}}
else if(msg.method==="tools/call"&&msg.params?.name==="weixin_poll"){let msgs=[],cursor="empty-"+Date.now(); const files=readdirSync(inbox).sort(); if(files.length){const file=files[0], value=JSON.parse(readFileSync(inbox+"/"+file,"utf8")); unlinkSync(inbox+"/"+file); msgs=[value.msg];cursor=value.cursor;log("poll-delivery",{cursor,text:value.msg.item_list?.[0]?.text_item?.text})}else await new Promise(r=>setTimeout(r,25)); result={content:[{type:"text",text:JSON.stringify({msgs,get_updates_buf:cursor})}]}}
else throw new Error("unexpected MCP method "+msg.method); process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:msg.id,result})+"\n")}}); log("started",{pid:process.pid});
process.on("exit",()=>log("exit"));`
await writeFile(mcp, mcpSource)

const weixinCalls: Json[] = []
const weixin = Bun.serve({ hostname: "127.0.0.1", port: 0, tls: { cert: Bun.file(tlsCert), key: Bun.file(tlsKey) }, async fetch(request) {
	const body = await request.json().catch(() => ({})), pathname = new URL(request.url).pathname
	weixinCalls.push({ at: Date.now(), pathname, body })
	if (pathname.endsWith("/getconfig")) return Response.json({ typing_ticket: "smoke-ticket", context_token: "ctx-smoke" })
	if (pathname.endsWith("/sendtyping")) return Response.json({ ret: 0 })
	return Response.json({ error: "unexpected fake Weixin request" }, { status: 404 })
} })
await writeFile(path.join(accounts, "smoke.json"), JSON.stringify({ token: "local-token", baseUrl: weixin.url.toString(), accountId: "smoke" }))

let providerSequence = 0, releaseFirst!: () => void
const firstHeld = new Promise<void>((resolve) => { releaseFirst = resolve })
const providerRequests: Json[] = [], providerEvents: Json[] = []
function latestUser(body: Json): string { return [...(body.messages ?? [])].reverse().find((message: Json) => message.role === "user")?.content?.toString() ?? "" }
function toolResults(body: Json): string { return (body.messages ?? []).filter((message: Json) => message.role === "tool").map((message: Json) => message.content).join("\n") }
function hasToolCall(body: Json, callId: string): boolean { return JSON.stringify(body.messages ?? []).includes(callId) }
function sseChunk(controller: ReadableStreamDefaultController<Uint8Array>, value: Json): void { controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(value)}\n\n`)) }
function completion(requestId: string, model: string, delta: Json, finish: string | null): Json { return { id: requestId, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta, finish_reason: finish }] } }
function toolCalls(requestId: string, model: string, calls: Array<{ id: string; name: string; args: Json }>): Response {
	return new Response(new ReadableStream({ start(controller) { sseChunk(controller, completion(requestId, model, { role: "assistant", tool_calls: calls.map((call, index) => ({ index, id: call.id, type: "function", function: { name: call.name, arguments: JSON.stringify(call.args) } })) }, null)); sseChunk(controller, completion(requestId, model, {}, "tool_calls")); controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n")); controller.close() } }), { headers: { "content-type": "text/event-stream" } })
}
function textResponse(requestId: string, model: string, text: string, held = false): Response {
	return new Response(new ReadableStream({ async start(controller) { if (held) { providerEvents.push({ at: Date.now(), kind: "first-held", requestId }); await firstHeld; providerEvents.push({ at: Date.now(), kind: "first-released", requestId }) }; sseChunk(controller, completion(requestId, model, { role: "assistant", content: text }, null)); sseChunk(controller, completion(requestId, model, {}, "stop")); controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n")); controller.close() } }), { headers: { "content-type": "text/event-stream" } })
}
const provider = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
	const url = new URL(request.url); if (request.method === "GET" && url.pathname.endsWith("/models")) return Response.json({ object: "list", data: [{ id: "smoke-model", object: "model", owned_by: "local" }] })
	if (request.method !== "POST" || !url.pathname.endsWith("/chat/completions")) return Response.json({ error: "local provider only" }, { status: 404 })
	const body = await request.json() as Json, requestId = `chatcmpl-${++providerSequence}`, user = latestUser(body), results = toolResults(body)
	providerRequests.push({ at: Date.now(), requestId, user, tools: (body.tools ?? []).map((item: Json) => item.function?.name), toolResults: results, messages: body.messages })
	if (user.includes("ORDINARY_ONE")) return textResponse(requestId, body.model, "ordinary one complete", true)
	if (user.includes("ORDINARY_TWO")) return textResponse(requestId, body.model, "ordinary two complete")
	if (user.includes("ASK_QUESTION") && !results.includes("Blue")) return toolCalls(requestId, body.model, [{ id: "call-question-smoke", name: "question", args: { questions: [{ header: "Color", question: "Pick a color", options: [{ label: "Blue", description: "smoke answer" }], multiple: false, custom: false }] } }])
	if (user.includes("ASK_PERMISSION") && !hasToolCall(body, "call-bash-smoke")) return toolCalls(requestId, body.model, [{ id: "call-bash-smoke", name: "bash", args: { command: "echo permission-smoke", description: "Permission smoke echo" } }])
	if (user.includes("CUSTOM_TOOLS") && !hasToolCall(body, "call-status-smoke")) return toolCalls(requestId, body.model, [
		{ id: "call-status-smoke", name: "wechat_control_status", args: {} },
		{ id: "call-compat-smoke", name: "wechat_send_text", args: { text: "must-not-send" } },
		{ id: "call-reply-smoke", name: "wechat_reply", args: { text: "exact custom reply" } },
	])
	return textResponse(requestId, body.model, "tool phase complete")
} })

const config = JSON.stringify({
	model: "smoke/smoke-model", permission: { bash: "ask" },
	provider: { smoke: { npm: "@ai-sdk/openai-compatible", name: "Local Smoke", options: { baseURL: `${provider.url}v1`, apiKey: "local-only" }, models: { "smoke-model": { name: "Smoke Model" } } } },
	plugin: [[pathToFileURL(path.join(repo, "dist", "index.js")).href, { enabled: true, weixinCommand: ["node", mcp] }]],
})
const probe = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() }), port = probe.port; probe.stop()
const childEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("OPENCODE_")))
const env = { ...childEnv, HOME: root, USERPROFILE: root, APPDATA: path.join(root, "roaming"), LOCALAPPDATA: local, XDG_CONFIG_HOME: configHome, XDG_DATA_HOME: path.join(root, "data"), XDG_CACHE_HOME: path.join(root, "cache"), XDG_STATE_HOME: path.join(root, "state"), OPENCODE_TEST_HOME: root, NODE_EXTRA_CA_CERTS: tlsCert, WEIXIN_MCP_DIR: accounts, WEIXIN_ACCOUNT_ID: "smoke", OCX_SMOKE_MCP_INBOX: inbox, OCX_SMOKE_MCP_EVIDENCE: evidenceFile, OPENCODE_CONFIG_DIR: opencodeConfigDir, OPENCODE_CONFIG_CONTENT: config, OPENCODE_DISABLE_MODELS_FETCH: "true", OPENCODE_DISABLE_LSP_DOWNLOAD: "true", OPENCODE_DISABLE_DEFAULT_PLUGINS: "true" }
const opencode = Bun.spawn([executable, "serve", "--hostname", "127.0.0.1", "--port", String(port)], { cwd: project, env, stdout: "pipe", stderr: "pipe" })
const base = `http://127.0.0.1:${port}`, ownedPids = { opencode: opencode.pid, worker: 0 }, observedEvents: Json[] = []
let stdoutOutput = "", stderrOutput = "", eventAbort = new AbortController(), inboundCounter = 0, mcpSnapshot: Json[] = [], workerHealthSnapshot: Json | undefined
let nativeRequestSnapshot: NativeRequestRow | undefined
function drain(stream: ReadableStream<Uint8Array>, capture: (text: string) => void) { const reader = stream.getReader(), decoder = new TextDecoder(); const done = (async () => { while (true) { const chunk = await reader.read(); if (chunk.done) break; capture(decoder.decode(chunk.value, { stream: true })) }; capture(decoder.decode()) })(); return { done, cancel: () => reader.cancel().catch(() => {}) } }
const stdoutDrain = drain(opencode.stdout, (text) => { stdoutOutput = `${stdoutOutput}${text}`.slice(-64 * 1024) }), stderrDrain = drain(opencode.stderr, (text) => { stderrOutput = `${stderrOutput}${text}`.slice(-128 * 1024) })
const diagnostic = () => JSON.stringify({ base, opencodeExitCode: opencode.exitCode, workerHealth: workerHealthSnapshot, nativeRequest: nativeRequestSnapshot, providerRequests: providerRequests.map(({ messages, ...item }) => item), providerEvents, observedEvents: observedEvents.slice(-30), mcp: mcpSnapshot.slice(-40), root, stdoutOutput, stderrOutput }, null, 2)
function assert(condition: unknown, message: string): asserts condition { if (!condition) throw new Error(`${message}\n${diagnostic()}`) }
async function eventually<T>(name: string, action: () => Promise<T> | T, accept: (value: T) => boolean, timeout = 20_000): Promise<T> { const end = Date.now() + timeout; let last: unknown; while (Date.now() < end) { try { const value = await action(); last = value; if (accept(value)) return value } catch (error) { last = error }; await Bun.sleep(50) }; throw new Error(`timeout waiting for ${name}: ${last instanceof Error ? last.message : JSON.stringify(last)}\n${diagnostic()}`) }
async function json(url: string, init?: RequestInit): Promise<any> { const response = await fetch(url, init); const text = await response.text(); if (!response.ok) throw new Error(`${response.status} ${url}: ${text}`); return text ? JSON.parse(text) : undefined }
async function opencodeFetch(route: string, init: RequestInit = {}): Promise<Response> { return fetch(`${base}${route}`, init) }
async function opencodeJson(route: string, init?: RequestInit): Promise<any> { const response = await opencodeFetch(route, init), text = await response.text(); if (!response.ok) throw new Error(`${response.status} ${route}: ${text}`); return text ? JSON.parse(text) : undefined }
async function messages(sessionId: string): Promise<Json[]> { return opencodeJson(`/session/${sessionId}/message`) }
async function enqueue(text: string): Promise<void> { const name = `${String(++inboundCounter).padStart(3, "0")}.json`, msg = { message_type: 1, from_user_id: "wx-user-smoke", context_token: "ctx-smoke", item_list: [{ type: 1, text_item: { text } }] }; await writeFile(path.join(inbox, name), JSON.stringify({ cursor: `cursor-${inboundCounter}`, msg })) }
async function mcpEvidence(): Promise<Json[]> { if (!(await Bun.file(evidenceFile).exists())) return []; mcpSnapshot = (await readFile(evidenceFile, "utf8")).trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line)); return mcpSnapshot }
async function sends(): Promise<Json[]> { return (await mcpEvidence()).filter((item) => item.kind === "send") }
function permissionRequest(code: string): NativeRequestRow | undefined {
	const db = new Database(stateDb, { readonly: true })
	try {
		const row = db.query("SELECT request_id AS requestId,request_key AS requestKey,code,root_session_id AS rootSessionId,kind,state,payload_json AS payload FROM native_requests WHERE code=? AND kind='PERMISSION'").get(code) as NativeRequestRow | null
		nativeRequestSnapshot = row ?? undefined
		return row ?? undefined
	} finally { db.close() }
}
async function subscribeEvents(): Promise<void> { const response = await opencodeFetch("/event", { signal: eventAbort.signal }); if (!response.body) return; const reader = response.body.getReader(), decoder = new TextDecoder(); let buffer = ""; while (true) { const part = await reader.read(); if (part.done) break; buffer += decoder.decode(part.value, { stream: true }); let split; while ((split = buffer.indexOf("\n\n")) >= 0) { const frame = buffer.slice(0, split); buffer = buffer.slice(split + 2); const data = frame.split(/\r?\n/).find((line) => line.startsWith("data:"))?.slice(5).trim(); if (data) try { observedEvents.push(JSON.parse(data)) } catch {} } } }
async function killOwned(pid: number): Promise<void> { if (!pid) return; if (process.platform === "win32") { const child = Bun.spawn(["taskkill", "/PID", String(pid), "/T", "/F"], { stdout: "pipe", stderr: "pipe" }); await child.exited } else { try { process.kill(-pid, "SIGTERM") } catch { try { process.kill(pid, "SIGTERM") } catch {} } } }
async function pidAlive(pid: number): Promise<boolean> { if (!pid) return false; if (process.platform === "win32") { const child = Bun.spawn(["tasklist", "/FI", `PID eq ${pid}`, "/FO", "CSV", "/NH"], { stdout: "pipe", stderr: "ignore" }); return (await new Response(child.stdout).text()).includes(`"${pid}"`) }; try { process.kill(pid, 0); return true } catch { return false } }

let finalEvidence: Json | undefined, cleanupProof: Json = {}
try {
	const listening = await eventually("OpenCode listen announcement", () => stdoutOutput.match(/opencode server listening on (http:\/\/127\.0\.0\.1:\d+)/)?.[1], (value) => Boolean(value))
	assert(listening === base, `OpenCode listened on unexpected address ${listening}`)
	await eventually("OpenCode health", async () => { if (opencode.exitCode !== null) throw new Error(`OpenCode exited with code ${opencode.exitCode}`); const response = await opencodeFetch("/global/health", { signal: AbortSignal.timeout(1_000) }); return { ok: response.ok, body: await response.json() as Json } }, (value) => value.ok && value.body.healthy === true && value.body.version === version)
	void subscribeEvents().catch((error) => { if (!eventAbort.signal.aborted) stderrOutput += `\nevent stream: ${error}` })
	const session = await eventually("OpenCode project/session initialization", () => opencodeJson("/session", { method: "POST", headers: { "content-type": "application/json", "x-opencode-directory": project }, body: "{}", signal: AbortSignal.timeout(5_000) }), (value) => typeof value?.id === "string", 60_000)
	const lock = await eventually("worker lock", async () => JSON.parse(await readFile(lockFile, "utf8")), (value) => value.endpoint?.startsWith("http://127.0.0.1:") && value.pid > 0)
	ownedPids.worker = lock.pid
	const secret = (await readFile(path.join(state, "rpc.secret"), "utf8")).trim()
	const readyHealth = await eventually("worker health ready", async () => { const response = await fetch(lock.endpoint, { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": secret }, body: JSON.stringify({ method: "health", challenge: lock.workerToken }), signal: AbortSignal.timeout(1_000) }); const body = await response.json() as Json; workerHealthSnapshot = { status: response.status, ...body }; return { response, body } }, (value) => value.response.ok && value.body.packageVersion === "0.2.4" && value.body.protocolVersion === 1 && value.body.schemaVersion === 7 && value.body.adapter === "Ready", 30_000)
	const healthResponse = readyHealth.response, health = readyHealth.body
	const handshake = await eventually("MCP handshake", mcpEvidence, (items) => items.some((item) => item.kind === "handshake") && items.some((item) => item.kind === "initialized"))
	assert(healthResponse.ok && health.packageVersion === "0.2.4" && health.protocolVersion === 1 && health.schemaVersion === 7 && health.adapter === "Ready", "worker health metadata mismatch")
	assert(Array.isArray(health.capabilities) && health.capabilities.includes("native-question-permission"), "worker capabilities missing")

	const beforeLeave = await messages(session.id), leave = await opencodeFetch(`/session/${session.id}/command`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ command: "leave", arguments: "", model: "smoke/smoke-model" }) }), afterLeave = await messages(session.id)
	assert(leave.status === 204 && beforeLeave.length === 0 && afterLeave.length === 0 && providerRequests.length === 0, "/leave crossed model boundary")
	const control = await json(lock.endpoint, { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": secret }, body: JSON.stringify({ method: "health", challenge: lock.workerToken }) })

	await enqueue("id")
	await eventually("route establishment", sends, (items) => items.some((item) => item.arguments?.text?.includes("#1")))
	await enqueue("#1\nORDINARY_ONE")
	const firstProviderRequest = await eventually("first provider hold", async () => ({ request: providerRequests.find((item) => item.user.includes("ORDINARY_ONE")), held: providerEvents.find((item) => item.kind === "first-held"), released: providerEvents.some((item) => item.kind === "first-released") }), (value) => Boolean(value.request && value.held && !value.released))
	await enqueue("#1\nORDINARY_TWO")
	await eventually("MCP inbound deliveries", mcpEvidence, (items) => items.filter((item) => item.kind === "poll-delivery").length >= 3)
	await eventually("both prompt admissions", () => messages(session.id), (items) => items.filter((item) => item.info?.role === "user").length >= 2)
	const admissionMessages = await messages(session.id), admissionAt = Date.now(), held = providerEvents.find((item) => item.kind === "first-held")
	assert(Boolean(held) && !providerEvents.some((item) => item.kind === "first-released"), "first generation completed before both admissions")
	assert(admissionMessages.filter((item) => item.info?.role === "user").some((item) => JSON.stringify(item).includes("ORDINARY_TWO")), "second inbound prompt was not admitted")
	await eventually("typing ON", async () => weixinCalls, (items) => items.some((item) => item.pathname.endsWith("/sendtyping") && item.body.status === 1))
	releaseFirst()
	await eventually("first ordinary prompt complete", () => messages(session.id), (items) => items.some((item) => item.info?.role === "assistant" && item.info?.time?.completed), 30_000)
	await eventually("ordinary idle", async () => observedEvents, (items) => items.some((item) => item.type === "session.idle"))

	await enqueue("#1\nASK_QUESTION")
	const questionSend = await eventually("question relay", sends, (items) => items.find((item) => /Q[A-Z2-7]{6} 问题请求/.test(item.arguments?.text))) as Json[]
	const questionText = questionSend.find((item) => /问题请求/.test(item.arguments?.text))!.arguments.text as string, questionCode = questionText.match(/Q[A-Z2-7]{6}/)![0]
	await enqueue(`#1\n${questionCode} Blue`)
	await eventually("question.replied", async () => observedEvents, (items) => items.some((item) => item.type === "question.replied"))
	await eventually("question closure", () => opencodeJson("/question"), (items) => Array.isArray(items) && !items.some((item) => item.id === "call-question-smoke"))

	await enqueue("#1\nASK_PERMISSION")
	const permissionSend = await eventually("permission relay", sends, (items) => items.find((item) => /P[A-Z2-7]{6} 权限请求/.test(item.arguments?.text))) as Json[]
	const permissionText = permissionSend.find((item) => /权限请求/.test(item.arguments?.text))!.arguments.text as string, permissionCode = permissionText.match(/P[A-Z2-7]{6}/)![0]
	const permissionRequestRow = await eventually("persisted permission request", () => permissionRequest(permissionCode), (row) => row?.state === "OPEN")
	assert(permissionRequestRow.rootSessionId === session.id && permissionRequestRow.code === permissionCode && permissionRequestRow.kind === "PERMISSION" && JSON.parse(permissionRequestRow.payload).permission === "bash", `permission relay did not map to the expected native request: ${JSON.stringify(permissionRequestRow)}`)
	await enqueue(`#1\n${permissionCode} reject`)
	await eventually("permission.replied closure", async () => observedEvents, (items) => items.some((item) => item.type === "permission.replied" || item.type === "permission.rejected"))
	const rejectedPermission = await eventually("persisted permission rejection", () => permissionRequest(permissionCode), (row) => row?.requestId === permissionRequestRow.requestId && row.requestKey === permissionRequestRow.requestKey && row.rootSessionId === permissionRequestRow.rootSessionId && row.state === "REJECTED")
	assert(!(await Bun.file(path.join(project, "permission-smoke")).exists()), "permission tool created an unexpected file")

	const sendsBeforeTools = (await sends()).length; await enqueue("#1\nCUSTOM_TOOLS")
	await eventually("custom tool results", async () => providerRequests, (items) => items.some((item) => item.user.includes("CUSTOM_TOOLS") && item.toolResults.includes("broker=Ready") && item.toolResults.includes("拒绝")))
	const toolSends = (await sends()).slice(sendsBeforeTools), exact = toolSends.filter((item) => item.arguments?.text === "#1\nexact custom reply")
	assert(exact.length === 1, `wechat_reply exact send count was ${exact.length}`)
	assert(!toolSends.some((item) => item.arguments?.text?.includes("must-not-send") || item.arguments?.text?.includes("tool phase complete")), "compatibility or automatic assistant text escaped")
	await eventually("authoritative final idle", async () => weixinCalls, (items) => { const typing = items.filter((item) => item.pathname.endsWith("/sendtyping")); return typing.some((item) => item.body.status === 1) && typing.at(-1)?.body.status === 2 }, 30_000)

	const back = await opencodeFetch(`/session/${session.id}/command`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ command: "back", arguments: "", model: "smoke/smoke-model" }) })
	assert(back.status === 204, "/back failed")
	finalEvidence = { version, plugin: "built-dist", worker: { pid: lock.pid, packageVersion: health.packageVersion, protocolVersion: health.protocolVersion, schemaVersion: health.schemaVersion, capabilities: health.capabilities, adapter: health.adapter }, mcp: { handshake: handshake.some((item) => item.kind === "handshake"), initialized: handshake.some((item) => item.kind === "initialized"), pollDeliveries: handshake.filter((item) => item.kind === "poll-delivery").length }, leave: { status: leave.status, messages: [beforeLeave.length, afterLeave.length], providerRequests: 0, registeredRoot: session.id }, concurrency: { admittedUsers: admissionMessages.filter((item) => item.info?.role === "user").length, firstProviderAt: firstProviderRequest.request.at, firstHeldAt: held.at, bothObservedAt: admissionAt, firstReleasedAt: providerEvents.find((item) => item.kind === "first-released")?.at }, question: { code: questionCode, repliedEvent: true }, permission: { code: permissionCode, requestId: rejectedPermission.requestId, decision: "reject", closureEvent: true, nativeState: rejectedPermission.state, sideEffect: false }, tools: { status: true, compatibilityRejected: true, wechatReplyText: "#1\nexact custom reply", sendCount: exact.length, automaticAssistantSends: 0 }, typing: weixinCalls.filter((item) => item.pathname.endsWith("/sendtyping")).map((item) => item.body.status === 1 ? "ON" : "OFF"), network: { provider: provider.url.toString(), weixin: weixin.url.toString(), external: false } }
} finally {
	eventAbort.abort()
	if (opencode.exitCode === null) await killOwned(opencode.pid)
	await Promise.race([opencode.exited, Bun.sleep(5_000)])
	if (ownedPids.worker) await killOwned(ownedPids.worker)
	await Bun.sleep(200)
	cleanupProof = { opencodePid: ownedPids.opencode, opencodeExited: !(await pidAlive(ownedPids.opencode)), workerPid: ownedPids.worker, workerExited: !(await pidAlive(ownedPids.worker)), serversStopped: true, tempRemoved: false }
	provider.stop(true); weixin.stop(true)
	await Promise.allSettled([stdoutDrain.done, stderrDrain.done])
	if (cleanupProof.opencodeExited && cleanupProof.workerExited) { await rm(root, { recursive: true, force: true }); cleanupProof.tempRemoved = true }
}
assert(finalEvidence, "smoke did not produce evidence")
assert(cleanupProof.opencodeExited && cleanupProof.workerExited && cleanupProof.tempRemoved, `cleanup failed: ${JSON.stringify(cleanupProof)}`)
console.log(JSON.stringify({ ...finalEvidence, cleanup: cleanupProof }))
