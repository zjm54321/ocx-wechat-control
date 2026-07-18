import { mkdir, rm, writeFile } from "node:fs/promises"
import * as path from "node:path"
import { pathToFileURL } from "node:url"

const repo = path.resolve(import.meta.dir, ".."), shim = Bun.which("opencode")
if (!shim) throw new Error("opencode is not installed")
const executable = process.platform === "win32"
	? path.join(path.dirname(shim), "node_modules", "opencode-ai", "bin", "opencode.exe")
	: shim
if (!(await Bun.file(executable).exists())) throw new Error(`OpenCode executable not found: ${executable}`)
const version = (await Bun.$`${executable} --version`.text()).trim()
if (version !== "1.18.3") throw new Error(`expected OpenCode 1.18.3, found ${version}`)
const root = path.join(process.env.TEMP ?? import.meta.dir, `ocx-command-smoke-${crypto.randomUUID()}`), local = path.join(root, "local"), configHome = path.join(root, "config"), project = path.join(root, "project"), state = path.join(local, "opencode", "wechat-control"), secret = "smoke-secret"
const opencodeConfigDir = path.join(configHome, "opencode")
await Promise.all([mkdir(path.join(state, "broker.lock"), { recursive: true }), mkdir(opencodeConfigDir, { recursive: true }), mkdir(project, { recursive: true })]); await writeFile(path.join(state, "rpc.secret"), secret)
async function bootstrapPlugin(): Promise<void> {
	await writeFile(path.join(opencodeConfigDir, "package.json"), JSON.stringify({ private: true, dependencies: { "@opencode-ai/plugin": "1.18.3" } }, null, 2))
	const child = Bun.spawn([process.execPath, "install", "--ignore-scripts", "--no-progress"], { cwd: opencodeConfigDir, env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("OPENCODE_"))), stdout: "pipe", stderr: "pipe" })
	const output = await Promise.race([Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(async ([stdout, stderr]) => { await child.exited; return { stdout, stderr, timedOut: false } }), Bun.sleep(120_000).then(() => ({ stdout: "", stderr: "", timedOut: true }))])
	if (output.timedOut) { child.kill(9); await child.exited; throw new Error(`plugin bootstrap timed out after 120s in ${opencodeConfigDir}; command=${process.execPath} install --ignore-scripts --no-progress`) }
	if (child.exitCode !== 0) throw new Error(`plugin bootstrap failed (exit ${child.exitCode}) in ${opencodeConfigDir}\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`)
}
try { await bootstrapPlugin() } catch (error) { await rm(root, { recursive: true, force: true }); throw error }
let enabled = false, registrations = 0
const brokerMethods: string[] = []
const adapter = ["node", path.join(repo, "node_modules", "weixin-mcp", "dist", "cli.js")], workerEntrypoint = path.join(repo, "dist", "worker.js")
const metadata = { packageVersion: "0.2.2", protocolVersion: 1, capabilities: ["v2-callbacks", "async-prompt-admission", "native-question-permission", "legacy-inject-disabled"], workerPid: process.pid, workerEntrypoint, adapterCommand: adapter, adapterProvenance: { kind: "fixed-local-dependency", package: "weixin-mcp", version: "1.7.7", entrypoint: adapter[1] }, schemaVersion: 6 }
const broker = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
	const body = await request.json() as any
	brokerMethods.push(String(body.method))
	if (body.method === "health") return Response.json({ ok: true, challenge: body.challenge, adapter: "Ready", ...metadata })
	if (body.method === "register") { registrations++; return Response.json({ ok: true }) }
	if (body.method === "leave-root") { enabled = true; return Response.json({ ok: true, binding: { alias: 1 } }) }
	if (body.method === "back-global") { enabled = false; return Response.json({ ok: true, enabled, revision: 2 }) }
	if (body.method === "unregister" || body.method === "heartbeat") return Response.json({ ok: true })
	return Response.json({ ok: true, enabled, adapter: "Ready", registered: enabled, alias: enabled ? 1 : null, routeReady: false, routable: false })
} })
const now = new Date().toISOString(); await writeFile(path.join(state, "broker.lock", "owner.json"), JSON.stringify({ pid: process.pid, startedAt: now, workerToken: "worker", endpoint: broker.url.toString(), heartbeat: now, ...metadata }))
const probe = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() }), port = probe.port; probe.stop()
const config = JSON.stringify({ model: "opencode/big-pickle", plugin: [[pathToFileURL(path.join(repo, "dist", "index.js")).href, { enabled: true, weixinCommand: adapter }]] })
const childEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("OPENCODE_")))
const opencode = Bun.spawn([executable, "serve", "--print-logs", "--log-level", "DEBUG", "--hostname", "127.0.0.1", "--port", String(port)], { cwd: project, env: { ...childEnv, HOME: root, USERPROFILE: root, LOCALAPPDATA: local, APPDATA: path.join(root, "roaming"), XDG_CONFIG_HOME: configHome, XDG_DATA_HOME: path.join(root, "data"), XDG_CACHE_HOME: path.join(root, "cache"), OPENCODE_TEST_HOME: root, OPENCODE_CONFIG_DIR: opencodeConfigDir, OPENCODE_CONFIG_CONTENT: config, OPENCODE_DISABLE_PROJECT_CONFIG: "true", OPENCODE_DISABLE_MODELS_FETCH: "true", OPENCODE_DISABLE_LSP_DOWNLOAD: "true", OPENCODE_DISABLE_DEFAULT_PLUGINS: "true" }, stdout: "pipe", stderr: "pipe" }), base = `http://127.0.0.1:${port}`
const startedAt = Date.now(), deadline = startedAt + 120_000
let stdoutOutput = "", stderrOutput = "", lastError: unknown, lastResponse: Record<string, unknown> | undefined
function drain(stream: ReadableStream<Uint8Array>, capture?: (text: string) => void) {
	const reader = stream.getReader(), decoder = new TextDecoder()
	const done = (async () => { while (true) { const chunk = await reader.read(); if (chunk.done) break; capture?.(decoder.decode(chunk.value, { stream: true })) }; capture?.(decoder.decode()) })()
	return { done, cancel: () => reader.cancel().catch(() => {}) }
}
const stdoutDrain = drain(opencode.stdout, (text) => { stdoutOutput = `${stdoutOutput}${text}`.slice(-64 * 1024) }), stderrDrain = drain(opencode.stderr, (text) => { stderrOutput = `${stderrOutput}${text}`.slice(-64 * 1024) })
const errorText = (error: unknown) => error instanceof Error ? `${error.name}: ${error.message}` : String(error)
const diagnostic = () => JSON.stringify({ base, root, elapsedMs: Date.now() - startedAt, opencodePid: opencode.pid, opencodeExitCode: opencode.exitCode, registrations, enabled, brokerMethods, lastError: lastError === undefined ? undefined : errorText(lastError), lastResponse, stdoutOutput, stderrOutput }, null, 2)
async function eventually<T>(name: string, action: () => Promise<T> | T, accept: (value: T) => boolean, timeout: number): Promise<T> {
	const end = Math.min(deadline, Date.now() + timeout)
	while (Date.now() < end) {
		if (opencode.exitCode !== null) throw new Error(`OpenCode exited while waiting for ${name}\n${diagnostic()}`)
		try { const value = await action(); if (accept(value)) return value; lastError = new Error(`${name} returned an unacceptable value: ${JSON.stringify(value)}`) }
		catch (error) { lastError = error }
		await Bun.sleep(Math.min(100, Math.max(0, end - Date.now())))
	}
	throw new Error(`timeout waiting for ${name}\n${diagnostic()}`)
}
async function opencodeFetch(route: string, init: RequestInit = {}, timeout = 5_000): Promise<Response> {
	const remaining = deadline - Date.now()
	if (remaining <= 0) throw new Error(`overall OpenCode HTTP deadline exceeded before ${route}`)
	lastResponse = { route, method: init.method ?? "GET", phase: "pending" }
	const response = await fetch(`${base}${route}`, { ...init, signal: AbortSignal.timeout(Math.max(1, Math.min(timeout, remaining))) })
	lastResponse = { route, method: init.method ?? "GET", status: response.status, statusText: response.statusText }
	return response
}
async function opencodeJson(route: string, init: RequestInit = {}, timeout = 5_000): Promise<any> {
	const response = await opencodeFetch(route, init, timeout), text = await response.text()
	lastResponse = { ...lastResponse, body: text.slice(0, 4_096) }
	if (!response.ok) throw new Error(`${init.method ?? "GET"} ${route} returned ${response.status}: ${text}`)
	try { return text ? JSON.parse(text) : undefined } catch (error) { throw new Error(`${init.method ?? "GET"} ${route} returned invalid JSON: ${errorText(error)}; body=${text.slice(0, 4_096)}`) }
}
async function stopOpenCode(): Promise<void> {
	if (opencode.exitCode !== null) { await opencode.exited; return }
	opencode.kill()
	if (await Promise.race([opencode.exited.then(() => true), Bun.sleep(5_000).then(() => false)])) return
	opencode.kill(9)
	if (!(await Promise.race([opencode.exited.then(() => true), Bun.sleep(5_000).then(() => false)]))) throw new Error(`owned OpenCode child ${opencode.pid} did not exit after force-kill`)
}
async function finishDrains(): Promise<void> {
	const drains = [stdoutDrain.done, stderrDrain.done]
	if (await Promise.race([Promise.allSettled(drains).then(() => true), Bun.sleep(2_000).then(() => false)])) return
	await Promise.all([stdoutDrain.cancel(), stderrDrain.cancel()]); await Promise.allSettled(drains)
}
try {
	const listening = await eventually("OpenCode listen announcement", () => stdoutOutput.match(/opencode server listening on (http:\/\/127\.0\.0\.1:\d+)/)?.[1], Boolean, 20_000)
	if (listening !== base) throw new Error(`OpenCode listened on unexpected address ${listening}\n${diagnostic()}`)
	await eventually("OpenCode health", async () => { const response = await opencodeFetch("/global/health", {}, 1_000), text = await response.text(); lastResponse = { ...lastResponse, body: text.slice(0, 4_096) }; return { ok: response.ok, body: text ? JSON.parse(text) : undefined } }, (value) => value.ok && value.body?.healthy === true && value.body?.version === version, 20_000)
	const session = await eventually("OpenCode project/session initialization", () => opencodeJson("/session", { method: "POST", headers: { "content-type": "application/json", "x-opencode-directory": project }, body: "{}" }), (value) => typeof value?.id === "string", 30_000)
	await eventually("plugin broker registration", () => registrations, (value) => value > 0, 20_000)
	const messages = async () => (await opencodeJson(`/session/${session.id}/message`) as any[]).length, before = await messages()
	const leave = await opencodeFetch(`/session/${session.id}/command`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ command: "leave", arguments: "", model: "opencode/big-pickle" }) }), middle = await messages(), afterLeave = enabled
	const back = await opencodeFetch(`/session/${session.id}/command`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ command: "back", arguments: "", model: "opencode/big-pickle" }) }), after = await messages()
	const evidence = { version, registrations, leaveStatus: leave.status, afterLeave, backStatus: back.status, afterBack: enabled, messages: [before, middle, after] }; console.log(JSON.stringify(evidence))
	if (leave.status !== 204 || back.status !== 204 || !afterLeave || enabled || evidence.messages.some(Boolean)) throw new Error(`acceptance failed: ${JSON.stringify(evidence)}\n${diagnostic()}`)
} finally {
	let childExited = false, stopError: unknown
	try { await stopOpenCode(); childExited = true } catch (error) { stopError = error }
	await finishDrains()
	await broker.stop(true)
	if (childExited) await rm(root, { recursive: true, force: true })
	if (stopError) throw stopError
}
