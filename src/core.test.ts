import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdirSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import * as path from "node:path"
import { AdapterSendError, assertWeixinSendSuccess, JsonRpcPendingMap, MockWeChatAdapter, WeixinMcpAdapter, type McpClient } from "./adapter"
import { BrokerService, clampCallbackTimeout } from "./broker"
import { ClientLifecycleRegistry, createCallbackHandler, extractPromptAssistant, resolveRootSession } from "./client"
import { CONTROL_OFF_TEXT, HELP_TEXT, MAX_CONTEXT_TOKEN_LENGTH, MAX_ROUTE_ID_LENGTH, PERMISSION_DENIED_TEXT, Store, formatOutbound, formatRegistrationList, parseInboundText, parsePollToolResult, sanitizeTitle } from "./core"
import { runWorker } from "./worker"
import { decideExistingBroker, pidStatus, readLock } from "./worker-runtime"
import { ControlCommandHandled, captureRequestInputCallID, createControlCommandHook, createControlEventHook, createPermissionHook, executeRequestInputTool, registerControlCommands, requestInputToolOutcome, resolveWeixinCommand } from "./plugin-runtime"

const tempRoot = path.join(tmpdir(), "ocx-wechat-control-tests")
mkdirSync(tempRoot, { recursive: true })
function tempFile(name: string): string { return path.join(tempRoot, `${name}-${crypto.randomUUID()}.sqlite`) }
function cleanup(...paths: Array<string | undefined>): void { for (const value of paths) if (value) { try { rmSync(value, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 }) } catch {}; try { rmSync(`${value}-wal`, { force: true }) } catch {}; try { rmSync(`${value}-shm`, { force: true }) } catch {} } }

test("strict parser, safe alias and AI reply prefix", () => {
	expect(parseInboundText("#12 \r\nhello")).toEqual({ ok: true, kind: "route", alias: 12, body: "hello" })
	expect(parseInboundText(" #1\nhello")).toEqual({ ok: false, reason: "invalid-route" })
	expect(parseInboundText(`#${"9".repeat(40)}\nhello`)).toEqual({ ok: false, reason: "invalid-route" })
	expect(parseInboundText(" help ")).toEqual({ ok: true, kind: "help" })
	expect(parseInboundText(" id ")).toEqual({ ok: true, kind: "list" }); expect(parseInboundText("id\nx")).toEqual({ ok: false, reason: "invalid-route" })
	expect(parseInboundText("x".repeat(4001))).toEqual({ ok: false, reason: "invalid-text" })
	expect(HELP_TEXT).toContain("#编号"); expect(formatOutbound(2, "reply")).toBe("#2\nreply")
})

test("root registrations allocate permanent aliases idempotently and sanitize latest title", async () => {
	const store = new Store(":memory:")
	const registrations = await Promise.all(["one", "two", "three"].map(async (root) => store.bind({ rootSessionId: root, directory: `d-${root}`, ownerInstance: "owner", title: `${root}\r\n\u0001title` })))
	expect(registrations.map((item) => item.alias)).toEqual([1, 2, 3])
	const repeated = store.bind({ rootSessionId: "two", directory: "new", ownerInstance: "owner", title: `  refreshed\n${"x".repeat(200)}  ` })
	expect(repeated.alias).toBe(2); expect(repeated.title).toBe(`refreshed ${"x".repeat(110)}`); expect(repeated.title?.length).toBe(120)
	expect(sanitizeTitle("\r\n\u0000  ")).toBeNull(); expect((store.db.query("SELECT seq FROM sqlite_sequence WHERE name='bindings'").get() as any).seq).toBe(3)
	store.close()
})

test("registration list is ordered, hides roots and remains within adapter limit", () => {
	const bindings = Array.from({ length: 80 }, (_, index) => ({ alias: 80 - index, rootSessionId: `secret-${index}`, directory: "d", ownerInstance: "o", title: "会".repeat(120) }))
	const text = formatRegistrationList(bindings); expect(text.length).toBeLessThanOrEqual(4000); expect(text).toContain("#1  "); expect(text).toContain("另有"); expect(text).not.toContain("secret-")
})

test("wechat id lists registrations on and off without callback and refreshes global route", async () => {
	let callbacks = 0
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => { callbacks++; return Response.json({}) })
	store.bind({ rootSessionId: "r1", directory: "d", ownerInstance: "owner", title: "First" }); store.bind({ rootSessionId: "r2", directory: "d", ownerInstance: "owner", title: null }); store.setControl(false)
	await broker.handleInbound({ id: "list-off", fromUserId: "recipient-a", contextToken: "ctx-a", text: "id", cursorHint: "a" })
	expect(adapter.sent.at(-1)).toMatchObject({ to: "recipient-a", contextToken: "ctx-a", text: "#1  First\n#2  未命名会话" }); expect(store.route()).toMatchObject({ conversationId: "recipient-a", contextToken: "ctx-a" })
	store.setControl(true); await broker.handleInbound({ id: "list-on", fromUserId: "recipient-a", contextToken: "ctx-b", text: " id ", cursorHint: "b" })
	expect(store.route()).toMatchObject({ conversationId: "recipient-a", contextToken: "ctx-b" }); expect(callbacks).toBe(0); store.close()
})

test("invalid and echoed inbound do not replace the global route", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({}))
	store.refreshRoute("safe", "safe-ctx"); await broker.handleInbound({ id: "invalid-route-refresh", fromUserId: "bad", contextToken: "bad-ctx", text: "not valid", cursorHint: "a" }); expect(store.route()).toMatchObject({ conversationId: "safe", contextToken: "safe-ctx" })
	store.recordEcho("echo", "echo-ctx", "help"); await broker.handleInbound({ id: "echo-route-refresh", fromUserId: "echo", contextToken: "echo-ctx", text: "help", cursorHint: "b" }); expect(store.route()).toMatchObject({ conversationId: "safe", contextToken: "safe-ctx" }); store.close()
})

test("outbound claims fail closed until the global route is ready", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" })
	expect(store.claimControlOutbound({ dedupeKey: "missing", root: "root", kind: "test", payload: "#1\nx" })).toBeUndefined(); store.refreshRoute("recipient", "context"); expect(store.claimControlOutbound({ dedupeKey: "ready", root: "root", kind: "test", payload: "#1\nx" })?.binding).toMatchObject({ conversationId: "recipient", contextToken: "context" }); store.close()
})

test("initial route claim is id-only and atomic", async () => {
	const store = new Store(":memory:")
	expect(store.acceptInboundRoute("help-sender", "h", false)).toBe("REJECTED"); expect(store.route().conversationId).toBeNull()
	const results = await Promise.all([Promise.resolve().then(() => store.acceptInboundRoute("first", "a", true)), Promise.resolve().then(() => store.acceptInboundRoute("second", "b", true))])
	expect(results.filter((value) => value === "CLAIMED")).toHaveLength(1); expect(results.filter((value) => value === "REJECTED")).toHaveLength(1)
	const winner = store.route().conversationId!; expect(store.acceptInboundRoute(winner, "new-context", false)).toBe("REFRESHED"); expect(store.route().contextToken).toBe("new-context"); expect(store.acceptInboundRoute(winner === "first" ? "second" : "first", "bad", true)).toBe("REJECTED"); store.close()
})

test("help and routed text cannot make the initial claim or receive a reply", async () => {
	let callbacks = 0; const store = new Store(":memory:"), adapter = new CountingAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => { callbacks++; return Response.json({}) })
	for (const [id, text] of [["pre-help", "help"], ["pre-route", "#1\nx"]]) expect((await broker.handleInbound({ id, fromUserId: "candidate", contextToken: "ctx", text, cursorHint: id })).reason).toBe("route-rejected")
	expect(store.route().conversationId).toBeNull(); expect(adapter.attempts).toBe(0); expect(callbacks).toBe(0); store.close()
})

test("malformed route metadata has no durable side effect and cannot block a later id claim", async () => {
	const store = new Store(":memory:"), adapter = new CountingAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({}))
	const malformed = [
		{ id: "empty-sender", fromUserId: "", contextToken: "ctx" },
		{ id: "empty-context", fromUserId: "sender", contextToken: "" },
		{ id: "long-sender", fromUserId: "s".repeat(MAX_ROUTE_ID_LENGTH + 1), contextToken: "ctx" },
		{ id: "long-context", fromUserId: "sender", contextToken: "c".repeat(MAX_CONTEXT_TOKEN_LENGTH + 1) },
	]
	for (const item of malformed) expect((await broker.handleInbound({ ...item, text: "id", cursorHint: item.id })).reason).toBe("invalid-route-metadata")
	expect((store.db.query("SELECT COUNT(*) AS count FROM inbound").get() as any).count).toBe(0); expect(store.route().conversationId).toBeNull(); expect(adapter.attempts).toBe(0)
	expect(store.acceptInboundRoute("", "ctx", true)).toBe("REJECTED"); expect(store.acceptInboundRoute("sender", "", true)).toBe("REJECTED")
	expect((await broker.handleInbound({ id: "valid-after-malformed", fromUserId: "controller", contextToken: "valid-context", text: "id", cursorHint: "valid" })).ok).toBe(true); expect(store.route()).toMatchObject({ conversationId: "controller", contextToken: "valid-context" }); expect(adapter.attempts).toBe(1); store.close()
})

test("poll key includes cursor and message index", () => {
	const msg = { message_type: 1, from_user_id: "user", context_token: "ctx", item_list: [{ type: 1, text_item: { text: "same" } }] }
	const result = { content: [{ type: "text", text: JSON.stringify({ get_updates_buf: "cursor", msgs: [msg, msg] }) }] }
	const parsed = parsePollToolResult(result); expect(parsed).toHaveLength(2); expect(parsed[0].id).not.toBe(parsed[1].id); expect(parsePollToolResult(result)[0].id).toBe(parsed[0].id)
})

class FakeMcp implements McpClient {
	readonly calls: string[] = []
	closed = false
	constructor(private readonly pollFailure = false) {}
	async request(method: string, params?: any): Promise<any> {
		this.calls.push(`${method}:${params?.name ?? ""}`)
		if (method === "initialize") return {}
		if (method === "tools/list") return { tools: [{ name: "weixin_poll" }, { name: "weixin_send" }] }
		if (params?.name === "weixin_poll") { if (this.pollFailure) throw new Error("exited"); return new Promise(() => {}) }
		if (params?.name === "weixin_send") return { content: [{ type: "text", text: JSON.stringify({ ret: 0 }) }] }
		throw new Error("unexpected call")
	}
	async notify(method: string): Promise<void> { this.calls.push(method) }
	close(): void { this.closed = true }
}

test("MCP handshake, degraded exit, fail-closed send and fast JSON-RPC response", async () => {
	const fake = new FakeMcp(), adapter = new WeixinMcpAdapter({ enabled: true, command: ["node", "fixed.js"], clientFactory: () => fake, retry: false })
	await adapter.start(async () => {}); expect(adapter.status()).toBe("Ready"); await adapter.send("u", "#1\nok", "ctx")
	expect(fake.calls.slice(0, 4)).toEqual(["initialize:", "notifications/initialized", "tools/list:", "tools/call:weixin_poll"])
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: "{}" }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ ret: 0 }) }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ errcode: 0 }) }] })).not.toThrow(); adapter.stop()
	const exited = new WeixinMcpAdapter({ enabled: true, command: ["node", "fixed.js"], clientFactory: () => new FakeMcp(true), retry: false }); await exited.start(async () => {}); await Bun.sleep(5); expect(exited.status()).toBe("Degraded")
	const map = new JsonRpcPendingMap(); expect(await map.request((message: any) => map.accept({ jsonrpc: "2.0", id: message.id, result: "fast" }), "fast", {})).toBe("fast")
})

test("weixin_send parser rejects MCP, business, malformed, ambiguous and unknown failures", () => {
	const failures: Array<[unknown, string]> = [
		[null, "malformed-result"],
		[{ isError: true, content: [{ type: "text", text: "{}" }] }, "mcp-error"],
		[{ content: [{ type: "text", text: "not-json" }] }, "malformed-result"],
		[{ content: [{ type: "text", text: "[]" }] }, "malformed-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 1 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ errcode: 400 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errcode: 1 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errmsg: "failed" }) }] }, "ambiguous-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errmsg: "" }) }] }, "ambiguous-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ok: true }) }] }, "unknown-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ message: "sent" }) }] }, "unknown-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: "0" }) }] }, "explicit-business-failure"],
	]
	for (const [value, classification] of failures) {
		try { assertWeixinSendSuccess(value); throw new Error("expected parser failure") }
		catch (error) { expect(error).toBeInstanceOf(AdapterSendError); expect((error as AdapterSendError).classification).toBe(classification) }
	}
})

function fakeClient(prompt: () => Promise<any>) {
	let prompts = 0
	let promptOptions: any
	return { get prompts() { return prompts }, get promptOptions() { return promptOptions }, session: { get: async () => ({ data: { id: "root" } }), prompt: async (options: any) => { prompts++; promptOptions = options; return prompt() } } }
}
function injectRequest(method = "POST", envelope: any = { kind: "inbound" }): Request { return new Request("http://127.0.0.1/inject", { method, headers: { "x-wechat-control-key": "secret", "x-wechat-instance-token": "token", "content-type": "application/json" }, body: method === "POST" ? JSON.stringify({ rootSessionId: "root", directory: "d", text: "hello", inboundId: "in", envelope }) : undefined }) }

test("callback waits for one synchronous prompt and returns only its direct assistant", async () => {
	const client = fakeClient(async () => { await Bun.sleep(10); return { data: { info: { id: "assistant", parentID: "msg_user_generated", role: "assistant" }, parts: [{ type: "reasoning", text: "hidden" }, { type: "text", text: "final" }] } } })
	const response = await createCallbackHandler(client, "secret", "token")(injectRequest()); expect(response.status).toBe(200)
	const body = await response.json() as any; expect(body.promptMessageId).toBe("msg_user_generated"); expect(body.assistantMessageId).toBe("assistant"); expect(body.text).toBe("final"); expect(client.prompts).toBe(1)
	expect(Object.hasOwn(client.promptOptions.body, "messageID")).toBe(false)
	expect(extractPromptAssistant({ data: { info: { id: "a", parentID: "msg_parent", role: "assistant" }, parts: [{ type: "text", text: "x" }] } })).toEqual({ promptMessageId: "msg_parent", assistantMessageId: "a", text: "x" })
})

test("callback no-text, thrown prompt, invalid shape and GET all fail without reinjection", async () => {
	for (const result of [
		async () => ({ data: { info: { id: "a", parentID: "msg_parent", role: "assistant" }, parts: [{ type: "tool" }] } }),
		async () => { throw new Error("prompt failed") },
		async () => ({ data: { info: { id: "u", parentID: "msg_parent", role: "user" }, parts: [{ type: "text", text: "bad" }] } }),
		async () => ({ data: { info: { id: "a", role: "assistant" }, parts: [{ type: "text", text: "missing parent" }] } }),
		async () => ({ data: { info: { id: "a", parentID: "not-a-message-id", role: "assistant" }, parts: [{ type: "text", text: "bad parent" }] } }),
	]) {
		const client = fakeClient(result); const response = await createCallbackHandler(client, "secret", "token")(injectRequest()); expect(response.status).toBe(409); expect(client.prompts).toBe(1)
	}
	const getClient = fakeClient(async () => { throw new Error("must not run") }); expect((await createCallbackHandler(getClient, "secret", "token")(injectRequest("GET"))).status).toBe(405); expect(getClient.prompts).toBe(0)
})

function readyBroker(fetcher: typeof fetch) {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter()
	store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true)
	return { store, adapter, broker: new BrokerService(store, adapter, "secret", "worker", fetcher) }
}

test("one inbound calls callback once, sends direct final once, and deduplicates", async () => {
	let callbacks = 0
	const { store, adapter, broker } = readyBroker(async () => { callbacks++; return Response.json({ promptMessageId: "prompt", assistantMessageId: "assistant", text: "final" }) })
	const message = { id: "in", fromUserId: "user", contextToken: "ctx", text: "#1\nhello", cursorHint: "c" }
	expect((await broker.handleInbound(message)).ok).toBe(true); expect((await broker.handleInbound(message)).reason).toBe("duplicate-at-least-once-key")
	expect(callbacks).toBe(1); expect(adapter.sent).toEqual([{ to: "user", text: "#1\nfinal", contextToken: "ctx" }]); expect(store.pendingState("in")).toBe("SENT"); store.close()
})

test("callback timeout and invalid direct result become UNKNOWN without replay", async () => {
	for (const fetcher of [
		async () => { throw new DOMException("timed out", "TimeoutError") },
		async () => Response.json({ promptMessageId: "p", assistantMessageId: "a" }),
	]) {
		const { store, adapter, broker } = readyBroker(fetcher as typeof fetch); const result = await broker.handleInbound({ id: crypto.randomUUID(), fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" })
		expect(result.reason).toBe("callback-failed-or-timeout"); expect(adapter.sent).toHaveLength(0); expect(store.state((store.db.query("SELECT message_id AS id FROM inbound").get() as any).id)).toBe("UNKNOWN"); store.close()
	}
	expect(clampCallbackTimeout(1)).toBe(30_000); expect(clampCallbackTimeout(99_999_999)).toBe(600_000)
})

test("duplicate completion cannot send twice and orphan WAITING sweeps to UNKNOWN", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); const revision = store.setControl(true).revision
	store.beginInbound({ id: "complete", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); store.beginPending("complete", "root", 1, revision)
	expect(store.completePendingAndClaim("owner", "root", "complete", "p", "a", "final", revision)).toBeDefined(); expect(store.completePendingAndClaim("owner", "root", "complete", "p", "a", "final", revision)).toBeUndefined()
	store.beginInbound({ id: "orphan", fromUserId: "user", contextToken: "ctx", text: "#1\ny", cursorHint: "d" }); store.beginPending("orphan", "root", 1, revision); store.db.query("UPDATE pending_replies SET updated_at='2000-01-01T00:00:00.000Z' WHERE inbound_id='orphan'").run()
	expect(store.sweepOrphanWaiting(Date.now(), 1000)).toBe(1); expect(store.pendingState("orphan")).toBe("UNKNOWN"); expect(store.state("orphan")).toBe("UNKNOWN"); store.close()
})

test("broker rejects deprecated binding and requires Ready adapter for inbound", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({ ok: true }))
	const get = new Request("http://127.0.0.1", { method: "GET", headers: { "x-wechat-control-key": "secret" } }); expect((await broker.handleRequest(get)).status).toBe(405); expect(store.instance("new")).toBeUndefined()
	store.register("owner", "token", "http://127.0.0.1:1"); adapter.statusValue = "Degraded"
	const request = () => new Request("http://127.0.0.1", { method: "POST", headers: { "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "bind-current", instanceId: "owner", instanceToken: "token", rootSessionId: "root", directory: "d", conversationId: "user" }) })
	expect((await broker.handleRequest(request())).status).toBe(410); expect((await broker.handleInbound({ id: "x", fromUserId: "user", contextToken: "c", text: "#1\nx", cursorHint: "c" })).reason).toBe("adapter-not-ready")
	const health = await broker.handleRequest(new Request("http://127.0.0.1", { method: "POST", headers: { "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "health", challenge: "worker" }) })); expect((await health.json() as any).adapter).toBe("Degraded")
	adapter.statusValue = "Ready"; expect((await broker.handleRequest(request())).status).toBe(410); store.close()
})

function createWalV2(filename: string): Database {
	const db = new Database(filename); db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=2; CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE bindings(alias INTEGER PRIMARY KEY,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL UNIQUE,context_token TEXT,created_at TEXT NOT NULL); CREATE TABLE inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL); CREATE TABLE outbound(message_id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,payload TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,updated_at TEXT NOT NULL); CREATE TABLE audit(id INTEGER PRIMARY KEY,at TEXT NOT NULL,reason TEXT NOT NULL);")
	db.exec("INSERT INTO meta VALUES('custom','wal-data'); INSERT INTO bindings VALUES(7,'root-wal','d','owner','conversation-wal','ctx','now'); INSERT INTO inbound VALUES('in-wal','user','ctx','x','RECEIVED',NULL,NULL,NULL,'now'); INSERT INTO outbound VALUES('out-wal','legacy-in','PENDING','#7\\nold','now'); INSERT INTO pending_replies VALUES('pending-wal','root-wal','prompt-wal',7,'WAITING',NULL,NULL,NULL,'now');")
	return db
}

test("WAL-consistent legacy-to-v5 snapshot preserves registrations and sequence", () => {
	const filename = tempFile("wal-v2"), writer = createWalV2(filename), store = new Store(filename); const backup = store.migrationBackupPath
	expect(backup).toBeDefined(); expect(backup).toContain("pre-v5"); expect(store.bindingForAlias(7)?.rootSessionId).toBe("root-wal"); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(5); expect(store.control()).toEqual({ enabled: false, revision: 0 }); expect(store.route()).toMatchObject({ conversationId: "conversation-wal", contextToken: null })
	expect((store.db.query("PRAGMA table_info(session_activity)").all() as any[]).map((row) => row.name)).toContain("epoch"); expect((store.db.query("PRAGMA table_info(checkpoints)").all() as any[]).map((row) => row.name)).toContain("request_key"); expect((store.db.query("PRAGMA table_info(outbound_echoes)").all() as any[]).map((row) => row.name)).toContain("expires_ms")
	const snapshot = new Database(backup!); expect((snapshot.query("SELECT value FROM meta WHERE key='custom'").get() as any).value).toBe("wal-data"); expect((snapshot.query("SELECT conversation_id AS value FROM bindings WHERE alias=7").get() as any).value).toBe("conversation-wal"); expect(snapshot.query("SELECT 1 FROM pending_replies WHERE inbound_id='pending-wal'").get()).toBeDefined(); snapshot.close()
	expect(store.bind({ rootSessionId: "root-two", directory: "d", ownerInstance: "owner" }).alias).toBe(8)
	store.close(); writer.close(); const reopened = new Store(filename); expect(reopened.migrationBackupPath).toBeUndefined(); expect(reopened.bindingForAlias(7)?.title).toBeNull(); reopened.close(); cleanup(filename, backup)
})

function createWalV3(filename: string): Database {
	const db = new Database(filename); db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=3; CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE bindings(alias INTEGER PRIMARY KEY,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT,created_at TEXT NOT NULL); CREATE TABLE inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL); CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,updated_at TEXT NOT NULL); CREATE TABLE outbound(message_id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,payload TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE control_state(singleton INTEGER PRIMARY KEY,enabled INTEGER NOT NULL,revision INTEGER NOT NULL); CREATE TABLE checkpoints(checkpoint_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,alias INTEGER NOT NULL,question TEXT NOT NULL,choices_json TEXT NOT NULL,state TEXT NOT NULL,inbound_id TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE session_activity(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,running INTEGER NOT NULL,idle INTEGER NOT NULL,last_assistant_id TEXT,last_assistant_error INTEGER NOT NULL,direct_assistant_id TEXT,updated_at TEXT NOT NULL); CREATE TABLE control_outbound(outbound_id TEXT PRIMARY KEY,dedupe_key TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE outbound_echoes(echo_hash TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL); CREATE TABLE audit(id INTEGER PRIMARY KEY,at TEXT NOT NULL,reason TEXT NOT NULL);")
	db.exec("INSERT INTO meta VALUES('schema_version','3'); INSERT INTO bindings VALUES(4,'root-v3','d','old-owner','conversation-v3','ctx-v3','now'); INSERT INTO control_state VALUES(1,1,17); INSERT INTO inbound VALUES('in-v3','conversation-v3','ctx-v3','x','INJECTED','root-v3','prompt-v3',NULL,'now'); INSERT INTO pending_replies VALUES('in-v3','root-v3','prompt-v3',4,'UNKNOWN','assistant-v3','#4\\nx',1,'now'); INSERT INTO outbound VALUES('assistant-v3','in-v3','UNKNOWN','#4\\nx','now'); INSERT INTO checkpoints VALUES('cp-v3','root-v3','old-owner','conversation-v3',4,'q','[]','UNKNOWN','in-v3','now','now');")
	return db
}

function createWalV4(filename: string): Database {
	const db = new Database(filename)
	db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=4; CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE bindings(alias INTEGER PRIMARY KEY,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT,created_at TEXT NOT NULL); INSERT INTO meta VALUES('schema_version','4'); INSERT INTO bindings VALUES(12,'root-v4','dir-v4','owner-v4','recipient-v4','old-context','created');")
	return db
}

test("v4-to-v5 migration snapshots once and preserves alias sequence", () => {
	const filename = tempFile("wal-v4"), writer = createWalV4(filename), store = new Store(filename), backup = store.migrationBackupPath
	expect(backup).toContain("pre-v5"); expect(store.bindingForAlias(12)).toMatchObject({ rootSessionId: "root-v4", directory: "dir-v4", ownerInstance: "owner-v4", title: null }); expect(store.route()).toMatchObject({ conversationId: "recipient-v4", contextToken: null }); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(5)
	expect(store.bind({ rootSessionId: "next-v5", directory: "d", ownerInstance: "o" }).alias).toBe(13); store.close(); writer.close()
	const reopened = new Store(filename); expect(reopened.migrationBackupPath).toBeUndefined(); expect((reopened.db.query("SELECT value FROM meta WHERE key='schema_version'").get() as any).value).toBe("5"); reopened.close(); cleanup(filename, backup)
})

test("v4 multiple conversations migrate fail-closed and alias holes advance from max", () => {
	const filename = tempFile("wal-v4-multiple"), writer = createWalV4(filename)
	writer.exec("DELETE FROM bindings; INSERT INTO bindings VALUES(2,'root-a','a','owner-a','recipient-a','ctx-a','created'); INSERT INTO bindings VALUES(9,'root-b','b','owner-b','recipient-b','ctx-b','created'); CREATE TABLE control_state(singleton INTEGER PRIMARY KEY,enabled INTEGER NOT NULL,revision INTEGER NOT NULL); INSERT INTO control_state VALUES(1,1,7); CREATE TABLE checkpoints(checkpoint_id TEXT PRIMARY KEY,request_key TEXT,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,alias INTEGER NOT NULL,question TEXT NOT NULL,choices_json TEXT NOT NULL,state TEXT NOT NULL,inbound_id TEXT,control_revision INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); INSERT INTO checkpoints VALUES('multi-cp','key','root-a','owner-a','recipient-a',2,'q','[]','UNKNOWN',NULL,7,'created','created'); CREATE TABLE session_activity(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,running INTEGER NOT NULL DEFAULT 0,idle INTEGER NOT NULL DEFAULT 1,last_assistant_id TEXT,last_assistant_error INTEGER NOT NULL DEFAULT 0,direct_assistant_id TEXT,epoch INTEGER NOT NULL DEFAULT 0,run_id INTEGER NOT NULL DEFAULT 0,origin TEXT NOT NULL DEFAULT 'NONE',candidate_run INTEGER,claimed_run INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL); INSERT INTO session_activity VALUES('root-a','owner-a',1,0,NULL,0,NULL,7,1,'LOCAL',1,0,'created'); CREATE TABLE inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL); INSERT INTO inbound VALUES('multi-wait','recipient-a','ctx-a','#2 x','INJECTING','root-a',NULL,NULL,'created'); CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,control_revision INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL); INSERT INTO pending_replies VALUES('multi-wait','root-a',NULL,2,'WAITING',NULL,NULL,NULL,7,'created');")
	const store = new Store(filename), backup = store.migrationBackupPath; expect(store.route()).toMatchObject({ conversationId: null, contextToken: null }); expect(store.control().enabled).toBe(false); expect(store.checkpointState("multi-cp")).toBe("CANCELLED"); expect(store.pendingState("multi-wait")).toBe("UNKNOWN"); expect(store.state("multi-wait")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='multi-wait'").get() as any).reason).toBe("migration-multiple-global-routes"); expect((store.db.query("SELECT running,origin FROM session_activity WHERE root_session_id='root-a'").get() as any)).toMatchObject({ running: 0, origin: "NONE" }); expect((store.db.query("SELECT reason FROM audit WHERE reason='migration-multiple-global-routes'").get() as any).reason).toBe("migration-multiple-global-routes"); expect(store.bind({ rootSessionId: "root-c", directory: "c", ownerInstance: "owner-c" }).alias).toBe(10)
	store.close(); writer.close(); cleanup(filename, backup)
})

test("old deployed v3 receives a pre-v5 snapshot and preserves control/pending/checkpoint data", () => {
	const filename = tempFile("wal-v3"), writer = createWalV3(filename), store = new Store(filename), backup = store.migrationBackupPath
	expect(backup).toContain("pre-v5"); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(5); expect(store.bindingForAlias(4)?.rootSessionId).toBe("root-v3"); expect(store.control()).toEqual({ enabled: true, revision: 17 }); expect(store.pendingState("in-v3")).toBe("UNKNOWN"); expect(store.checkpointState("cp-v3")).toBe("UNKNOWN"); expect(store.checkpointForRequest("cp-v3", "root-v3")?.checkpointId).toBe("cp-v3")
	const snapshot = new Database(backup!); expect((snapshot.query("PRAGMA user_version").get() as any).user_version).toBe(3); expect((snapshot.query("SELECT revision FROM control_state").get() as any).revision).toBe(17); expect((snapshot.query("SELECT state FROM pending_replies WHERE inbound_id='in-v3'").get() as any).state).toBe("UNKNOWN"); expect((snapshot.query("SELECT state FROM checkpoints WHERE checkpoint_id='cp-v3'").get() as any).state).toBe("UNKNOWN"); snapshot.close()
	store.close(); writer.close(); const reopened = new Store(filename); expect(reopened.migrationBackupPath).toBeUndefined(); expect(reopened.control().revision).toBe(17); reopened.close(); cleanup(filename, backup)
})

test("forced migration failure reports an openable WAL-consistent backup", () => {
	const filename = tempFile("wal-fail"), writer = createWalV2(filename); let backup: string | undefined
	expect(() => new Store(filename, { onSnapshot: (value) => { backup = value }, migrationFault: () => { throw new Error("forced") } })).toThrow(/consistent backup=/)
	const snapshot = new Database(backup!); expect((snapshot.query("SELECT root_session_id AS root FROM bindings WHERE alias=7").get() as any).root).toBe("root-wal"); expect(snapshot.query("SELECT 1 FROM inbound WHERE message_id='in-wal'").get()).toBeDefined(); snapshot.close(); writer.close(); cleanup(filename, backup)
})

test("worker releases its lock when Store construction fails", async () => {
	let released = 0
	await expect(runWorker({ enabled: true, weixinCommand: ["node", "fixed.js"] }, {
		initializeState: async () => ({ directory: tempRoot, secret: "secret" }),
		acquireLock: async () => ({ update: async () => {}, release: async () => { released++ } }),
		createStore: () => { throw new Error("migration failed") },
	})).rejects.toThrow("migration failed")
	expect(released).toBe(1)
})

test("legacy lock policy and client lifecycle remain controlled", async () => {
	expect(decideExistingBroker(true, false)).toBe("refuse"); expect(decideExistingBroker(false, false)).toBe("takeover"); expect(decideExistingBroker("unknown", false)).toBe("refuse")
	expect(pidStatus(10, () => {})).toBe("alive"); expect(pidStatus(10, () => { throw Object.assign(new Error(), { code: "ESRCH" }) })).toBe("dead")
	const directory = path.join(tempRoot, `wechat-lock-${crypto.randomUUID()}`), lockDirectory = path.join(directory, "broker.lock"); mkdirSync(lockDirectory, { recursive: true }); writeFileSync(path.join(lockDirectory, "owner.json"), JSON.stringify({ pid: 99, instanceToken: "legacy", endpoint: "http://127.0.0.1:9" })); expect((await readLock(directory))?.format).toBe("v1"); cleanup(directory)
	const registry = new ClientLifecycleRegistry(); let stopped = 0; await registry.replace("d", { stop: async () => { stopped++ } }); await registry.replace("d", { stop: async () => { stopped++ } }); expect(stopped).toBe(1); await registry.stopAll(); expect(stopped).toBe(2)
})

function authenticatedRequest(method: string, extra: object = {}): Request { return new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner", instanceToken: "token", ...extra }) }) }

test("leave-root RPC verifies exact root and allocates or refreshes registrations", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), checked: string[] = []
	adapter.statusValue = "Degraded"
	store.register("owner", "token", "http://127.0.0.1:1")
	const broker = new BrokerService(store, adapter, "secret", "worker", async (_url, init) => { const body = JSON.parse(String(init?.body)); checked.push(body.rootSessionId); return body.rootSessionId === "child" ? Response.json({ error: "not-root" }, { status: 409 }) : Response.json({ ok: true }) })
	const leave = async (rootSessionId: string, title: string) => broker.handleRequest(authenticatedRequest("leave-root", { rootSessionId, directory: "d", title }))
	const aliases: number[] = []; for (const root of ["r1", "r2", "r3"]) aliases.push(((await (await leave(root, root)).json()) as any).binding.alias)
	expect(aliases).toEqual([1, 2, 3]); expect(((await (await leave("r2", "new-title")).json()) as any).binding).toMatchObject({ alias: 2, title: "new-title" }); expect(store.control().enabled).toBe(true)
	expect((await leave("child", "bad")).status).toBe(409); expect(store.bindingForRoot("child")).toBeUndefined(); expect(checked).toEqual(["r1", "r2", "r3", "r2", "child"]); store.close()
})

test("live owner blocks rebind; stale exact-root rebind clears checkpoint and activity", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(); store.register("old", "old-token", "http://127.0.0.1:1"); store.register("next", "next-token", "http://127.0.0.1:2")
	const broker = new BrokerService(store, adapter, "secret", "worker", async (_url, init) => { const body = JSON.parse(String(init?.body)); return body.rootSessionId === "root" ? Response.json({ ok: true }) : Response.json({ error: "not-root" }, { status: 409 }) })
	const request = (instanceId: string, instanceToken: string, rootSessionId = "root") => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "leave-root", instanceId, instanceToken, rootSessionId, directory: "d", title: instanceId }) })
	expect((await broker.handleRequest(request("old", "old-token"))).status).toBe(200); store.refreshRoute("controller", "ctx"); const revision = store.control().revision
	expect(store.openCheckpoint({ checkpointId: "rebind-cp", requestKey: "rebind-key", root: "root", owner: "old", alias: 1, question: "q", choices: [], revision })).toBe(true); store.observeStatus("root", "old", "busy"); store.beginInbound({ id: "old-flight", fromUserId: "controller", contextToken: "ctx", text: "#1\nx", cursorHint: "x" }); store.beginPending("old-flight", "root", 1, revision)
	const live = await broker.handleRequest(request("next", "next-token")); expect(live.status).toBe(409); expect(await live.json()).toMatchObject({ error: "owner-live" }); expect(store.bindingForRoot("root")?.ownerInstance).toBe("old"); expect(store.pendingState("old-flight")).toBe("WAITING")
	store.unregister("old", "old-token"); expect((await broker.handleRequest(request("next", "next-token"))).status).toBe(200); expect(store.bindingForRoot("root")?.ownerInstance).toBe("next"); expect(store.checkpointState("rebind-cp")).toBe("CANCELLED"); expect((store.db.query("SELECT running,origin,owner_instance AS owner FROM session_activity WHERE root_session_id='root'").get() as any)).toMatchObject({ running: 0, origin: "NONE", owner: "next" }); expect(store.pendingState("old-flight")).toBe("UNKNOWN"); expect(store.state("old-flight")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='old-flight'").get() as any).reason).toBe("owner-rebound")
	expect(store.completePendingAndClaim("old", "root", "old-flight", "p", "a", "late", store.control().revision)).toBeUndefined(); store.close()
})

test("callback health and injection reject mismatched returned session IDs", async () => {
	const client = { session: { get: async () => ({ data: { id: "different" } }), prompt: async () => { throw new Error("must not prompt") } } }, handler = createCallbackHandler(client, "secret", "token")
	const health = new Request("http://127.0.0.1/health", { method: "POST", headers: { "x-wechat-control-key": "secret", "x-wechat-instance-token": "token", "content-type": "application/json" }, body: JSON.stringify({ rootSessionId: "root" }) }); expect((await handler(health)).status).toBe(409)
	expect((await handler(injectRequest())).status).toBe(409)
})

test("global control revision, back cancellation and v3 crash recovery are durable", () => {
	const filename = tempFile("v3-recovery"), store = new Store(filename)
	expect(store.control()).toEqual({ enabled: false, revision: 0 }); expect(store.setControl(true)).toEqual({ enabled: true, revision: 1 })
	store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx")
	expect(store.openCheckpoint({ checkpointId: "cp", requestKey: "call", root: "root", owner: "owner", alias: 1, question: "q", choices: [], revision: 1 })).toBe(true)
	const claim = store.claimControlOutbound({ dedupeKey: "d", root: "root", kind: "completion", payload: "#1\nx" }); expect(claim).toBeDefined()
	store.observeStatus("root", "owner", "busy"); const before = store.bindingForRoot("root"), route = store.route()
	store.beginInbound({ id: "cancel-now", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); store.beginPending("cancel-now", "root", 1, store.control().revision)
	expect(store.setControl(false)).toEqual({ enabled: false, revision: 2 }); expect(store.checkpointState("cp")).toBe("CANCELLED"); expect(store.pendingState("cancel-now")).toBe("UNKNOWN"); expect(store.state("cancel-now")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='cancel-now'").get() as any).reason).toBe("control-cancelled"); expect(store.bindingForRoot("root")).toEqual(before); expect(store.route()).toEqual(route); expect((store.db.query("SELECT running,origin FROM session_activity WHERE root_session_id='root'").get() as any)).toMatchObject({ running: 0, origin: "NONE" }); expect(store.bind({ rootSessionId: "root", directory: "d2", ownerInstance: "owner", title: "again" }).alias).toBe(1); store.close()
	const reopened = new Store(filename); expect((reopened.db.query("SELECT state FROM control_outbound WHERE dedupe_key='d'").get() as any).state).toBe("UNKNOWN"); reopened.close(); cleanup(filename)
})

test("checkpoint is asynchronous, answer targets exact binding, and failures never replay", async () => {
	let envelope: any
	const { store, adapter, broker } = readyBroker(async (_url, init) => { envelope = JSON.parse(String(init?.body)).envelope; return Response.json({ promptMessageId: "prompt", assistantMessageId: "direct", text: "accepted" }) })
	store.refreshRoute("user", "ctx")
	const response = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "call-checkpoint", question: "Choose", choices: ["A", "B"] })); expect(response.status).toBe(200)
	const checkpointId = (await response.json() as any).checkpointId; expect(store.checkpointState(checkpointId)).toBe("OPEN"); expect(adapter.sent[0].to).toBe("user"); expect(adapter.sent[0].text).toContain("1. A")
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "tool-turn", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); expect(adapter.sent).toHaveLength(1)
	const result = await broker.handleInbound({ id: "answer", fromUserId: "user", contextToken: "ctx2", text: "#1\nB", cursorHint: "c" })
	expect(result.ok).toBe(true); expect(envelope).toEqual({ kind: "checkpoint", checkpointId }); expect(store.checkpointState(checkpointId)).toBe("ANSWERED"); expect(adapter.sent.at(-1)?.text).toBe("#1\naccepted")
	await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "direct", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(0); expect((store.db.query("SELECT running FROM session_activity").get() as any).running).toBe(0)
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "local-after-checkpoint", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(1)
	expect((await broker.handleInbound({ id: "answer", fromUserId: "user", contextToken: "ctx2", text: "#1\nB", cursorHint: "c" })).reason).toBe("duplicate-at-least-once-key"); store.close()
})

test("back refuses inbound/checkpoints/permission/completion while help remains available", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ promptMessageId: "p", assistantMessageId: "a", text: "x" })); store.refreshRoute("user", "ctx"); store.setControl(false)
	const checkpoint = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "off-call", question: "q", choices: [] })); expect(checkpoint.status).toBe(409)
	const denied = await broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "perm" })); expect((await denied.json() as any).handled).toBe(false)
	expect((await broker.handleInbound({ id: "off", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" })).reason).toBe("control-disabled"); expect(adapter.sent.at(-1)?.text).toBe(CONTROL_OFF_TEXT)
	expect((await broker.handleInbound({ id: "help", fromUserId: "user", contextToken: "ctx", text: "help", cursorHint: "d" })).ok).toBe(true); expect(adapter.sent.at(-1)?.text).toBe(HELP_TEXT)
	expect(store.db.query("SELECT kind,state FROM control_outbound WHERE kind IN ('control-off','help') ORDER BY kind").all()).toEqual([{ kind: "control-off", state: "SENT" }, { kind: "help", state: "SENT" }])
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "a", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); expect(adapter.sent).toHaveLength(2); store.close()
})

test("permission denial and terminal completion use fixed durable dedupe messages", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({})); store.refreshRoute("user", "ctx")
	const permission = await broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "perm" })); expect((await permission.json() as any).handled).toBe(true); await broker.drainBackground(); expect(adapter.sent[0].text).toBe(`#1\n${PERMISSION_DENIED_TEXT}`)
	await broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "perm" })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(1)
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "terminal", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" }))
	await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(1); expect((store.db.query("SELECT state FROM control_outbound WHERE kind='completion'").get() as any).state).toBe("SENT"); store.close()
})

test("exact outbound echoes are suppressed before routing", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ promptMessageId: "p", assistantMessageId: "direct", text: "reply" }))
	await broker.handleInbound({ id: "one", fromUserId: "user", contextToken: "ctx", text: "#1\nhello", cursorHint: "a" }); const payload = adapter.sent[0].text
	const echoed = await broker.handleInbound({ id: "echo", fromUserId: "user", contextToken: "ctx", text: payload, cursorHint: "b" }); expect(echoed.reason).toBe("outbound-echo"); expect(adapter.sent).toHaveLength(1); expect(store.state("echo")).toBe("UNKNOWN"); store.close()
})

test("commands are conflict-safe, clear sentinels, reject arguments and always throw handled", async () => {
	const config: any = {}; registerControlCommands(config); expect(config.command.leave.template).toContain("LEAVE_HANDLED"); expect(() => registerControlCommands(config)).not.toThrow(); expect(() => registerControlCommands({ command: { back: {} } })).toThrow("command conflict")
	const calls: any[] = [], toasts: boolean[] = []; const rpcCall: any = async (_e: string, _s: string, body: any) => { calls.push(body); return { ok: true, binding: { alias: 1 } } }
	const hook = createControlCommandHook(rpcCall, async (enabled) => { toasts.push(enabled) }, "e", "s", "i", "t", async (id) => ({ data: { id, title: "Title" } }), "d"), output = { parts: [{ type: "text", text: "sentinel" }] }
	await expect(hook({ command: "leave", arguments: "", sessionID: "root" }, output)).rejects.toBeInstanceOf(ControlCommandHandled); expect(output.parts).toHaveLength(0); expect(calls[0]).toMatchObject({ method: "leave-root", rootSessionId: "root", title: "Title" }); expect(toasts).toEqual([true])
	await expect(hook({ command: "back", arguments: "bad" }, { parts: [1] })).rejects.toBeInstanceOf(ControlCommandHandled); expect(calls).toHaveLength(1)
	const failed = createControlCommandHook(async () => { throw new Error("rpc") }, async () => {}, "e", "s", "i", "t"); await expect(failed({ command: "back", arguments: "" }, { parts: [1] })).rejects.toBeInstanceOf(ControlCommandHandled)
})

test("leave rejects child sessions before RPC", async () => {
	let calls = 0
	const hook = createControlCommandHook((async () => { calls++; return {} }) as any, async () => {}, "e", "s", "i", "t", async (id) => ({ data: { id, parentID: "root", title: "child" } }), "d")
	await expect(hook({ command: "leave", arguments: "", sessionID: "child" }, { parts: [1] })).rejects.toBeInstanceOf(ControlCommandHandled); expect(calls).toBe(0)
})

test("permission hook denies only authenticated routed handling and RPC failure stays ask", async () => {
	const calls: any[] = [], deny = createPermissionHook(async () => "root", (async (_e: string, _s: string, body: any) => { calls.push(body); return body.method === "control-get" ? { enabled: true, routable: true } : { handled: true } }) as any, "e", "s", "i", "t"), denied: any = { status: "allow" }; await deny({ sessionID: "child", id: "p" }, denied); expect(denied.status).toBe("deny"); await Bun.sleep(0); expect(calls.map((item) => item.method)).toEqual(["control-get", "permission-denied-notice"])
	const unavailable = createPermissionHook(async () => "root", (async () => { throw new Error("down") }) as any, "e", "s", "i", "t"), asked: any = { status: "allow" }; await unavailable({ sessionID: "child", id: "p" }, asked); expect(asked.status).toBe("ask")
})

test("resolveRootSession is bounded and rejects cycles", async () => {
	const parents = new Map([["child", "parent"], ["parent", "root"], ["root", undefined]])
	const client = { session: { get: async ({ path }: any) => ({ data: { id: path.id, parentID: parents.get(path.id) } }) } }; expect(await resolveRootSession(client, "child")).toBe("root")
	const cyclic = { session: { get: async ({ path }: any) => ({ data: { id: path.id, parentID: path.id === "a" ? "b" : "a" } }) } }; await expect(resolveRootSession(cyclic, "a")).rejects.toThrow("cycle")
	const deep = { session: { get: async ({ path }: any) => ({ data: { id: path.id, parentID: String(Number(path.id) + 1) } }) } }; await expect(resolveRootSession(deep, "0")).rejects.toThrow("depth")
})

async function runCompletionOrder(events: Array<["status", "busy" | "idle"] | ["assistant", string]>) {
	const { store, adapter, broker } = readyBroker(async () => Response.json({})); store.refreshRoute("user", "ctx")
	for (const [kind, value] of events) await broker.handleRequest(authenticatedRequest(kind === "status" ? "observe-status" : "observe-assistant", kind === "status" ? { rootSessionId: "root", status: value } : { rootSessionId: "root", assistantMessageId: value, failed: false }))
	await broker.drainBackground(); return { store, adapter, broker }
}

test("completion claims all supported orderings and duplicate busy never clears candidate", async () => {
	for (const events of [
		[["status", "busy"], ["assistant", "a"], ["status", "idle"]],
		[["status", "busy"], ["status", "idle"], ["assistant", "a"]],
		[["status", "busy"], ["assistant", "a"], ["status", "busy"], ["status", "idle"]],
		[["status", "busy"], ["assistant", "a"], ["assistant", "a"], ["status", "idle"], ["status", "idle"]],
	] as any[]) {
		const { store, adapter } = await runCompletionOrder(events); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(1); expect((store.db.query("SELECT run_id AS runId FROM session_activity").get() as any).runId).toBe(1); store.close()
	}
})

test("same assistant cannot complete twice across a late duplicate run", async () => {
	const { store, adapter, broker } = await runCompletionOrder([["status", "busy"], ["assistant", "same"], ["status", "idle"]]); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "same", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(1); store.close()
})

test("assistant preserves idle, child events stay child and cannot complete a bound root", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({})); store.refreshRoute("user", "ctx")
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "late", failed: false })); expect((store.db.query("SELECT idle FROM session_activity").get() as any).idle).toBe(1); await broker.drainBackground(); expect(adapter.sent).toHaveLength(1)
	const calls: any[] = [], hook = createControlEventHook((async (_e: string, _s: string, body: any) => { calls.push(body); if (body.status === "busy") await Bun.sleep(5); return {} }) as any, "e", "s", "owner", "token")
	await Promise.all([hook({ event: { type: "session.status", properties: { sessionID: "child", status: { type: "busy" } } } }), hook({ event: { type: "message.updated", properties: { info: { role: "assistant", sessionID: "child", id: "child-a", time: { completed: 1 } } } } })]); expect(calls.map((item) => item.rootSessionId)).toEqual(["child", "child"]); expect(calls.map((item) => item.method)).toEqual(["observe-status", "observe-assistant"])
	const childResponse = await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "child", status: "busy" })); expect((await childResponse.json() as any).observed).toBe(false); store.close()
})

class CountingAdapter extends MockWeChatAdapter { attempts = 0; slow?: Promise<void>; override async send(to: string, text: string, contextToken: string): Promise<void> { this.attempts++; if (this.slow) return this.slow; return super.send(to, text, contextToken) } }
function brokerWithAdapter(adapter: MockWeChatAdapter, fetcher: typeof fetch = async () => Response.json({ promptMessageId: "p", assistantMessageId: "a", text: "ok" })) { const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true); return { store, broker: new BrokerService(store, adapter, "secret", "worker", fetcher) } }

test("locked controller alone refreshes context, receives replies and consumes checkpoints", async () => {
	let callbacks = 0
	const adapter = new CountingAdapter(), store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner", title: "Root" })
	const broker = new BrokerService(store, adapter, "secret", "worker", async () => { callbacks++; return Response.json({ promptMessageId: "p", assistantMessageId: `a-${callbacks}`, text: "ok" }) })
	await broker.handleInbound({ id: "claim", fromUserId: "controller", contextToken: "c1", text: "id", cursorHint: "1" }); const sentAfterClaim = adapter.attempts
	for (const [id, text] of [["other-id", "id"], ["other-help", "help"], ["other-route", "#1\nx"]]) expect((await broker.handleInbound({ id, fromUserId: "other", contextToken: "evil", text, cursorHint: id })).reason).toBe("route-rejected")
	expect(adapter.attempts).toBe(sentAfterClaim); expect(callbacks).toBe(0); expect(store.route()).toMatchObject({ conversationId: "controller", contextToken: "c1" })
	const revision = store.control().revision; expect(store.openCheckpoint({ checkpointId: "locked-cp", requestKey: "locked-call", root: "root", owner: "owner", alias: 1, question: "q", choices: [], revision })).toBe(true); expect(store.activateCheckpoint("locked-cp")).toBe(true)
	await broker.handleInbound({ id: "attacker-answer", fromUserId: "other", contextToken: "evil2", text: "#1\na", cursorHint: "5" }); expect(store.checkpointState("locked-cp")).toBe("OPEN")
	await broker.handleInbound({ id: "controller-help", fromUserId: "controller", contextToken: "c2", text: "help", cursorHint: "6" }); expect(store.route()).toMatchObject({ conversationId: "controller", contextToken: "c2" }); expect(adapter.sent.at(-1)).toMatchObject({ to: "controller", contextToken: "c2", text: HELP_TEXT })
	await broker.handleInbound({ id: "controller-answer", fromUserId: "controller", contextToken: "c3", text: "#1\na", cursorHint: "7" }); expect(store.checkpointState("locked-cp")).toBe("ANSWERED"); expect(adapter.sent.at(-1)).toMatchObject({ to: "controller", contextToken: "c3", text: "#1\nok" }); store.close()
})

test("failed id list is durable UNKNOWN and duplicate never resends", async () => {
	const adapter = new CountingAdapter(), store = new Store(":memory:"), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({})); adapter.failSend = true
	const message = { id: "failed-list", fromUserId: "controller", contextToken: "ctx", text: "id", cursorHint: "c" }
	await broker.handleInbound(message); expect(adapter.attempts).toBe(1); expect(store.controlOutboundState("inbound:failed-list:list")).toBe("UNKNOWN"); expect((store.db.query("SELECT kind FROM control_outbound WHERE dedupe_key='inbound:failed-list:list'").get() as any).kind).toBe("list")
	expect((await broker.handleInbound(message)).reason).toBe("duplicate-at-least-once-key"); expect(adapter.attempts).toBe(1); store.close()
})

test("request-input validates content/readiness and UNKNOWN request keys never resend", async () => {
	const adapter = new CountingAdapter(), { store, broker } = brokerWithAdapter(adapter); adapter.failSend = true
	const request = (requestKey: string, question: string, choices: string[] = []) => broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey, question, choices }))
	const unknown = await request("same-call", "Question"); expect((await unknown.json() as any).state).toBe("UNKNOWN"); expect(adapter.attempts).toBe(1)
	adapter.failSend = false; const replay = await request("same-call", "Question"); expect(await replay.json()).toMatchObject({ replayed: true, state: "UNKNOWN" }); expect(adapter.attempts).toBe(1); expect((await request("other-call", "Other")).status).toBe(409)
	store.setControl(false); store.setControl(true); expect((await request("blank", "   ")).status).toBe(409); expect((await request("control", "bad\u0001")).status).toBe(409); expect((await request("choice-newline", "q", ["a\nb"])).status).toBe(409)
	adapter.statusValue = "Degraded"; expect((await request("degraded", "q")).status).toBe(503); expect(store.checkpointForRequest("degraded", "root", "owner")).toBeUndefined(); store.close()
})

test("request-input tool converts UNKNOWN and thrown RPC into fixed no-retry endings", async () => {
	expect(await requestInputToolOutcome(async () => ({ state: "UNKNOWN" }))).toContain("禁止重复发送")
	expect(await requestInputToolOutcome(async () => { throw new Error("timeout") })).toContain("禁止重复发送")
	expect(await requestInputToolOutcome(async () => ({ state: "OPEN" }))).toContain("立即结束本回合")
})

test("actual tool before-to-execute bridge preserves the stable callID request key", async () => {
	const args: any = { question: "q", choices: [] }, output = { args }; captureRequestInputCallID({ tool: "wechat_request_input", callID: "call-stable", sessionID: "root" }, output); let received = ""
	const result = await executeRequestInputTool(args, { sessionID: "root", messageID: "message" }, async (requestKey) => { received = requestKey; return { state: "OPEN" } }); expect(received).toBe("call-stable"); expect(result).toContain("立即结束本回合")
})

test("request-key replay survives owner rebind, validates current owner, and never resends", async () => {
	const adapter = new CountingAdapter(), { store, broker } = brokerWithAdapter(adapter); const first = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "rebind-call", question: "q", choices: [] })); expect(first.status).toBe(200); expect(adapter.attempts).toBe(1)
	store.unregister("owner", "token"); store.register("owner2", "token2", "http://127.0.0.1:2"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner2" })
	const owner2 = new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "request-input", instanceId: "owner2", instanceToken: "token2", rootSessionId: "root", requestKey: "rebind-call", question: "q", choices: [] }) }), replay = await broker.handleRequest(owner2); expect(await replay.json()).toMatchObject({ replayed: true, ownerChanged: true, state: "CANCELLED" }); expect(adapter.attempts).toBe(1)
	expect((await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "rebind-call", question: "q", choices: [] }))).status).toBe(403); store.close()
})

test("background rejection is audited and fully consumed", async () => {
	const { store, broker } = readyBroker(async () => Response.json({})); (broker as any).defer(Promise.reject(new Error("background"))); await broker.drainBackground(); expect((store.db.query("SELECT reason FROM audit WHERE reason='background-action-failed'").get() as any).reason).toBe("background-action-failed"); store.close()
})

test("concurrent global control transitions are idempotent", async () => {
	const store = new Store(":memory:"); await Promise.all(Array.from({ length: 20 }, async () => store.setControl(true))); expect(store.control()).toEqual({ enabled: true, revision: 1 }); await Promise.all(Array.from({ length: 20 }, async () => store.setControl(false))); expect(store.control()).toEqual({ enabled: false, revision: 2 }); store.close()
})

test("permission starts from allow, confirms quickly, and broker does not await a hung adapter", async () => {
	const never = new Promise<void>(() => {}), adapter = new CountingAdapter(); adapter.slow = never; const { store, broker } = brokerWithAdapter(adapter)
	const response = await Promise.race([broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "slow" })), Bun.sleep(30).then(() => "timeout" as const)]); expect(response).not.toBe("timeout"); expect(adapter.attempts).toBe(1); expect((store.db.query("SELECT state FROM control_outbound WHERE dedupe_key='permission:slow'").get() as any).state).toBe("SENDING")
	const timeoutHook = createPermissionHook(async () => "root", (async () => { throw new DOMException("timed out", "TimeoutError") }) as any, "e", "s", "i", "t"), output: any = { status: "allow" }; await timeoutHook({ sessionID: "root", id: "p" }, output); expect(output.status).toBe("ask"); store.close()
})

test("back during callback blocks direct send before adapter entry", async () => {
	let release!: (value: Response) => void, entered!: () => void; const started = new Promise<void>((resolve) => { entered = resolve }), callback = new Promise<Response>((resolve) => { release = resolve })
	const { store, adapter, broker } = readyBroker(async () => { entered(); return callback }); const running = broker.handleInbound({ id: "race", fromUserId: "user", contextToken: "ctx", text: "#1\nwork", cursorHint: "c" }); await started
	await broker.handleRequest(authenticatedRequest("back-global")); release(Response.json({ promptMessageId: "p", assistantMessageId: "a", text: "late" })); expect((await running).reason).toBe("control-changed-before-send"); expect(adapter.sent).toHaveLength(0); expect(store.pendingState("race")).toBe("UNKNOWN"); store.close()
})

test("back cancels an ANSWERING checkpoint while callback is in flight", async () => {
	let release!: (value: Response) => void, entered!: () => void; const started = new Promise<void>((resolve) => { entered = resolve }), callback = new Promise<Response>((resolve) => { release = resolve })
	const { store, adapter, broker } = readyBroker(async () => { entered(); return callback }); store.refreshRoute("user", "ctx"); const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "race-checkpoint", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId
	const running = broker.handleInbound({ id: "race-answer", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" }); await started; await broker.handleRequest(authenticatedRequest("back-global")); release(Response.json({ promptMessageId: "p", assistantMessageId: "a", text: "late" })); expect((await running).reason).toBe("control-changed-before-send"); expect(store.checkpointState(checkpointId)).toBe("CANCELLED"); expect(adapter.sent).toHaveLength(1); store.close()
})

test("checkpoint becomes ANSWERED after injection even when direct outbound is UNKNOWN", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ promptMessageId: "p", assistantMessageId: "answer-a", text: "reply" })); store.refreshRoute("user", "ctx")
	const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "answer-call", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId; adapter.failSend = true
	expect((await broker.handleInbound({ id: "answer-in", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" })).reason).toBe("unknown-no-replay"); expect(store.checkpointState(checkpointId)).toBe("ANSWERED"); expect((store.db.query("SELECT state FROM outbound WHERE message_id='answer-a'").get() as any).state).toBe("UNKNOWN"); store.close()
})

test("checkpoint callback uncertainty stays UNKNOWN and blocks new requests until back", async () => {
	const { store, broker } = readyBroker(async () => { throw new DOMException("timeout", "TimeoutError") }); store.refreshRoute("user", "ctx"); const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "uncertain", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId
	await broker.handleInbound({ id: "uncertain-answer", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" }); expect(store.checkpointState(checkpointId)).toBe("UNKNOWN"); expect((await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "new-call", question: "q2", choices: [] }))).status).toBe(409); store.setControl(false); expect(store.checkpointState(checkpointId)).toBe("CANCELLED"); store.close()
})

test("callback UNKNOWN direct run is consumed without generic then next LOCAL run completes once", async () => {
	const { store, adapter, broker } = readyBroker(async () => { throw new DOMException("timeout", "TimeoutError") })
	await broker.handleInbound({ id: "direct-timeout", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "direct-a", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(0); expect((store.db.query("SELECT origin,running FROM session_activity").get() as any)).toMatchObject({ origin: "INBOUND:direct-timeout", running: 0 }); expect((store.db.query("SELECT COUNT(*) AS count FROM audit WHERE reason LIKE 'completion-suppressed:%'").get() as any).count).toBe(1)
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "local-after-timeout", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(1); store.close()
})

test("normal direct success is consumed without generic then next LOCAL run completes once", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ promptMessageId: "p", assistantMessageId: "direct-success", text: "reply" })); await broker.handleInbound({ id: "direct-success-in", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "direct-success", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(0); expect((store.db.query("SELECT running FROM session_activity").get() as any).running).toBe(0)
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "local-after-direct", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(1); store.close()
})

test("completion excludes assistant IDs already owned by direct outbound", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true); store.observeStatus("root", "owner", "busy"); store.db.query("INSERT INTO outbound VALUES('same-assistant','inbound-x','SENT','#1\\nx',?)").run(new Date().toISOString()); store.observeAssistant("root", "owner", "same-assistant", false); store.observeStatus("root", "owner", "idle"); expect(store.claimCompletion("root", "owner")).toBeUndefined(); store.close()
})

test("echo fingerprint includes context, suppresses repeats throughout TTL, and expires", async () => {
	const store = new Store(":memory:"); store.recordEcho("user", "ctx", "#1\nx", 100, 100); expect(store.matchesEcho("user", "ctx", "#1\nx", 150)).toBe(true); expect(store.matchesEcho("user", "ctx", "#1\nx", 160)).toBe(true); expect(store.matchesEcho("user", "other", "#1\nx", 160)).toBe(false); expect(store.matchesEcho("user", "ctx", "#1\nx", 201)).toBe(false); expect(store.sweepOutboundEchoes(201)).toBe(0); store.close()
	const ready = readyBroker(async () => Response.json({ promptMessageId: "p", assistantMessageId: "a", text: "reply" })); await ready.broker.handleInbound({ id: "source", fromUserId: "user", contextToken: "ctx", text: "#1\nstart", cursorHint: "a" }); const payload = ready.adapter.sent[0].text
	for (const id of ["echo-1", "echo-2"]) expect((await ready.broker.handleInbound({ id, fromUserId: "user", contextToken: "ctx", text: payload, cursorHint: id })).reason).toBe("outbound-echo"); expect(ready.adapter.sent).toHaveLength(1); ready.store.close()
})

test("RPC rejects bad method, shared secret and instance token and reports live adapter", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({})); store.register("owner", "token", "http://127.0.0.1:1")
	expect((await broker.handleRequest(new Request("http://127.0.0.1", { method: "GET" }))).status).toBe(405)
	expect((await broker.handleRequest(new Request("http://127.0.0.1", { method: "POST", headers: { "x-wechat-control-key": "bad" }, body: "{}" }))).status).toBe(401)
	expect((await broker.handleRequest(new Request("http://127.0.0.1", { method: "POST", headers: { "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "control-get", instanceId: "owner", instanceToken: "bad" }) }))).status).toBe(403)
	adapter.statusValue = "Degraded"; const live = await broker.handleRequest(authenticatedRequest("control-get", { rootSessionId: "root" })); expect((await live.json() as any).adapter).toBe("Degraded"); store.close()
})

test("plugin implementation remains metadata-only and adapter command is configurable", async () => {
	const source = await Bun.file(new URL("./plugin-runtime.ts", import.meta.url)).text()
	expect(source).toContain("event:"); expect(source).not.toContain("session.messages"); expect(source).toContain("observe-assistant"); expect(source).toContain("observe-status"); expect(source).not.toContain("C:\\\\Users")
	expect(resolveWeixinCommand(["node", path.resolve("custom", "weixin-mcp", "dist", "cli.js")])[0]).toBe("node"); expect(() => resolveWeixinCommand(["npx", "weixin-mcp"])).toThrow()
})
