import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdirSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import * as path from "node:path"
import { AdapterSendError, assertWeixinSendSuccess, JsonRpcPendingMap, MockWeChatAdapter, WeixinMcpAdapter, type McpClient } from "./adapter"
import { BrokerService, clampCallbackTimeout } from "./broker"
import { ClientLifecycleRegistry, createCallbackHandler, resolveRootSession } from "./client"
import { CONTROL_OFF_TEXT, HELP_TEXT, MAX_CONTEXT_TOKEN_LENGTH, MAX_ROUTE_ID_LENGTH, PERMISSION_DENIED_TEXT, Store, allocateRequestCode, formatOutbound, formatRegistrationList, parseInboundText, parsePollToolResult, sanitizeTitle, sha256 } from "./core"
import { runWorker } from "./worker"
import { decideExistingBroker, pidStatus, readLock } from "./worker-runtime"
import { captureReplyCallID, createControlCommandHook, createControlEventHook, createPermissionHook, registerControlCommands, resolveWeixinCommand } from "./plugin-runtime"
import { HttpServerResponse } from "effect/unstable/http"

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

test("binding activity defaults on old v6 data and aliases are never reused", () => {
	const filename = tempFile("binding-active-v6"), legacy = new Database(filename)
	legacy.exec("PRAGMA user_version=6; CREATE TABLE bindings(alias INTEGER PRIMARY KEY AUTOINCREMENT,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,title TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); INSERT INTO bindings VALUES(4,'historical','d','owner','Old','now','now');")
	legacy.close()
	const store = new Store(filename)
	expect(store.bindingForRoot("historical")).toMatchObject({ alias: 1, registrationAlias: 4, active: true })
	expect(store.deactivateBinding("historical", "owner")).toBe(true); expect(store.bindings()).toEqual([])
	expect(store.bindingForRoot("historical")).toBeUndefined()
	expect(store.bind({ rootSessionId: "new-root", directory: "d", ownerInstance: "owner" })).toMatchObject({ alias: 1, registrationAlias: 5 })
	expect(store.bind({ rootSessionId: "historical", directory: "new-d", ownerInstance: "owner", title: "Refreshed" })).toMatchObject({ alias: 1, registrationAlias: 4, active: true, title: "Refreshed" })
	store.close(); cleanup(filename)
})

test("active aliases are dense over immutable registration order", () => {
	const store = new Store(":memory:"), now = new Date().toISOString()
	for (const [alias, root] of [[4, "four"], [10, "ten"], [25, "twenty-five"]] as const) store.db.query("INSERT INTO bindings(alias,root_session_id,directory,owner_instance,title,active,created_at,updated_at) VALUES(?,?,?,'owner',NULL,1,?,?)").run(alias, root, root, now, now)
	expect(store.bindings().map(({ alias, registrationAlias, rootSessionId }) => [alias, registrationAlias, rootSessionId])).toEqual([[1, 4, "four"], [2, 10, "ten"], [3, 25, "twenty-five"]])
	expect(store.bindingForAlias(2)).toMatchObject({ rootSessionId: "ten", alias: 2, registrationAlias: 10 })
	expect(store.bindingForRoot("twenty-five")).toMatchObject({ alias: 3, registrationAlias: 25 })
	expect(store.deactivateBinding("ten", "owner")).toBe(true)
	expect(store.bindings().map((binding) => [binding.rootSessionId, binding.alias])).toEqual([["four", 1], ["twenty-five", 2]])
	expect(store.bindingForRoot("ten")).toBeUndefined()
	expect(store.bind({ rootSessionId: "ten", directory: "ten-new", ownerInstance: "owner" })).toMatchObject({ alias: 2, registrationAlias: 10 })
	expect(store.bindings().map((binding) => [binding.rootSessionId, binding.alias])).toEqual([["four", 1], ["ten", 2], ["twenty-five", 3]])
	expect((store.db.query("SELECT group_concat(alias,',') AS aliases FROM bindings ORDER BY alias").get() as any).aliases).toBe("4,10,25")
	expect((store.db.query("SELECT seq FROM sqlite_sequence WHERE name='bindings'").get() as any).seq).toBe(25)
	store.close()
})

test("v6 to v7 preserves durable outbound history and conservatively backfills logical replies", () => {
	const filename = tempFile("logical-reply-v6"), legacy = new Database(filename), expires = Date.now() + 60_000
	legacy.exec("PRAGMA user_version=6; CREATE TABLE bindings(alias INTEGER PRIMARY KEY AUTOINCREMENT,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,title TEXT,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE control_outbound(outbound_id TEXT PRIMARY KEY,dedupe_key TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL,updated_at TEXT NOT NULL); CREATE TABLE outbound_echoes(echo_hash TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL DEFAULT '',payload TEXT NOT NULL,expires_ms INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL);")
	legacy.query("INSERT INTO bindings VALUES(10,'root','d','owner','Root',1,'now','now')").run()
	legacy.query("INSERT INTO control_outbound VALUES('generated','wechat-reply:root:one','root','wechat-reply','SENT','#10\nbody','user','ctx','now'),('noncanonical','wechat-reply:root:two','root','wechat-reply','UNKNOWN','#010\nbody','user','ctx','now'),('bad-key','wechat-reply:root:','root','wechat-reply','SENT','#10\nbody','user','ctx','now'),('wrong-root','wechat-reply:other:three','root','wechat-reply','SENT','#10\nbody','user','ctx','now'),('other','other','root','permission','SENT','#10\nbody','user','ctx','now')").run()
	legacy.query("INSERT INTO outbound_echoes VALUES('echo','user','ctx','#10\nbody',?,'now')").run(expires); legacy.close()
	const store = new Store(filename), backup = store.migrationBackupPath
	expect(backup).toContain("pre-v7"); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8)
	expect(store.bindingForRoot("root")).toMatchObject({ alias: 1, registrationAlias: 10 })
	expect(store.db.query("SELECT outbound_id AS id,state,payload,logical_text AS text,logical_hash AS hash FROM control_outbound ORDER BY outbound_id").all()).toEqual([
		{ id: "bad-key", state: "SENT", payload: "#10\nbody", text: null, hash: null },
		{ id: "generated", state: "SENT", payload: "#10\nbody", text: "body", hash: Bun.CryptoHasher.hash("sha256", "body", "hex") },
		{ id: "noncanonical", state: "UNKNOWN", payload: "#010\nbody", text: null, hash: null },
		{ id: "other", state: "SENT", payload: "#10\nbody", text: null, hash: null },
		{ id: "wrong-root", state: "SENT", payload: "#10\nbody", text: null, hash: null },
	])
	expect((store.db.query("SELECT payload FROM outbound_echoes WHERE echo_hash='echo'").get() as any).payload).toBe("#10\nbody")
	expect((store.db.query("SELECT seq FROM sqlite_sequence WHERE name='bindings'").get() as any).seq).toBe(10)
	store.close(); cleanup(filename, backup)
})

test("logical wechat reply identity replays across alias movement without rewriting history", () => {
	const store = new Store(":memory:"), now = new Date().toISOString()
	for (const [alias, root] of [[4, "earlier"], [10, "target"]] as const) store.db.query("INSERT INTO bindings(alias,root_session_id,directory,owner_instance,title,active,created_at,updated_at) VALUES(?,?,?,'owner',NULL,1,?,?)").run(alias, root, root, now, now)
	store.refreshRoute("user", "ctx"); store.setControl(true)
	const first = store.claimControlOutbound({ dedupeKey: "wechat-reply:target:call", root: "target", kind: "wechat-reply", payload: "#2\nsame", logicalText: "same" })
	expect(first?.result).toBe("CLAIMED"); store.finishControlOutbound((first as any).outboundId, true); store.recordEcho("user", "ctx", "#2\nsame")
	store.deactivateBinding("earlier", "owner"); expect(store.bindingForRoot("target")?.alias).toBe(1)
	expect(store.claimControlOutbound({ dedupeKey: "wechat-reply:target:call", root: "target", kind: "wechat-reply", payload: "#1\nsame", logicalText: "same" })).toEqual({ result: "REPLAY", state: "SENT", payload: "#2\nsame" })
	expect(store.claimControlOutbound({ dedupeKey: "wechat-reply:target:call", root: "target", kind: "wechat-reply", payload: "#1\nchanged", logicalText: "changed" })).toEqual({ result: "CONFLICT" })
	expect(store.controlOutboundPayload("wechat-reply:target:call")).toBe("#2\nsame"); expect(store.matchesEcho("user", "ctx", "#2\nsame")).toBe(true)
	store.close()
})

test("UNKNOWN logical wechat reply replays after alias compaction without changing history or echo", () => {
	const store = new Store(":memory:"), now = new Date().toISOString()
	for (const [alias, root] of [[4, "earlier"], [10, "target"]] as const) store.db.query("INSERT INTO bindings(alias,root_session_id,directory,owner_instance,title,active,created_at,updated_at) VALUES(?,?,?,'owner',NULL,1,?,?)").run(alias, root, root, now, now)
	store.refreshRoute("user", "ctx"); store.setControl(true)
	const first = store.claimControlOutbound({ dedupeKey: "wechat-reply:target:unknown", root: "target", kind: "wechat-reply", payload: "#2\nuncertain", logicalText: "uncertain" }) as any
	store.finishControlOutbound(first.outboundId, false); store.recordEcho("user", "ctx", "#2\nuncertain")
	store.deactivateBinding("earlier", "owner")
	const replay = store.claimControlOutbound({ dedupeKey: "wechat-reply:target:unknown", root: "target", kind: "wechat-reply", payload: "#1\nuncertain", logicalText: "uncertain" })
	expect(replay).toEqual({ result: "REPLAY", state: "UNKNOWN", payload: "#2\nuncertain" })
	expect(store.controlOutboundState("wechat-reply:target:unknown")).toBe("UNKNOWN")
	expect(store.controlOutboundPayload("wechat-reply:target:unknown")).toBe("#2\nuncertain")
	expect(store.matchesEcho("user", "ctx", "#2\nuncertain")).toBe(true)
	store.close()
})

test("checkpoint request keys are scoped by root", () => {
	const store = new Store(":memory:")
	store.bind({ rootSessionId: "root-a", directory: "a", ownerInstance: "owner" }); store.bind({ rootSessionId: "root-b", directory: "b", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); const revision = store.setControl(true).revision
	expect(store.openCheckpoint({ checkpointId: "checkpoint-a", requestKey: "same-request", root: "root-a", owner: "owner", alias: 1, question: "a", choices: [], revision })).toBe(true)
	expect(store.openCheckpoint({ checkpointId: "checkpoint-b", requestKey: "same-request", root: "root-b", owner: "owner", alias: 2, question: "b", choices: [], revision })).toBe(true)
	expect(store.checkpointForRequest("same-request", "root-a")?.checkpointId).toBe("checkpoint-a")
	expect(store.checkpointForRequest("same-request", "root-b")?.checkpointId).toBe("checkpoint-b")
	expect(store.checkpointForRequest("same-request", "missing")).toBeUndefined()
	expect((store.db.query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_checkpoint_root_request_key'").get() as any).name).toBe("idx_checkpoint_root_request_key")
	store.close()
})

test("registration list is ordered, hides roots and remains within adapter limit", () => {
	const bindings = Array.from({ length: 80 }, (_, index) => ({ alias: 80 - index, rootSessionId: `secret-${index}`, directory: "d", ownerInstance: "o", title: "会".repeat(120), active: true }))
	const text = formatRegistrationList(bindings); expect(text.length).toBeLessThanOrEqual(4000); expect(text).toContain("#1  "); expect(text).toContain("另有"); expect(text).not.toContain("secret-")
})

test("registration list uses exact blank-line spacing and readable truncation", () => {
	const base = (alias: number, title: string) => ({ alias, rootSessionId: `root-${alias}`, directory: "d", ownerInstance: "o", title, active: true })
	expect(formatRegistrationList([base(2, "two"), base(1, "one")])).toBe("#1  one\n\n#2  two")
	expect(formatRegistrationList([base(1, "one")])).toBe("#1  one")
	const long = "x".repeat(1900), truncated = formatRegistrationList([base(1, long), base(2, long), base(3, long)])
	expect(truncated).toBe(`#1  ${long}\n\n#2  ${long}\n\n……另有 1 个会话未显示。`)
	expect(truncated.length).toBeLessThanOrEqual(4000); expect(truncated).toContain("\n\n……另有 1 个会话未显示。")
})

test("wechat id lists registrations on and off without callback and refreshes global route", async () => {
	let callbacks = 0
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => { callbacks++; return Response.json({}) })
	store.bind({ rootSessionId: "r1", directory: "d", ownerInstance: "owner", title: "First" }); store.bind({ rootSessionId: "r2", directory: "d", ownerInstance: "owner", title: null }); store.deactivateBinding("r2", "owner"); store.setControl(false)
	await broker.handleInbound({ id: "list-off", fromUserId: "recipient-a", contextToken: "ctx-a", text: "id", cursorHint: "a" })
	expect(adapter.sent.at(-1)).toMatchObject({ to: "recipient-a", contextToken: "ctx-a", text: "#1  First" }); expect(store.route()).toMatchObject({ conversationId: "recipient-a", contextToken: "ctx-a" })
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

test("prompt message IDs are canonical monotonic and durable across duplicate claims restarts and clock rollback", async () => {
	const filename = tempFile("prompt-message-id"), future = Date.now() + 60_000
	let store = new Store(filename), peer = new Store(filename)
	store.db.query("INSERT OR REPLACE INTO meta(key,value) VALUES('prompt_message_id_clock',?)").run(`${future}:7`)
	const input = { submissionId: "same", inboundId: "same-inbound", root: "root", owner: "owner", alias: 1, body: "one" }
	const [first, duplicate] = await Promise.all([Promise.resolve().then(() => store.claimPromptSubmission(input)!), Promise.resolve().then(() => peer.claimPromptSubmission(input)!)])
	expect(first.messageId).toMatch(/^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$/); expect(duplicate.messageId).toBe(first.messageId)
	const second = store.claimPromptSubmission({ ...input, submissionId: "second", inboundId: "second-inbound", body: "two" })!
	expect(second.messageId > first.messageId).toBe(true)
	peer.close(); store.close(); store = new Store(filename)
	const afterRestart = store.claimPromptSubmission({ ...input, submissionId: "restart", inboundId: "restart-inbound", body: "three" })!
	expect(afterRestart.messageId > second.messageId).toBe(true); expect(store.promptSubmission("same")?.messageId).toBe(first.messageId)
	store.close(); cleanup(filename)
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
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ msg_id: "sent" }) }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ ok: true }) }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ message: "sent" }) }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ ret: 0 }) }] })).not.toThrow()
	expect(() => assertWeixinSendSuccess({ content: [{ type: "text", text: JSON.stringify({ errcode: 0 }) }] })).not.toThrow(); adapter.stop()
	const exited = new WeixinMcpAdapter({ enabled: true, command: ["node", "fixed.js"], clientFactory: () => new FakeMcp(true), retry: false }); await exited.start(async () => {}); await Bun.sleep(5); expect(exited.status()).toBe("Degraded")
	const map = new JsonRpcPendingMap(); expect(await map.request((message: any) => map.accept({ jsonrpc: "2.0", id: message.id, result: "fast" }), "fast", {})).toBe("fast")
})

test("weixin_send parser rejects MCP, business, malformed and ambiguous failures", () => {
	const failures: Array<[unknown, string]> = [
		[null, "malformed-result"],
		[{ isError: true, content: [{ type: "text", text: "{}" }] }, "mcp-error"],
		[{ content: [{ type: "text", text: "not-json" }] }, "malformed-result"],
		[{ content: [{ type: "text", text: "[]" }] }, "malformed-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 1 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ errcode: 400 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errcode: 1 }) }] }, "explicit-business-failure"],
		[{ content: [{ type: "text", text: JSON.stringify({ error: "failed" }) }] }, "ambiguous-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errmsg: "failed" }) }] }, "ambiguous-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: 0, errmsg: "" }) }] }, "ambiguous-result"],
		[{ content: [{ type: "text", text: JSON.stringify({ ret: "0" }) }] }, "explicit-business-failure"],
	]
	for (const [value, classification] of failures) {
		try { assertWeixinSendSuccess(value); throw new Error("expected parser failure") }
		catch (error) { expect(error).toBeInstanceOf(AdapterSendError); expect((error as AdapterSendError).classification).toBe(classification) }
	}
})

function injectRequest(method = "POST", envelope: any = { kind: "inbound" }): Request { return new Request("http://127.0.0.1/inject", { method, headers: { "x-wechat-control-key": "secret", "x-wechat-instance-token": "token", "content-type": "application/json" }, body: method === "POST" ? JSON.stringify({ rootSessionId: "root", directory: "d", text: "hello", inboundId: "in", envelope }) : undefined }) }

test("legacy inject is rejected without SDK mutation", async () => {
	let gets = 0, prompts = 0
	const client = { session: { get: async () => { gets++; return { data: { id: "root" } } }, prompt: async () => { prompts++; return {} } } }
	const response = await createCallbackHandler(client, "secret", "token")(injectRequest()); expect(response.status).toBe(410); expect(gets).toBe(0); expect(prompts).toBe(0)
	expect((await createCallbackHandler(client, "secret", "token")(injectRequest("GET"))).status).toBe(405); expect(gets).toBe(0); expect(prompts).toBe(0)
})

function readyBroker(fetcher: typeof fetch) {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter()
	store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true)
	return { store, adapter, broker: new BrokerService(store, adapter, "secret", "worker", fetcher) }
}

test("one inbound admits once without assistant body and deduplicates", async () => {
	let callbacks = 0
	const { store, adapter, broker } = readyBroker(async () => { callbacks++; return Response.json({ ok: true, accepted: true }) })
	const message = { id: "in", fromUserId: "user", contextToken: "ctx", text: "#1\nhello", cursorHint: "c" }
	expect((await broker.handleInbound(message)).ok).toBe(true); expect((await broker.handleInbound(message)).reason).toBe("duplicate-at-least-once-key")
	expect(callbacks).toBe(1); expect(adapter.sent).toHaveLength(0); expect(store.promptSubmission("in")?.state).toBe("SUBMITTED"); expect((store.db.query("SELECT COUNT(*) AS count FROM pending_replies").get() as any).count).toBe(0); store.close()
})

test("callback timeout and invalid direct result become UNKNOWN without replay", async () => {
	for (const fetcher of [
		async () => { throw new DOMException("timed out", "TimeoutError") },
		async () => Response.json({ promptMessageId: "p", assistantMessageId: "a" }),
	]) {
		const { store, adapter, broker } = readyBroker(fetcher as typeof fetch); const result = await broker.handleInbound({ id: crypto.randomUUID(), fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" })
		expect(result.reason).toBe("unknown-no-replay"); expect(adapter.sent).toHaveLength(0); expect(store.state((store.db.query("SELECT message_id AS id FROM inbound").get() as any).id)).toBe("UNKNOWN"); store.close()
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
	expect(backup).toBeDefined(); expect(backup).toContain("pre-v5"); expect(store.bindingForRoot("root-wal")).toMatchObject({ alias: 1, registrationAlias: 7 }); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8); expect(store.control()).toEqual({ enabled: false, revision: 0 }); expect(store.route()).toMatchObject({ conversationId: "conversation-wal", contextToken: null })
	expect((store.db.query("PRAGMA table_info(session_activity)").all() as any[]).map((row) => row.name)).toContain("epoch"); expect((store.db.query("PRAGMA table_info(checkpoints)").all() as any[]).map((row) => row.name)).toContain("request_key"); expect((store.db.query("PRAGMA table_info(outbound_echoes)").all() as any[]).map((row) => row.name)).toContain("expires_ms")
	const snapshot = new Database(backup!); expect((snapshot.query("SELECT value FROM meta WHERE key='custom'").get() as any).value).toBe("wal-data"); expect((snapshot.query("SELECT conversation_id AS value FROM bindings WHERE alias=7").get() as any).value).toBe("conversation-wal"); expect(snapshot.query("SELECT 1 FROM pending_replies WHERE inbound_id='pending-wal'").get()).toBeDefined(); snapshot.close()
	expect(store.bind({ rootSessionId: "root-two", directory: "d", ownerInstance: "owner" })).toMatchObject({ alias: 2, registrationAlias: 8 })
	store.close(); writer.close(); const reopened = new Store(filename); expect(reopened.migrationBackupPath).toBeUndefined(); expect(reopened.bindingForRoot("root-wal")?.title).toBeNull(); reopened.close(); cleanup(filename, backup)
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
	expect(backup).toContain("pre-v5"); expect(store.bindingForRoot("root-v4")).toMatchObject({ alias: 1, registrationAlias: 12, directory: "dir-v4", ownerInstance: "owner-v4", title: null }); expect(store.route()).toMatchObject({ conversationId: "recipient-v4", contextToken: null }); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8)
	expect(store.bind({ rootSessionId: "next-v5", directory: "d", ownerInstance: "o" })).toMatchObject({ alias: 2, registrationAlias: 13 }); store.close(); writer.close()
	const reopened = new Store(filename); expect(reopened.migrationBackupPath).toBeUndefined(); expect((reopened.db.query("SELECT value FROM meta WHERE key='schema_version'").get() as any).value).toBe("8"); reopened.close(); cleanup(filename, backup)
})

test("v4 multiple conversations migrate fail-closed and alias holes advance from max", () => {
	const filename = tempFile("wal-v4-multiple"), writer = createWalV4(filename)
	writer.exec("DELETE FROM bindings; INSERT INTO bindings VALUES(2,'root-a','a','owner-a','recipient-a','ctx-a','created'); INSERT INTO bindings VALUES(9,'root-b','b','owner-b','recipient-b','ctx-b','created'); CREATE TABLE control_state(singleton INTEGER PRIMARY KEY,enabled INTEGER NOT NULL,revision INTEGER NOT NULL); INSERT INTO control_state VALUES(1,1,7); CREATE TABLE checkpoints(checkpoint_id TEXT PRIMARY KEY,request_key TEXT,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,alias INTEGER NOT NULL,question TEXT NOT NULL,choices_json TEXT NOT NULL,state TEXT NOT NULL,inbound_id TEXT,control_revision INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL); INSERT INTO checkpoints VALUES('multi-cp','key','root-a','owner-a','recipient-a',2,'q','[]','UNKNOWN',NULL,7,'created','created'); CREATE TABLE session_activity(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,running INTEGER NOT NULL DEFAULT 0,idle INTEGER NOT NULL DEFAULT 1,last_assistant_id TEXT,last_assistant_error INTEGER NOT NULL DEFAULT 0,direct_assistant_id TEXT,epoch INTEGER NOT NULL DEFAULT 0,run_id INTEGER NOT NULL DEFAULT 0,origin TEXT NOT NULL DEFAULT 'NONE',candidate_run INTEGER,claimed_run INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL); INSERT INTO session_activity VALUES('root-a','owner-a',1,0,NULL,0,NULL,7,1,'LOCAL',1,0,'created'); CREATE TABLE inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL); INSERT INTO inbound VALUES('multi-wait','recipient-a','ctx-a','#2 x','INJECTING','root-a',NULL,NULL,'created'); CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,control_revision INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL); INSERT INTO pending_replies VALUES('multi-wait','root-a',NULL,2,'WAITING',NULL,NULL,NULL,7,'created');")
	const store = new Store(filename), backup = store.migrationBackupPath; expect(store.route()).toMatchObject({ conversationId: null, contextToken: null }); expect(store.control().enabled).toBe(false); expect(store.checkpointState("multi-cp")).toBe("CANCELLED"); expect(store.pendingState("multi-wait")).toBe("UNKNOWN"); expect(store.state("multi-wait")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='multi-wait'").get() as any).reason).toBe("migration-multiple-global-routes"); expect((store.db.query("SELECT running,origin FROM session_activity WHERE root_session_id='root-a'").get() as any)).toMatchObject({ running: 0, origin: "NONE" }); expect((store.db.query("SELECT reason FROM audit WHERE reason='migration-multiple-global-routes'").get() as any).reason).toBe("migration-multiple-global-routes"); expect(store.bind({ rootSessionId: "root-c", directory: "c", ownerInstance: "owner-c" })).toMatchObject({ alias: 3, registrationAlias: 10 })
	store.close(); writer.close(); cleanup(filename, backup)
})

test("old deployed v3 receives a pre-v5 snapshot and preserves control/pending/checkpoint data", () => {
	const filename = tempFile("wal-v3"), writer = createWalV3(filename), store = new Store(filename), backup = store.migrationBackupPath
	expect(backup).toContain("pre-v5"); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8); expect(store.bindingForRoot("root-v3")).toMatchObject({ alias: 1, registrationAlias: 4 }); expect(store.control()).toEqual({ enabled: true, revision: 17 }); expect(store.pendingState("in-v3")).toBe("UNKNOWN"); expect(store.checkpointState("cp-v3")).toBe("CANCELLED"); expect(store.checkpointForRequest("cp-v3", "root-v3")?.checkpointId).toBe("cp-v3")
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
	expect(aliases).toEqual([1, 2, 3]); store.deactivateBinding("r2", "owner"); expect(((await (await leave("r2", "new-title")).json()) as any).binding).toMatchObject({ alias: 2, title: "new-title", active: true }); expect(store.control().enabled).toBe(true)
	expect((await leave("child", "bad")).status).toBe(409); expect(store.bindingForRoot("child")).toBeUndefined(); expect(checked).toEqual(["r1", "r2", "r3", "r2", "child"]); store.close()
})

test("explicit not-root health and runtime observations deactivate only their binding", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner", title: "Root" })
	const broker = new BrokerService(store, adapter, "secret", "worker", async (url) => Response.json({ error: "not-root" }, { status: 409 }))
	const observed = await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); expect(await observed.json()).toMatchObject({ observed: false }); expect(store.bindingForRoot("root")).toBeUndefined()
	store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner", title: "Again" }); const leave = await broker.handleRequest(authenticatedRequest("leave-root", { rootSessionId: "root", directory: "d", title: "No longer root" })); expect(leave.status).toBe(409); expect(store.bindingForRoot("root")).toBeUndefined(); expect((store.db.query("SELECT alias FROM bindings WHERE root_session_id='root'").get() as any).alias).toBe(1)
	store.close()
})

test("live owner blocks rebind; stale exact-root rebind clears checkpoint and activity", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(); store.register("old", "old-token", "http://127.0.0.1:1"); store.register("next", "next-token", "http://127.0.0.1:2")
	const broker = new BrokerService(store, adapter, "secret", "worker", async (_url, init) => { const body = JSON.parse(String(init?.body)); return body.rootSessionId === "root" ? Response.json({ ok: true }) : Response.json({ error: "not-root" }, { status: 409 }) })
	const request = (instanceId: string, instanceToken: string, rootSessionId = "root") => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "leave-root", instanceId, instanceToken, rootSessionId, directory: "d", title: instanceId }) })
	expect((await broker.handleRequest(request("old", "old-token"))).status).toBe(200); store.refreshRoute("controller", "ctx"); const revision = store.control().revision
	expect(store.openCheckpoint({ checkpointId: "rebind-cp", requestKey: "rebind-key", root: "root", owner: "old", alias: 1, question: "q", choices: [], revision })).toBe(true); store.observeStatus("root", "old", "busy"); store.beginInbound({ id: "old-flight", fromUserId: "controller", contextToken: "ctx", text: "#1\nx", cursorHint: "x" }); store.beginPending("old-flight", "root", 1, revision)
	store.openNativeRequest({ requestId: "rebind-native", requestKey: "rebind-native-key", root: "root", owner: "old", alias: 1, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement("rebind-native", true); store.claimPromptSubmission({ submissionId: "rebind-prompt", inboundId: "rebind-prompt-in", root: "root", owner: "old", alias: 1, messageId: "rebind-message", body: "x" }); store.beginRuntimeAdmission("rebind-prompt", "root", "old")
	store.bind({ rootSessionId: "other-root", directory: "d", ownerInstance: "next" }); store.claimPromptSubmission({ submissionId: "other-prompt", inboundId: "other-in", root: "other-root", owner: "next", alias: 2, messageId: "other-message", body: "x" }); const otherGeneration = store.beginRuntimeAdmission("other-prompt", "other-root", "next")!; store.observeRuntimeStatus("other-root", "next", "BUSY", otherGeneration); expect(store.desiredTyping()).toBe(true)
	const live = await broker.handleRequest(request("next", "next-token")); expect(live.status).toBe(409); expect(await live.json()).toMatchObject({ error: "owner-live" }); expect(store.bindingForRoot("root")?.ownerInstance).toBe("old"); expect(store.pendingState("old-flight")).toBe("WAITING")
	store.unregister("old", "old-token"); expect((await broker.handleRequest(request("next", "next-token"))).status).toBe(200); expect(store.bindingForRoot("root")?.ownerInstance).toBe("next"); expect(store.checkpointState("rebind-cp")).toBe("CANCELLED"); expect((store.db.query("SELECT running,origin,owner_instance AS owner FROM session_activity WHERE root_session_id='root'").get() as any)).toMatchObject({ running: 0, origin: "NONE", owner: "next" }); expect(store.pendingState("old-flight")).toBe("UNKNOWN"); expect(store.state("old-flight")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='old-flight'").get() as any).reason).toBe("binding-deactivated")
	expect(store.nativeRequest("rebind-native")?.state).toBe("CANCELLED_REMOTE"); expect(store.promptSubmission("rebind-prompt")?.state).toBe("CANCELLED"); expect(store.runtime("root")).toMatchObject({ ownerInstance: "next", status: "IDLE", workPending: false })
	expect(store.runtime("other-root")?.workPending).toBe(true); expect(store.typingDesired()).toBe(true)
	expect(store.completePendingAndClaim("old", "root", "old-flight", "p", "a", "late", store.control().revision)).toBeUndefined(); store.close()
})

test("callback health rejects mismatched returned session IDs while inject stays removed", async () => {
	const client = { session: { get: async () => ({ data: { id: "different" } }), prompt: async () => { throw new Error("must not prompt") } } }, handler = createCallbackHandler(client, "secret", "token")
	const health = new Request("http://127.0.0.1/health", { method: "POST", headers: { "x-wechat-control-key": "secret", "x-wechat-instance-token": "token", "content-type": "application/json" }, body: JSON.stringify({ rootSessionId: "root" }) }); expect((await handler(health)).status).toBe(409)
	expect((await handler(injectRequest())).status).toBe(410)
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

test("legacy checkpoint stays historical while ordinary answer uses prompt admission", async () => {
	let callbackPath = ""
	const { store, adapter, broker } = readyBroker(async (url) => { callbackPath = String(url); return Response.json({ ok: true, accepted: true }) })
	store.refreshRoute("user", "ctx")
	const response = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "call-checkpoint", question: "Choose", choices: ["A", "B"] })); expect(response.status).toBe(200)
	const checkpointId = (await response.json() as any).checkpointId; expect(store.checkpointState(checkpointId)).toBe("OPEN"); expect(adapter.sent[0].to).toBe("user"); expect(adapter.sent[0].text).toContain("1. A")
	const result = await broker.handleInbound({ id: "answer", fromUserId: "user", contextToken: "ctx2", text: "#1\nB", cursorHint: "c" })
	expect(result.ok).toBe(true); expect(callbackPath).toEndWith("/submit-prompt"); expect(store.checkpointState(checkpointId)).toBe("OPEN"); expect(store.promptSubmission("answer")?.state).toBe("SUBMITTED"); expect(adapter.sent).toHaveLength(1)
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

test("permission denial remains fixed while completion observers never send", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({})); store.refreshRoute("user", "ctx")
	const permission = await broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "perm" })); expect((await permission.json() as any).handled).toBe(true); await broker.drainBackground(); expect(adapter.sent[0].text).toBe(`#1\n${PERMISSION_DENIED_TEXT}`)
	await broker.handleRequest(authenticatedRequest("permission-denied-notice", { rootSessionId: "root", permissionId: "perm" })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(1)
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "terminal", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" }))
	await broker.drainBackground(); expect(adapter.sent.filter((item) => item.text === "#1\n任务已完成。")).toHaveLength(0); expect(store.db.query("SELECT state FROM control_outbound WHERE kind='completion'").get()).toBeNull(); store.close()
})

test("exact outbound echoes are suppressed before routing", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ ok: true, accepted: true })), payload = "#1\nreply"; store.recordEcho("user", "ctx", payload)
	const echoed = await broker.handleInbound({ id: "echo", fromUserId: "user", contextToken: "ctx", text: payload, cursorHint: "b" }); expect(echoed.reason).toBe("outbound-echo"); expect(adapter.sent).toHaveLength(0); expect(store.state("echo")).toBe("UNKNOWN"); store.close()
})

test("commands are conflict-safe, clear sentinels, reject arguments and always throw handled", async () => {
	const config: any = {}; registerControlCommands(config); expect(config.command.leave.template).toContain("LEAVE_HANDLED"); expect(() => registerControlCommands(config)).not.toThrow(); expect(() => registerControlCommands({ command: { back: {} } })).toThrow("command conflict")
	const calls: any[] = [], toasts: boolean[] = []; const rpcCall: any = async (_e: string, _s: string, body: any) => { calls.push(body); return { ok: true, binding: { alias: 1 } } }
	const hook = createControlCommandHook(rpcCall, async (enabled) => { toasts.push(enabled) }, "e", "s", "i", "t", async (id) => ({ data: { id, title: "Title" } }), "d"), output = { parts: [{ type: "text", text: "sentinel" }] }
	const leave = await hook({ command: "leave", arguments: "", sessionID: "root" }, output).catch((error) => error); expect(HttpServerResponse.isHttpServerResponse(leave)).toBe(true); expect(leave.status).toBe(204); expect(leave.headers["x-ocx-command"]).toBe("leave:1"); expect(output.parts).toHaveLength(0); expect(calls[0]).toMatchObject({ method: "leave-root", rootSessionId: "root", title: "Title" }); expect(toasts).toEqual([true])
	const invalid = await hook({ command: "back", arguments: "bad" }, { parts: [1] }).catch((error) => error); expect(HttpServerResponse.isHttpServerResponse(invalid)).toBe(true); expect(invalid.status).toBe(400); expect(calls).toHaveLength(1)
	const failed = createControlCommandHook(async () => { throw new Error("rpc") }, async () => {}, "e", "s", "i", "t"), unavailable = await failed({ command: "back", arguments: "" }, { parts: [1] }).catch((error) => error); expect(HttpServerResponse.isHttpServerResponse(unavailable)).toBe(true); expect(unavailable.status).toBe(503)
})

test("leave rejects child sessions before RPC", async () => {
	let calls = 0
	const hook = createControlCommandHook((async () => { calls++; return {} }) as any, async () => {}, "e", "s", "i", "t", async (id) => ({ data: { id, parentID: "root", title: "child" } }), "d")
	const response = await hook({ command: "leave", arguments: "", sessionID: "child" }, { parts: [1] }).catch((error) => error); expect(HttpServerResponse.isHttpServerResponse(response)).toBe(true); expect(response.status).toBe(503); expect(calls).toBe(0)
})

test("permission hook always leaves controlled permission at native ask", async () => {
	const calls: any[] = [], ask = createPermissionHook(async () => "root", (async (_e: string, _s: string, body: any) => { calls.push(body); return { enabled: true, routable: true } }) as any, "e", "s", "i", "t"), output: any = { status: "allow" }; await ask({ sessionID: "child", id: "p" }, output); expect(output.status).toBe("ask"); expect(calls).toHaveLength(0)
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

test("legacy completion observers are accepted but never dispatch", async () => {
	for (const events of [
		[["status", "busy"], ["assistant", "a"], ["status", "idle"]],
		[["status", "busy"], ["status", "idle"], ["assistant", "a"]],
		[["status", "busy"], ["assistant", "a"], ["status", "busy"], ["status", "idle"]],
		[["status", "busy"], ["assistant", "a"], ["assistant", "a"], ["status", "idle"], ["status", "idle"]],
	] as any[]) {
		const { store, adapter } = await runCompletionOrder(events); expect(adapter.sent).toHaveLength(0); expect(store.db.query("SELECT 1 FROM session_activity").get()).toBeNull(); store.close()
	}
})

test("duplicate assistant observers remain no-op", async () => {
	const { store, adapter, broker } = await runCompletionOrder([["status", "busy"], ["assistant", "same"], ["status", "idle"]]); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "same", failed: false })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(0); store.close()
})

test("assistant preserves idle, child events stay child and cannot complete a bound root", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({})); store.refreshRoute("user", "ctx")
	await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "busy" })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "late", failed: false })); expect(store.db.query("SELECT 1 FROM session_activity").get()).toBeNull(); await broker.drainBackground(); expect(adapter.sent).toHaveLength(0)
	const calls: any[] = [], hook = createControlEventHook((async (_e: string, _s: string, body: any) => { calls.push(body); if (body.status === "busy") await Bun.sleep(5); return {} }) as any, "e", "s", "owner", "token")
	await Promise.all([hook({ event: { type: "session.status", properties: { sessionID: "child", status: { type: "busy" } } } }), hook({ event: { type: "message.updated", properties: { info: { role: "assistant", sessionID: "child", id: "child-a", time: { completed: 1 } } } } })]); expect(calls.map((item) => item.rootSessionId)).toEqual(["child", "child"]); expect(calls.map((item) => item.method)).toEqual(["observe-status", "observe-assistant"])
	const childResponse = await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "child", status: "busy" })); expect((await childResponse.json() as any).observed).toBe(false); store.close()
})

class CountingAdapter extends MockWeChatAdapter { attempts = 0; slow?: Promise<void>; override async send(to: string, text: string, contextToken: string): Promise<void> { this.attempts++; if (this.slow) return this.slow; return super.send(to, text, contextToken) } }
function brokerWithAdapter(adapter: MockWeChatAdapter, fetcher: typeof fetch = async () => Response.json({ ok: true, accepted: true })) { const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true); return { store, broker: new BrokerService(store, adapter, "secret", "worker", fetcher) } }

test("locked controller alone refreshes context, receives replies and consumes checkpoints", async () => {
	let callbacks = 0
	const adapter = new CountingAdapter(), store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner", title: "Root" })
	const broker = new BrokerService(store, adapter, "secret", "worker", async () => { callbacks++; return Response.json({ ok: true, accepted: true }) })
	await broker.handleInbound({ id: "claim", fromUserId: "controller", contextToken: "c1", text: "id", cursorHint: "1" }); const sentAfterClaim = adapter.attempts
	for (const [id, text] of [["other-id", "id"], ["other-help", "help"], ["other-route", "#1\nx"]]) expect((await broker.handleInbound({ id, fromUserId: "other", contextToken: "evil", text, cursorHint: id })).reason).toBe("route-rejected")
	expect(adapter.attempts).toBe(sentAfterClaim); expect(callbacks).toBe(0); expect(store.route()).toMatchObject({ conversationId: "controller", contextToken: "c1" })
	const revision = store.control().revision; expect(store.openCheckpoint({ checkpointId: "locked-cp", requestKey: "locked-call", root: "root", owner: "owner", alias: 1, question: "q", choices: [], revision })).toBe(true); expect(store.activateCheckpoint("locked-cp")).toBe(true)
	await broker.handleInbound({ id: "attacker-answer", fromUserId: "other", contextToken: "evil2", text: "#1\na", cursorHint: "5" }); expect(store.checkpointState("locked-cp")).toBe("OPEN")
	await broker.handleInbound({ id: "controller-help", fromUserId: "controller", contextToken: "c2", text: "help", cursorHint: "6" }); expect(store.route()).toMatchObject({ conversationId: "controller", contextToken: "c2" }); expect(adapter.sent.at(-1)).toMatchObject({ to: "controller", contextToken: "c2", text: HELP_TEXT })
	await broker.handleInbound({ id: "controller-answer", fromUserId: "controller", contextToken: "c3", text: "#1\na", cursorHint: "7" }); expect(store.checkpointState("locked-cp")).toBe("OPEN"); expect(store.promptSubmission("controller-answer")?.state).toBe("SUBMITTED"); expect(callbacks).toBe(1); store.close()
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

test("wechat reply tool call ID capture is explicit and bounded", () => {
	const output = { args: {} }; captureReplyCallID({ tool: "wechat_reply", callID: "call-stable" }, output); expect(output.args).toEqual({ __wechatCallID: "call-stable" }); captureReplyCallID({ tool: "other", callID: "ignored" }, output); expect(output.args.__wechatCallID).toBe("call-stable")
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
	await broker.handleRequest(authenticatedRequest("back-global")); release(Response.json({ ok: true, accepted: true })); expect((await running).reason).toBe("control-changed-after-admission"); expect(adapter.sent).toHaveLength(0); expect(store.promptSubmission("race")?.state).toBe("UNKNOWN"); store.close()
})

test("back cancels legacy checkpoint while prompt admission is in flight", async () => {
	let release!: (value: Response) => void, entered!: () => void; const started = new Promise<void>((resolve) => { entered = resolve }), callback = new Promise<Response>((resolve) => { release = resolve })
	const { store, adapter, broker } = readyBroker(async () => { entered(); return callback }); store.refreshRoute("user", "ctx"); const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "race-checkpoint", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId
	const running = broker.handleInbound({ id: "race-answer", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" }); await started; await broker.handleRequest(authenticatedRequest("back-global")); release(Response.json({ ok: true, accepted: true })); expect((await running).reason).toBe("control-changed-after-admission"); expect(store.checkpointState(checkpointId)).toBe("CANCELLED"); expect(adapter.sent).toHaveLength(1); store.close()
})

test("legacy checkpoint remains OPEN when ordinary prompt admission is uncertain", async () => {
	const { store, broker } = readyBroker(async () => Response.json({ error: "uncertain" }, { status: 409 })); store.refreshRoute("user", "ctx")
	const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "answer-call", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId
	expect((await broker.handleInbound({ id: "answer-in", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" })).reason).toBe("unknown-no-replay"); expect(store.checkpointState(checkpointId)).toBe("OPEN"); expect(store.promptSubmission("answer-in")?.state).toBe("UNKNOWN"); store.close()
})

test("prompt callback uncertainty does not consume legacy checkpoint", async () => {
	const { store, broker } = readyBroker(async () => { throw new DOMException("timeout", "TimeoutError") }); store.refreshRoute("user", "ctx"); const opened = await broker.handleRequest(authenticatedRequest("request-input", { rootSessionId: "root", requestKey: "uncertain", question: "q", choices: [] })), checkpointId = (await opened.json() as any).checkpointId
	await broker.handleInbound({ id: "uncertain-answer", fromUserId: "user", contextToken: "ctx", text: "#1\na", cursorHint: "c" }); expect(store.checkpointState(checkpointId)).toBe("OPEN"); expect(store.promptSubmission("uncertain-answer")?.state).toBe("UNKNOWN"); store.setControl(false); expect(store.checkpointState(checkpointId)).toBe("CANCELLED"); store.close()
})

test("callback UNKNOWN and later observer events never emit generic completion", async () => {
	const { store, adapter, broker } = readyBroker(async () => { throw new DOMException("timeout", "TimeoutError") })
	await broker.handleInbound({ id: "direct-timeout", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "direct-a", failed: false })); await broker.handleRequest(authenticatedRequest("observe-status", { rootSessionId: "root", status: "idle" })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(0); expect(store.db.query("SELECT 1 FROM session_activity").get()).toBeNull(); store.close()
})

test("normal admission never creates direct-reply or completion state", async () => {
	const { store, adapter, broker } = readyBroker(async () => Response.json({ ok: true, accepted: true })); await broker.handleInbound({ id: "direct-success-in", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "c" }); await broker.handleRequest(authenticatedRequest("observe-assistant", { rootSessionId: "root", assistantMessageId: "direct-success", failed: false })); await broker.drainBackground(); expect(adapter.sent).toHaveLength(0); expect(store.db.query("SELECT 1 FROM pending_replies").get()).toBeNull(); expect(store.db.query("SELECT 1 FROM outbound").get()).toBeNull(); store.close()
})

test("completion excludes assistant IDs already owned by direct outbound", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); store.setControl(true); store.observeStatus("root", "owner", "busy"); store.db.query("INSERT INTO outbound VALUES('same-assistant','inbound-x','SENT','#1\\nx',?)").run(new Date().toISOString()); store.observeAssistant("root", "owner", "same-assistant", false); store.observeStatus("root", "owner", "idle"); expect(store.claimCompletion("root", "owner")).toBeUndefined(); store.close()
})

test("echo fingerprint includes context, suppresses repeats throughout TTL, and expires", async () => {
	const store = new Store(":memory:"); store.recordEcho("user", "ctx", "#1\nx", 100, 100); expect(store.matchesEcho("user", "ctx", "#1\nx", 150)).toBe(true); expect(store.matchesEcho("user", "ctx", "#1\nx", 160)).toBe(true); expect(store.matchesEcho("user", "other", "#1\nx", 160)).toBe(false); expect(store.matchesEcho("user", "ctx", "#1\nx", 201)).toBe(false); expect(store.sweepOutboundEchoes(201)).toBe(0); store.close()
	const ready = readyBroker(async () => Response.json({ ok: true, accepted: true })), payload = "#1\nreply"; ready.store.recordEcho("user", "ctx", payload)
	for (const id of ["echo-1", "echo-2"]) expect((await ready.broker.handleInbound({ id, fromUserId: "user", contextToken: "ctx", text: payload, cursorHint: id })).reason).toBe("outbound-echo"); expect(ready.adapter.sent).toHaveLength(0); ready.store.close()
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

test("clean schema v8 has bounded state tables, metadata and AUTOINCREMENT holes", () => {
	const store = new Store(":memory:")
	expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8); expect((store.db.query("SELECT value FROM meta WHERE key='schema_version'").get() as any).value).toBe("8")
	for (const table of ["prompt_submissions", "native_requests", "root_runtime", "typing_state", "binding_reap_state"]) expect(store.db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table)).toBeDefined()
	store.bind({ rootSessionId: "one", directory: "d", ownerInstance: "o" }); store.bind({ rootSessionId: "hole", directory: "d", ownerInstance: "o" }); store.db.query("DELETE FROM bindings WHERE root_session_id='hole'").run(); expect(store.bind({ rootSessionId: "three", directory: "d", ownerInstance: "o" })).toMatchObject({ alias: 2, registrationAlias: 3 })
	expect(() => store.db.query("UPDATE typing_state SET desired=2").run()).toThrow(); store.close()
})

test("existing schema v8 reopens idempotently and creates missing storage", () => {
	const filename = tempFile("v7-native-answer-guards"); let store = new Store(filename)
	store.db.exec("DROP TABLE native_answer_guards"); store.close(); store = new Store(filename)
	expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8)
	expect(store.db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_answer_guards'").get()).toBeDefined()
	expect(store.db.query("SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_native_answer_guard_request'").get()).toBeDefined()
	store.close(); cleanup(filename)
})

function createIndependentV7Fixture(filename: string): Database {
	const db = new Database(filename)
	db.exec(`PRAGMA user_version=7;
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE instances(instance_id TEXT PRIMARY KEY,endpoint TEXT NOT NULL,instance_token TEXT NOT NULL,heartbeat_ms INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE bindings(alias INTEGER PRIMARY KEY AUTOINCREMENT,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,title TEXT,active INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL);
CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,control_revision INTEGER NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE outbound(message_id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,payload TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE control_state(singleton INTEGER PRIMARY KEY,enabled INTEGER NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE checkpoints(checkpoint_id TEXT PRIMARY KEY,request_key TEXT,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,alias INTEGER NOT NULL,question TEXT NOT NULL,choices_json TEXT NOT NULL,state TEXT NOT NULL,inbound_id TEXT,control_revision INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE session_activity(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,running INTEGER NOT NULL,idle INTEGER NOT NULL,last_assistant_id TEXT,last_assistant_error INTEGER NOT NULL,direct_assistant_id TEXT,epoch INTEGER NOT NULL,run_id INTEGER NOT NULL,origin TEXT NOT NULL,candidate_run INTEGER,claimed_run INTEGER NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE control_outbound(outbound_id TEXT PRIMARY KEY,dedupe_key TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL,updated_at TEXT NOT NULL,logical_text TEXT,logical_hash TEXT);
CREATE TABLE outbound_echoes(echo_hash TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL,payload TEXT NOT NULL,expires_ms INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE audit(id INTEGER PRIMARY KEY,at TEXT NOT NULL,reason TEXT NOT NULL);
CREATE TABLE prompt_submissions(submission_id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,alias INTEGER NOT NULL,message_id TEXT NOT NULL UNIQUE,body TEXT NOT NULL,state TEXT NOT NULL,call_started INTEGER NOT NULL,prompt_message_id TEXT,rejection TEXT,control_revision INTEGER NOT NULL,admission_generation INTEGER,admission_finished INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE native_requests(request_id TEXT PRIMARY KEY,request_key TEXT NOT NULL UNIQUE,code TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,alias INTEGER NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,payload_json TEXT NOT NULL,inbound_id TEXT UNIQUE,resolution_json TEXT,control_revision INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE native_answer_guards(root_session_id TEXT NOT NULL,request_id TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(root_session_id,request_id));
CREATE TABLE root_runtime(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,status TEXT NOT NULL,generation INTEGER NOT NULL,busy_generation INTEGER,admission_count INTEGER NOT NULL,work_pending INTEGER NOT NULL,observed_ms INTEGER NOT NULL,lease_expires_ms INTEGER NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE typing_state(singleton INTEGER PRIMARY KEY,desired INTEGER NOT NULL,actual INTEGER,context_hash TEXT,attempt_ms INTEGER NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE global_route(singleton INTEGER PRIMARY KEY,conversation_id TEXT,context_token TEXT,updated_at TEXT);
INSERT INTO meta VALUES('schema_version','7');
INSERT INTO instances VALUES('owner','endpoint','secret-token',7,'instance-created');
INSERT INTO bindings VALUES(9,'root','directory','owner','Title',1,'binding-created','legacy-not-a-date');
INSERT INTO inbound VALUES('in','user','ctx','#9\nexact inbound','INJECTED','root','prompt-id',NULL,'inbound-time');
INSERT INTO pending_replies VALUES('pending','root','prompt-id',9,'UNKNOWN','assistant-id','#9\nexact pending',123,4,'pending-time');
INSERT INTO outbound VALUES('out','out-in','SENT','#9\nexact outbound','out-time');
INSERT INTO control_state VALUES(1,1,4);
INSERT INTO checkpoints VALUES('checkpoint','request-key','root','owner','user',9,'Question','["A","B"]','CANCELLED',NULL,4,'checkpoint-created','checkpoint-updated');
INSERT INTO session_activity VALUES('root','owner',0,1,NULL,0,NULL,1,2,'NONE',NULL,0,'activity-time');
INSERT INTO control_outbound VALUES('control','control-key','root','reply','SENT','#9\nexact control','user','ctx','control-time','exact control','logical-hash');
INSERT INTO outbound_echoes VALUES('echo','user','ctx','#9\nexact echo',9999999999999,'echo-time');
INSERT INTO audit VALUES(17,'audit-time','historical-audit');
INSERT INTO prompt_submissions VALUES('submission','submission-in','root','owner',9,'submission-message','exact prompt body','SUBMITTED',1,'prompt-id',NULL,4,3,1,'submission-created','submission-updated');
INSERT INTO native_requests VALUES('native','native-key','QABC234','root','owner',9,'QUESTION','RESOLVED','{"nested":["exact",1]}','native-in','[["answer"]]',4,'native-created','native-updated');
INSERT INTO root_runtime VALUES('root','owner','IDLE',4,NULL,0,0,0,0,'runtime-time');
INSERT INTO typing_state VALUES(1,0,NULL,NULL,0,'typing-time');
INSERT INTO global_route VALUES(1,'user','ctx','route-time');`)
	return db
}

function v7PreservationSnapshot(db: Database): Record<string, unknown> {
	return {
		bindings: db.query("SELECT alias,root_session_id,directory,owner_instance,title,active,created_at,updated_at FROM bindings ORDER BY alias").all(), instances: db.query("SELECT instance_id,endpoint,instance_token,heartbeat_ms,created_at FROM instances ORDER BY instance_id").all(), inbound: db.query("SELECT * FROM inbound ORDER BY message_id").all(), pending: db.query("SELECT * FROM pending_replies ORDER BY inbound_id").all(), prompt: db.query("SELECT * FROM prompt_submissions ORDER BY submission_id").all(), native: db.query("SELECT * FROM native_requests ORDER BY request_id").all(), checkpoint: db.query("SELECT * FROM checkpoints ORDER BY checkpoint_id").all(), outbound: db.query("SELECT * FROM outbound ORDER BY message_id").all(), controlOutbound: db.query("SELECT * FROM control_outbound ORDER BY outbound_id").all(), audit: db.query("SELECT * FROM audit ORDER BY id").all(), sequence: db.query("SELECT name,seq FROM sqlite_sequence ORDER BY name").all(),
	}
}

test("independent v7 to v8 migration preserves full durable rows and populated v8 reopens idempotently", () => {
	const filename = tempFile("v7-v8-preservation"), legacy = createIndependentV7Fixture(filename), before = v7PreservationSnapshot(legacy); legacy.close()
	let store = new Store(filename), backup = store.migrationBackupPath; expect(backup).toContain("pre-v8"); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8); expect(v7PreservationSnapshot(store.db)).toEqual(before); expect((store.db.query("SELECT binding_revision FROM bindings WHERE root_session_id='root'").get() as any).binding_revision).toBe(1); expect((store.db.query("SELECT instance_revision FROM instances WHERE instance_id='owner'").get() as any).instance_revision).toBe(1)
	const candidate = store.staleBindingCandidates(50_000)[0], recorded = store.recordBindingReapFailure(candidate, "timeout", 1); expect(recorded.recorded).toBe(true); const reapBefore = store.bindingReapState("root"); store.close(); store = new Store(filename); expect(store.migrationBackupPath).toBeUndefined(); expect(store.bindingReapState("root")).toEqual(reapBefore); expect(v7PreservationSnapshot(store.db)).toEqual(before); store.close(); cleanup(filename, backup)
})

test("stale candidate snapshots fingerprint tokens and persist spaced inconclusive failures", () => {
	const store = new Store(":memory:"); store.register("owner", "secret-token", "endpoint"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.db.query("UPDATE instances SET heartbeat_ms=0 WHERE instance_id='owner'").run()
	const candidate = store.staleBindingCandidates(50_000) [0]
	expect(candidate).toMatchObject({ rootSessionId: "root", ownerInstance: "owner", endpoint: "endpoint", observedHeartbeatMs: 0 }); expect(candidate.tokenFingerprint).toBe(sha256("secret-token")); expect(JSON.stringify(candidate)).not.toContain("secret-token")
	expect(store.recordBindingReapFailure(candidate, "timeout", 1)).toMatchObject({ recorded: true, state: { consecutiveFailures: 1 } })
	expect(store.recordBindingReapFailure(candidate, "refused", 2)).toMatchObject({ recorded: false, state: { consecutiveFailures: 1 } })
	const result = store.recordBindingReapFailure(candidate, "refused", 30_001), state = result.state!
	expect(result.recorded).toBe(true)
	expect(state).toMatchObject({ consecutiveFailures: 2, firstFailureMs: 1, lastFailureMs: 30_001, lastResult: "refused", observedHeartbeatMs: 0 }); expect(state.updatedAt).toBe(new Date(30_001).toISOString())
	store.touch("owner", "secret-token"); expect(store.bindingReapState("root")).toBeUndefined(); store.close()
})

test("threshold CAS requires persistent identity, spacing, current-run observations, and isolates roots", () => {
	const store = new Store(":memory:"); store.register("owner-a", "token-a", "endpoint-a"); store.register("owner-b", "token-b", "endpoint-b"); store.bind({ rootSessionId: "root-a", directory: "a", ownerInstance: "owner-a" }); store.bind({ rootSessionId: "root-b", directory: "b", ownerInstance: "owner-b" }); store.db.query("UPDATE instances SET heartbeat_ms=0").run()
	const [a, b] = store.staleBindingCandidates(50_000); for (const time of [0, 30_000, 60_000, 90_000, 120_000]) expect(store.recordBindingReapFailure(a, "timeout", time)).toMatchObject({ recorded: true, state: { consecutiveFailures: time / 30_000 + 1 } })
	expect(store.deactivateStaleBinding(a, 1)).toBe(false); expect(store.deactivateStaleBinding(b, 2)).toBe(false); expect(store.deactivateStaleBinding(a, 2)).toBe(true); expect(store.bindingForRoot("root-a")).toBeUndefined(); expect(store.bindingForRoot("root-b")).toBeDefined(); expect((store.db.query("SELECT reason FROM audit WHERE reason='stale-owner-probe-threshold'").get() as any).reason).toBe("stale-owner-probe-threshold")
	store.bind({ rootSessionId: "root-a", directory: "a2", ownerInstance: "owner-a" }); const rebound = store.staleBindingCandidates(50_000).find((item) => item.rootSessionId === "root-a")!; expect(store.bindingReapState("root-a")).toBeUndefined(); expect(store.recordBindingReapFailure(a, "timeout", 200_000)).toMatchObject({ recorded: false, state: undefined }); expect(store.deactivateDefinitiveNotRoot(a)).toBe(false); expect(store.deactivateDefinitiveNotRoot(rebound)).toBe(true); expect(store.bindingForRoot("root-a")).toBeUndefined(); expect((store.db.query("SELECT reason FROM audit WHERE reason='stale-owner-probe-not-root'").get() as any).reason).toBe("stale-owner-probe-not-root"); store.close()
})

test("one newly spaced observation cannot satisfy the current-run CAS count", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "endpoint"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.db.query("UPDATE instances SET heartbeat_ms=0").run()
	const candidate = store.staleBindingCandidates(50_000)[0]
	for (const time of [0, 30_000, 60_000, 90_000, 120_000]) expect(store.recordBindingReapFailure(candidate, "timeout", time).recorded).toBe(true)
	const accepted = store.recordBindingReapFailure(candidate, "timeout", 150_000); expect(accepted.recorded).toBe(true); expect(accepted.state?.consecutiveFailures).toBe(6); expect(store.deactivateStaleBinding(candidate, 1)).toBe(false); expect(store.deactivateStaleBinding(candidate, 2)).toBe(true); store.close()
})

test("revision identity rejects heartbeat races and tolerates malformed legacy audit timestamps", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "endpoint"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); const firstRevision = (store.db.query("SELECT binding_revision FROM bindings WHERE root_session_id='root'").get() as any).binding_revision; const firstInstanceRevision = (store.db.query("SELECT instance_revision FROM instances WHERE instance_id='owner'").get() as any).instance_revision
	store.db.query("UPDATE bindings SET updated_at='legacy-not-a-date' WHERE root_session_id='root'").run(); store.bind({ rootSessionId: "root", directory: "new-d", ownerInstance: "owner" }); expect((store.db.query("SELECT binding_revision,updated_at FROM bindings WHERE root_session_id='root'").get() as any)).toMatchObject({ binding_revision: firstRevision + 1, updated_at: expect.any(String) })
	store.db.query("UPDATE instances SET heartbeat_ms=0").run(); const candidate = store.staleBindingCandidates(50_000)[0]; store.touch("owner", "token"); expect((store.db.query("SELECT instance_revision FROM instances WHERE instance_id='owner'").get() as any).instance_revision).toBe(firstInstanceRevision + 1); expect(store.recordBindingReapFailure(candidate, "timeout", 1)).toMatchObject({ recorded: false, state: undefined }); store.close()
})

test("reaper threshold CAS has full cleanup parity and preserves durable payload bytes", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "endpoint"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("user", "ctx"); const revision = store.setControl(true).revision
	store.openCheckpoint({ checkpointId: "reap-checkpoint", requestKey: "reap-checkpoint", root: "root", owner: "owner", alias: 1, question: "q", choices: ["a"], revision }); store.beginInbound({ id: "reap-inbound", fromUserId: "user", contextToken: "ctx", text: "#1\nx", cursorHint: "reap" }); store.beginPending("reap-inbound", "root", 1, revision)
	store.claimPromptSubmission({ submissionId: "reap-prompt", inboundId: "reap-prompt-in", root: "root", owner: "owner", alias: 1, messageId: "reap-message", body: "prompt body" }); store.beginRuntimeAdmission("reap-prompt", "root", "owner"); store.markPromptCallStarted("reap-prompt")
	store.openNativeRequest({ requestId: "reap-native", requestKey: "reap-native", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: { exact: "native payload" } }); store.finishNativeAnnouncement("reap-native", true)
	store.db.query("INSERT INTO outbound VALUES('reap-out','reap-in','SENT',?,?)").run("#1\nbyte exact", "outbound-time"); store.db.query("INSERT INTO control_outbound(outbound_id,dedupe_key,root_session_id,kind,state,payload,conversation_id,context_token,updated_at,logical_text,logical_hash) VALUES('reap-control','reap-control','root','reply','SENT',?,'user','ctx','control-time','body','hash')").run("#1\ncontrol exact")
	const durableBefore = store.db.query("SELECT message_id,inbound_id,state,payload,updated_at FROM outbound UNION ALL SELECT outbound_id,dedupe_key,state,payload,updated_at FROM control_outbound ORDER BY message_id").all(); const nativePayload = (store.db.query("SELECT payload_json FROM native_requests WHERE request_id='reap-native'").get() as any).payload_json, promptBody = (store.db.query("SELECT body FROM prompt_submissions WHERE submission_id='reap-prompt'").get() as any).body, inboundText = (store.db.query("SELECT text FROM inbound WHERE message_id='reap-inbound'").get() as any).text
	store.db.query("UPDATE instances SET heartbeat_ms=0").run(); const candidate = store.staleBindingCandidates(50_000)[0]; for (const time of [0, 30_000, 60_000, 90_000, 120_000]) store.recordBindingReapFailure(candidate, "timeout", time); expect(store.deactivateStaleBinding(candidate, 2)).toBe(true)
	expect(store.checkpointState("reap-checkpoint")).toBe("CANCELLED"); expect(store.pendingState("reap-inbound")).toBe("UNKNOWN"); expect(store.state("reap-inbound")).toBe("UNKNOWN"); expect(store.promptSubmission("reap-prompt")).toMatchObject({ state: "UNKNOWN", admissionFinished: true }); expect(store.nativeRequest("reap-native")?.state).toBe("CANCELLED_REMOTE"); expect(store.runtime("root")).toMatchObject({ status: "IDLE", workPending: false }); expect(store.db.query("SELECT message_id,inbound_id,state,payload,updated_at FROM outbound UNION ALL SELECT outbound_id,dedupe_key,state,payload,updated_at FROM control_outbound ORDER BY message_id").all()).toEqual(durableBefore); expect((store.db.query("SELECT payload_json FROM native_requests WHERE request_id='reap-native'").get() as any).payload_json).toBe(nativePayload); expect((store.db.query("SELECT body FROM prompt_submissions WHERE submission_id='reap-prompt'").get() as any).body).toBe(promptBody); expect((store.db.query("SELECT text FROM inbound WHERE message_id='reap-inbound'").get() as any).text).toBe(inboundText); expect((store.db.query("SELECT reason FROM audit WHERE reason='stale-owner-probe-threshold'").get() as any).reason).toBe("stale-owner-probe-threshold"); store.close()
})

test("v5 to v6 snapshot preserves history and cancels only active legacy work without replay", () => {
	const filename = tempFile("v5-v6"), seed = new Store(filename); seed.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); seed.refreshRoute("recipient", "context"); const revision = seed.control().revision
	seed.beginInbound({ id: "legacy", fromUserId: "recipient", contextToken: "context", text: "#1\nx", cursorHint: "c" }); seed.beginPending("legacy", "root", 1, revision)
	seed.db.query("INSERT INTO checkpoints(checkpoint_id,request_key,root_session_id,owner_instance,conversation_id,alias,question,choices_json,state,inbound_id,control_revision,created_at,updated_at) VALUES('active','active','root','owner','recipient',1,'q','[]','OPEN',NULL,?,'now','now')").run(revision)
	seed.db.query("INSERT INTO checkpoints(checkpoint_id,request_key,root_session_id,owner_instance,conversation_id,alias,question,choices_json,state,inbound_id,control_revision,created_at,updated_at) VALUES('unknown','unknown','other-root','owner','recipient',2,'q','[]','UNKNOWN',NULL,?,'now','now')").run(revision)
	seed.db.query("INSERT INTO audit(at,reason) VALUES('then','historical')").run(); seed.db.exec("DROP TABLE prompt_submissions; DROP TABLE native_requests; DROP TABLE root_runtime; DROP TABLE typing_state; DELETE FROM meta WHERE key='schema_version'; INSERT INTO meta VALUES('schema_version','5'); PRAGMA user_version=5"); seed.close()
	const store = new Store(filename), backup = store.migrationBackupPath!; expect(backup).toContain("pre-v6"); expect(store.bindingForRoot("root")?.alias).toBe(1); expect(store.route()).toMatchObject({ conversationId: "recipient", contextToken: "context" }); expect(store.control().revision).toBe(revision); expect(store.checkpointState("active")).toBe("CANCELLED"); expect(store.checkpointState("unknown")).toBe("CANCELLED"); expect(store.pendingState("legacy")).toBe("UNKNOWN"); expect(store.state("legacy")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='legacy'").get() as any).reason).toBe("schema-v6-semantic-change"); expect(store.db.query("SELECT 1 FROM audit WHERE reason='historical'").get()).toBeDefined()
	const snapshot = new Database(backup); expect((snapshot.query("PRAGMA user_version").get() as any).user_version).toBe(5); expect(snapshot.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prompt_submissions'").get()).toBeNull(); snapshot.close(); store.close(); cleanup(filename, backup)
})

test("prompt claims enforce legal conditional transitions and back uses pre-call certainty", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" })
	expect(store.claimPromptSubmission({ submissionId: "before", inboundId: "in-before", root: "root", owner: "owner", alias: 1, messageId: "msg-before", body: "x" })?.state).toBe("SUBMITTING"); expect(store.promptSubmission("before")?.controlRevision).toBe(store.control().revision)
	expect(store.claimPromptSubmission({ submissionId: "illegal", inboundId: "in-illegal", root: "root", owner: "owner", alias: 1, messageId: "msg-illegal", body: "x" })).toBeDefined(); expect(store.finishPromptSubmission("illegal", "SUBMITTED", "impossible")).toBe(false); expect(store.promptSubmission("illegal")?.state).toBe("SUBMITTING")
	expect(store.claimPromptSubmission({ submissionId: "success", inboundId: "in-success", root: "root", owner: "owner", alias: 1, messageId: "msg-success", body: "x" })).toBeDefined(); expect(store.markPromptCallStarted("success")).toBe(true); expect(store.markPromptCallStarted("success")).toBe(true); expect(store.finishPromptSubmission("success", "REJECTED", "uncertain")).toBe(false); expect(store.finishPromptSubmission("success", "CANCELLED")).toBe(false); expect(store.finishPromptSubmission("success", "SUBMITTED", "sdk-message")).toBe(true); expect(store.finishPromptSubmission("success", "SUBMITTED", "sdk-message")).toBe(true); expect(store.finishPromptSubmission("success", "UNKNOWN")).toBe(false); expect(store.promptSubmission("success")).toMatchObject({ state: "SUBMITTED", promptMessageId: "sdk-message" })
	expect(store.claimPromptSubmission({ submissionId: "during", inboundId: "in-during", root: "root", owner: "owner", alias: 1, messageId: "msg-during", body: "x" })).toBeDefined(); expect(store.markPromptCallStarted("during")).toBe(true); expect(store.claimPromptSubmission({ submissionId: "collision", inboundId: "in-during", root: "root", owner: "owner", alias: 1, messageId: "another-message", body: "x" })).toBeUndefined(); store.setControl(false); expect(store.promptSubmission("before")?.state).toBe("CANCELLED"); expect(store.promptSubmission("during")?.state).toBe("UNKNOWN"); store.close()
})

test("native requests use root identity independent of copied aliases", () => {
	const occupied = new Set<string>(), first = allocateRequestCode("QUESTION", "same", (code) => occupied.has(code)); occupied.add(first); const second = allocateRequestCode("QUESTION", "same", (code) => occupied.has(code)); expect(first).toMatch(/^Q[A-Z2-7]{6}$/); expect(second).not.toBe(first)
	const store = new Store(":memory:"); store.bind({ rootSessionId: "earlier", directory: "d", ownerInstance: "owner" }); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); expect(store.nativeQuery("root")).toEqual({ kind: "NONE" })
	const one = store.openNativeRequest({ requestId: "one", requestKey: "key-one", root: "root", owner: "owner", alias: 2, kind: "QUESTION", payload: { questions: [["A"]] } })!; expect(one.state).toBe("ANNOUNCING"); expect(store.finishNativeAnnouncement("one", false)).toBe(true); expect(store.nativeQuery("root").kind).toBe("ONE"); expect(store.nativeQuery("earlier")).toEqual({ kind: "NONE" })
	store.deactivateBinding("earlier", "owner"); expect(store.bindingForRoot("root")?.alias).toBe(1); expect(store.nativeQuery("root").kind).toBe("ONE")
	expect(store.nativeRequestReplay({ requestKey: "key-one", requestId: "one", root: "root", owner: "owner", kind: "QUESTION", payload: { questions: [["A"]] }, controlRevision: one.controlRevision })).toMatchObject({ result: "EXACT" })
	expect(store.nativeRequestReplay({ requestKey: "key-one", requestId: "one", root: "earlier", owner: "owner", kind: "QUESTION", payload: { questions: [["A"]] }, controlRevision: one.controlRevision })).toEqual({ result: "MISMATCH" })
	expect(store.claimNativeResolution("one", "answer")).toBe(true); expect(store.finishNativeResolution("one", "UNKNOWN")).toBe(true); expect(store.claimNativeResolution("one", "retry")).toBe(false)
	store.openNativeRequest({ requestId: "two", requestKey: "key-two", root: "root", owner: "owner", alias: 1, kind: "PERMISSION", payload: { permission: "write" } }); expect(store.finishNativeAnnouncement("two", true)).toBe(true); expect(store.nativeQuery("root").kind).toBe("MULTIPLE"); expect(store.claimNativeResolution("two", "answer")).toBe(false); expect(store.settleNativeTerminal("one", "RESOLVED", [["A"]])).toBe(true); expect(store.settleNativeTerminal("two", "REJECTED", "reject")).toBe(true); expect(store.nativeQuery("root").kind).toBe("NONE"); store.close()
})

test("native answer guards are root scoped and cleared by inactive lifecycle transitions", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "a", directory: "d", ownerInstance: "owner" }); store.bind({ rootSessionId: "b", directory: "d", ownerInstance: "owner" })
	for (const [root, id, alias] of [["a", "guard-a", 1], ["b", "guard-b", 2]] as const) { store.openNativeRequest({ requestId: id, requestKey: id, root, owner: "owner", alias, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement(id, true); expect(store.recordNativeAnswerGuard(root, id)).toBe(true) }
	expect(store.consumeNativeAnswerGuard("a", "guard-b")).toBe(false); expect(store.consumeNativeAnswerGuard("a", "guard-a")).toBe(true); expect(store.consumeNativeAnswerGuard("a", "guard-a")).toBe(false)
	expect(store.recordNativeAnswerGuard("a", "guard-a")).toBe(true); expect(store.deactivateBinding("a", "owner")).toBe(true); expect(store.consumeNativeAnswerGuard("a", "guard-a")).toBe(false); expect(store.consumeNativeAnswerGuard("b", "guard-b")).toBe(true)
	expect(store.recordNativeAnswerGuard("b", "guard-b")).toBe(true); expect(store.settleNativeTerminal("guard-b", "RESOLVED")).toBe(true); expect(store.consumeNativeAnswerGuard("b", "guard-b")).toBe(false)
	store.close()
})

test("native answer guards clear on unregister and control shutdown", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" })
	store.openNativeRequest({ requestId: "guard", requestKey: "guard", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement("guard", true); expect(store.recordNativeAnswerGuard("root", "guard")).toBe(true); expect(store.unregister("owner", "token")).toBe(true); expect(store.consumeNativeAnswerGuard("root", "guard")).toBe(false)
	store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.openNativeRequest({ requestId: "guard-next", requestKey: "guard-next", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement("guard-next", true); expect(store.recordNativeAnswerGuard("root", "guard-next")).toBe(true); store.setControl(false); expect(store.consumeNativeAnswerGuard("root", "guard-next")).toBe(false); expect((store.db.query("PRAGMA user_version").get() as any).user_version).toBe(8); expect(store.db.query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_native_answer_guard_request'").get()).toBeDefined(); store.close()
})

test("unregister fully deactivates one owned root and all dependent lifecycle state", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("recipient", "ctx"); const revision = store.setControl(true).revision
	store.openCheckpoint({ checkpointId: "checkpoint", requestKey: "checkpoint", root: "root", owner: "owner", alias: 1, question: "q", choices: [], revision })
	store.db.query("INSERT INTO session_activity(root_session_id,owner_instance,running,idle,origin,candidate_run,updated_at) VALUES('root','owner',1,0,'LOCAL',1,'now')").run()
	store.beginInbound({ id: "waiting", fromUserId: "recipient", contextToken: "ctx", text: "#1\nx", cursorHint: "cursor" }); store.beginPending("waiting", "root", 1, revision)
	store.claimPromptSubmission({ submissionId: "prompt", inboundId: "prompt-in", root: "root", owner: "owner", alias: 1, body: "x" }); store.beginRuntimeAdmission("prompt", "root", "owner", 100, 1000); store.markPromptCallStarted("prompt")
	store.openNativeRequest({ requestId: "native", requestKey: "native", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement("native", true); expect(store.recordNativeAnswerGuard("root", "native")).toBe(true)
	expect(store.unregister("owner", "wrong")).toBe(false); expect(store.bindingForRoot("root")).toBeDefined()
	expect(store.unregister("owner", "token")).toBe(true); expect(store.instance("owner")).toBeUndefined(); expect(store.bindingForRoot("root")).toBeUndefined(); expect(store.checkpointState("checkpoint")).toBe("CANCELLED")
	expect(store.pendingState("waiting")).toBe("UNKNOWN"); expect(store.state("waiting")).toBe("UNKNOWN"); expect((store.db.query("SELECT reason FROM inbound WHERE message_id='waiting'").get() as any).reason).toBe("binding-deactivated")
	expect((store.db.query("SELECT running,idle,origin,candidate_run AS candidate FROM session_activity WHERE root_session_id='root'").get() as any)).toEqual({ running: 0, idle: 1, origin: "NONE", candidate: null })
	expect(store.promptSubmission("prompt")).toMatchObject({ state: "UNKNOWN", admissionFinished: true }); expect(store.nativeRequest("native")?.state).toBe("CANCELLED_REMOTE"); expect(store.consumeNativeAnswerGuard("root", "native")).toBe(false); expect(store.runtime("root")).toMatchObject({ status: "IDLE", workPending: false }); store.close()
})

test("unregister deactivates multiple roots and later reactivation restores dense current aliases", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.register("live", "live-token", "http://127.0.0.1:2")
	store.bind({ rootSessionId: "first", directory: "first", ownerInstance: "owner" }); store.bind({ rootSessionId: "second", directory: "second", ownerInstance: "owner" }); store.bind({ rootSessionId: "live-root", directory: "live", ownerInstance: "live" })
	store.db.query("UPDATE instances SET heartbeat_ms=0 WHERE instance_id='owner'").run(); expect(store.authenticate("owner", "token")).toBe(false); expect(store.unregister("owner", "wrong-token")).toBe(false); expect(store.instance("owner")).toBeDefined(); expect(store.bindings()).toHaveLength(3)
	expect(store.unregister("owner", "token")).toBe(true); expect(store.unregister("owner", "token")).toBe(false); expect(store.instance("owner")).toBeUndefined(); expect(store.bindings().map((binding) => [binding.rootSessionId, binding.alias])).toEqual([["live-root", 1]])
	store.register("next", "next-token", "http://127.0.0.1:3"); expect(store.bind({ rootSessionId: "second", directory: "second-new", ownerInstance: "next" })).toMatchObject({ alias: 1, registrationAlias: 2 }); expect(store.bindingForRoot("live-root")?.alias).toBe(2)
	expect(store.bind({ rootSessionId: "first", directory: "first-new", ownerInstance: "next" })).toMatchObject({ alias: 1, registrationAlias: 1 }); expect(store.bindings().map((binding) => [binding.rootSessionId, binding.alias])).toEqual([["first", 1], ["second", 2], ["live-root", 3]]); store.close()
})

test("unregister rolls back binding cleanup when instance removal aborts", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.claimPromptSubmission({ submissionId: "prompt", inboundId: "in", root: "root", owner: "owner", alias: 1, body: "x" }); store.beginRuntimeAdmission("prompt", "root", "owner", 100, 1000)
	store.db.exec("CREATE TRIGGER abort_instance_delete BEFORE DELETE ON instances WHEN OLD.instance_id='owner' BEGIN SELECT RAISE(ABORT,'forced rollback'); END")
	expect(() => store.unregister("owner", "token")).toThrow("forced rollback"); expect(store.instance("owner")).toBeDefined(); expect(store.bindingForRoot("root")).toMatchObject({ active: true, ownerInstance: "owner" }); expect(store.promptSubmission("prompt")?.state).toBe("SUBMITTING"); expect(store.runtime("root")).toMatchObject({ status: "QUEUED", workPending: true }); store.close()
})

test("orphan binding sweep is row-liveness based, idempotent, and preserves live owners", () => {
	const store = new Store(":memory:"); store.register("orphan", "token", "http://127.0.0.1:1"); store.register("live", "live-token", "http://127.0.0.1:2"); store.bind({ rootSessionId: "orphan-root", directory: "orphan", ownerInstance: "orphan" }); store.bind({ rootSessionId: "live-root", directory: "live", ownerInstance: "live" })
	store.claimPromptSubmission({ submissionId: "orphan-prompt", inboundId: "orphan-in", root: "orphan-root", owner: "orphan", alias: 1, body: "x" }); store.beginRuntimeAdmission("orphan-prompt", "orphan-root", "orphan", 100, 1000); store.openNativeRequest({ requestId: "orphan-native", requestKey: "orphan-native", root: "orphan-root", owner: "orphan", alias: 1, kind: "QUESTION", payload: {} })
	store.db.query("DELETE FROM instances WHERE instance_id='orphan'").run(); store.db.query("UPDATE instances SET heartbeat_ms=0 WHERE instance_id='live'").run()
	expect(store.sweepOrphanBindings()).toBe(1); expect(store.sweepOrphanBindings()).toBe(0); expect(store.bindingForRoot("orphan-root")).toBeUndefined(); expect(store.bindingForRoot("live-root")).toMatchObject({ alias: 1, ownerInstance: "live" }); expect(store.promptSubmission("orphan-prompt")?.state).toBe("CANCELLED"); expect(store.nativeRequest("orphan-native")?.state).toBe("CANCELLED_REMOTE"); expect(store.runtime("orphan-root")).toMatchObject({ status: "IDLE", workPending: false })
	store.register("next", "next-token", "http://127.0.0.1:3"); expect(store.bind({ rootSessionId: "orphan-root", directory: "again", ownerInstance: "next" })).toMatchObject({ alias: 1, registrationAlias: 1 }); expect(store.bindingForRoot("live-root")?.alias).toBe(2); store.close()
})

test("uncertain native relay stays OPEN and resolves remotely without relay retry", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.openNativeRequest({ requestId: "uncertain-relay", requestKey: "uncertain-key", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: { question: "Choose" } }); expect(store.finishNativeAnnouncement("uncertain-relay", false)).toBe(true); expect(store.finishNativeAnnouncement("uncertain-relay", false)).toBe(true); expect(store.nativeRequest("uncertain-relay")?.state).toBe("OPEN"); expect(store.claimNativeResolution("uncertain-relay", "remote-answer")).toBe(true); expect(store.finishNativeResolution("uncertain-relay", "RESOLVED", [["yes"]])).toBe(true); expect(store.nativeRequest("uncertain-relay")?.state).toBe("RESOLVED"); store.close()
})

test("startup recovery transitions v6 uncertainty states and resets admission counts", () => {
	const filename = tempFile("v6-recovery"), store = new Store(filename); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); store.refreshRoute("recipient", "ctx")
	store.claimPromptSubmission({ submissionId: "prompt", inboundId: "in", root: "root", owner: "owner", alias: 1, messageId: "msg", body: "x" }); store.openNativeRequest({ requestId: "announce", requestKey: "announce", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} }); store.openNativeRequest({ requestId: "resolve", requestKey: "resolve", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} }); store.finishNativeAnnouncement("resolve", true); store.claimNativeResolution("resolve", "answer"); expect(store.beginRuntimeAdmission("prompt", "root", "owner", 100, 1000)).toBe(1); store.close()
	const recovered = new Store(filename); expect(recovered.promptSubmission("prompt")).toMatchObject({ state: "UNKNOWN", admissionFinished: true }); expect(recovered.nativeRequest("announce")?.state).toBe("OPEN"); expect(recovered.nativeRequest("resolve")?.state).toBe("UNKNOWN"); expect(recovered.runtime("root")).toMatchObject({ status: "IDLE", admissionCount: 0, workPending: false }); recovered.close(); cleanup(filename)
})

test("runtime generation protects newer work, aggregates roots, honors authoritative idle and lease expiry", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "a", directory: "d", ownerInstance: "owner" }); store.bind({ rootSessionId: "b", directory: "d", ownerInstance: "owner" }); store.refreshRoute("recipient", "ctx")
	const claim = (id: string, root: string, alias: number) => store.claimPromptSubmission({ submissionId: id, inboundId: `in-${id}`, root, owner: "owner", alias, messageId: `msg-${id}`, body: "x" })
	claim("a1", "a", 1); const a1 = store.beginRuntimeAdmission("a1", "a", "owner", 100, 1000)!; expect(store.beginRuntimeAdmission("a1", "a", "owner", 101, 1000)).toBe(a1); expect(store.observeRuntimeStatus("a", "owner", "IDLE", a1, 102)).toBe(false); expect(store.runtime("a")).toMatchObject({ status: "QUEUED", admissionCount: 1, workPending: true, busyGeneration: null })
	expect(store.finishRuntimeAdmission("a1", "a", "owner")).toBe(true); expect(store.finishRuntimeAdmission("a1", "a", "owner")).toBe(true); expect(store.runtime("a")?.admissionCount).toBe(0); expect(store.observeRuntimeStatus("a", "owner", "BUSY", a1, 103)).toBe(true); expect(store.observeRuntimeStatus("a", "owner", "IDLE", a1, 104)).toBe(true); expect(store.runtime("a")?.workPending).toBe(false)
	claim("duplicate-one", "a", 1); const duplicateOne = store.beginRuntimeAdmission("duplicate-one", "a", "owner", 105, 1000)!; claim("duplicate-two", "a", 1); const duplicateTwo = store.beginRuntimeAdmission("duplicate-two", "a", "owner", 106, 1000)!; expect(store.runtime("a")?.admissionCount).toBe(2); expect(store.finishRuntimeAdmission("duplicate-one", "a", "owner")).toBe(true); expect(store.runtime("a")?.admissionCount).toBe(1); expect(store.finishRuntimeAdmission("duplicate-one", "a", "owner")).toBe(true); expect(store.runtime("a")?.admissionCount).toBe(1); expect(store.finishRuntimeAdmission("duplicate-two", "a", "owner")).toBe(true); expect(store.runtime("a")?.admissionCount).toBe(0); expect(store.syncRuntimeAuthoritative("a", "owner", "IDLE", duplicateTwo, 107)).toBe(true); expect(duplicateTwo).toBeGreaterThan(duplicateOne)
	claim("a2", "a", 1); const a2 = store.beginRuntimeAdmission("a2", "a", "owner", 110, 1000)!; expect(store.finishRuntimeAdmission("a2", "a", "owner")).toBe(true); expect(store.observeRuntimeStatus("a", "owner", "BUSY", a2, 111)).toBe(true); claim("a3", "a", 1); const a3 = store.beginRuntimeAdmission("a3", "a", "owner", 112, 1000)!; expect(a3).toBeGreaterThan(a2); expect(store.finishRuntimeAdmission("a3", "a", "owner")).toBe(true); expect(store.observeRuntimeStatus("a", "owner", "IDLE", a2, 113)).toBe(false); expect(store.runtime("a")).toMatchObject({ generation: a3, status: "BUSY", workPending: true }); expect(store.observeRuntimeStatus("a", "owner", "IDLE", a3, 114)).toBe(false); expect(store.observeRuntimeStatus("a", "owner", "BUSY", a3, 115)).toBe(true); expect(store.observeRuntimeStatus("a", "owner", "IDLE", a3, 116)).toBe(true)
	claim("b1", "b", 2); const b1 = store.beginRuntimeAdmission("b1", "b", "owner", 120, 1000)!; expect(store.finishRuntimeAdmission("b1", "b", "owner")).toBe(true); expect(store.desiredTyping(121)).toBe(true); expect(store.syncRuntimeAuthoritative("b", "owner", "IDLE", b1, 122)).toBe(true); expect(store.desiredTyping(123)).toBe(false)
	claim("lease", "a", 1); const leased = store.beginRuntimeAdmission("lease", "a", "owner", 200, 10)!; expect(store.desiredTyping(205)).toBe(true); expect(store.expireRuntimeLeases(211)).toBe(0); expect(store.runtime("a")).toMatchObject({ generation: leased, status: "QUEUED", admissionCount: 1, workPending: true }); expect(store.syncRuntimeAuthoritative("a", "owner", "IDLE", leased, 212, 10)).toBe(true); expect(store.runtime("a")).toMatchObject({ generation: leased, status: "QUEUED", admissionCount: 1, workPending: true, leaseExpiresMs: 222 }); expect(store.finishRuntimeAdmission("lease", "a", "owner")).toBe(true); expect(store.expireRuntimeLeases(223)).toBe(1); store.close()
})

test("definite prompt rejection clears only its current queued generation", () => {
	const store = new Store(":memory:"), claim = (id: string) => store.claimPromptSubmission({ submissionId: id, inboundId: `in-${id}`, root: "root", owner: "owner", alias: 1, body: id })!
	claim("rejected"); const rejectedGeneration = store.beginRuntimeAdmission("rejected", "root", "owner", 100, 1000)!; expect(store.rejectPromptSubmissionNoEffect("rejected", "root", "owner", "not-root")).toBe(true); expect(store.promptSubmission("rejected")).toMatchObject({ state: "REJECTED", admissionFinished: true }); expect(store.runtime("root")).toMatchObject({ status: "IDLE", generation: rejectedGeneration + 1, admissionCount: 0, workPending: false })
	claim("busy"); const busyGeneration = store.beginRuntimeAdmission("busy", "root", "owner", 110, 1000)!; expect(store.observeRuntimeStatus("root", "owner", "BUSY", busyGeneration, 111, 1000)).toBe(true); expect(store.rejectPromptSubmissionNoEffect("busy", "root", "owner")).toBe(true); expect(store.runtime("root")).toMatchObject({ status: "BUSY", generation: busyGeneration, admissionCount: 0, workPending: true })
	claim("stale"); const staleGeneration = store.beginRuntimeAdmission("stale", "root", "owner", 120, 1000)!; claim("newer"); const newerGeneration = store.beginRuntimeAdmission("newer", "root", "owner", 121, 1000)!; expect(newerGeneration).toBeGreaterThan(staleGeneration); expect(store.observeRuntimeStatus("root", "owner", "BUSY", newerGeneration, 122, 1000)).toBe(true); expect(store.rejectPromptSubmissionNoEffect("stale", "root", "owner")).toBe(true); expect(store.runtime("root")).toMatchObject({ status: "BUSY", generation: newerGeneration, admissionCount: 1, workPending: true }); expect(store.rejectPromptSubmissionNoEffect("stale", "root", "owner")).toBe(true); expect(store.runtime("root")?.admissionCount).toBe(1); expect(store.finishRuntimeAdmission("newer", "root", "owner")).toBe(true); expect(store.runtime("root")).toMatchObject({ status: "BUSY", generation: newerGeneration, admissionCount: 0, workPending: true }); store.close()
})

test("queued active admissions are maintenance snapshots and authoritative idle only renews them", () => {
	const store = new Store(":memory:"); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "directory", ownerInstance: "owner" })
	store.claimPromptSubmission({ submissionId: "queued", inboundId: "queued-in", root: "root", owner: "owner", alias: 1, body: "queued" }); const generation = store.beginRuntimeAdmission("queued", "root", "owner", 100, 10)!
	expect(store.activeRuntimeSnapshots()).toEqual([expect.objectContaining({ rootSessionId: "root", ownerInstance: "owner", directory: "directory", generation, status: "QUEUED", admissionCount: 1 })])
	expect(store.syncRuntimeAuthoritative("root", "owner", "IDLE", generation, 105, 10)).toBe(true); expect(store.runtime("root")).toMatchObject({ generation, status: "QUEUED", admissionCount: 1, workPending: true, leaseExpiresMs: 115 }); expect(store.expireRuntimeLeases(116)).toBe(0)
	expect(store.finishRuntimeAdmission("queued", "root", "owner")).toBe(true); expect(store.activeRuntimeSnapshots()).toEqual([]); expect(store.expireRuntimeLeases(116)).toBe(1); store.close()
})

test("deactivation atomically cancels owned work and cannot affect reactivation", () => {
	const store = new Store(":memory:"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner", title: "History" }); store.refreshRoute("recipient", "ctx")
	store.claimPromptSubmission({ submissionId: "prompt", inboundId: "in", root: "root", owner: "owner", alias: 1, body: "x" }); store.beginRuntimeAdmission("prompt", "root", "owner", 100, 1000); store.markPromptCallStarted("prompt"); store.openNativeRequest({ requestId: "native", requestKey: "native", root: "root", owner: "owner", alias: 1, kind: "QUESTION", payload: {} })
	expect(store.deactivateBinding("root", "owner")).toBe(true); expect(store.bindingForRoot("root")).toBeUndefined(); expect((store.db.query("SELECT alias,title,active FROM bindings WHERE root_session_id='root'").get() as any)).toMatchObject({ alias: 1, title: "History", active: 0 }); expect(store.promptSubmission("prompt")).toMatchObject({ state: "UNKNOWN", admissionFinished: true }); expect(store.nativeRequest("native")?.state).toBe("CANCELLED_REMOTE"); expect(store.runtime("root")).toMatchObject({ status: "IDLE", workPending: false }); expect(store.typingDesired()).toBe(false); expect(store.deactivateBinding("root", "owner")).toBe(false)
	store.bind({ rootSessionId: "root", directory: "new", ownerInstance: "owner", title: "Again" }); store.claimPromptSubmission({ submissionId: "new", inboundId: "new-in", root: "root", owner: "owner", alias: 1, body: "new" }); const generation = store.beginRuntimeAdmission("new", "root", "owner", 200, 1000)!; expect(store.deactivateBinding("root", "other")).toBe(false); expect(store.finishRuntimeAdmission("prompt", "root", "owner")).toBe(true); expect(store.rejectPromptSubmissionNoEffect("prompt", "root", "owner")).toBe(false); expect(store.runtime("root")).toMatchObject({ generation, status: "QUEUED", admissionCount: 1, workPending: true }); store.close()
})
