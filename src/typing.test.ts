import { describe, expect, test } from "bun:test"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import * as path from "node:path"
import { Store } from "./core"
import { accountConfig, TypingCoordinator } from "./typing"
import { BrokerService } from "./broker"
import { MockWeChatAdapter } from "./adapter"
import { runWorker } from "./worker"

function setup() {
	const store = new Store(":memory:")
	store.setControl(true)
	store.acceptInboundRoute("user", "context", true)
	return store
}

describe("account loading", () => {
	test("loads a regular contained HTTPS account file", async () => {
		await withAccounts(async (accountsDir) => {
			await writeFile(path.join(accountsDir, "selected.json"), JSON.stringify({ token: "secret", baseUrl: "https://example.test/api" }))
			await expect(accountConfig("selected", { accountsDir })).resolves.toEqual({ token: "secret", baseUrl: "https://example.test/api", accountId: "selected" })
		})
	})
	test("rejects plaintext HTTP base URLs", async () => {
		await withAccounts(async (accountsDir) => {
			await writeFile(path.join(accountsDir, "selected.json"), JSON.stringify({ token: "secret", baseUrl: "http://127.0.0.1:8080" }))
			await expect(accountConfig("selected", { accountsDir })).rejects.toThrow("invalid Weixin base URL")
		})
	})
	test("rejects oversized account files before parsing", async () => {
		await withAccounts(async (accountsDir) => {
			await writeFile(path.join(accountsDir, "selected.json"), " ".repeat(64 * 1024 + 1))
			await expect(accountConfig("selected", { accountsDir })).rejects.toThrow("Weixin account file too large")
		})
	})
	test("rejects symlinked account files outside the account directory", async () => {
		await withAccounts(async (accountsDir, root) => {
			const outside = path.join(root, "outside.json")
			await writeFile(outside, JSON.stringify({ token: "secret", baseUrl: "https://example.test" }))
			await symlink(outside, path.join(accountsDir, "selected.json"), "file")
			await expect(accountConfig("selected", { accountsDir })).rejects.toThrow("invalid Weixin account file")
		})
	})
	test("does not select non-regular JSON entries", async () => {
		await withAccounts(async (accountsDir) => {
			await mkdir(path.join(accountsDir, "selected.json"))
			await expect(accountConfig("selected", { accountsDir })).rejects.toThrow("invalid Weixin account file")
		})
	})
})

describe("typing coordinator", () => {
	test("turns off at startup and aggregates active work", async () => {
		const store = setup(), calls: number[] = []
		const typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://example.test", accountId: "a" }), api: { getConfig: async () => ({ typing_ticket: "ticket" }), sendTyping: async (_u, _t, status) => { calls.push(status) }, }, debounceMs: 0 })
		await typing.startup()
		expect(calls).toEqual([2])
		const submission = store.claimPromptSubmission({ submissionId: "s", inboundId: "i", root: "root", owner: "owner", alias: 1, messageId: "m", body: "hello", revision: 1 })
		expect(submission).toBeTruthy()
		store.beginRuntimeAdmission("s", "root", "owner")
		await typing.flush()
		expect(calls.at(-1)).toBe(1)
		store.finishRuntimeAdmission("s", "root", "owner")
		const generation = store.runtime("root")!.generation
		store.observeRuntimeStatus("root", "owner", "BUSY", generation)
		store.observeRuntimeStatus("root", "owner", "IDLE", generation)
		await typing.flush()
		expect(calls.at(-1)).toBe(2)
		await typing.shutdown(); store.close()
	})
	test("retries API failures without throwing", async () => {
		const store = setup(), calls: number[] = [], typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://example.test", accountId: "a" }), api: { getConfig: async () => ({ typing_ticket: "ticket" }), sendTyping: async () => { calls.push(1); throw new Error("offline") } }, retryMs: 5, debounceMs: 0 })
		await expect(typing.flush()).resolves.toBeUndefined()
		expect(calls.length).toBe(1)
		await Bun.sleep(50)
		expect(calls.length).toBeGreaterThan(1)
		await typing.shutdown(); store.close()
	})
	test("keeps typing on after lease expiry until the active admission finishes", async () => {
		const store = setup(), calls: number[] = [], typing = coordinator(store, calls)
		const now = Date.now(), expiredAt = now + 60_001; seedWork(store, now, 60_000)
		await typing.flush(); expect(calls.at(-1)).toBe(1)
		expect(store.expireRuntimeLeases(expiredAt)).toBe(0)
		expect(store.runtime("root")).toMatchObject({ status: "QUEUED", admissionCount: 1, workPending: true }); expect(store.desiredTyping()).toBe(true)
		await typing.flush(); expect(calls.at(-1)).toBe(1)
		expect(store.finishRuntimeAdmission("s", "root", "owner")).toBe(true); expect(store.expireRuntimeLeases(expiredAt)).toBe(1)
		expect(store.runtime("root")).toMatchObject({ status: "IDLE", admissionCount: 0, workPending: false }); expect(store.desiredTyping()).toBe(false)
		await typing.flush(); expect(calls.at(-1)).toBe(2)
		await typing.shutdown(); store.close()
	})
	test("authenticated broker status, back, and reply replay refresh production typing", async () => {
		const store = setup(), calls: number[] = [], typing = coordinator(store, calls), adapter = new MockWeChatAdapter()
		store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" })
		const broker = new BrokerService(store, adapter, "secret", "worker", async (url) => String(url).endsWith("/runtime-status") ? Response.json({ ok: true, statuses: [{ rootSessionId: "root", status: "BUSY" }] }) : Response.json({ ok: true }), { typing })
		const request = (method: string, extra: object = {}) => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner", instanceToken: "token", ...extra }) })
		const busy = await broker.handleRequest(request("observe-status", { rootSessionId: "root", status: "busy" })); expect(await busy.json()).toMatchObject({ observed: true }); await Bun.sleep(5); expect(store.desiredTyping()).toBe(true); expect(calls.at(-1)).toBe(1)
		await broker.handleRequest(request("wechat-reply", { rootSessionId: "root", callId: "reply", text: "working" })); await Bun.sleep(5); const before = calls.length; const replay = await broker.handleRequest(request("wechat-reply", { rootSessionId: "root", callId: "reply", text: "working" })); expect(await replay.json()).toMatchObject({ replayed: true, state: "SENT" }); await Bun.sleep(5); expect(calls.slice(before)).toEqual([1])
		await broker.handleRequest(request("back-global")); await Bun.sleep(5); expect(store.desiredTyping()).toBe(false); expect(calls.at(-1)).toBe(2)
		await typing.shutdown(); store.close()
	})
	test("broker unregister accepts an exact stale token, cleans multiple roots, and immediately turns typing off", async () => {
		const store = setup(), calls: number[] = [], typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://example.test", accountId: "a" }), api: { getConfig: async () => ({ typing_ticket: "ticket" }), sendTyping: async (_u, _t, status) => { calls.push(status) } }, debounceMs: 0 })
		store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "a", directory: "a", ownerInstance: "owner" }); store.bind({ rootSessionId: "b", directory: "b", ownerInstance: "owner" })
		for (const [root, alias] of [["a", 1], ["b", 2]] as const) { const id = `${root}-prompt`; store.claimPromptSubmission({ submissionId: id, inboundId: `${id}-in`, root, owner: "owner", alias, body: root }); store.beginRuntimeAdmission(id, root, "owner") }
		await typing.startup(); await typing.flush(); expect(calls.at(-1)).toBe(1); store.db.query("UPDATE instances SET heartbeat_ms=0 WHERE instance_id='owner'").run()
		const refresh = typing.refresh.bind(typing); let refreshes = 0
		const brokerTyping = { refresh: () => { refreshes++; refresh() } } as TypingCoordinator
		const broker = new BrokerService(store, new MockWeChatAdapter(), "secret", "worker", async () => Response.json({ ok: true }), { typing: brokerTyping }), request = (token?: string, method = "unregister") => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner", ...(token === undefined ? {} : { instanceToken: token }) }) })
		expect((await broker.handleRequest(request("token", "status"))).status).toBe(403)
		expect((await broker.handleRequest(request())).status).toBe(403); expect((await broker.handleRequest(request(""))).status).toBe(403)
		const failed = await broker.handleRequest(request("wrong")); expect(await failed.json()).toEqual({ ok: false }); expect(refreshes).toBe(0); expect(store.bindings()).toHaveLength(2); const before = calls.length
		const response = await broker.handleRequest(request("token")); expect(await response.json()).toEqual({ ok: true }); expect(refreshes).toBe(1); await Bun.sleep(5); expect(store.bindings()).toEqual([]); expect(store.instance("owner")).toBeUndefined(); expect(store.desiredTyping()).toBe(false); expect(calls.slice(before)).toEqual([2]); await typing.shutdown(); store.close()
	})
	test("production worker constructs, starts, refreshes, and shuts down coordinator", async () => {
		const store = new Store(":memory:"), events: string[] = [], typing = { startup: async () => { events.push("startup") }, refresh: () => { events.push("refresh") }, shutdown: async () => { events.push("shutdown") } } as unknown as TypingCoordinator
		await runWorker({ enabled: true, weixinCommand: ["node", "fixed.js"] }, {
			initializeState: async () => ({ directory: ".", secret: "secret" }), acquireLock: async () => ({ update: async () => {}, release: async () => {} }), createStore: () => store, createAdapter: () => new MockWeChatAdapter(), createTyping: () => typing,
			createBroker: (_store, _adapter, _secret, _token, received) => { expect(received).toBe(typing); return { start: () => "http://127.0.0.1:1", startAdapter: async () => {}, stop: () => {} } as BrokerService }, waitForShutdown: async () => {},
		})
		expect(events).toContain("startup"); expect(events).toContain("refresh"); expect(events.indexOf("startup")).toBeLessThan(events.indexOf("refresh")); expect(events.filter((event) => event === "shutdown")).toHaveLength(1)
	})
	test("UNKNOWN reply replay reasserts ON without retrying message send", async () => {
		const store = setup(), calls: number[] = [], typing = coordinator(store, calls); seedWork(store)
		class AttemptAdapter extends MockWeChatAdapter { attempts = 0; override async send(to: string, text: string, context: string) { this.attempts++; return super.send(to, text, context) } }
		const adapter = new AttemptAdapter(); adapter.failSend = true; store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" })
		const broker = new BrokerService(store, adapter, "secret", "worker", fetch, { typing }), request = (callId: string) => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "wechat-reply", instanceId: "owner", instanceToken: "token", rootSessionId: "root", callId, text: "working" }) })
		await broker.handleRequest(request("unknown")); await Bun.sleep(5); const before = calls.length; adapter.failSend = false; const replay = await broker.handleRequest(request("unknown")); expect(await replay.json()).toMatchObject({ replayed: true, state: "UNKNOWN" }); await Bun.sleep(5); expect(adapter.attempts).toBe(1); expect(calls.slice(before)).toEqual([1]); await typing.shutdown(); store.close()
	})
	test("broker discards authoritative status when runtime generation changes in flight", async () => {
		const store = setup(); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d", ownerInstance: "owner" }); seedWork(store)
		let release!: (response: Response) => void, entered!: () => void; const pending = new Promise<Response>((resolve) => release = resolve), started = new Promise<void>((resolve) => entered = resolve); let refreshes = 0
		const broker = new BrokerService(store, new MockWeChatAdapter(), "secret", "worker", async (url) => { if (String(url).endsWith("/runtime-status")) { entered(); return pending } return Response.json({ ok: true }) }, { typing: { refresh: () => refreshes++ } as unknown as TypingCoordinator })
		const request = new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method: "observe-status", instanceId: "owner", instanceToken: "token", rootSessionId: "root", status: "idle" }) }), observation = broker.handleRequest(request); await started
		store.claimPromptSubmission({ submissionId: "s2", inboundId: "i2", root: "root", owner: "owner", alias: 1, messageId: "m2", body: "next" }); store.beginRuntimeAdmission("s2", "root", "owner"); const generation = store.runtime("root")!.generation
		release(Response.json({ ok: true, statuses: [{ rootSessionId: "root", status: "IDLE" }] })); expect(await observation.then((value) => value.json())).toMatchObject({ observed: false }); expect(store.runtime("root")).toMatchObject({ generation, status: "QUEUED", workPending: true }); expect(refreshes).toBe(0); store.close()
	})
	test("blocked ON is followed by OFF and shutdown owns final OFF", async () => {
		const store = setup(), calls: number[] = []; seedWork(store)
		let release!: () => void, entered!: () => void; const blocked = new Promise<void>((resolve) => release = resolve), started = new Promise<void>((resolve) => entered = resolve)
		const typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://x", accountId: "a" }), api: { getConfig: async () => ({ typing_ticket: "t" }), sendTyping: async (_u, _t, status) => { calls.push(status); if (status === 1 && calls.filter(x => x === 1).length === 1) { entered(); await blocked } } }, debounceMs: 0 })
		const on = typing.flush(); await started; const shutdown = typing.shutdown(); expect(calls).toEqual([1]); release(); await Promise.all([on, shutdown, typing.shutdown()]); expect(calls).toEqual([1, 2]); store.close()
	})
	test("route change during ON cannot commit stale context and performs correction", async () => {
		const store = setup(), calls: string[] = []; seedWork(store)
		let release!: () => void, entered!: () => void; const blocked = new Promise<void>((resolve) => release = resolve), started = new Promise<void>((resolve) => entered = resolve)
		const typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://x", accountId: "a" }), api: { getConfig: async (_u, _t, _b, context) => ({ typing_ticket: context }), sendTyping: async (_u, ticket, status) => { calls.push(`${ticket}:${status}`); if (calls.length === 1) { entered(); await blocked } } }, debounceMs: 0 })
		const first = typing.flush(); await started; store.refreshRoute("user", "new-context"); typing.refresh(); release(); await first; await Bun.sleep(10); expect(calls).toEqual(["context:1", "new-context:1"]); expect(store.typingState().contextHash).toBe("user:new-context"); await typing.shutdown(); store.close()
	})
	test("successful superseding attempt clears retry", async () => {
		const store = setup(), calls: number[] = []; seedWork(store); let fail = true
		const typing = new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://x", accountId: "a" }), api: { getConfig: async () => ({ typing_ticket: "t" }), sendTyping: async (_u, _t, status) => { calls.push(status); if (fail) { fail = false; throw new Error("once") } } }, debounceMs: 0, retryMs: 30 })
		await typing.flush(); await typing.flush(); const count = calls.length; await Bun.sleep(60); expect(calls).toHaveLength(count); await typing.shutdown(); store.close()
	})
})

function coordinator(store: Store, calls: number[]): TypingCoordinator { return new TypingCoordinator(store, { loadAccount: async () => ({ token: "t", baseUrl: "https://example.test", accountId: "selected" }), api: { getConfig: async () => ({ typing_ticket: "ticket" }), sendTyping: async (_u, _t, status) => { calls.push(status) } }, debounceMs: 0, retryMs: 5 }) }
function seedWork(store: Store, now = Date.now(), leaseMs = 1000): void { store.claimPromptSubmission({ submissionId: "s", inboundId: "i", root: "root", owner: "owner", alias: 1, messageId: "m", body: "hello", revision: store.control().revision }); store.beginRuntimeAdmission("s", "root", "owner", now, leaseMs) }
async function withAccounts(run: (accountsDir: string, root: string) => Promise<void>): Promise<void> { const root = await mkdtemp(path.join(tmpdir(), "ocx-typing-")), accountsDir = path.join(root, "accounts"); await mkdir(accountsDir); try { await run(accountsDir, root) } finally { await rm(root, { recursive: true, force: true }) } }
