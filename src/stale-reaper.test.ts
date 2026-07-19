import { expect, test } from "bun:test"
import { MockWeChatAdapter } from "./adapter"
import { BrokerService, STALE_REAPER_GRACE_MS, StaleBindingReaper } from "./broker"
import { createCallbackHandler } from "./client"
import { Store, sha256, type BindingReapCandidate } from "./core"
import { STALE_REAPER_PROOF_DOMAIN, STALE_REAPER_PROOF_VERSION, signStaleReaperResponse } from "./stale-reaper-auth"

type Fetcher = (input: string | URL | Request, init?: RequestInit) => Promise<Response>

function setup(roots = ["root"]): Store {
	const store = new Store(":memory:")
	for (const [index, root] of roots.entries()) { const owner = `owner-${index}`; store.register(owner, `token-${index}`, `http://127.0.0.1:${index + 1}`); store.bind({ rootSessionId: root, directory: root, ownerInstance: owner }) }
	store.db.query("UPDATE instances SET heartbeat_ms=0").run()
	return store
}

function candidate(store: Store, root = "root", now = 50_000): BindingReapCandidate { return store.staleBindingCandidates(now).find((item) => item.rootSessionId === root)! }
function health(status = 200, body: object = { ok: true }): Response { return Response.json(body, { status }) }
function callbackFetcher(handler: (request: Request) => Promise<Response>): Fetcher { return (url, init) => handler(new Request(String(url), init)) }

test("fresh owners are not probed and startup grace defers stale probes", async () => {
	const store = setup(), calls: string[] = [], reaper = new StaleBindingReaper(store, "secret", async (url) => { calls.push(String(url)); return health() }, { now: () => 0 })
	store.db.query("UPDATE instances SET heartbeat_ms=60_000 WHERE instance_id='owner-0'").run()
	expect((await reaper.run(59_999)).inGrace).toBe(true); expect(calls).toHaveLength(0); expect((await reaper.run(STALE_REAPER_GRACE_MS)).probed).toBe(0); store.close()
})

test("authenticated exact-root success clears suspicion and preserves binding", async () => {
	const store = setup(), item = candidate(store), recorded = store.recordBindingReapFailure(item, "timeout", 0); expect(recorded.recorded).toBe(true)
	let received!: Request, receivedBody = ""
	const callback = createCallbackHandler({ session: { get: async () => ({ data: { id: "root" } }) } }, "secret", "token-0"), reaper = new StaleBindingReaper(store, "secret", async (url, init) => { receivedBody = String(init?.body); received = new Request(String(url), init); return callback(received) }, { now: () => 0, challenge: () => "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" })
	await reaper.run(STALE_REAPER_GRACE_MS); expect(received.headers.get("x-wechat-control-key")).toBe("secret"); expect(received.headers.get("x-wechat-instance-token")).toBeNull(); expect(String(received.url)).not.toContain("token-0"); expect(receivedBody).not.toContain("token-0"); expect(store.bindingForRoot("root")).toBeDefined(); expect(store.bindingReapState("root")).toBeUndefined(); store.close()
})

test("only authenticated exact not-root deactivates its target root", async () => {
	let refreshes = 0; const store = setup(["a", "b"]), callbackA = createCallbackHandler({ session: { get: async () => ({ data: { id: "child", parentID: "a" } }) } }, "secret", "token-0"), callbackB = createCallbackHandler({ session: { get: async () => ({ data: { id: "b" } }) } }, "secret", "token-1"), reaper = new StaleBindingReaper(store, "secret", (url, init) => { const body = JSON.parse(String(init?.body)); return (body.rootSessionId === "a" ? callbackA : callbackB)(new Request(String(url), init)) }, { now: () => 0, onDeactivate: () => { refreshes++ } })
	await reaper.run(STALE_REAPER_GRACE_MS); expect(store.bindingForRoot("a")).toBeUndefined(); expect(store.bindingForRoot("b")).toBeDefined(); expect(refreshes).toBe(1); store.close()
})

test("port-reuse forged ok and not-root responses are inconclusive", async () => {
	const store = setup(["forged-ok", "forged-not-root"]), reaper = new StaleBindingReaper(store, "secret", async (_url, init) => { const request = JSON.parse(String(init?.body)), notRoot = request.rootSessionId === "forged-not-root", proof = { proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: STALE_REAPER_PROOF_VERSION, challenge: request.challenge, rootSessionId: request.rootSessionId, outcome: notRoot ? "not-root" : "ok", responseProof: "A".repeat(43) }; return notRoot ? health(409, { error: "not-root", ...proof }) : health(200, { ok: true, ...proof }) }, { now: () => 0 })
	await reaper.run(STALE_REAPER_GRACE_MS); expect(store.bindingForRoot("forged-ok")).toBeDefined(); expect(store.bindingForRoot("forged-not-root")).toBeDefined(); expect(store.bindingReapStates()).toHaveLength(2); store.close()
})

test("replayed and tampered response fields status outcome root and proof are inconclusive", async () => {
	const roots = ["replay", "status", "outcome", "root", "proof"], store = setup(roots), reaper = new StaleBindingReaper(store, "secret", async (url, init) => {
		const request = JSON.parse(String(init?.body)), index = Number(new URL(String(url)).port) - 1, token = `token-${index}`, root = String(request.rootSessionId), challenge = String(request.challenge)
		if (root === "replay") { const replayChallenge = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", responseProof = await signStaleReaperResponse(token, replayChallenge, root, "ok"); return health(200, { ok: true, proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: 1, challenge: replayChallenge, rootSessionId: root, outcome: "ok", responseProof }) }
		if (root === "status") { const responseProof = await signStaleReaperResponse(token, challenge, root, "ok"); return health(409, { ok: true, proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: 1, challenge, rootSessionId: root, outcome: "ok", responseProof }) }
		if (root === "outcome") { const responseProof = await signStaleReaperResponse(token, challenge, root, "not-root"); return health(200, { ok: true, proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: 1, challenge, rootSessionId: root, outcome: "ok", responseProof }) }
		if (root === "root") { const responseProof = await signStaleReaperResponse(token, challenge, "other", "ok"); return health(200, { ok: true, proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: 1, challenge, rootSessionId: "other", outcome: "ok", responseProof }) }
		const responseProof = await signStaleReaperResponse("wrong-token", challenge, root, "not-root"); return health(409, { error: "not-root", proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: 1, challenge, rootSessionId: root, outcome: "not-root", responseProof })
	}, { now: () => 0, challenge: () => "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" })
	await reaper.run(STALE_REAPER_GRACE_MS); for (const root of roots) expect(store.bindingForRoot(root)).toBeDefined(); expect(store.bindingReapStates()).toHaveLength(roots.length); store.close()
})

test("wrong token, port reuse, timeout and malformed responses remain inconclusive", async () => {
	const store = setup(["wrong", "reuse", "timeout", "malformed"]), modes = new Map([["wrong", 401], ["reuse", 200], ["timeout", 0], ["malformed", 200]]), reaper = new StaleBindingReaper(store, "secret", async (url) => { const root = new URL(String(url)).hostname === "127.0.0.1" ? [...modes.keys()][Number(new URL(String(url)).port) - 1] : ""; if (root === "timeout") throw new Error("refused"); if (root === "malformed") return health(200, { ok: false }); return modes.get(root) === 401 ? health(401, { ok: true }) : health(200, { ok: false }) }, { now: () => 0 })
	const result = await reaper.run(STALE_REAPER_GRACE_MS); expect(result.probed).toBe(4); for (const root of modes.keys()) expect(store.bindingForRoot(root)).toBeDefined(); expect(store.bindingReapStates()).toHaveLength(4); store.close()
})

test("threshold requires spacing and two recorded current-run observations", async () => {
	const store = setup(), reaper = new StaleBindingReaper(store, "secret", async () => { throw new Error("timeout") }, { now: () => 0 })
	for (const time of [60_000, 90_000, 120_000, 150_000]) { await reaper.run(time); expect(store.bindingForRoot("root")).toBeDefined() }
	await reaper.run(180_000); expect(store.bindingForRoot("root")).toBeUndefined(); store.close()
})

test("continuity gap and restart reset current-run observations while persistence remains", async () => {
	const store = setup(), item = candidate(store), reaper = new StaleBindingReaper(store, "secret", async () => { throw new Error("timeout") }, { now: () => 0 })
	for (const time of [0, 30_000, 60_000, 90_000, 120_000]) store.recordBindingReapFailure(item, "timeout", time)
	await reaper.run(60_000); const gap = await reaper.run(120_001); expect(gap.inGrace).toBe(true); expect(store.bindingReapState("root")).toBeDefined()
	const restarted = new StaleBindingReaper(store, "secret", async () => { throw new Error("timeout") }, { now: () => 120_001 }); await restarted.run(180_001); expect(store.bindingForRoot("root")).toBeDefined(); await restarted.run(210_001); expect(store.bindingForRoot("root")).toBeUndefined(); store.close()
})

test("rebind during probe fails CAS safely and refreshes typing after deactivation", async () => {
	const store = setup(), item = candidate(store), refreshes: number[] = [], reaper = new StaleBindingReaper(store, "secret", async () => { store.bind({ rootSessionId: "root", directory: "rebound", ownerInstance: "owner-0" }); return health(409, { error: "not-root" }) }, { now: () => 0, onDeactivate: () => refreshes.push(1) })
	await reaper.run(STALE_REAPER_GRACE_MS); expect(store.bindingForRoot("root")).toMatchObject({ directory: "rebound" }); expect(refreshes).toHaveLength(0); expect(store.bindingReapState("root")).toBeUndefined(); store.close()
})

test("maintenance is bounded and non-overlapping", async () => {
	const store = setup(Array.from({ length: 8 }, (_, index) => `root-${index}`)); let active = 0, maximum = 0
	const reaper = new StaleBindingReaper(store, "secret", async () => { active++; maximum = Math.max(maximum, active); await Bun.sleep(2); active--; return health() }, { now: () => 0, concurrency: 2 })
	const first = reaper.run(STALE_REAPER_GRACE_MS), second = reaper.run(STALE_REAPER_GRACE_MS); expect(first).toBe(second); await first; expect(maximum).toBeLessThanOrEqual(2); store.close()
})

test("heartbeat recovery requires the exact persisted token and clears stale suspicion", async () => {
	const store = setup(), item = candidate(store); store.recordBindingReapFailure(item, "timeout", 0); const broker = new BrokerService(store, new MockWeChatAdapter(), "secret", "worker", async () => health())
	const request = (token: string, method: string) => new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner-0", instanceToken: token, rootSessionId: "root" }) })
	expect((await broker.handleRequest(request("token-0", "control-get"))).status).toBe(403); expect((await broker.handleRequest(request("wrong", "heartbeat"))).status).toBe(403); expect((await broker.handleRequest(request("token-0", "heartbeat"))).status).toBe(200); expect((await broker.handleRequest(request("token-0", "control-get"))).status).toBe(200); expect(store.bindingReapState("root")).toBeUndefined(); expect(store.staleBindingCandidates(50_000)).toHaveLength(0); expect(sha256("token-0")).toBe(item.tokenFingerprint); store.close()
})
