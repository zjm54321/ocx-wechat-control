import { expect, test } from "bun:test"
import { MockWeChatAdapter } from "./adapter"
import { BrokerService, parseQuestionAnswers, type NativeQuestionPayload } from "./broker"
import { Store } from "./core"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import * as path from "node:path"

type Fetcher = (input: string | URL | Request, init?: RequestInit) => Promise<Response>

function flow(fetcher: Fetcher, roots = ["root"]) {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter()
	store.register("owner", "token", "http://127.0.0.1:1")
	for (const root of roots) store.bind({ rootSessionId: root, directory: `d-${root}`, ownerInstance: "owner" })
	store.refreshRoute("controller", "ctx")
	return { store, adapter, broker: new BrokerService(store, adapter, "secret", "worker", fetcher) }
}

function inbound(id: string, alias: number, body: string, fromUserId = "controller") { return { id, fromUserId, contextToken: "ctx", text: `#${alias}\n${body}`, cursorHint: id } }

function rpc(method: string, extra: Record<string, unknown> = {}): Request {
	return new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner", instanceToken: "token", ...extra }) })
}

function rpcAs(instanceId: string, instanceToken: string, method: string, extra: Record<string, unknown> = {}): Request { return new Request("http://127.0.0.1", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId, instanceToken, ...extra }) }) }

async function openNative(broker: BrokerService, input: { requestId: string; requestKey?: string; rootSessionId?: string; kind: "QUESTION" | "PERMISSION"; payload: unknown }) {
	const response = await broker.handleRequest(rpc("native-request-open", { ...input, requestKey: input.requestKey ?? input.requestId, rootSessionId: input.rootSessionId ?? "root" }))
	return { response, body: await response.json() as Record<string, unknown> }
}

const questionPayload = (sourceSessionId = "root"): NativeQuestionPayload => ({ sourceSessionId, questions: [{ question: "Choose", options: [{ label: "A" }, { label: "B" }], multiple: false, custom: true }] })
function tempDatabase(name: string): string { return path.join(tmpdir(), `${name}-${crypto.randomUUID()}.sqlite`) }
function removeDatabase(filename: string): void { for (const suffix of ["", "-wal", "-shm"]) try { rmSync(`${filename}${suffix}`, { force: true }) } catch {} }

test("same-root admissions serialize only callback admission and never send assistant text", async () => {
	const releases: Array<(response: Response) => void> = [], calls: Array<Record<string, unknown>> = []
	const { store, adapter, broker } = flow(async (_url, init) => { calls.push(JSON.parse(String(init?.body))); return new Promise<Response>((resolve) => releases.push(resolve)) })
	const first = broker.handleInbound(inbound("first", 1, "one")); await Bun.sleep(0); const second = broker.handleInbound(inbound("second", 1, "two")); await Bun.sleep(0)
	expect(calls).toHaveLength(1); expect(calls[0]).toMatchObject({ inboundId: "first", messageId: store.promptSubmission("first")?.messageId, text: "one" }); expect(calls[0].messageId).toMatch(/^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$/)
	releases[0](Response.json({ ok: true, accepted: true })); expect((await first).ok).toBe(true); await Bun.sleep(0); expect(calls).toHaveLength(2); releases[1](Response.json({ ok: true, accepted: true })); expect((await second).ok).toBe(true)
	expect(store.promptSubmission("first")?.state).toBe("SUBMITTED"); expect(store.promptSubmission("second")?.state).toBe("SUBMITTED"); expect(adapter.sent).toHaveLength(0); expect((store.db.query("SELECT COUNT(*) AS count FROM pending_replies").get() as { count: number }).count).toBe(0); store.close()
})

test("different roots admit in parallel and duplicate inbound never resubmits", async () => {
	const releases = new Map<string, (response: Response) => void>(), calls: string[] = []
	const { store, broker } = flow(async (_url, init) => { const body = JSON.parse(String(init?.body)) as { rootSessionId: string }; calls.push(body.rootSessionId); return new Promise<Response>((resolve) => releases.set(body.rootSessionId, resolve)) }, ["a", "b"])
	const one = broker.handleInbound(inbound("a-in", 1, "one")), two = broker.handleInbound(inbound("b-in", 2, "two")); await Bun.sleep(0); expect(new Set(calls)).toEqual(new Set(["a", "b"])); releases.get("a")!(Response.json({ ok: true, accepted: true })); releases.get("b")!(Response.json({ ok: true, accepted: true })); await Promise.all([one, two])
	expect((await broker.handleInbound(inbound("a-in", 1, "one"))).reason).toBe("duplicate-at-least-once-key"); expect(calls).toHaveLength(2); store.close()
})

test("prompt rejection and uncertainty map to durable terminal states without replay", async () => {
	let mode: "reject" | "throw" = "reject", calls = 0
	const { store, broker } = flow(async () => { calls++; if (mode === "throw") throw new DOMException("timeout", "TimeoutError"); return Response.json({ ok: false, certainty: "REJECTED", error: "not-root" }, { status: 409 }) })
	expect((await broker.handleInbound(inbound("rejected", 1, "x"))).reason).toBe("prompt-rejected"); expect(store.promptSubmission("rejected")?.state).toBe("REJECTED"); expect(store.bindingForRoot("root")).toBeUndefined()
	store.bind({ rootSessionId: "root", directory: "d-root", ownerInstance: "owner" }); mode = "throw"; expect((await broker.handleInbound(inbound("unknown", 1, "y"))).reason).toBe("unknown-no-replay"); expect(store.promptSubmission("unknown")?.state).toBe("UNKNOWN"); await broker.handleInbound(inbound("unknown", 1, "y")); expect(calls).toBe(2); store.close()
})

test("maintenance renews authoritative busy and applies idle", async () => {
	let status: "BUSY" | "IDLE" = "BUSY", calls = 0
	const { store, broker } = flow(async (url, init) => { if (String(url).endsWith("/runtime-status")) { calls++; expect((JSON.parse(String(init?.body)) as any).rootSessionIds).toEqual(["root"]); return Response.json({ ok: true, statuses: [{ rootSessionId: "root", status }] }) }; return Response.json({ ok: true }) })
	store.claimPromptSubmission({ submissionId: "one", inboundId: "one-in", root: "root", owner: "owner", alias: 1, body: "one" }); const generation = store.beginRuntimeAdmission("one", "root", "owner", 100, 10)!; store.finishRuntimeAdmission("one", "root", "owner"); store.observeRuntimeStatus("root", "owner", "BUSY", generation, 100, 10)
	await broker.reconcileActiveRuntimes(105, 10); expect(store.runtime("root")).toMatchObject({ status: "BUSY", leaseExpiresMs: 115 }); expect(store.expireRuntimeLeases(111)).toBe(0); status = "IDLE"; await broker.reconcileActiveRuntimes(112, 10); expect(store.runtime("root")).toMatchObject({ status: "IDLE", workPending: false }); expect(calls).toBe(2); store.close()
})

test("maintenance isolates batch not-root and renews the remaining root", async () => {
	const calls: string[][] = [], { store, broker } = flow(async (url, init) => {
		if (!String(url).endsWith("/runtime-status")) return Response.json({ ok: true })
		const roots = (JSON.parse(String(init?.body)) as { rootSessionIds: string[] }).rootSessionIds; calls.push(roots)
		if (roots.length > 1 || roots[0] === "a") return Response.json({ error: "not-root" }, { status: 409 })
		return Response.json({ ok: true, statuses: [{ rootSessionId: "b", status: "BUSY" }] })
	}, ["a", "b"])
	store.bind({ rootSessionId: "a", directory: "shared", ownerInstance: "owner" }); store.bind({ rootSessionId: "b", directory: "shared", ownerInstance: "owner" })
	for (const [root, alias] of [["a", 1], ["b", 2]] as const) { const id = `${root}-prompt`; store.claimPromptSubmission({ submissionId: id, inboundId: `${id}-in`, root, owner: "owner", alias, body: root }); const generation = store.beginRuntimeAdmission(id, root, "owner", 100, 10)!; store.finishRuntimeAdmission(id, root, "owner"); store.observeRuntimeStatus(root, "owner", "BUSY", generation, 100, 10) }
	await broker.reconcileActiveRuntimes(105, 20)
	expect(calls).toEqual([["a", "b"], ["a"], ["b"]]); expect(store.bindingForRoot("a")).toBeUndefined(); expect(store.runtime("a")).toMatchObject({ status: "IDLE", workPending: false }); expect(store.bindingForRoot("b")?.active).toBe(true); expect(store.runtime("b")).toMatchObject({ status: "BUSY", leaseExpiresMs: 125 }); store.close()
})

test("maintenance isolates malformed batch and per-root network failure", async () => {
	const calls: string[][] = [], { store, broker } = flow(async (url, init) => {
		if (!String(url).endsWith("/runtime-status")) return Response.json({ ok: true })
		const roots = (JSON.parse(String(init?.body)) as { rootSessionIds: string[] }).rootSessionIds; calls.push(roots)
		if (roots.length > 1) return Response.json({ ok: true, statuses: "malformed" })
		if (roots[0] === "a") throw new DOMException("timeout", "TimeoutError")
		return Response.json({ ok: true, statuses: [{ rootSessionId: "b", status: "BUSY" }] })
	}, ["a", "b"])
	store.bind({ rootSessionId: "a", directory: "shared", ownerInstance: "owner" }); store.bind({ rootSessionId: "b", directory: "shared", ownerInstance: "owner" })
	for (const [root, alias] of [["a", 1], ["b", 2]] as const) { const id = `${root}-prompt`; store.claimPromptSubmission({ submissionId: id, inboundId: `${id}-in`, root, owner: "owner", alias, body: root }); const generation = store.beginRuntimeAdmission(id, root, "owner", 100, 10)!; store.finishRuntimeAdmission(id, root, "owner"); store.observeRuntimeStatus(root, "owner", "BUSY", generation, 100, 10) }
	await broker.reconcileActiveRuntimes(105, 20)
	expect(calls).toEqual([["a", "b"], ["a"], ["b"]]); expect(store.bindingForRoot("a")?.active).toBe(true); expect(store.runtime("a")).toMatchObject({ status: "BUSY", leaseExpiresMs: 110 }); expect(store.runtime("b")).toMatchObject({ status: "BUSY", leaseExpiresMs: 125 }); store.close()
})

test("maintenance discards authoritative status after generation changes in flight", async () => {
	let release!: (response: Response) => void, entered!: () => void; const pending = new Promise<Response>((resolve) => release = resolve), started = new Promise<void>((resolve) => entered = resolve)
	const { store, broker } = flow(async (url) => { if (String(url).endsWith("/runtime-status")) { entered(); return pending }; return Response.json({ ok: true }) })
	store.claimPromptSubmission({ submissionId: "one", inboundId: "one-in", root: "root", owner: "owner", alias: 1, body: "one" }); const one = store.beginRuntimeAdmission("one", "root", "owner", 100, 10)!; store.finishRuntimeAdmission("one", "root", "owner"); store.observeRuntimeStatus("root", "owner", "BUSY", one, 100, 10)
	const maintenance = broker.reconcileActiveRuntimes(105, 10); await started; store.claimPromptSubmission({ submissionId: "two", inboundId: "two-in", root: "root", owner: "owner", alias: 1, body: "two" }); const two = store.beginRuntimeAdmission("two", "root", "owner", 106, 10)!; release(Response.json({ ok: true, statuses: [{ rootSessionId: "root", status: "IDLE" }] })); await maintenance; expect(store.runtime("root")).toMatchObject({ generation: two, status: "BUSY", workPending: true }); store.close()
})

test("zero one and multiple native precedence never accidentally prompts", async () => {
	const callbackPaths: string[] = [], { store, broker } = flow(async (url) => { callbackPaths.push(new URL(String(url)).pathname); return Response.json(callbackPaths.at(-1) === "/submit-prompt" ? { ok: true, accepted: true } : { ok: true, resolved: true }) })
	expect((await broker.handleInbound(inbound("zero", 1, "ordinary"))).ok).toBe(true); expect(callbackPaths).toEqual(["/submit-prompt"])
	await openNative(broker, { requestId: "q-one", kind: "QUESTION", payload: questionPayload() }); const one = store.nativeRequest("q-one")!; expect((await broker.handleInbound(inbound("one", 1, "A"))).ok).toBe(true); expect(callbackPaths.at(-1)).toBe("/resolve-question"); expect(store.nativeRequest("q-one")?.state).toBe("RESOLVED")
	await openNative(broker, { requestId: "q-two", kind: "QUESTION", payload: questionPayload() }); await openNative(broker, { requestId: "p-two", kind: "PERMISSION", payload: { sourceSessionId: "root", permission: "write" } }); const before = callbackPaths.length; expect((await broker.handleInbound(inbound("multiple", 1, "A"))).reason).toBe("native-code-required"); expect(callbackPaths).toHaveLength(before)
	const explicit = store.nativeRequest("q-two")!; expect((await broker.handleInbound(inbound("explicit", 1, `${explicit.code} B`))).ok).toBe(true); expect(store.nativeRequest("q-two")?.state).toBe("RESOLVED"); expect(one.code).toMatch(/^Q[A-Z2-7]{6}$/); store.close()
})

test("wrong expired and cross-root codes are rejected before callback", async () => {
	let callbacks = 0; const { store, adapter, broker } = flow(async () => { callbacks++; return Response.json({ ok: true, resolved: true }) }, ["a", "b"])
	expect((await broker.handleInbound(inbound("wrong", 1, "QAAAAAA A"))).reason).toBe("native-code-invalid")
	await openNative(broker, { requestId: "cross", rootSessionId: "b", kind: "QUESTION", payload: questionPayload("b") }); const cross = store.nativeRequest("cross")!; const crossResult = await broker.handleInbound(inbound("cross-in", 1, `${cross.code} A`)); expect(crossResult.reason).toBe("native-code-cross-root"); expect((adapter.sent.at(-1)?.text ?? "")).toContain("另一个当前会话")
	store.settleNativeTerminal("cross", "RESOLVED"); expect((await broker.handleInbound(inbound("expired", 2, `${cross.code} A`))).reason).toBe("native-code-invalid"); expect(callbacks).toBe(0); store.close()
})

test("id and newly generated prompts use the current compact alias", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({ ok: true }), ["a", "b"])
	store.deactivateBinding("a", "owner")
	await broker.handleInbound({ id: "list", fromUserId: "controller", contextToken: "ctx", text: "id", cursorHint: "list" })
	expect(adapter.sent.at(-1)?.text).toBe("#1  未命名会话")
	const response = await broker.handleRequest(rpc("request-input", { rootSessionId: "b", requestKey: "compact-prompt", question: "Choose", choices: ["A"] }))
	expect(response.status).toBe(200); expect(adapter.sent.at(-1)?.text).toContain("#1\n"); expect(adapter.sent.at(-1)?.text).toContain("请回复 #1")
	store.close()
})

test("id excludes swept orphan bindings and compacts remaining aliases", async () => {
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(), broker = new BrokerService(store, adapter, "secret", "worker", async () => Response.json({ ok: true }))
	store.register("orphan", "orphan-token", "http://127.0.0.1:1"); store.register("live", "live-token", "http://127.0.0.1:2"); store.bind({ rootSessionId: "orphan-root", directory: "orphan", ownerInstance: "orphan", title: "Closed" }); store.bind({ rootSessionId: "live-root", directory: "live", ownerInstance: "live", title: "Live" }); store.refreshRoute("controller", "ctx")
	store.db.query("DELETE FROM instances WHERE instance_id='orphan'").run(); expect(store.sweepOrphanBindings()).toBe(1)
	await broker.handleInbound({ id: "orphan-list", fromUserId: "controller", contextToken: "ctx", text: "id", cursorHint: "orphan-list" }); expect(adapter.sent.at(-1)?.text).toBe("#1  Live"); store.close()
})

test("explicit native request remains answerable after its displayed alias moves", async () => {
	const callbacks: string[] = [], { store, adapter, broker } = flow(async (url) => { callbacks.push(new URL(String(url)).pathname); return Response.json({ ok: true, resolved: true }) }, ["a", "b"])
	await openNative(broker, { requestId: "moving-q", rootSessionId: "b", kind: "QUESTION", payload: questionPayload("b") })
	const native = store.nativeRequest("moving-q")!; expect(adapter.sent[0]?.text).toContain("#2\n"); store.deactivateBinding("a", "owner")
	const result = await broker.handleInbound(inbound("moving-answer", 1, `${native.code} 1`)); expect(result.ok).toBe(true); expect(store.nativeRequest("moving-q")?.state).toBe("RESOLVED"); expect(callbacks).toContain("/resolve-question"); store.close()
})

test("multiple native questions render readable blank-line blocks", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({ ok: true }), ["root"])
	const payload: NativeQuestionPayload = { sourceSessionId: "root", questions: [{ question: "First", options: [{ label: "A" }], multiple: false, custom: false }, { question: "Second", options: [{ label: "B" }], multiple: false, custom: false }] }
	await openNative(broker, { requestId: "formatted-multi", kind: "QUESTION", payload }); const code = store.nativeRequest("formatted-multi")!.code
	expect(adapter.sent.at(-1)?.text).toBe(`#1\n${code} 问题请求\n1: First\n1. A\n\n2: Second\n1. B\n请一次性回复 #1\n${code} 后按“N: 答案”逐行填写。`)
	store.close()
})

test("native answers require atomic explicit forms and keep invalid requests OPEN", async () => {
	const { store, adapter, broker } = flow(async (url) => String(url).endsWith("/resolve-question") ? Response.json({ ok: true, resolved: true }) : Response.json({ ok: true }))
	await openNative(broker, { requestId: "atomic-q", kind: "QUESTION", payload: questionPayload() }); const native = store.nativeRequest("atomic-q")!
	expect((await broker.handleInbound(inbound("code-only", 1, native.code))).reason).toBe("native-answer-invalid"); expect(store.nativeRequest("atomic-q")?.state).toBe("OPEN"); expect(adapter.sent.at(-1)?.text).toContain(`#1\n${native.code} 1`)
	expect((await broker.handleInbound(inbound("split-answer", 1, "1"))).reason).toBe("native-answer-invalid"); expect(store.nativeRequest("atomic-q")?.state).toBe("OPEN")
	expect((await broker.handleInbound(inbound("explicit-answer", 1, `${native.code} 1`))).ok).toBe(true); expect(store.nativeRequest("atomic-q")?.state).toBe("RESOLVED"); store.close()
})

test("fresh sole native question accepts exact implicit #1 newline 1", async () => {
	const callbacks: Array<Record<string, unknown>> = [], { store, broker } = flow(async (url, init) => { if (String(url).endsWith("/resolve-question")) callbacks.push(JSON.parse(String(init?.body))); return Response.json({ ok: true, resolved: true }) })
	await openNative(broker, { requestId: "implicit-q", kind: "QUESTION", payload: questionPayload() })
	expect((await broker.handleInbound(inbound("implicit-answer", 1, "1"))).ok).toBe(true)
	expect(callbacks).toEqual([expect.objectContaining({ rootSessionId: "root", requestId: "implicit-q", answers: [["A"]] })]); expect(store.nativeRequest("implicit-q")?.state).toBe("RESOLVED"); store.close()
})

test("code-only split-answer guard survives Store and broker restart", async () => {
	const filename = tempDatabase("native-answer-guard-restart"), firstAdapter = new MockWeChatAdapter()
	let store = new Store(filename); store.register("owner", "token", "http://127.0.0.1:1"); store.bind({ rootSessionId: "root", directory: "d-root", ownerInstance: "owner" }); store.refreshRoute("controller", "ctx")
	let broker = new BrokerService(store, firstAdapter, "secret", "worker", async () => Response.json({ ok: true, resolved: true }))
	await openNative(broker, { requestId: "restart-q", kind: "QUESTION", payload: questionPayload() }); const code = store.nativeRequest("restart-q")!.code
	expect((await broker.handleInbound(inbound("restart-code-only", 1, code))).reason).toBe("native-answer-invalid"); expect(store.nativeRequest("restart-q")?.state).toBe("OPEN"); store.close()
	store = new Store(filename); const callbacks: Array<Record<string, unknown>> = [], secondAdapter = new MockWeChatAdapter(); broker = new BrokerService(store, secondAdapter, "secret", "worker", async (url, init) => { if (String(url).endsWith("/resolve-question")) callbacks.push(JSON.parse(String(init?.body))); return Response.json({ ok: true, resolved: true }) })
	expect((await broker.handleInbound(inbound("restart-split", 1, "1"))).reason).toBe("native-answer-invalid"); expect(store.nativeRequest("restart-q")?.state).toBe("OPEN"); expect(callbacks).toHaveLength(0)
	expect((await broker.handleInbound(inbound("restart-atomic", 1, `${code} 1`))).ok).toBe(true); expect(callbacks).toEqual([expect.objectContaining({ requestId: "restart-q", answers: [["A"]] })]); expect(store.nativeRequest("restart-q")?.state).toBe("RESOLVED")
	store.close(); removeDatabase(filename)
})

test("question grammar maps ordered single multiple custom and multi-question answers", () => {
	const single = questionPayload(); expect(parseQuestionAnswers(single, "2")).toEqual([["B"]]); expect(parseQuestionAnswers(single, "A")).toEqual([["A"]]); expect(parseQuestionAnswers(single, "=other")).toEqual([["other"]]); expect(parseQuestionAnswers(single, "other")).toBeUndefined()
	const multiple: NativeQuestionPayload = { sourceSessionId: "root", questions: [{ question: "Many", options: [{ label: "A" }, { label: "B" }], multiple: true, custom: false }] }; expect(parseQuestionAnswers(multiple, "2,A")).toEqual([["B", "A"]]); expect(parseQuestionAnswers(multiple, "=x")).toBeUndefined()
	const multi: NativeQuestionPayload = { sourceSessionId: "root", questions: [single.questions[0], { question: "Second", options: [{ label: "X" }], multiple: false, custom: false }] }; expect(parseQuestionAnswers(multi, "2: X\n1: B")).toEqual([["B"], ["X"]]); expect(parseQuestionAnswers(multi, "1: B")).toBeUndefined(); expect(parseQuestionAnswers(multi, "1: B\n1: X")).toBeUndefined()
})

test("malformed question stays OPEN and permission accepts only exact once or reject", async () => {
	const resolutionBodies: JsonObject[] = [], { store, broker } = flow(async (url, init) => { if (String(url).includes("/resolve-")) resolutionBodies.push(JSON.parse(String(init?.body))); return Response.json({ ok: true, resolved: true }) })
	await openNative(broker, { requestId: "bad-q", kind: "QUESTION", payload: questionPayload() }); expect((await broker.handleInbound(inbound("bad-answer", 1, "C"))).reason).toBe("native-answer-invalid"); expect(store.nativeRequest("bad-q")?.state).toBe("OPEN"); expect(resolutionBodies).toHaveLength(0)
	store.settleNativeTerminal("bad-q", "REJECTED"); for (const [id, answer, expected] of [["always-p", "always", "OPEN"], ["fuzzy-p", "yes", "OPEN"], ["once-p", "once", "RESOLVED"], ["reject-p", "reject", "REJECTED"]] as const) { await openNative(broker, { requestId: id, kind: "PERMISSION", payload: { sourceSessionId: "root", permission: "write" } }); await broker.handleInbound(inbound(`in-${id}`, 1, answer)); expect(store.nativeRequest(id)?.state).toBe(expected); if (expected === "OPEN") store.settleNativeTerminal(id, "REJECTED") }
	expect(resolutionBodies.map((body) => body.decision)).toEqual(["once", "reject"]); store.close()
})

test("resolution timeout becomes UNKNOWN without retry and terminal race settles once", async () => {
	let calls = 0, release!: (response: Response) => void, raceMode = false
	const { store, broker } = flow(async (url) => { if (!String(url).includes("/resolve-question")) return Response.json({ ok: true }); calls++; if (!raceMode) throw new DOMException("timeout", "TimeoutError"); return new Promise<Response>((resolve) => { release = resolve }) })
	await openNative(broker, { requestId: "unknown-q", kind: "QUESTION", payload: questionPayload() }); expect((await broker.handleInbound(inbound("unknown-answer", 1, "A"))).reason).toBe("unknown-no-replay"); expect(store.nativeRequest("unknown-q")?.state).toBe("UNKNOWN"); expect((await broker.handleInbound(inbound("unknown-retry", 1, "A"))).reason).toBe("native-unknown-local"); expect(calls).toBe(1)
	store.settleNativeTerminal("unknown-q", "REJECTED"); raceMode = true; await openNative(broker, { requestId: "race-q", kind: "QUESTION", payload: questionPayload() }); const running = broker.handleInbound(inbound("race-answer", 1, "A")); await Bun.sleep(0); expect((await broker.handleInbound(inbound("race-duplicate", 1, "A"))).reason).toBe("native-processing"); const terminal = await broker.handleRequest(rpc("native-request-terminal", { rootSessionId: "root", requestId: "race-q", state: "RESOLVED", resolution: [["local"]] })); expect(await terminal.json()).toMatchObject({ settled: true }); release(Response.json({ ok: true, resolved: true })); await running; expect(store.nativeRequest("race-q")?.state).toBe("RESOLVED"); expect(calls).toBe(2); store.close()
})

test("definite native pre-call rejection safely returns claimed request to OPEN", async () => {
	let calls = 0; const { store, broker } = flow(async (url) => { if (String(url).includes("/resolve-question")) { calls++; return Response.json({ ok: false, certainty: "REJECTED", error: "request-unavailable" }, { status: 409 }) }; return Response.json({ ok: true }) })
	await openNative(broker, { requestId: "definite-q", kind: "QUESTION", payload: questionPayload() }); expect((await broker.handleInbound(inbound("definite-answer", 1, "A"))).reason).toBe("native-definite-rejection"); expect(store.nativeRequest("definite-q")).toMatchObject({ state: "OPEN", inboundId: null }); expect(calls).toBe(1); store.close()
})

test("uncertain native relay remains OPEN and duplicate event does not resend", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({ ok: true })); adapter.failSend = true
	const first = await openNative(broker, { requestId: "relay-q", requestKey: "relay-key", kind: "QUESTION", payload: questionPayload() }); expect(first.body).toMatchObject({ state: "OPEN", relay: "UNKNOWN" }); expect(store.nativeRequest("relay-q")?.state).toBe("OPEN"); expect(adapter.sent).toHaveLength(0)
	adapter.failSend = false; const duplicate = await openNative(broker, { requestId: "relay-q", requestKey: "relay-key", kind: "QUESTION", payload: questionPayload() }); expect(duplicate.body).toMatchObject({ replayed: true }); expect(adapter.sent).toHaveLength(0); store.close()
})

test("native requestKey replay requires exact identity and never exposes mismatched request", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({ ok: true }), ["a", "b"]), base = { requestId: "identity-q", requestKey: "identity-key", rootSessionId: "a", kind: "QUESTION" as const, payload: questionPayload("a") }
	const opened = await openNative(broker, base); expect(opened.response.status).toBe(200); const relays = adapter.sent.length
	const exact = await openNative(broker, base); expect(exact.body).toMatchObject({ replayed: true }); expect(adapter.sent).toHaveLength(relays)
	const mismatches = [
		{ ...base, requestId: "different-id" },
		{ ...base, kind: "PERMISSION" as const, payload: { sourceSessionId: "a", permission: "write" } },
		{ ...base, payload: questionPayload("different-source") },
		{ ...base, rootSessionId: "b", payload: questionPayload("b") },
	]
	for (const mismatch of mismatches) { const result = await openNative(broker, mismatch); expect(result.response.status).toBe(409); expect(result.body).toEqual({ error: "native-request-conflict" }); expect(adapter.sent).toHaveLength(relays); expect(store.nativeRequest("identity-q")?.state).toBe("OPEN") }
	store.unregister("owner", "token"); store.register("next", "next-token", "http://127.0.0.1:2"); store.bind({ rootSessionId: "a", directory: "d-a", ownerInstance: "next" })
	const ownerResponse = await broker.handleRequest(rpcAs("next", "next-token", "native-request-open", base)), ownerBody = await ownerResponse.json(); expect(ownerResponse.status).toBe(409); expect(ownerBody).toEqual({ error: "native-request-conflict" }); expect(JSON.stringify(ownerBody)).not.toContain("identity-q"); expect(adapter.sent).toHaveLength(relays)
	store.close()
})

test("non-controller cannot prompt resolve or receive native notices", async () => {
	let callbacks = 0; const { store, adapter, broker } = flow(async () => { callbacks++; return Response.json({ ok: true, accepted: true }) }); await openNative(broker, { requestId: "isolated", kind: "QUESTION", payload: questionPayload() }); const code = store.nativeRequest("isolated")!.code, sent = adapter.sent.length
	expect((await broker.handleInbound(inbound("attacker", 1, `${code} A`, "attacker"))).reason).toBe("route-rejected"); expect(callbacks).toBe(0); expect(adapter.sent).toHaveLength(sent); expect(store.nativeRequest("isolated")?.state).toBe("OPEN"); store.close()
})

test("wechat reply uses durable call-ID dedupe and UNKNOWN never retries", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({})); const send = () => broker.handleRequest(rpc("wechat-reply", { rootSessionId: "root", callId: "call-1", text: "hello" }))
	const first = await send(); expect(first.status).toBe(200); expect(await first.json()).toMatchObject({ ok: true, state: "SENT" }); expect(adapter.sent.at(-1)?.text).toBe("#1\nhello"); const duplicate = await send(); expect(await duplicate.json()).toMatchObject({ replayed: true, state: "SENT" }); expect(adapter.sent).toHaveLength(1)
	const conflict = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "root", callId: "call-1", text: "different" })); expect(conflict.status).toBe(409); expect(await conflict.json()).toEqual({ error: "reply-call-conflict" }); expect(adapter.sent).toHaveLength(1)
	adapter.failSend = true; const unknown = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "root", callId: "call-2", text: "uncertain" })); expect(unknown.status).toBe(409); expect(await unknown.json()).toMatchObject({ state: "UNKNOWN" }); adapter.failSend = false; const retry = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "root", callId: "call-2", text: "uncertain" })); expect(await retry.json()).toMatchObject({ replayed: true, state: "UNKNOWN" }); expect(adapter.sent).toHaveLength(1); expect(store.controlOutboundState("wechat-reply:root:call-2")).toBe("UNKNOWN"); store.close()
})

test("wechat reply replay uses logical text after alias movement and preserves historical rendering", async () => {
	const { store, adapter, broker } = flow(async () => Response.json({}), ["a", "b"])
	const first = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "b", callId: "move-call", text: "hello" })); expect(first.status).toBe(200); expect(adapter.sent.at(-1)?.text).toBe("#2\nhello"); store.deactivateBinding("a", "owner")
	const replay = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "b", callId: "move-call", text: "hello" })); expect(await replay.json()).toMatchObject({ replayed: true, state: "SENT" }); expect(adapter.sent).toHaveLength(1); expect(store.controlOutboundPayload("wechat-reply:b:move-call")).toBe("#2\nhello")
	const conflict = await broker.handleRequest(rpc("wechat-reply", { rootSessionId: "b", callId: "move-call", text: "changed" })); expect(conflict.status).toBe(409); expect(await conflict.json()).toEqual({ error: "reply-call-conflict" }); store.close()
})

type JsonObject = Record<string, unknown>
