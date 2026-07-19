import { expect, test } from "bun:test"
import { createCallbackHandler } from "./client"
import type { ControlResult, V2ControlClient } from "./control-client"
import { STALE_REAPER_PROOF_DOMAIN, STALE_REAPER_PROOF_VERSION, signStaleReaperRequest, verifyStaleReaperResponse } from "./stale-reaper-auth"

const ok = (data: unknown, status = 200): ControlResult => ({ data, error: undefined, status })
const legacy = { session: { get: async () => ({ data: { id: "root" } }), prompt: async () => ({}) } }

class FakeControlClient implements V2ControlClient {
	readonly sessions = new Map<string, { id: string; parentID?: string }>([["root", { id: "root" }], ["child", { id: "child", parentID: "root" }]])
	readonly sessionGets: Array<{ sessionID: string; directory: string }> = []
	readonly prompts: Array<{ sessionID: string; directory: string; messageID: string; system?: string; parts: Array<{ type: "text"; text: string }> }> = []
	readonly questionReplies: Array<{ requestID: string; directory: string; answers: string[][] }> = []
	readonly permissionReplies: Array<{ requestID: string; directory: string; reply: "once" | "reject" }> = []
	questionRequests: unknown = [{ id: "question", sessionID: "child" }]
	permissionRequests: unknown = [{ id: "permission", sessionID: "child" }]
	statuses: unknown = { root: { type: "busy" } }
	promptResult = ok(undefined, 204)
	async sessionGet(input: { sessionID: string; directory: string }): Promise<ControlResult> { this.sessionGets.push(input); const value = this.sessions.get(input.sessionID); return value ? ok(value) : { data: undefined, error: { name: "not-found" }, status: 404 } }
	async sessionPromptAsync(input: { sessionID: string; directory: string; messageID: string; system?: string; parts: Array<{ type: "text"; text: string }> }, signal?: AbortSignal): Promise<ControlResult> { this.prompts.push(input); if (signal?.aborted) throw new DOMException("aborted", "AbortError"); return this.promptResult }
	async sessionStatus(_input: { directory: string }): Promise<ControlResult> { return ok(this.statuses) }
	async questionList(_input: { directory: string }): Promise<ControlResult> { return ok(this.questionRequests) }
	async questionReply(input: { requestID: string; directory: string; answers: string[][] }): Promise<ControlResult> { this.questionReplies.push(input); return ok(true) }
	async permissionList(_input: { directory: string }): Promise<ControlResult> { return ok(this.permissionRequests) }
	async permissionReply(input: { requestID: string; directory: string; reply: "once" | "reject" }): Promise<ControlResult> { this.permissionReplies.push(input); return ok(true) }
}

function request(path: string, body: unknown, headers: Record<string, string> = {}, signal?: AbortSignal): Request {
	return new Request(`http://127.0.0.1${path}`, { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret", "x-wechat-instance-token": "token", ...headers }, body: JSON.stringify(body), signal })
}
function probeRequest(body: unknown): Request { return new Request("http://127.0.0.1/health", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify(body) }) }

function handler(client: V2ControlClient, directory = "d"): (request: Request) => Promise<Response> { return createCallbackHandler(legacy, "secret", "token", client, directory) }

test("submit-prompt admits exact stable message asynchronously with explicit 204", async () => {
	const client = new FakeControlClient(), response = await handler(client, "C:/work")(request("/submit-prompt", { rootSessionId: "root", directory: "C:/work", inboundId: "inbound-stable", messageId: "msg_stable", text: "do work" }))
	expect(response.status).toBe(200); expect(await response.json()).toEqual({ ok: true, accepted: true }); expect(client.prompts).toEqual([{ sessionID: "root", directory: "C:/work", messageID: "msg_stable", system: "This turn came from the bound WeChat conversation. After composing your answer, you must call wechat_reply({text}) with the answer. Do not leave the answer only in the TUI.", parts: [{ type: "text", text: "do work" }] }])
})

test("submit-prompt rejects non-root before call and maps post-call ambiguity to UNKNOWN", async () => {
	const client = new FakeControlClient(), run = handler(client)
	const child = await run(request("/submit-prompt", { rootSessionId: "child", directory: "d", inboundId: "in", messageId: "msg", text: "x" })); expect(child.status).toBe(409); expect(await child.json()).toMatchObject({ certainty: "REJECTED", error: "not-root" }); expect(client.prompts).toHaveLength(0)
	client.promptResult = { data: undefined, error: { name: "transport" }, status: 0 }; const uncertain = await run(request("/submit-prompt", { rootSessionId: "root", directory: "d", inboundId: "in", messageId: "msg", text: "x" })); expect(uncertain.status).toBe(409); expect(await uncertain.json()).toMatchObject({ certainty: "UNKNOWN" }); expect(client.prompts).toHaveLength(1)
	client.promptResult = ok(undefined, 204); const abort = new AbortController(); abort.abort(); const aborted = await run(request("/submit-prompt", { rootSessionId: "root", directory: "d", inboundId: "abort-in", messageId: "abort-message", text: "x" }, {}, abort.signal)); expect(aborted.status).toBe(409); expect(await aborted.json()).toMatchObject({ certainty: "UNKNOWN" }); expect(client.prompts).toHaveLength(2)
})

test("health uses configured directory and callbacks reject directory mismatch before transport", async () => {
	const client = new FakeControlClient(), run = handler(client, "configured-directory")
	const health = await run(request("/health", { rootSessionId: "root", directory: "body-directory-must-be-ignored" })); expect(health.status).toBe(200); expect(client.sessionGets).toEqual([{ sessionID: "root", directory: "configured-directory" }])
	client.sessionGets.splice(0); const mismatch = await run(request("/submit-prompt", { rootSessionId: "root", directory: "other-directory", inboundId: "in", messageId: "msg", text: "x" })); expect(mismatch.status).toBe(409); expect(await mismatch.json()).toMatchObject({ certainty: "REJECTED", error: "directory-mismatch" }); expect(client.sessionGets).toHaveLength(0); expect(client.prompts).toHaveLength(0)
})

test("question resolution validates child ancestry and preserves ordered string answers", async () => {
	const client = new FakeControlClient(), response = await handler(client)(request("/resolve-question", { rootSessionId: "root", sourceSessionId: "child", requestId: "question", directory: "d", answers: [["B", "A"], ["custom"]] }))
	expect(response.status).toBe(200); expect(client.questionReplies).toEqual([{ requestID: "question", directory: "d", answers: [["B", "A"], ["custom"]] }]); expect(client.prompts).toHaveLength(0)
	client.sessions.set("other", { id: "other" }); client.sessions.set("foreign", { id: "foreign", parentID: "other" }); client.questionRequests = [{ id: "foreign-question", sessionID: "foreign" }]
	const rejected = await handler(client)(request("/resolve-question", { rootSessionId: "root", sourceSessionId: "foreign", requestId: "foreign-question", directory: "d", answers: [["A"]] })); expect(rejected.status).toBe(409); expect(client.questionReplies).toHaveLength(1)
})

test("permission accepts only once or reject and never forwards always or invalid input", async () => {
	const client = new FakeControlClient(), run = handler(client), base = { rootSessionId: "root", sourceSessionId: "child", requestId: "permission", directory: "d" }
	for (const decision of ["once", "reject"] as const) { const response = await run(request("/resolve-permission", { ...base, decision })); expect(response.status).toBe(200) }
	expect(client.permissionReplies.map((item) => item.reply)).toEqual(["once", "reject"])
	for (const decision of ["always", "allow", true, null]) { const response = await run(request("/resolve-permission", { ...base, decision })); expect(response.status).toBe(400) }
	expect(client.permissionReplies).toHaveLength(2)
})

test("callbacks reject malformed, oversized, unavailable requests and authentication failures", async () => {
	const client = new FakeControlClient(), run = handler(client)
	const malformed = await run(request("/resolve-question", { rootSessionId: "root", sourceSessionId: "child", requestId: "question", directory: "d", answers: "A" })); expect(malformed.status).toBe(400)
	const oversized = await run(request("/submit-prompt", { rootSessionId: "root", directory: "d", inboundId: "in", messageId: "msg", text: "x".repeat(130_000) })); expect(oversized.status).toBe(400)
	client.questionRequests = []; const unavailable = await run(request("/resolve-question", { rootSessionId: "root", sourceSessionId: "child", requestId: "question", directory: "d", answers: [["A"]] })); expect(unavailable.status).toBe(409); expect(client.questionReplies).toHaveLength(0)
	const badSecret = await run(request("/runtime-status", { rootSessionId: "root", rootSessionIds: ["root"], directory: "d" }, { "x-wechat-control-key": "bad" })); expect(badSecret.status).toBe(401)
	const badToken = await run(request("/runtime-status", { rootSessionId: "root", rootSessionIds: ["root"], directory: "d" }, { "x-wechat-instance-token": "bad" })); expect(badToken.status).toBe(401)
})

test("runtime-status validates exact roots and normalizes authoritative statuses", async () => {
	const client = new FakeControlClient(); client.sessions.set("idle", { id: "idle" }); client.sessions.set("retry", { id: "retry" }); client.statuses = { root: { type: "busy" }, retry: { type: "retry", attempt: 1 }, extra: { type: "busy" } }
	const response = await handler(client)(request("/runtime-status", { rootSessionId: "root", rootSessionIds: ["root", "idle", "retry"], directory: "d" })); expect(response.status).toBe(200); expect(await response.json()).toEqual({ ok: true, statuses: [{ rootSessionId: "root", status: "BUSY" }, { rootSessionId: "idle", status: "IDLE" }, { rootSessionId: "retry", status: "RETRY" }] })
	const child = await handler(client)(request("/runtime-status", { rootSessionId: "root", rootSessionIds: ["child"], directory: "d" })); expect(child.status).toBe(409); expect(await child.json()).toMatchObject({ certainty: "REJECTED", error: "not-root" })
})

test("legacy inject is gone and cannot mutate session state", async () => {
	let gets = 0, prompts = 0
	const legacyClient = { session: { get: async () => { gets++; return { data: { id: "root" } } }, prompt: async () => { prompts++; return {} } } }
	const response = await createCallbackHandler(legacyClient, "secret", "token", new FakeControlClient(), "d")(request("/inject", { rootSessionId: "root", directory: "d", inboundId: "in", text: "do not inject", envelope: { kind: "inbound" } }))
	expect(response.status).toBe(410); expect(await response.json()).toEqual({ error: "legacy-inject-removed" }); expect(gets).toBe(0); expect(prompts).toBe(0)
})

test("stale reaper callback signs authenticated exact-root ok and not-root outcomes", async () => {
	const client = new FakeControlClient(), run = handler(client), challenge = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", proof = await signStaleReaperRequest("token", challenge, "root")
	const okResponse = await run(probeRequest({ proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: STALE_REAPER_PROOF_VERSION, challenge, rootSessionId: "root", requestProof: proof })); expect(okResponse.status).toBe(200); const okBody = await okResponse.json() as any; expect(okBody).toMatchObject({ ok: true, outcome: "ok", challenge, rootSessionId: "root" }); expect(await verifyStaleReaperResponse("token", challenge, "root", "ok", okBody.responseProof)).toBe(true)
	client.sessions.set("child-root", { id: "child-root", parentID: "root" }); const childProof = await signStaleReaperRequest("token", challenge, "child-root"), notRoot = await run(probeRequest({ proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: STALE_REAPER_PROOF_VERSION, challenge, rootSessionId: "child-root", requestProof: childProof })); expect(notRoot.status).toBe(409); const notRootBody = await notRoot.json() as any; expect(notRootBody).toMatchObject({ error: "not-root", outcome: "not-root" }); expect(await verifyStaleReaperResponse("token", challenge, "child-root", "not-root", notRootBody.responseProof)).toBe(true)
})

test("stale reaper request proof is required before exact-root lookup", async () => {
	let lookups = 0; const client = { session: { get: async () => { lookups++; return { data: { id: "root" } } }, prompt: async () => ({}) } }, run = createCallbackHandler(client, "secret", "token")
	const response = await run(probeRequest({ proofDomain: STALE_REAPER_PROOF_DOMAIN, proofVersion: STALE_REAPER_PROOF_VERSION, challenge: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", rootSessionId: "root" })); expect(response.status).toBe(401); expect(lookups).toBe(0)
})
