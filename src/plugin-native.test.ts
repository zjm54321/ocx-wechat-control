import { expect, test } from "bun:test"
import { createControlEventHook, createPluginEventHook } from "./plugin-runtime"

test("native event hook relays guarded question and permission events and terminals", async () => {
	const calls: object[] = [], hook = createControlEventHook(async (_endpoint, _secret, body) => { calls.push(body); return {} }, "e", "s", "instance", "token", async () => "root")
	await hook({ event: { type: "question.asked", properties: { sessionID: "child", id: "q1", questions: [{ question: "Q", options: [{ label: "A", description: "a" }] }] } } })
	await hook({ event: { type: "permission.asked", properties: { sessionID: "child", id: "p1", permission: "write" } } })
	await hook({ event: { type: "question.replied", properties: { sessionID: "child", requestID: "q1", answers: [["A"]] } } })
	await hook({ event: { type: "permission.rejected", properties: { sessionID: "child", requestID: "p1" } } })
	expect(calls.map((call) => (call as { method?: string }).method)).toEqual(["native-request-open", "native-request-open", "native-request-terminal", "native-request-terminal"])
	expect(calls[0]).toMatchObject({ kind: "QUESTION", rootSessionId: "root", requestId: "q1" }); expect(calls[1]).toMatchObject({ kind: "PERMISSION", requestId: "p1" }); expect(calls[2]).toMatchObject({ state: "RESOLVED" }); expect(calls[3]).toMatchObject({ state: "REJECTED" })
})

test("native event hook ignores malformed payloads and preserves permission ask", async () => {
	const calls: unknown[] = [], hook = createControlEventHook(async () => { calls.push(true); return {} }, "e", "s", "instance", "token", async () => "root")
	await hook({ event: { type: "permission.asked", properties: { sessionID: "child", id: "p", permission: 1 } } }); await hook({ event: { type: "question.asked", properties: { sessionID: "child", id: "q", questions: "bad" } } }); await hook({ event: { type: "permission.replied", properties: { sessionID: "child", requestID: "p", reply: "invalid" } } }); expect(calls).toHaveLength(0)
})

test("permission.replied maps reject and once to native terminal states", async () => {
	const calls: object[] = [], hook = createControlEventHook(async (_endpoint, _secret, body) => { calls.push(body); return {} }, "e", "s", "instance", "token", async () => "root")
	await hook({ event: { type: "permission.replied", properties: { sessionID: "child", requestID: "p-reject", reply: "reject" } } })
	await hook({ event: { type: "permission.replied", properties: { sessionID: "child", requestID: "p-once", reply: "once" } } })
	expect(calls).toEqual([
		{ method: "native-request-terminal", instanceId: "instance", instanceToken: "token", rootSessionId: "root", requestId: "p-reject", state: "REJECTED", resolution: "reject" },
		{ method: "native-request-terminal", instanceId: "instance", instanceToken: "token", rootSessionId: "root", requestId: "p-once", state: "RESOLVED", resolution: "once" },
	])
})

test("production event-hook factory resolves child sessions for open and terminal RPCs", async () => {
	const calls: object[] = [], resolved: string[] = [], hook = createPluginEventHook(async (sessionId) => { resolved.push(sessionId); return "root" }, async (_endpoint, _secret, body) => { calls.push(body); return {} }, "e", "s", "instance", "token")
	await hook({ event: { type: "question.asked", properties: { sessionID: "child", id: "q-child", questions: [{ question: "Q", options: [] }] } } })
	await hook({ event: { type: "question.replied", properties: { sessionID: "child", requestID: "q-child", answers: [[]] } } })
	expect(resolved).toEqual(["child", "child"]); expect(calls).toHaveLength(2); expect(calls[0]).toMatchObject({ method: "native-request-open", rootSessionId: "root", requestId: "q-child" }); expect(calls[1]).toMatchObject({ method: "native-request-terminal", rootSessionId: "root", requestId: "q-child" })
})
