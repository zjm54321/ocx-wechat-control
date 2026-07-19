import { expect, test } from "bun:test"
import { createOpencodeClient } from "@opencode-ai/sdk/client"
import { MockWeChatAdapter } from "./adapter"
import { BrokerService } from "./broker"
import { startCallbackServer } from "./client"
import { startPluginCallbackServer } from "./plugin-runtime"
import { Store } from "./core"
import type { V2ControlClient } from "./control-client"

test("plugin callback and broker integrate prompt admission, native resolution, and explicit-only replies", async () => {
	const prompts: string[] = [], questionReplies: string[][][] = [], permissionReplies: string[] = [], pendingQuestions = new Set(["question-1"]), pendingPermissions = new Set(["permission-1"])
	const controlClient: V2ControlClient = {
		sessionGet: async ({ sessionID }) => ({ data: { id: sessionID }, error: undefined, status: 200 }),
		sessionPromptAsync: async ({ parts }) => { prompts.push(parts[0].text); return { data: undefined, error: undefined, status: 204 } },
		sessionStatus: async () => ({ data: {}, error: undefined, status: 200 }),
		questionList: async () => ({ data: [...pendingQuestions].map((id) => ({ id, sessionID: "root" })), error: undefined, status: 200 }),
		questionReply: async ({ requestID, answers }) => { pendingQuestions.delete(requestID); questionReplies.push(answers); return { data: true, error: undefined, status: 200 } },
		permissionList: async () => ({ data: [...pendingPermissions].map((id) => ({ id, sessionID: "root" })), error: undefined, status: 200 }),
		permissionReply: async ({ requestID, reply }) => { pendingPermissions.delete(requestID); permissionReplies.push(reply); return { data: true, error: undefined, status: 200 } },
	}
	const callback = startCallbackServer({ session: { get: async () => ({ data: { id: "root" } }), prompt: async () => ({}) } }, "secret", "token", controlClient, "directory")
	const store = new Store(":memory:"), adapter = new MockWeChatAdapter(); store.register("owner", "token", callback.endpoint); store.bind({ rootSessionId: "root", directory: "directory", ownerInstance: "owner" }); store.refreshRoute("controller", "context")
	const broker = new BrokerService(store, adapter, "secret", "worker"), auth = (method: string, extra: object) => new Request("http://broker", { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret" }, body: JSON.stringify({ method, instanceId: "owner", instanceToken: "token", rootSessionId: "root", ...extra }) }), inbound = (id: string, text: string) => broker.handleInbound({ id, fromUserId: "controller", contextToken: "context", text: `#1\n${text}`, cursorHint: id })
	try {
		expect((await inbound("one", "first")).ok).toBe(true); expect((await inbound("two", "second")).ok).toBe(true); expect(prompts).toEqual(["first", "second"]); expect(adapter.sent).toHaveLength(0)
		await broker.handleRequest(auth("native-request-open", { requestId: "question-1", requestKey: "q-key", kind: "QUESTION", payload: { sourceSessionId: "root", questions: [{ question: "Choose", options: [{ label: "A" }, { label: "B" }], multiple: false, custom: false }] } })); const question = store.nativeRequest("question-1")!; expect(adapter.sent.at(-1)?.text).toContain(question.code)
		expect((await inbound("question-answer", `${question.code} 2`)).ok).toBe(true); expect(questionReplies).toEqual([[['B']]])
		await broker.handleRequest(auth("native-request-open", { requestId: "permission-1", requestKey: "p-key", kind: "PERMISSION", payload: { sourceSessionId: "root", permission: "write" } })); const permission = store.nativeRequest("permission-1")!; expect((await inbound("permission-answer", `${permission.code} once`)).ok).toBe(true); expect(permissionReplies).toEqual(["once"])
		const sendsBeforeReply = adapter.sent.length; await broker.handleRequest(auth("observe-assistant", { assistantMessageId: "assistant-1", failed: false })); expect(adapter.sent).toHaveLength(sendsBeforeReply)
		const reply = await broker.handleRequest(auth("wechat-reply", { callId: "reply-1", text: "explicit" })); expect(reply.status).toBe(200); expect(adapter.sent.at(-1)?.text).toBe("#1\nexplicit")
	} finally { callback.stop(); store.close() }
})

test("actual SDK v2 HTTP contract admits concurrently and closes native requests", async () => {
	const paths: string[] = [], prompts: string[] = [], questionBodies: unknown[] = [], permissionBodies: unknown[] = []
	let releaseFirst!: () => void, firstEntered!: () => void; const firstGate = new Promise<void>((resolve) => releaseFirst = resolve), firstStarted = new Promise<void>((resolve) => firstEntered = resolve)
	const upstream = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) => {
		const url = new URL(request.url); paths.push(`${request.method} ${url.pathname}`)
		if (request.method === "GET" && url.pathname === "/session/root") return Response.json({ id: "root" })
		if (request.method === "POST" && url.pathname === "/session/root/prompt_async") { const body = await request.json() as { parts: Array<{ text: string }> }; prompts.push(body.parts[0].text); if (prompts.length === 1) { firstEntered(); await firstGate } return new Response(null, { status: 204 }) }
		if (request.method === "GET" && url.pathname === "/question") return Response.json([{ id: "sdk-question", sessionID: "root" }])
		if (request.method === "POST" && url.pathname === "/question/sdk-question/reply") { questionBodies.push(await request.json()); return Response.json(true) }
		if (request.method === "GET" && url.pathname === "/permission") return Response.json([{ id: "sdk-permission", sessionID: "root" }])
		if (request.method === "POST" && url.pathname === "/permission/sdk-permission/reply") { permissionBodies.push(await request.json()); return Response.json(true) }
		return Response.json({ error: "not-found" }, { status: 404 })
	} })
	const supplied = createOpencodeClient({ baseUrl: upstream.url.toString(), directory: "directory" })
	const callback = startPluginCallbackServer(supplied, new URL("http://unreachable-server-url.invalid:1"), "directory", "secret", "token"), headers = { "content-type": "application/json", "x-wechat-control-key": "secret", "x-wechat-instance-token": "token" }
	const post = (pathname: string, body: object) => fetch(new URL(pathname, callback.endpoint), { method: "POST", headers, body: JSON.stringify({ rootSessionId: "root", directory: "directory", ...body }) })
	try {
		const first = post("/submit-prompt", { inboundId: "one", messageId: "message-one", text: "first" }); await firstStarted
		const second = post("/submit-prompt", { inboundId: "two", messageId: "message-two", text: "second" }); const secondResponse = await second; expect(secondResponse.status).toBe(200); expect(prompts).toEqual(["first", "second"]); releaseFirst(); expect((await first).status).toBe(200)
		const question = await post("/resolve-question", { sourceSessionId: "root", requestId: "sdk-question", answers: [["A"]] }); expect(question.status).toBe(200); expect(await question.json()).toMatchObject({ resolved: true }); expect(questionBodies).toEqual([{ answers: [["A"]] }])
		const permission = await post("/resolve-permission", { sourceSessionId: "root", requestId: "sdk-permission", decision: "reject" }); expect(permission.status).toBe(200); expect(await permission.json()).toMatchObject({ resolved: true }); expect(permissionBodies).toEqual([{ reply: "reject" }])
		expect(paths).toContain("POST /session/root/prompt_async"); expect(paths).toContain("POST /question/sdk-question/reply"); expect(paths).toContain("POST /permission/sdk-permission/reply")
	} finally { releaseFirst?.(); callback.stop(); upstream.stop() }
})
