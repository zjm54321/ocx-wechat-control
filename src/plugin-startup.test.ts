import { expect, test } from "bun:test"
import { createOpencodeClient } from "@opencode-ai/sdk/client"
import { startPluginCallbackServer } from "./plugin-runtime"

const callbackHeaders = { "content-type": "application/json", "x-wechat-control-key": "secret", "x-wechat-instance-token": "token" }
type TransportCall = { method: string; url: string; authorization: string | null; customAuth: string | null; body: unknown }

test("all callback SDK operations share the supplied authenticated 1.18.3 transport", async () => {
	const calls: TransportCall[] = []
	const supplied = createOpencodeClient({
		baseUrl: "http://unreachable-base.invalid:1",
		directory: "plugin-directory",
		headers: { Authorization: "Bearer in-process", "x-custom-auth": "custom-secret" },
		fetch: async (request) => {
			const url = new URL(request.url), text = request.body ? await request.clone().text() : ""
			calls.push({ method: request.method, url: url.toString(), authorization: request.headers.get("authorization"), customAuth: request.headers.get("x-custom-auth"), body: text ? JSON.parse(text) : undefined })
			if (request.method === "GET" && url.pathname === "/session/root") return Response.json({ id: "root" })
			if (request.method === "POST" && url.pathname === "/session/root/prompt_async") return new Response(null, { status: 204 })
			if (request.method === "GET" && url.pathname === "/session/status") return Response.json({ root: { type: "busy" } })
			if (request.method === "GET" && url.pathname === "/question") return Response.json([{ id: "question", sessionID: "root" }])
			if (request.method === "POST" && url.pathname === "/question/question/reply") return Response.json(true)
			if (request.method === "GET" && url.pathname === "/permission") return Response.json([{ id: "permission", sessionID: "root" }])
			if (request.method === "POST" && url.pathname === "/permission/permission/reply") return Response.json(true)
			return Response.json({ error: "not-found" }, { status: 404 })
		},
	})
	const callback = startPluginCallbackServer(supplied, new URL("http://unreachable-server-url.invalid:2"), "plugin-directory", "secret", "token")
	const post = (pathname: string, body: object) => Bun.fetch(new URL(pathname, callback.endpoint), { method: "POST", headers: callbackHeaders, body: JSON.stringify(body) })
	try {
		const health = await post("/health", { rootSessionId: "root", directory: "untrusted-body-directory" }); expect(health.status).toBe(200)
		const prompt = await post("/submit-prompt", { rootSessionId: "root", directory: "plugin-directory", inboundId: "inbound", messageId: "stable-message", text: "hello" }); expect(prompt.status).toBe(200); expect(await prompt.json()).toEqual({ ok: true, accepted: true })
		const status = await post("/runtime-status", { rootSessionId: "root", rootSessionIds: ["root"], directory: "plugin-directory" }); expect(status.status).toBe(200); expect(await status.json()).toEqual({ ok: true, statuses: [{ rootSessionId: "root", status: "BUSY" }] })
		const question = await post("/resolve-question", { rootSessionId: "root", sourceSessionId: "root", requestId: "question", directory: "plugin-directory", answers: [["A", "custom"]] }); expect(question.status).toBe(200); expect(await question.json()).toEqual({ ok: true, resolved: true })
		const permission = await post("/resolve-permission", { rootSessionId: "root", sourceSessionId: "root", requestId: "permission", directory: "plugin-directory", decision: "reject" }); expect(permission.status).toBe(200); expect(await permission.json()).toEqual({ ok: true, resolved: true })

		expect(calls.map(({ method, url }) => `${method} ${url}`)).toEqual([
			"GET http://unreachable-base.invalid:1/session/root?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/session/root?directory=plugin-directory",
			"POST http://unreachable-base.invalid:1/session/root/prompt_async?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/session/root?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/session/status?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/session/root?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/question?directory=plugin-directory",
			"POST http://unreachable-base.invalid:1/question/question/reply?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/session/root?directory=plugin-directory",
			"GET http://unreachable-base.invalid:1/permission?directory=plugin-directory",
			"POST http://unreachable-base.invalid:1/permission/permission/reply?directory=plugin-directory",
		])
		expect(calls.every((call) => call.authorization === "Bearer in-process" && call.customAuth === "custom-secret")).toBe(true)
		expect(calls[2].body).toEqual({ messageID: "stable-message", system: "This turn came from the bound WeChat conversation. After composing your answer, you must call wechat_reply({text}) with the answer. Do not leave the answer only in the TUI.", parts: [{ type: "text", text: "hello" }] })
		expect(calls[7].body).toEqual({ answers: [["A", "custom"]] })
		expect(calls[10].body).toEqual({ reply: "reject" })
		expect(calls.every((call) => !call.url.includes("directory=") || !call.url.endsWith("directory="))).toBe(true)
	} finally { callback.stop() }
})

test("supplied transport failure after prompt call is UNKNOWN and never retried", async () => {
	let promptCalls = 0
	const supplied = createOpencodeClient({ baseUrl: "http://unreachable-base.invalid:1", directory: "plugin-directory", fetch: async (request) => {
		const url = new URL(request.url)
		if (request.method === "GET" && url.pathname === "/session/root") return Response.json({ id: "root" })
		if (request.method === "POST" && url.pathname === "/session/root/prompt_async") { promptCalls++; throw new TypeError("in-process transport failed") }
		return Response.json({ error: "not-found" }, { status: 404 })
	} })
	const callback = startPluginCallbackServer(supplied, new URL("http://unreachable-server-url.invalid:2"), "plugin-directory", "secret", "token")
	try {
		const response = await Bun.fetch(new URL("/submit-prompt", callback.endpoint), { method: "POST", headers: callbackHeaders, body: JSON.stringify({ rootSessionId: "root", directory: "plugin-directory", inboundId: "inbound", messageId: "stable-message", text: "hello" }) })
		expect(response.status).toBe(409); expect(await response.json()).toMatchObject({ certainty: "UNKNOWN", error: "prompt-admission-uncertain" }); expect(promptCalls).toBe(1)
	} finally { callback.stop() }
})

test("incompatible plugin transport and empty directory fail before callback startup", () => {
	const incompatible = { session: { get: async () => ({ data: { id: "root" } }), prompt: async () => ({}) } }
	expect(() => startPluginCallbackServer(incompatible, new URL("http://unreachable.invalid:1"), "directory", "secret", "token")).toThrow("authenticated transport is unavailable")
	const supplied = createOpencodeClient({ baseUrl: "http://unreachable.invalid:1", fetch: async () => Response.json({}) })
	expect(() => startPluginCallbackServer(supplied, new URL("http://unreachable.invalid:2"), "", "secret", "token")).toThrow("Invalid callback plugin directory")
})
