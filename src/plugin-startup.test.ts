import { expect, test } from "bun:test"
import { startPluginCallbackServer } from "./plugin-runtime"

test("production plugin callback startup wires the v2 client and promptAsync path", async () => {
	const calls: string[] = [], upstream = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) => {
		const url = new URL(request.url); calls.push(url.pathname)
		if (url.pathname === "/session/root") return Response.json({ id: "root" })
		if (url.pathname === "/session/root/prompt_async") return new Response(null, { status: 204 })
		return Response.json({ error: "not-found" }, { status: 404 })
	} })
	const callback = startPluginCallbackServer({ session: { get: async () => ({ data: { id: "root" } }), prompt: async () => ({}) } }, new URL(upstream.url), "directory", "secret", "token")
	try {
		const response = await fetch(new URL("/submit-prompt", callback.endpoint), { method: "POST", headers: { "content-type": "application/json", "x-wechat-control-key": "secret", "x-wechat-instance-token": "token" }, body: JSON.stringify({ rootSessionId: "root", directory: "directory", inboundId: "inbound", messageId: "message", text: "hello" }) })
		expect(response.status).toBe(200); expect(await response.json()).toEqual({ ok: true, accepted: true }); expect(calls).toEqual(["/session/root", "/session/root/prompt_async"])
	} finally { callback.stop(); upstream.stop() }
})
