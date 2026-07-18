import { expect, test } from "bun:test"
import { wechatReplyRpc } from "./plugin-runtime"

const body = { method: "wechat-reply", callId: "call-1", text: "hello" }

test("wechat reply RPC returns the durable 409 UNKNOWN result without retrying", async () => {
	let requests = 0
	const result = await wechatReplyRpc("endpoint", "secret", body, async () => {
		requests++
		return Response.json({ ok: false, state: "UNKNOWN", replayed: false }, { status: 409 })
	})
	expect(result).toEqual({ ok: false, state: "UNKNOWN", replayed: false })
	expect(requests).toBe(1)
})

test("wechat reply RPC rejects call conflicts", async () => {
	await expect(wechatReplyRpc("endpoint", "secret", body, async () => Response.json({ error: "reply-call-conflict" }, { status: 409 }))).rejects.toThrow("reply-call-conflict")
})

test("wechat reply RPC rejects malformed UNKNOWN responses", async () => {
	await expect(wechatReplyRpc("endpoint", "secret", body, async () => Response.json({ ok: false, state: "UNKNOWN", replayed: true }, { status: 409 }))).rejects.toThrow("broker RPC failed")
	await expect(wechatReplyRpc("endpoint", "secret", body, async () => new Response("not-json", { status: 409 }))).rejects.toThrow()
})
