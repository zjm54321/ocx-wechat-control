import { expect, test } from "bun:test"

test("published entry exposes only the default OpenCode plugin", async () => {
	const runtime = await import(new URL(`../dist/index.js?entry-test=${crypto.randomUUID()}`, import.meta.url).href)
	expect(Object.keys(runtime)).toEqual(["default"])
	expect(typeof runtime.default).toBe("function")
})
