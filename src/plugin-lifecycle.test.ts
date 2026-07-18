import { expect, test } from "bun:test"
import type { ClientLifecycle } from "./client"

test("plugin lifecycle registry and process handlers survive distinct module identities", async () => {
	const moduleA = await import("./plugin-runtime?lifecycle-a")
	const moduleB = await import("./plugin-runtime?lifecycle-b")
	const hooksA = moduleA.pluginLifecycleTestHooks, hooksB = moduleB.pluginLifecycleTestHooks
	await hooksA.reset()
	const listenerCounts = () => [process.listenerCount("beforeExit"), process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]
	const before = listenerCounts()
	let stoppedA = 0, stoppedB = 0
	const lifecycleA: ClientLifecycle = { stop: async () => { stoppedA++ } }
	const lifecycleB: ClientLifecycle = { stop: async () => { stoppedB++ } }
	try {
		expect(hooksA.registry).toBe(hooksB.registry)
		await hooksA.registry.replace("directory", lifecycleA)
		await hooksB.registry.replace("directory", lifecycleB)
		expect(stoppedA).toBe(1)
		await hooksA.registry.remove("directory", lifecycleA)
		expect(moduleB.lifecycleRegistrySize()).toBe(1)
		hooksA.installExitHandlers(); hooksB.installExitHandlers()
		expect(hooksA.exitHandlersInstalled()).toBe(true)
		expect(listenerCounts()).toEqual(before.map((count) => count + 1))
	} finally {
		await hooksB.reset()
	}
	expect(stoppedA).toBe(1)
	expect(stoppedB).toBe(1)
	expect(listenerCounts()).toEqual(before)
})
