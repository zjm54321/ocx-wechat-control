import { expect, test } from "bun:test"
import type { WeChatAdapter } from "./adapter"
import type { BrokerService } from "./broker"
import type { Store } from "./core"
import { runWorker } from "./worker"

test("shutdown waits for active maintenance before closing store and releasing lock", async () => {
	const events: string[] = []
	let reconcileCount = 0
	let releaseReconcile!: () => void
	let maintenanceStarted!: () => void
	const blocked = new Promise<void>((resolve) => { releaseReconcile = resolve })
	const started = new Promise<void>((resolve) => { maintenanceStarted = resolve })
	const store = {
		sweepOrphanBindings: () => { events.push("binding.sweep") },
		sweepOrphanWaiting: () => { events.push("sweep") },
		sweepOutboundEchoes: () => {},
		expireRuntimeLeases: () => { events.push("expire") },
		close: () => { events.push("store.close") },
	} as unknown as Store
	const broker = {
		start: () => "http://127.0.0.1:1",
		startAdapter: async () => {},
		startStaleBindingReaper: () => { events.push("reaper.start") },
		reapStaleBindings: async () => { events.push("reaper.run") },
		reconcileActiveRuntimes: async () => {
			reconcileCount++
			if (reconcileCount === 2) { events.push("reconcile.start"); maintenanceStarted(); await blocked; events.push("reconcile.end") }
		},
		stop: () => { events.push("broker.stop") },
	} as unknown as BrokerService
	const originalSetInterval = globalThis.setInterval
	const originalClearInterval = globalThis.clearInterval
	globalThis.setInterval = ((callback: TimerHandler) => { callback(); return 1 as unknown as ReturnType<typeof setInterval> }) as typeof setInterval
	globalThis.clearInterval = (() => {}) as typeof clearInterval
	try {
		const worker = runWorker({ enabled: true, weixinCommand: ["node", "fixed.js"] }, {
			initializeState: async () => ({ directory: ".", secret: "secret" }),
			acquireLock: async () => ({ update: async () => { events.push("lock.update") }, release: async () => { events.push("lock.release") } }),
			createStore: () => store,
			createAdapter: () => ({ stop: () => {} }) as WeChatAdapter,
			createTyping: undefined,
			createBroker: () => broker,
			waitForShutdown: async () => { events.push("shutdown") },
		})
		await started
		await Promise.resolve()
		expect(events).not.toContain("store.close")
		expect(events).not.toContain("lock.release")
		releaseReconcile()
		await worker
		expect(events.indexOf("reconcile.end")).toBeLessThan(events.indexOf("store.close"))
		expect(events.indexOf("store.close")).toBeLessThan(events.indexOf("lock.release"))
		expect(events.filter((event) => event === "lock.update")).toHaveLength(2)
		expect(events.filter((event) => event === "expire")).toHaveLength(1)
		expect(events.filter((event) => event === "binding.sweep")).toHaveLength(2)
		expect(events.indexOf("binding.sweep")).toBeLessThan(events.indexOf("reaper.run")); expect(events.indexOf("reaper.run")).toBeLessThan(events.indexOf("reconcile.start"))
	} finally {
		globalThis.setInterval = originalSetInterval
		globalThis.clearInterval = originalClearInterval
		releaseReconcile()
	}
})
