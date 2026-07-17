import * as path from "node:path"
import { existsSync } from "node:fs"
import { WeixinMcpAdapter, type WeChatAdapter } from "./adapter"
import { BrokerService } from "./broker"
import { Store, initializeState } from "./core"
import { acquireWorkerLock, type LockMetadata } from "./worker-runtime"

export interface WorkerConfig { enabled: true; weixinCommand: string[] }
export interface WorkerDependencies {
	initializeState: typeof initializeState
	acquireLock: typeof acquireWorkerLock
	createStore(databasePath: string): Store
	createAdapter(config: WorkerConfig): WeChatAdapter
	createBroker(store: Store, adapter: WeChatAdapter, secret: string, token: string): BrokerService
	waitForShutdown(): Promise<void>
}

export function parseWorkerConfig(encoded: string | undefined): WorkerConfig {
	if (!encoded) throw new Error("missing worker config")
	const config = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")) as any
	if (config?.enabled !== true || !Array.isArray(config.weixinCommand) || config.weixinCommand.length !== 2 || config.weixinCommand[0] !== "node" || typeof config.weixinCommand[1] !== "string" || !config.weixinCommand[1].endsWith(`${path.sep}weixin-mcp${path.sep}dist${path.sep}cli.js`) || !existsSync(config.weixinCommand[1])) throw new Error("worker adapter script is missing or not the fixed local dependency")
	return config
}

export async function runWorker(config: WorkerConfig, overrides: Partial<WorkerDependencies> = {}): Promise<void> {
	const defaults: WorkerDependencies = {
		initializeState,
		acquireLock: acquireWorkerLock,
		createStore: (databasePath) => new Store(databasePath),
		createAdapter: (value) => new WeixinMcpAdapter({ enabled: value.enabled, command: value.weixinCommand }),
		createBroker: (store, adapter, secret, token) => new BrokerService(store, adapter, secret, token),
		waitForShutdown: () => new Promise<void>((resolve) => { process.once("SIGINT", resolve); process.once("SIGTERM", resolve); process.once("beforeExit", resolve) }),
	}
	const deps = { ...defaults, ...overrides }
	const state = await deps.initializeState(), workerToken = crypto.randomUUID(), startedAt = new Date().toISOString()
	const initial: LockMetadata = { pid: process.pid, startedAt, workerToken, endpoint: "pending", heartbeat: startedAt }
	let lock: Awaited<ReturnType<typeof acquireWorkerLock>> | undefined
	let store: Store | undefined
	let adapter: WeChatAdapter | undefined
	let broker: BrokerService | undefined
	let heartbeat: Timer | undefined
	try {
		lock = await deps.acquireLock(state.directory, state.secret, initial)
		store = deps.createStore(path.join(state.directory, "state.sqlite"))
		adapter = deps.createAdapter(config)
		broker = deps.createBroker(store, adapter, state.secret, workerToken)
		const endpoint = broker.start()
		const update = () => { store?.sweepOrphanWaiting(); store?.sweepOutboundEchoes(); return lock!.update({ ...initial, endpoint, heartbeat: new Date().toISOString() }).catch(() => {}) }
		await update(); heartbeat = setInterval(update, 15_000)
		await broker.startAdapter()
		await deps.waitForShutdown()
	} finally {
		if (heartbeat) clearInterval(heartbeat)
		try { if (broker) broker.stop(); else adapter?.stop() } catch {}
		try { store?.close() } catch {}
		await lock?.release().catch(() => {})
	}
}

if (import.meta.main) {
	try { await runWorker(parseWorkerConfig(process.argv[2])) }
	catch { process.exitCode = 1 }
}
