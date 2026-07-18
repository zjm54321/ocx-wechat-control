import * as path from "node:path"
import { existsSync } from "node:fs"
import { WeixinMcpAdapter, type WeChatAdapter } from "./adapter"
import { BrokerService } from "./broker"
import { Store, initializeState } from "./core"
import { acquireWorkerLock, type LockMetadata } from "./worker-runtime"
import { TypingCoordinator } from "./typing"
import { createWorkerMetadata, type WorkerMetadata } from "./worker-protocol"

export interface WorkerConfig { enabled: true; weixinCommand: string[] }
export interface WorkerDependencies {
	initializeState: typeof initializeState
	acquireLock: typeof acquireWorkerLock
	createStore(databasePath: string): Store
	createAdapter(config: WorkerConfig): WeChatAdapter
	createBroker(store: Store, adapter: WeChatAdapter, secret: string, token: string, typing: TypingCoordinator | undefined, metadata: WorkerMetadata): BrokerService
	createTyping?(store: Store): TypingCoordinator
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
		createTyping: (store) => new TypingCoordinator(store),
		createBroker: (store, adapter, secret, token, typing, workerMetadata) => new BrokerService(store, adapter, secret, token, fetch, { typing, workerMetadata }),
		waitForShutdown: () => new Promise<void>((resolve) => { process.once("SIGINT", resolve); process.once("SIGTERM", resolve); process.once("beforeExit", resolve) }),
	}
	const deps = { ...defaults, ...overrides }
	const state = await deps.initializeState(), workerToken = crypto.randomUUID(), startedAt = new Date().toISOString()
	const workerMetadata = createWorkerMetadata(process.pid, path.resolve(process.argv[1] ?? import.meta.path), config.weixinCommand)
	const initial: LockMetadata = { pid: process.pid, startedAt, workerToken, endpoint: "pending", heartbeat: startedAt, ...workerMetadata }
	let lock: Awaited<ReturnType<typeof acquireWorkerLock>> | undefined
	let store: Store | undefined
	let adapter: WeChatAdapter | undefined
	let broker: BrokerService | undefined
	let typing: TypingCoordinator | undefined
	let heartbeat: Timer | undefined
	let maintenance: Promise<void> | undefined
	let shuttingDown = false
	try {
		lock = await deps.acquireLock(state.directory, state.secret, initial)
		store = deps.createStore(path.join(state.directory, "state.sqlite"))
		adapter = deps.createAdapter(config)
		typing = deps.createTyping?.(store)
		broker = deps.createBroker(store, adapter, state.secret, workerToken, typing, workerMetadata)
		const endpoint = broker.start()
		await lock.update({ ...initial, endpoint, heartbeat: new Date().toISOString() })
		await broker.startAdapter()
		if (typing) await typing.startup()
		const update = (): Promise<void> => {
			if (shuttingDown) return Promise.resolve()
			if (maintenance) return maintenance
			const current = (async () => {
				try {
					store?.sweepOrphanWaiting()
					store?.sweepOutboundEchoes()
					if (typeof broker?.reconcileActiveRuntimes === "function") await broker.reconcileActiveRuntimes()
					if (shuttingDown) return
					store?.expireRuntimeLeases()
					typing?.refresh()
					await lock!.update({ ...initial, endpoint, heartbeat: new Date().toISOString() }).catch(() => {})
				} catch {}
			})()
			maintenance = current
			void current.then(() => { if (maintenance === current) maintenance = undefined })
			return current
		}
		await update(); heartbeat = setInterval(() => { void update() }, 15_000)
		await deps.waitForShutdown()
	} finally {
		shuttingDown = true
		if (heartbeat) clearInterval(heartbeat)
		await maintenance?.catch(() => {})
		try { await typing?.shutdown() } catch {}
		try { if (broker) broker.stop(); else adapter?.stop() } catch {}
		try { store?.close() } catch {}
		await lock?.release().catch(() => {})
	}
}

if (import.meta.main) {
	try { await runWorker(parseWorkerConfig(process.argv[2])) }
	catch { process.exitCode = 1 }
}
