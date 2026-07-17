import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import * as path from "node:path"
import { makeRpcRequest } from "./broker"

export interface LockMetadata { pid: number; startedAt: string; workerToken: string; endpoint: string; heartbeat: string; format?: "v1" | "v2" }
export type ExistingDecision = "connect" | "refuse" | "takeover"
export type PidStatus = "alive" | "dead" | "unknown"

export function decideExistingBroker(pidAlive: boolean | "unknown", authenticatedHealth: boolean): ExistingDecision {
	if (authenticatedHealth) return "connect"
	if (pidAlive === true || pidAlive === "unknown") return "refuse"
	return "takeover"
}

export function pidStatus(pid: number, probe: (pid: number) => void = (value) => process.kill(value, 0)): PidStatus {
	if (!Number.isSafeInteger(pid) || pid <= 0) return "unknown"
	try { probe(pid); return "alive" } catch (error: any) { return error?.code === "ESRCH" ? "dead" : "unknown" }
}
export function isPidAlive(pid: number): boolean { return pidStatus(pid) === "alive" }

export async function readLock(directory: string): Promise<LockMetadata | undefined> {
	try {
		const value = JSON.parse(await readFile(path.join(directory, "broker.lock", "owner.json"), "utf8"))
		const workerToken = typeof value.workerToken === "string" ? value.workerToken : typeof value.instanceToken === "string" ? value.instanceToken : undefined
		if (!Number.isSafeInteger(value.pid) || !workerToken || typeof value.endpoint !== "string") return
		return { pid: value.pid, startedAt: String(value.startedAt ?? ""), workerToken, endpoint: value.endpoint, heartbeat: String(value.heartbeat ?? ""), format: typeof value.workerToken === "string" ? "v2" : "v1" }
	} catch { return }
}

export async function authenticatedHealth(metadata: LockMetadata, secret: string): Promise<boolean> {
	if (!metadata.endpoint.startsWith("http://127.0.0.1:")) return false
	try { const response = await makeRpcRequest(metadata.endpoint, secret, { method: "health", challenge: metadata.workerToken }); const body = await response.json() as any; return response.ok && body.challenge === metadata.workerToken }
	catch { return false }
}

export async function acquireWorkerLock(directory: string, secret: string, initial: LockMetadata): Promise<{ update(value: LockMetadata): Promise<void>; release(): Promise<void> }> {
	const lock = path.join(directory, "broker.lock"), owner = path.join(lock, "owner.json")
	const existing = await readLock(directory)
	if (existing) {
		const status = pidStatus(existing.pid)
		const decision = decideExistingBroker(status === "unknown" ? "unknown" : status === "alive", await authenticatedHealth(existing, secret))
		if (decision === "connect") throw new Error("ExistingBrokerHealthy")
		if (decision === "refuse") throw new Error("ExistingBrokerUnverifiableAndPidAlive")
		// Preserve the stale lock atomically for audit; never delete an unverified owner.
		await rename(lock, path.join(directory, `broker.lock.${existing.format ?? "v2"}-stale-${Date.now()}`))
	}
	await mkdir(lock)
	await writeFile(owner, JSON.stringify(initial), { mode: 0o600 })
	return {
		update: async (value) => writeFile(owner, JSON.stringify(value), { mode: 0o600 }),
		release: async () => { const current = await readLock(directory); if (current?.workerToken === initial.workerToken) await rm(lock, { recursive: true, force: true }) },
	}
}
