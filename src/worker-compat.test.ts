import { expect, test } from "bun:test"
import { createWorkerMetadata, WORKER_CAPABILITIES, WORKER_PACKAGE_VERSION, WORKER_PROTOCOL_VERSION, WORKER_SCHEMA_VERSION } from "./worker-protocol"
import { workerCompatibilityIssue, type LockMetadata } from "./worker-runtime"

function current(): { lock: LockMetadata; health: Record<string, unknown> } {
	const metadata = createWorkerMetadata(4321, "C:\\pkg\\dist\\worker.js", ["node", "C:\\pkg\\node_modules\\weixin-mcp\\dist\\cli.js"])
	return {
		lock: { pid: 4321, startedAt: "now", workerToken: "token", endpoint: "http://127.0.0.1:1", heartbeat: "now", ...metadata },
		health: { ok: true, challenge: "token", adapter: "Ready", ...metadata },
	}
}

test("stale 0.1.3 worker health is incompatible", () => {
	const { lock } = current()
	lock.packageVersion = "0.1.3"
	const issue = workerCompatibilityIssue(lock, { ok: true, challenge: "token", adapter: "Ready", packageVersion: "0.1.3" })
	expect(issue).toContain("package version mismatch")
})

test("current worker health and lock metadata are compatible", () => {
	const { lock, health } = current()
	expect(workerCompatibilityIssue(lock, health)).toBeUndefined()
	expect(health).toMatchObject({ packageVersion: WORKER_PACKAGE_VERSION, protocolVersion: WORKER_PROTOCOL_VERSION, schemaVersion: WORKER_SCHEMA_VERSION, workerPid: 4321, capabilities: [...WORKER_CAPABILITIES] })
})

test("fresh packaged worker records worker and fixed adapter provenance", () => {
	const { lock } = current()
	expect(lock.workerEntrypoint).toEndWith("dist\\worker.js")
	expect(lock.adapterCommand).toEqual(["node", "C:\\pkg\\node_modules\\weixin-mcp\\dist\\cli.js"])
	expect(lock.adapterProvenance).toEqual({ kind: "fixed-local-dependency", package: "weixin-mcp", version: "1.7.7", entrypoint: "C:\\pkg\\node_modules\\weixin-mcp\\dist\\cli.js" })
})
