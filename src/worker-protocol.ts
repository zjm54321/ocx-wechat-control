export const WORKER_PACKAGE_VERSION = "0.2.4"
export const WORKER_PROTOCOL_VERSION = 1
export const WORKER_SCHEMA_VERSION = 7
export const WORKER_CAPABILITIES = ["v2-callbacks", "async-prompt-admission", "native-question-permission", "legacy-inject-disabled", "dynamic-active-aliases", "logical-wechat-reply-identity"] as const

export interface AdapterProvenance { kind: "fixed-local-dependency"; package: "weixin-mcp"; version: "1.7.7"; entrypoint: string }
export interface WorkerMetadata {
	packageVersion: string
	protocolVersion: number
	capabilities: string[]
	workerPid: number
	workerEntrypoint: string
	adapterCommand: string[]
	adapterProvenance: AdapterProvenance
	schemaVersion: number
}

export function createWorkerMetadata(pid: number, workerEntrypoint: string, adapterCommand: string[]): WorkerMetadata {
	return {
		packageVersion: WORKER_PACKAGE_VERSION, protocolVersion: WORKER_PROTOCOL_VERSION, capabilities: [...WORKER_CAPABILITIES],
		workerPid: pid, workerEntrypoint, adapterCommand: [...adapterCommand],
		adapterProvenance: { kind: "fixed-local-dependency", package: "weixin-mcp", version: "1.7.7", entrypoint: adapterCommand[1] ?? "" },
		schemaVersion: WORKER_SCHEMA_VERSION,
	}
}
