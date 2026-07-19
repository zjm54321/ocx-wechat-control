export const STALE_REAPER_PROOF_DOMAIN = "ocx-wechat-control/stale-binding-reaper"
export const STALE_REAPER_PROOF_VERSION = 1
export const STALE_REAPER_CHALLENGE_BYTES = 32

export type StaleReaperOutcome = "ok" | "not-root"

const encoder = new TextEncoder()
const challengePattern = /^[A-Za-z0-9_-]{43}$/
const proofPattern = /^[A-Za-z0-9_-]{43}$/

function canonical(direction: "request" | "response", challenge: string, rootSessionId: string, outcome: "probe" | StaleReaperOutcome): Uint8Array {
	return encoder.encode(JSON.stringify([STALE_REAPER_PROOF_DOMAIN, STALE_REAPER_PROOF_VERSION, direction, challenge, rootSessionId, outcome]))
}

async function key(instanceToken: string, usage: KeyUsage[]): Promise<CryptoKey> {
	return crypto.subtle.importKey("raw", encoder.encode(instanceToken), { name: "HMAC", hash: "SHA-256" }, false, usage)
}

function decodeProof(proof: string): Uint8Array | undefined {
	if (!proofPattern.test(proof)) return
	const bytes = Uint8Array.from(Buffer.from(proof, "base64url"))
	return bytes.length === 32 ? bytes : undefined
}

async function sign(instanceToken: string, message: Uint8Array): Promise<string> {
	return Buffer.from(await crypto.subtle.sign("HMAC", await key(instanceToken, ["sign"]), message)).toString("base64url")
}

async function verify(instanceToken: string, proof: string, message: Uint8Array): Promise<boolean> {
	const bytes = decodeProof(proof)
	if (!bytes) return false
	try { return crypto.subtle.verify("HMAC", await key(instanceToken, ["verify"]), bytes, message) } catch { return false }
}

export function validStaleReaperChallenge(value: unknown): value is string { return typeof value === "string" && challengePattern.test(value) }
export function validStaleReaperProof(value: unknown): value is string { return typeof value === "string" && proofPattern.test(value) }

export function createStaleReaperChallenge(): string {
	const bytes = new Uint8Array(STALE_REAPER_CHALLENGE_BYTES)
	crypto.getRandomValues(bytes)
	return Buffer.from(bytes).toString("base64url")
}

export function signStaleReaperRequest(instanceToken: string, challenge: string, rootSessionId: string): Promise<string> {
	return sign(instanceToken, canonical("request", challenge, rootSessionId, "probe"))
}

export function verifyStaleReaperRequest(instanceToken: string, challenge: string, rootSessionId: string, proof: string): Promise<boolean> {
	return verify(instanceToken, proof, canonical("request", challenge, rootSessionId, "probe"))
}

export function signStaleReaperResponse(instanceToken: string, challenge: string, rootSessionId: string, outcome: StaleReaperOutcome): Promise<string> {
	return sign(instanceToken, canonical("response", challenge, rootSessionId, outcome))
}

export function verifyStaleReaperResponse(instanceToken: string, challenge: string, rootSessionId: string, outcome: StaleReaperOutcome, proof: string): Promise<boolean> {
	return verify(instanceToken, proof, canonical("response", challenge, rootSessionId, outcome))
}
