import { Database } from "bun:sqlite"
import { createHash, randomBytes } from "node:crypto"
import { mkdir, writeFile } from "node:fs/promises"
import { existsSync, renameSync, unlinkSync } from "node:fs"
import * as os from "node:os"
import * as path from "node:path"

export const MAX_TEXT_LENGTH = 4000
export const MAX_ROUTE_ID_LENGTH = 500
export const MAX_CONTEXT_TOKEN_LENGTH = 4000
export const HELP_TEXT = "用法：发送 id 查看当前编号；首行 #编号，下一行输入正文。Question 请一次性回复 #N\nQCODE 1 或单个问题用 #N\n1。"
export const UNKNOWN_TITLE = "未命名会话"
export const OFFLINE_TEXT = "目标 OpenCode 会话当前离线，请稍后重试。"
export const CONTROL_OFF_TEXT = "微信接管当前已关闭；发送 help 查看用法。"
export const PERMISSION_DENIED_TEXT = "OpenCode 需要本地权限确认；受限微信接管已拒绝该权限请求。"
export const COMPLETION_TEXT = "任务已完成。"
export const COMPLETION_ERROR_TEXT = "任务执行失败。"
export const INSTANCE_TTL_MS = 45_000
export const ORPHAN_PENDING_TTL_MS = 12 * 60_000
export const OUTBOUND_ECHO_TTL_MS = 10 * 60_000
export const REQUEST_CODE_PATTERN = /^[QP][A-Z2-7]{6}$/
export const MAX_NATIVE_PAYLOAD_LENGTH = 64 * 1024

export type PromptSubmissionState = "SUBMITTING" | "SUBMITTED" | "REJECTED" | "CANCELLED" | "UNKNOWN"
export type NativeRequestState = "ANNOUNCING" | "OPEN" | "RESOLVING" | "RESOLVED" | "REJECTED" | "UNKNOWN" | "CANCELLED_REMOTE"
export type NativeRequestKind = "QUESTION" | "PERMISSION"
export type RuntimeStatus = "IDLE" | "BUSY" | "RETRY" | "QUEUED" | "UNKNOWN"

export interface PromptSubmission {
	submissionId: string; inboundId: string; rootSessionId: string; ownerInstance: string; alias: number
	messageId: string; state: PromptSubmissionState; callStarted: boolean; promptMessageId: string | null; controlRevision: number
	admissionGeneration: number | null; admissionFinished: boolean
}
export interface NativeRequest {
	requestId: string; requestKey: string; code: string; rootSessionId: string; ownerInstance: string
	alias: number; kind: NativeRequestKind; state: NativeRequestState; payload: unknown; inboundId: string | null; resolution: unknown; controlRevision: number
}
export interface RootRuntime {
	rootSessionId: string; ownerInstance: string; status: RuntimeStatus; generation: number; busyGeneration: number | null
	admissionCount: number; workPending: boolean; observedMs: number; leaseExpiresMs: number
}

/** Deterministic, collision-safe code allocation. The callback is intentionally injectable for tests. */
export function allocateRequestCode(kind: NativeRequestKind, key: string, occupied: (code: string) => boolean): string {
	const prefix = kind === "QUESTION" ? "Q" : "P", alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	for (let attempt = 0; attempt < 1_000_000; attempt++) {
		const bytes = createHash("sha256").update(`${key}\0${attempt}`).digest()
		let code = prefix
		for (let index = 0; index < 6; index++) code += alphabet[bytes[index] & 31]
		if (!occupied(code)) return code
	}
	throw new Error("request code space exhausted")
}

export type ParsedInbound =
	| { ok: true; kind: "help" }
	| { ok: true; kind: "list" }
	| { ok: true; kind: "route"; alias: number; body: string }
	| { ok: false; reason: "empty" | "invalid-route" | "invalid-text" }

export function isPlainText(text: string): boolean {
	return text.length > 0 && text.length <= MAX_TEXT_LENGTH && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(text)
}

export function parseInboundText(input: string): ParsedInbound {
	const text = input.replace(/\r\n?/g, "\n")
	if (!isPlainText(text)) return { ok: false, reason: text.length ? "invalid-text" : "empty" }
	if (text.trim() === "help") return { ok: true, kind: "help" }
	if (text.trim() === "id" && !/[\r\n]/.test(text)) return { ok: true, kind: "list" }
	const match = /^#([1-9][0-9]*)[ \t]*(?:\n|$)/.exec(text)
	if (!match) return { ok: false, reason: "invalid-route" }
	const body = text.slice(match[0].length)
	if (!body.trim() || !isPlainText(body)) return { ok: false, reason: "invalid-route" }
	const alias = Number(match[1])
	if (!Number.isSafeInteger(alias) || alias < 1) return { ok: false, reason: "invalid-route" }
	return { ok: true, kind: "route", alias, body }
}

export function formatOutbound(alias: number, text: string): string {
	if (!Number.isSafeInteger(alias) || alias < 1 || !isPlainText(text)) throw new Error("invalid outbound text")
	return `#${alias}\n${text}`
}

export function sha256(value: string): string {
	return createHash("sha256").update(value).digest("hex")
}

const ASCENDING_MESSAGE_COUNTER_MAX = 0xfff
const ASCENDING_MESSAGE_RANDOM_LENGTH = 14
const ID_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

function ascendingMessageId(timestamp: number, counter: number): string {
	const head = ((BigInt(timestamp) << 12n) | BigInt(counter)).toString(16).slice(-12).padStart(12, "0")
	const bytes = randomBytes(ASCENDING_MESSAGE_RANDOM_LENGTH), suffix = Array.from(bytes, (byte) => ID_ALPHABET[byte % ID_ALPHABET.length]).join("")
	return `msg_${head}${suffix}`
}

export function boundedJson(value: unknown, maxLength = MAX_NATIVE_PAYLOAD_LENGTH): string {
	let json: string
	try { json = JSON.stringify(value) } catch { throw new Error("invalid JSON value") }
	if (typeof json !== "string" || json.length > maxLength) throw new Error("JSON value too large")
	return json
}

function canonicalJson(value: unknown): string {
	const normalize = (item: unknown): unknown => {
		if (Array.isArray(item)) return item.map(normalize)
		if (item && typeof item === "object") { const source = item as Record<string, unknown>, result: Record<string, unknown> = {}; for (const key of Object.keys(source).sort()) result[key] = normalize(source[key]); return result }
		return item
	}
	return boundedJson(normalize(value))
}

export interface WeixinInbound {
	id: string
	fromUserId: string
	contextToken: string
	text: string
	cursorHint: string
}

export function parseToolText(result: unknown): unknown {
	if (!result || typeof result !== "object" || (result as { isError?: boolean }).isError) throw new Error("MCP tool call failed")
	const content = (result as { content?: unknown }).content
	if (!Array.isArray(content) || content.length !== 1) throw new Error("unexpected MCP tool content")
	const item = content[0] as { type?: unknown; text?: unknown }
	if (item?.type !== "text" || typeof item.text !== "string") throw new Error("unexpected MCP tool text")
	try { return JSON.parse(item.text) } catch { throw new Error("invalid MCP tool JSON") }
}

export function parsePollToolResult(result: unknown): WeixinInbound[] {
	const parsed = parseToolText(result)
	if (!parsed || typeof parsed !== "object") throw new Error("invalid poll result")
	const value = parsed as { msgs?: unknown; get_updates_buf?: unknown }
	if (value.msgs !== undefined && !Array.isArray(value.msgs)) throw new Error("invalid poll msgs")
	const cursorHint = typeof value.get_updates_buf === "string" ? value.get_updates_buf : "no-cursor"
	const output: WeixinInbound[] = []
	for (const [index, raw] of (value.msgs ?? []).entries()) {
		if (!raw || typeof raw !== "object") continue
		const msg = raw as Record<string, unknown>
		if (msg.message_type !== 1 || typeof msg.from_user_id !== "string" || typeof msg.context_token !== "string" || !Array.isArray(msg.item_list)) continue
		const text = msg.item_list
			.filter((item): item is { type: 1; text_item: { text: string } } => Boolean(item && typeof item === "object" && (item as any).type === 1 && typeof (item as any).text_item?.text === "string"))
			.map((item) => item.text_item.text)
			.join("\n")
		if (!text) continue
		// Upstream exposes no message ID. This is an at-least-once/UNKNOWN-safe key,
		// derived from the persistent poll cursor plus sender, context and exact text.
		const id = sha256(`${cursorHint}\0${index}\0${msg.from_user_id}\0${msg.context_token}\0${text}`)
		output.push({ id, fromUserId: msg.from_user_id, contextToken: msg.context_token, text, cursorHint })
	}
	return output
}

export interface Binding {
	alias: number
	registrationAlias: number
	rootSessionId: string
	directory: string
	ownerInstance: string
	title: string | null
	active: boolean
}

export interface GlobalRoute { conversationId: string | null; contextToken: string | null; updatedAt: string | null }
export type RoutedBinding = Binding & { conversationId: string; contextToken: string }
export type RouteAcceptance = "CLAIMED" | "REFRESHED" | "REJECTED"

export function isValidRouteMetadata(conversationId: unknown, contextToken: unknown): conversationId is string {
	return typeof conversationId === "string" && conversationId.length > 0 && conversationId.length <= MAX_ROUTE_ID_LENGTH && typeof contextToken === "string" && contextToken.length > 0 && contextToken.length <= MAX_CONTEXT_TOKEN_LENGTH
}

export function sanitizeTitle(value: unknown): string | null {
	if (typeof value !== "string") return null
	const clean = Array.from(value.replace(/[\u0000-\u001f\u007f]/g, " ").trim()).slice(0, 120).join("").trim()
	return clean || null
}

export function formatRegistrationList(bindings: Binding[]): string {
	const ordered = [...bindings].sort((a, b) => a.alias - b.alias)
	if (!ordered.length) return "暂无已登记会话。"
	const lines: string[] = []
	for (let index = 0; index < ordered.length; index++) {
		const line = `#${ordered[index].alias}  ${ordered[index].title ?? UNKNOWN_TITLE}`
		const remaining = ordered.length - index
		if ([...lines, line].join("\n\n").length <= MAX_TEXT_LENGTH) { lines.push(line); continue }
		let suffix = `……另有 ${remaining} 个会话未显示。`
		while (lines.length && [...lines, suffix].join("\n\n").length > MAX_TEXT_LENGTH) { lines.pop(); suffix = `……另有 ${ordered.length - lines.length} 个会话未显示。` }
		lines.push(suffix.slice(0, MAX_TEXT_LENGTH)); break
	}
	return lines.join("\n\n")
}

export interface PendingReply {
	inboundId: string
	rootSessionId: string
	promptMessageId: string | null
	injectedAt: number | null
	alias: number
	state: "WAITING" | "SENDING" | "SENT" | "UNKNOWN"
}

export interface StoreOptions {
	migrationFault?: () => void
	onSnapshot?: (path: string) => void
}

export class Store {
	readonly db: Database
	readonly migrationBackupPath?: string
	private migrationSnapshotInProgress?: string
	constructor(databasePath: string, private readonly options: StoreOptions = {}) {
		this.db = new Database(databasePath, { create: true })
		this.db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
		let snapshot: string | undefined
		try { snapshot = this.migrate(databasePath) } catch (error) { this.failMigration(error, snapshot) }
		this.migrationBackupPath = snapshot
		this.recoverCrashStates()
		this.expireRuntimeLeases()
		this.sweepOrphanWaiting(Date.now(), ORPHAN_PENDING_TTL_MS)
		this.sweepOutboundEchoes()
	}
	private migrate(databasePath: string): string | undefined {
		const userVersion = (this.db.query("PRAGMA user_version").get() as any)?.user_version ?? 0
		if (userVersion > 7) throw new Error("unsupported database schema")
		const has = (table: string) => Boolean(this.db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table))
		const hasLegacyData = has("bindings") || has("inbound") || has("outbound")
		let snapshot: string | undefined
		if (databasePath !== ":memory:" && userVersion < 7 && (userVersion > 0 || hasLegacyData)) {
			snapshot = this.createConsistentSnapshot(databasePath)
			this.migrationSnapshotInProgress = snapshot
			this.options.onSnapshot?.(snapshot)
		}
		const columns = (table: string) => new Set((this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).map((row) => row.name))
		const tableInfo = (table: string) => this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string; notnull: number }>
		const add = (table: string, column: string, definition: string) => { if (!columns(table).has(column)) this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`) }
		this.db.transaction(() => {
			this.db.exec("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
			if (has("bindings")) add("bindings", "context_token", "TEXT")
			if (has("inbound")) {
				add("inbound", "from_user_id", "TEXT NOT NULL DEFAULT ''"); add("inbound", "context_token", "TEXT NOT NULL DEFAULT ''"); add("inbound", "text", "TEXT NOT NULL DEFAULT ''"); add("inbound", "root_session_id", "TEXT"); add("inbound", "prompt_message_id", "TEXT")
			}
			if (has("outbound")) { add("outbound", "inbound_id", "TEXT"); this.db.exec("UPDATE outbound SET inbound_id='legacy:'||message_id WHERE inbound_id IS NULL") }
			if (has("pending_replies")) {
				const prompt = tableInfo("pending_replies").find((row) => row.name === "prompt_message_id")
				if (prompt?.notnull) {
					this.db.exec("ALTER TABLE pending_replies RENAME TO pending_replies_legacy")
					this.db.exec("CREATE TABLE pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,updated_at TEXT NOT NULL)")
					this.db.exec("INSERT INTO pending_replies(inbound_id,root_session_id,prompt_message_id,alias,state,assistant_message_id,payload,updated_at) SELECT inbound_id,root_session_id,prompt_message_id,alias,state,assistant_message_id,payload,updated_at FROM pending_replies_legacy")
					this.db.exec("DROP TABLE pending_replies_legacy")
				}
				add("pending_replies", "injected_at", "INTEGER")
			}
			this.options.migrationFault?.()
			if (has("bindings") && userVersion < 3) {
				this.db.exec("ALTER TABLE bindings RENAME TO bindings_pre_v3")
				this.db.exec("CREATE TABLE bindings(alias INTEGER PRIMARY KEY,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT,created_at TEXT NOT NULL)")
				this.db.exec("INSERT INTO bindings(alias,root_session_id,directory,owner_instance,conversation_id,context_token,created_at) SELECT alias,root_session_id,directory,owner_instance,conversation_id,context_token,created_at FROM bindings_pre_v3")
				this.db.exec("DROP TABLE bindings_pre_v3")
			}
			this.db.exec(`
CREATE TABLE IF NOT EXISTS instances(instance_id TEXT PRIMARY KEY,endpoint TEXT NOT NULL,instance_token TEXT NOT NULL,heartbeat_ms INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bindings(alias INTEGER PRIMARY KEY,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS inbound(message_id TEXT PRIMARY KEY,from_user_id TEXT NOT NULL,context_token TEXT NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL,root_session_id TEXT,prompt_message_id TEXT,reason TEXT,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pending_replies(inbound_id TEXT PRIMARY KEY,root_session_id TEXT NOT NULL,prompt_message_id TEXT,alias INTEGER NOT NULL,state TEXT NOT NULL,assistant_message_id TEXT,payload TEXT,injected_at INTEGER,control_revision INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbound(message_id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,payload TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS control_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints(checkpoint_id TEXT PRIMARY KEY,request_key TEXT,root_session_id TEXT NOT NULL,owner_instance TEXT NOT NULL,conversation_id TEXT NOT NULL,alias INTEGER NOT NULL,question TEXT NOT NULL,choices_json TEXT NOT NULL,state TEXT NOT NULL,inbound_id TEXT,control_revision INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
DROP INDEX IF EXISTS idx_checkpoint_open_root;
DROP INDEX IF EXISTS idx_checkpoint_active_root;
CREATE TABLE IF NOT EXISTS session_activity(root_session_id TEXT PRIMARY KEY,owner_instance TEXT NOT NULL,running INTEGER NOT NULL DEFAULT 0,idle INTEGER NOT NULL DEFAULT 1,last_assistant_id TEXT,last_assistant_error INTEGER NOT NULL DEFAULT 0,direct_assistant_id TEXT,epoch INTEGER NOT NULL DEFAULT 0,run_id INTEGER NOT NULL DEFAULT 0,origin TEXT NOT NULL DEFAULT 'NONE',candidate_run INTEGER,claimed_run INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS control_outbound(outbound_id TEXT PRIMARY KEY,dedupe_key TEXT NOT NULL UNIQUE,root_session_id TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbound_echoes(echo_hash TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,context_token TEXT NOT NULL DEFAULT '',payload TEXT NOT NULL,expires_ms INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY,at TEXT NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS prompt_submissions(
 submission_id TEXT PRIMARY KEY CHECK(length(submission_id) BETWEEN 1 AND 500),inbound_id TEXT NOT NULL UNIQUE CHECK(length(inbound_id) BETWEEN 1 AND 500),
 root_session_id TEXT NOT NULL CHECK(length(root_session_id) BETWEEN 1 AND 500),owner_instance TEXT NOT NULL CHECK(length(owner_instance) BETWEEN 1 AND 500),alias INTEGER NOT NULL CHECK(alias>0),
 message_id TEXT NOT NULL UNIQUE CHECK(length(message_id) BETWEEN 1 AND 500),body TEXT NOT NULL CHECK(length(body) BETWEEN 1 AND 4000),
 state TEXT NOT NULL CHECK(state IN ('SUBMITTING','SUBMITTED','REJECTED','CANCELLED','UNKNOWN')),call_started INTEGER NOT NULL DEFAULT 0 CHECK(call_started IN (0,1)),
 prompt_message_id TEXT,rejection TEXT,control_revision INTEGER NOT NULL,admission_generation INTEGER CHECK(admission_generation IS NULL OR admission_generation>=0),
 admission_finished INTEGER NOT NULL DEFAULT 0 CHECK(admission_finished IN (0,1)),created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_requests(
 request_id TEXT PRIMARY KEY CHECK(length(request_id) BETWEEN 1 AND 500),request_key TEXT NOT NULL UNIQUE CHECK(length(request_key) BETWEEN 1 AND 500),
 code TEXT NOT NULL UNIQUE CHECK(code GLOB '[QP][A-Z2-7][A-Z2-7][A-Z2-7][A-Z2-7][A-Z2-7][A-Z2-7]'),
 root_session_id TEXT NOT NULL CHECK(length(root_session_id) BETWEEN 1 AND 500),owner_instance TEXT NOT NULL CHECK(length(owner_instance) BETWEEN 1 AND 500),alias INTEGER NOT NULL CHECK(alias>0),
 kind TEXT NOT NULL CHECK(kind IN ('QUESTION','PERMISSION')),state TEXT NOT NULL CHECK(state IN ('ANNOUNCING','OPEN','RESOLVING','RESOLVED','REJECTED','UNKNOWN','CANCELLED_REMOTE')),
 payload_json TEXT NOT NULL CHECK(length(payload_json)<=65536 AND json_valid(payload_json)),inbound_id TEXT UNIQUE,resolution_json TEXT CHECK(resolution_json IS NULL OR (length(resolution_json)<=65536 AND json_valid(resolution_json))),
 control_revision INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_answer_guards(
 root_session_id TEXT NOT NULL CHECK(length(root_session_id) BETWEEN 1 AND 500),request_id TEXT NOT NULL CHECK(length(request_id) BETWEEN 1 AND 500),created_at TEXT NOT NULL,
 PRIMARY KEY(root_session_id,request_id),FOREIGN KEY(request_id) REFERENCES native_requests(request_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS root_runtime(
 root_session_id TEXT PRIMARY KEY CHECK(length(root_session_id) BETWEEN 1 AND 500),owner_instance TEXT NOT NULL CHECK(length(owner_instance) BETWEEN 1 AND 500),
 status TEXT NOT NULL CHECK(status IN ('IDLE','BUSY','RETRY','QUEUED','UNKNOWN')),generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),busy_generation INTEGER CHECK(busy_generation IS NULL OR busy_generation>=0),
 admission_count INTEGER NOT NULL DEFAULT 0 CHECK(admission_count>=0),work_pending INTEGER NOT NULL DEFAULT 0 CHECK(work_pending IN (0,1)),observed_ms INTEGER NOT NULL DEFAULT 0 CHECK(observed_ms>=0),lease_expires_ms INTEGER NOT NULL DEFAULT 0 CHECK(lease_expires_ms>=0),updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS typing_state(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),desired INTEGER NOT NULL DEFAULT 0 CHECK(desired IN (0,1)),actual INTEGER CHECK(actual IS NULL OR actual IN (0,1)),
 context_hash TEXT,attempt_ms INTEGER NOT NULL DEFAULT 0 CHECK(attempt_ms>=0),updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_pending_root_state ON pending_replies(root_session_id,state);
CREATE INDEX IF NOT EXISTS idx_inbound_state ON inbound(state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbound_inbound ON outbound(inbound_id);
CREATE INDEX IF NOT EXISTS idx_prompt_root_state ON prompt_submissions(root_session_id,state);
CREATE INDEX IF NOT EXISTS idx_native_alias_state ON native_requests(alias,state);
CREATE INDEX IF NOT EXISTS idx_native_root_state ON native_requests(root_session_id,state);
CREATE INDEX IF NOT EXISTS idx_native_answer_guard_request ON native_answer_guards(request_id);
`)
			add("pending_replies", "control_revision", "INTEGER NOT NULL DEFAULT 0")
			add("checkpoints", "request_key", "TEXT"); add("checkpoints", "control_revision", "INTEGER NOT NULL DEFAULT 0")
			this.db.exec("UPDATE checkpoints SET request_key=checkpoint_id WHERE request_key IS NULL")
			add("session_activity", "epoch", "INTEGER NOT NULL DEFAULT 0"); add("session_activity", "run_id", "INTEGER NOT NULL DEFAULT 0"); add("session_activity", "origin", "TEXT NOT NULL DEFAULT 'NONE'"); add("session_activity", "candidate_run", "INTEGER"); add("session_activity", "claimed_run", "INTEGER NOT NULL DEFAULT 0")
			this.db.exec("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL WHERE epoch=0")
			add("outbound_echoes", "context_token", "TEXT NOT NULL DEFAULT ''"); add("outbound_echoes", "expires_ms", "INTEGER NOT NULL DEFAULT 0")
			add("control_outbound", "logical_text", "TEXT")
			add("control_outbound", "logical_hash", "TEXT")
			if (userVersion > 0 && userVersion < 7) {
				const rows = this.db.query("SELECT outbound_id AS outboundId,root_session_id AS root,dedupe_key AS dedupeKey,payload FROM control_outbound WHERE kind='wechat-reply' AND logical_text IS NULL AND logical_hash IS NULL").all() as Array<{ outboundId: string; root: string; dedupeKey: string; payload: string }>
				const update = this.db.query("UPDATE control_outbound SET logical_text=?,logical_hash=? WHERE outbound_id=? AND logical_text IS NULL AND logical_hash IS NULL")
				for (const row of rows) {
					const prefix = `wechat-reply:${row.root}:`
					if (!row.root || row.root.length > 500 || /[\u0000-\u001f\u007f]/.test(row.root) || !row.dedupeKey.startsWith(prefix)) continue
					const callId = row.dedupeKey.slice(prefix.length)
					if (!callId || callId.length > 500 || /[\u0000-\u001f\u007f]/.test(callId) || row.dedupeKey !== `${prefix}${callId}`) continue
					const match = /^#([1-9][0-9]*)\n([\s\S]+)$/.exec(row.payload)
					if (!match) continue
					const alias = Number(match[1]), text = match[2]
					if (!Number.isSafeInteger(alias) || alias < 1 || !isPlainText(text) || formatOutbound(alias, text) !== row.payload) continue
					update.run(text, sha256(text), row.outboundId)
				}
			}
			this.db.exec("DROP INDEX IF EXISTS idx_checkpoint_request_key; CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_root_request_key ON checkpoints(root_session_id,request_key); CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_active_root ON checkpoints(root_session_id) WHERE state IN ('SENDING','OPEN','ANSWERING')")
			this.db.exec("DELETE FROM native_answer_guards WHERE NOT EXISTS(SELECT 1 FROM native_requests r WHERE r.request_id=native_answer_guards.request_id AND r.root_session_id=native_answer_guards.root_session_id AND r.state='OPEN')")
			this.db.query("INSERT OR IGNORE INTO control_state(singleton,enabled,revision) VALUES(1,0,0)").run()
			this.db.query("INSERT OR IGNORE INTO typing_state(singleton,desired,actual,context_hash,attempt_ms,updated_at) VALUES(1,0,NULL,NULL,0,?)").run(new Date().toISOString())
			if (userVersion < 5 && has("bindings") && columns("bindings").has("conversation_id")) {
				const conversations = this.db.query("SELECT DISTINCT conversation_id AS id FROM bindings WHERE conversation_id<>''").all() as Array<{ id: string }>
				this.db.exec("ALTER TABLE bindings RENAME TO bindings_pre_v5")
				this.db.exec("CREATE TABLE bindings(alias INTEGER PRIMARY KEY AUTOINCREMENT,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,title TEXT,active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
				this.db.exec("INSERT INTO bindings(alias,root_session_id,directory,owner_instance,title,created_at,updated_at) SELECT alias,root_session_id,directory,owner_instance,NULL,created_at,created_at FROM bindings_pre_v5")
				this.db.exec("DROP TABLE bindings_pre_v5")
				this.db.exec("CREATE TABLE IF NOT EXISTS global_route(singleton INTEGER PRIMARY KEY CHECK(singleton=1),conversation_id TEXT,context_token TEXT,updated_at TEXT)")
				this.db.query("INSERT OR IGNORE INTO global_route(singleton,conversation_id,context_token,updated_at) VALUES(1,?,NULL,?)").run(conversations.length === 1 ? conversations[0].id : null, conversations.length === 1 ? new Date().toISOString() : null)
				if (conversations.length > 1) {
					const now = new Date().toISOString()
					this.db.query("UPDATE global_route SET conversation_id=NULL,context_token=NULL,updated_at=NULL WHERE singleton=1").run()
					this.db.query("UPDATE control_state SET enabled=0,revision=revision+1 WHERE singleton=1 AND enabled<>0").run()
					this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now)
					this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,updated_at=?").run(now)
					this.db.query("UPDATE inbound SET state='UNKNOWN',reason='migration-multiple-global-routes',updated_at=? WHERE state='INJECTING' AND message_id IN (SELECT inbound_id FROM pending_replies WHERE state='WAITING')").run(now)
					this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='WAITING'").run(now)
					this.db.query("INSERT INTO audit(at,reason) VALUES(?,?)").run(now, "migration-multiple-global-routes")
				}
			} else {
				this.db.exec("CREATE TABLE IF NOT EXISTS global_route(singleton INTEGER PRIMARY KEY CHECK(singleton=1),conversation_id TEXT,context_token TEXT,updated_at TEXT)")
				this.db.query("INSERT OR IGNORE INTO global_route(singleton,conversation_id,context_token,updated_at) VALUES(1,NULL,NULL,NULL)").run()
			}
			add("bindings", "active", "INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))")
			if (userVersion > 0 && userVersion < 6) {
				const now = new Date().toISOString()
				this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now)
				this.db.query("UPDATE inbound SET state='UNKNOWN',reason='schema-v6-semantic-change',updated_at=? WHERE state='INJECTING' AND message_id IN (SELECT inbound_id FROM pending_replies WHERE state IN ('WAITING','SENDING'))").run(now)
				this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state IN ('WAITING','SENDING')").run(now)
				this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,updated_at=?").run(now)
			}
			this.db.query("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','7')").run()
			this.db.exec("PRAGMA user_version=7")
		})()
		return snapshot
	}
	private createConsistentSnapshot(databasePath: string): string {
		const version = ((this.db.query("PRAGMA user_version").get() as any)?.user_version ?? 0)
		const label = version < 5 ? "pre-v5-v7" : version < 6 ? "pre-v6-v7" : "pre-v7"
		const backup = `${databasePath}.${label}-${Date.now()}-${crypto.randomUUID()}.bak`, temp = `${backup}.tmp`
		const escaped = temp.replaceAll("'", "''")
		try { this.db.exec(`VACUUM INTO '${escaped}'`); renameSync(temp, backup); return backup }
		catch (error) { if (existsSync(temp)) try { unlinkSync(temp) } catch {}; throw new Error(`consistent pre-v5 snapshot failed: ${error instanceof Error ? error.message : String(error)}`) }
	}
	private failMigration(error: unknown, snapshot?: string): never {
		this.db.close()
		const backup = snapshot ?? this.migrationSnapshotInProgress
		throw new Error(`database migration failed${backup ? `; consistent backup=${backup}` : " before snapshot"}: ${error instanceof Error ? error.message : String(error)}`)
	}
	close(): void { this.db.close() }
	recoverCrashStates(): void {
		this.db.transaction(() => {
			const now = new Date().toISOString()
			this.db.query("UPDATE inbound SET state='UNKNOWN',reason='crash-during-injection',updated_at=? WHERE state='INJECTING'").run(new Date().toISOString())
			this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='WAITING' AND inbound_id IN (SELECT message_id FROM inbound WHERE state='UNKNOWN' AND reason='crash-during-injection')").run(new Date().toISOString())
			this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE outbound SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE control_outbound SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE checkpoints SET state='UNKNOWN',updated_at=? WHERE state IN ('SENDING','ANSWERING')").run(new Date().toISOString())
			this.db.query("UPDATE prompt_submissions SET state='UNKNOWN',admission_finished=CASE WHEN admission_generation IS NULL THEN admission_finished ELSE 1 END,updated_at=? WHERE state='SUBMITTING'").run(now)
			this.db.query("UPDATE native_requests SET state='UNKNOWN',updated_at=? WHERE state='RESOLVING'").run(now)
			this.db.query("UPDATE native_requests SET state='OPEN',updated_at=? WHERE state='ANNOUNCING'").run(now)
			this.db.query("UPDATE root_runtime SET admission_count=0,updated_at=?").run(now)
			this.db.query("UPDATE root_runtime SET status='IDLE',generation=generation+1,busy_generation=NULL,admission_count=0,work_pending=0,observed_ms=0,lease_expires_ms=0,updated_at=?").run(now)
			this.db.query("UPDATE typing_state SET desired=0,actual=NULL,updated_at=? WHERE singleton=1").run(now)
		})()
	}
	register(instanceId: string, instanceToken: string, endpoint: string): void {
		this.db.query("INSERT OR REPLACE INTO instances VALUES(?,?,?,?,?)").run(instanceId, endpoint, instanceToken, Date.now(), new Date().toISOString())
	}
	authenticate(instanceId: string, instanceToken: string): boolean {
		const row = this.db.query("SELECT instance_token AS token,heartbeat_ms AS heartbeat FROM instances WHERE instance_id=?").get(instanceId) as { token: string; heartbeat: number } | null
		return Boolean(row && row.token === instanceToken && Date.now() - row.heartbeat <= INSTANCE_TTL_MS)
	}
	touch(instanceId: string, instanceToken: string): boolean {
		if (!this.authenticate(instanceId, instanceToken)) return false
		this.db.query("UPDATE instances SET heartbeat_ms=? WHERE instance_id=?").run(Date.now(), instanceId)
		return true
	}
	unregister(instanceId: string, instanceToken: string): boolean {
		if (!this.authenticate(instanceId, instanceToken)) return false
		return this.db.transaction(() => {
			this.db.query("DELETE FROM native_answer_guards WHERE request_id IN (SELECT request_id FROM native_requests WHERE owner_instance=?)").run(instanceId)
			return this.db.query("DELETE FROM instances WHERE instance_id=?").run(instanceId).changes === 1
		})()
	}
	instance(instanceId: string): { endpoint: string; instanceToken: string; online: boolean } | undefined {
		const row = this.db.query("SELECT endpoint,instance_token AS instanceToken,heartbeat_ms AS heartbeat FROM instances WHERE instance_id=?").get(instanceId) as { endpoint: string; instanceToken: string; heartbeat: number } | null
		return row ? { endpoint: row.endpoint, instanceToken: row.instanceToken, online: Date.now() - row.heartbeat <= INSTANCE_TTL_MS } : undefined
	}
	bind(input: { rootSessionId: string; directory: string; ownerInstance: string; title?: string | null; alias?: number }): Binding {
		if (input.alias !== undefined) throw new Error("manual alias is deprecated")
		const now = new Date().toISOString(), title = sanitizeTitle(input.title)
		let binding: Binding | undefined
		this.db.transaction(() => {
			const existing = this.storedBindingForRoot(input.rootSessionId)
			if (existing) {
				if (existing.active && existing.ownerInstance !== input.ownerInstance && this.instance(existing.ownerInstance)?.online) throw new Error("owner-live")
				this.db.query("UPDATE bindings SET directory=?,owner_instance=?,title=?,active=1,updated_at=? WHERE root_session_id=?").run(input.directory, input.ownerInstance, title, now, input.rootSessionId)
				if (existing.ownerInstance !== input.ownerInstance) { this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE root_session_id=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now, input.rootSessionId); this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,owner_instance=?,updated_at=? WHERE root_session_id=?").run(input.ownerInstance, now, input.rootSessionId); this.db.query("UPDATE inbound SET state='UNKNOWN',reason='owner-rebound',updated_at=? WHERE state='INJECTING' AND message_id IN (SELECT inbound_id FROM pending_replies WHERE root_session_id=? AND state='WAITING')").run(now, input.rootSessionId); this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE root_session_id=? AND state='WAITING'").run(now, input.rootSessionId); this.cancelV6Root(input.rootSessionId, input.ownerInstance, now) }
			} else this.db.query("INSERT INTO bindings(root_session_id,directory,owner_instance,title,active,created_at,updated_at) VALUES(?,?,?,?,1,?,?)").run(input.rootSessionId, input.directory, input.ownerInstance, title, now, now)
			this.db.query("UPDATE control_state SET enabled=1,revision=revision+1 WHERE singleton=1 AND enabled=0").run()
			binding = this.bindingForRoot(input.rootSessionId)
		})()
		return binding!
	}
	private bindingRow(row: any, displayAlias?: number): Binding | undefined { return row ? { ...row, registrationAlias: row.registrationAlias, alias: displayAlias ?? row.registrationAlias, active: row.active === 1 } : undefined }
	private storedBindingForRoot(root: string): Binding | undefined { return this.bindingRow(this.db.query("SELECT alias AS registrationAlias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title,active FROM bindings WHERE root_session_id=?").get(root)) }
	bindingForAlias(alias: number): Binding | undefined {
		if (!Number.isSafeInteger(alias) || alias < 1) return
		return this.bindingRow(this.db.query("SELECT alias AS registrationAlias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title,active FROM bindings WHERE active=1 ORDER BY alias LIMIT 1 OFFSET ?").get(alias - 1), alias)
	}
	bindingForRoot(root: string): Binding | undefined {
		const row = this.db.query("SELECT b.alias AS registrationAlias,b.root_session_id AS rootSessionId,b.directory,b.owner_instance AS ownerInstance,b.title,b.active,(SELECT COUNT(*) FROM bindings ranked WHERE ranked.active=1 AND ranked.alias<=b.alias) AS displayAlias FROM bindings b WHERE b.root_session_id=? AND b.active=1").get(root) as any
		return this.bindingRow(row, row?.displayAlias)
	}
	bindings(): Binding[] { return (this.db.query("SELECT alias AS registrationAlias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title,active FROM bindings WHERE active=1 ORDER BY alias").all() as any[]).map((row, index) => this.bindingRow(row, index + 1)!) }
	deactivateBinding(root: string, owner: string): boolean {
		return this.db.transaction(() => {
			const binding = this.storedBindingForRoot(root)
			if (!binding?.active || binding.ownerInstance !== owner) return false
			const now = new Date().toISOString()
			if (this.db.query("UPDATE bindings SET active=0,updated_at=? WHERE root_session_id=? AND owner_instance=? AND active=1").run(now, root, owner).changes !== 1) return false
			this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE root_session_id=? AND owner_instance=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now, root, owner)
			this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,updated_at=? WHERE root_session_id=? AND owner_instance=?").run(now, root, owner)
			this.db.query("UPDATE inbound SET state='UNKNOWN',reason='binding-deactivated',updated_at=? WHERE state='INJECTING' AND message_id IN (SELECT inbound_id FROM pending_replies WHERE root_session_id=? AND state='WAITING')").run(now, root)
			this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE root_session_id=? AND state='WAITING'").run(now, root)
			this.cancelV6Root(root, owner, now)
			return true
		})()
	}
	refreshRoute(conversationId: string, contextToken: string): void { this.db.query("UPDATE global_route SET conversation_id=?,context_token=?,updated_at=? WHERE singleton=1").run(conversationId, contextToken, new Date().toISOString()) }
	route(): GlobalRoute { return this.db.query("SELECT conversation_id AS conversationId,context_token AS contextToken,updated_at AS updatedAt FROM global_route WHERE singleton=1").get() as GlobalRoute }
	acceptInboundRoute(conversationId: string, contextToken: string, allowInitialClaim: boolean): RouteAcceptance {
		if (!isValidRouteMetadata(conversationId, contextToken)) return "REJECTED"
		return this.db.transaction(() => {
			const route = this.route(), now = new Date().toISOString()
			if (route.conversationId === null) {
				if (!allowInitialClaim) return "REJECTED"
				const claimed = this.db.query("UPDATE global_route SET conversation_id=?,context_token=?,updated_at=? WHERE singleton=1 AND conversation_id IS NULL").run(conversationId, contextToken, now)
				return claimed.changes === 1 ? "CLAIMED" : "REJECTED"
			}
			if (route.conversationId !== conversationId) return "REJECTED"
			this.db.query("UPDATE global_route SET context_token=?,updated_at=? WHERE singleton=1 AND conversation_id=?").run(contextToken, now, conversationId)
			return "REFRESHED"
		})()
	}
	control(): { enabled: boolean; revision: number } { const row = this.db.query("SELECT enabled,revision FROM control_state WHERE singleton=1").get() as { enabled: number; revision: number }; return { enabled: row.enabled === 1, revision: row.revision } }
	setControl(enabled: boolean): { enabled: boolean; revision: number } {
		this.db.transaction(() => {
			this.db.query("UPDATE control_state SET enabled=?,revision=revision+1 WHERE singleton=1 AND enabled<>?").run(enabled ? 1 : 0, enabled ? 1 : 0)
			if (!enabled) { const now = new Date().toISOString(); this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now); this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,updated_at=?").run(now); this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='WAITING'").run(now); this.db.query("UPDATE inbound SET state='UNKNOWN',reason='control-cancelled',updated_at=? WHERE state='INJECTING'").run(now); this.cancelV6Root(undefined, undefined, now) }
		})()
		return this.control()
	}
	private cancelV6Root(root?: string, replacementOwner?: string, now = new Date().toISOString()): void {
		const where = root ? " AND root_session_id=?" : "", args = root ? [now, root] : [now]
		this.db.query(`UPDATE native_requests SET state='CANCELLED_REMOTE',updated_at=? WHERE state IN ('ANNOUNCING','OPEN','RESOLVING','UNKNOWN')${where}`).run(...args)
		if (root) this.db.query("DELETE FROM native_answer_guards WHERE root_session_id=?").run(root)
		else this.db.query("DELETE FROM native_answer_guards").run()
		this.db.query(`UPDATE prompt_submissions SET state=CASE WHEN call_started=1 THEN 'UNKNOWN' ELSE 'CANCELLED' END,admission_finished=CASE WHEN admission_generation IS NULL THEN admission_finished ELSE 1 END,updated_at=? WHERE state='SUBMITTING'${where}`).run(...args)
		if (root) this.db.query("INSERT INTO root_runtime(root_session_id,owner_instance,status,generation,busy_generation,admission_count,work_pending,observed_ms,lease_expires_ms,updated_at) VALUES(?,?,'IDLE',0,NULL,0,0,0,0,?) ON CONFLICT(root_session_id) DO UPDATE SET owner_instance=excluded.owner_instance,status='IDLE',generation=generation+1,busy_generation=NULL,admission_count=0,work_pending=0,observed_ms=0,lease_expires_ms=0,updated_at=excluded.updated_at").run(root, replacementOwner ?? this.storedBindingForRoot(root)?.ownerInstance ?? "cancelled", now)
		else this.db.query("UPDATE root_runtime SET status='IDLE',generation=generation+1,busy_generation=NULL,admission_count=0,work_pending=0,observed_ms=0,lease_expires_ms=0,updated_at=?").run(now)
		this.recomputeTypingDesired(Date.parse(now))
	}
	claimPromptSubmission(input: { submissionId: string; inboundId: string; root: string; owner: string; alias: number; messageId?: string; body: string; revision?: number }): PromptSubmission | undefined {
		if (![input.submissionId, input.inboundId, input.root, input.owner].every((value) => typeof value === "string" && value.length > 0 && value.length <= 500) || (input.messageId !== undefined && (typeof input.messageId !== "string" || input.messageId.length < 1 || input.messageId.length > 500)) || !Number.isSafeInteger(input.alias) || input.alias < 1 || !isPlainText(input.body)) throw new Error("invalid prompt submission")
		try { this.db.transaction(() => {
			const existing = this.promptSubmission(input.submissionId)
			if (existing) {
				if (existing.inboundId === input.inboundId && existing.rootSessionId === input.root && existing.ownerInstance === input.owner && (input.messageId === undefined || existing.messageId === input.messageId)) return
				throw new Error("prompt submission identity conflict")
			}
			const nowMs = Date.now(), stored = this.db.query("SELECT value FROM meta WHERE key='prompt_message_id_clock'").get() as { value: string } | null
			let timestamp = nowMs, counter = 0
			if (stored) {
				const match = /^(\d+):(\d+)$/.exec(stored.value)
				if (!match) throw new Error("invalid persisted prompt message ID clock")
				const previousTimestamp = Number(match[1]), previousCounter = Number(match[2])
				if (!Number.isSafeInteger(previousTimestamp) || previousTimestamp < 0 || !Number.isInteger(previousCounter) || previousCounter < 0 || previousCounter > ASCENDING_MESSAGE_COUNTER_MAX) throw new Error("invalid persisted prompt message ID clock")
				if (timestamp <= previousTimestamp) { timestamp = previousTimestamp; counter = previousCounter + 1 }
				if (counter > ASCENDING_MESSAGE_COUNTER_MAX) { timestamp++; counter = 0 }
			}
			const messageId = input.messageId ?? ascendingMessageId(timestamp, counter), now = new Date(nowMs).toISOString()
			this.db.query("INSERT INTO prompt_submissions(submission_id,inbound_id,root_session_id,owner_instance,alias,message_id,body,state,call_started,prompt_message_id,rejection,control_revision,admission_generation,admission_finished,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'SUBMITTING',0,NULL,NULL,?,NULL,0,?,?)").run(input.submissionId, input.inboundId, input.root, input.owner, input.alias, messageId, input.body, input.revision ?? this.control().revision, now, now)
			if (input.messageId === undefined) this.db.query("INSERT OR REPLACE INTO meta(key,value) VALUES('prompt_message_id_clock',?)").run(`${timestamp}:${counter}`)
		})() } catch {
			const existing = this.promptSubmission(input.submissionId)
			return existing && existing.inboundId === input.inboundId && existing.rootSessionId === input.root && existing.ownerInstance === input.owner && (input.messageId === undefined || existing.messageId === input.messageId) ? existing : undefined
		}
		return this.promptSubmission(input.submissionId)
	}
	promptSubmission(id: string): PromptSubmission | undefined {
		const row = this.db.query("SELECT submission_id AS submissionId,inbound_id AS inboundId,root_session_id AS rootSessionId,owner_instance AS ownerInstance,alias,message_id AS messageId,state,call_started AS callStarted,prompt_message_id AS promptMessageId,control_revision AS controlRevision,admission_generation AS admissionGeneration,admission_finished AS admissionFinished FROM prompt_submissions WHERE submission_id=?").get(id) as any
		return row ? { ...row, callStarted: row.callStarted === 1, admissionFinished: row.admissionFinished === 1 } : undefined
	}
	markPromptCallStarted(id: string): boolean { const row = this.promptSubmission(id); if (row?.state !== "SUBMITTING") return false; if (row.callStarted) return true; return this.db.query("UPDATE prompt_submissions SET call_started=1,updated_at=? WHERE submission_id=? AND state='SUBMITTING' AND call_started=0").run(new Date().toISOString(), id).changes === 1 }
	finishPromptSubmission(id: string, state: Exclude<PromptSubmissionState, "SUBMITTING">, detail?: string, provenNoEffect = false): boolean {
		if (!["SUBMITTED", "REJECTED", "CANCELLED", "UNKNOWN"].includes(state)) return false
		const existing = this.promptSubmission(id); if (!existing) return false; if (existing.state === state) return true; if (existing.state !== "SUBMITTING") return false
		if (state === "SUBMITTED" && !existing.callStarted) return false
		if (existing.callStarted && (state === "CANCELLED" || state === "REJECTED") && !provenNoEffect) return false
		return this.db.query("UPDATE prompt_submissions SET state=?,prompt_message_id=CASE WHEN ?='SUBMITTED' THEN ? ELSE prompt_message_id END,rejection=CASE WHEN ?='REJECTED' THEN ? ELSE rejection END,updated_at=? WHERE submission_id=? AND state='SUBMITTING' AND call_started=?").run(state, state, detail ?? null, state, detail?.slice(0, 500) ?? null, new Date().toISOString(), id, existing.callStarted ? 1 : 0).changes === 1
	}
	finishInboundAdmission(inboundId: string, root: string, messageId: string): boolean { return this.db.query("UPDATE inbound SET state='INJECTED',root_session_id=?,prompt_message_id=?,reason=NULL,updated_at=? WHERE message_id=? AND state='RECEIVED'").run(root, messageId, new Date().toISOString(), inboundId).changes === 1 }
	openNativeRequest(input: { requestId: string; requestKey: string; root: string; owner: string; alias: number; kind: NativeRequestKind; payload: unknown; revision?: number; code?: string }): NativeRequest | undefined {
		if (![input.requestId, input.requestKey, input.root, input.owner].every((value) => typeof value === "string" && value.length > 0 && value.length <= 500) || !Number.isSafeInteger(input.alias) || input.alias < 1 || !["QUESTION", "PERMISSION"].includes(input.kind)) throw new Error("invalid native request")
		const payload = boundedJson(input.payload), code = input.code ?? allocateRequestCode(input.kind, input.requestKey, (value) => Boolean(this.db.query("SELECT 1 FROM native_requests WHERE code=?").get(value)))
		if (!REQUEST_CODE_PATTERN.test(code) || code[0] !== (input.kind === "QUESTION" ? "Q" : "P")) throw new Error("invalid request code")
		try { const now = new Date().toISOString(); this.db.query("INSERT INTO native_requests(request_id,request_key,code,root_session_id,owner_instance,alias,kind,state,payload_json,inbound_id,resolution_json,control_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'ANNOUNCING',?,NULL,NULL,?,?,?)").run(input.requestId, input.requestKey, code, input.root, input.owner, input.alias, input.kind, payload, input.revision ?? this.control().revision, now, now) } catch { return }
		return this.nativeRequest(input.requestId)
	}
	nativeRequest(idOrCode: string): NativeRequest | undefined {
		const row = this.db.query("SELECT request_id AS requestId,request_key AS requestKey,code,root_session_id AS rootSessionId,owner_instance AS ownerInstance,alias,kind,state,payload_json AS payload,inbound_id AS inboundId,resolution_json AS resolution,control_revision AS controlRevision FROM native_requests WHERE request_id=? OR code=?").get(idOrCode, idOrCode) as any
		return row ? { ...row, payload: JSON.parse(row.payload), resolution: row.resolution === null ? null : JSON.parse(row.resolution) } : undefined
	}
	nativeRequestForKey(requestKey: string): NativeRequest | undefined { const row = this.db.query("SELECT request_id AS id FROM native_requests WHERE request_key=?").get(requestKey) as { id: string } | null; return row ? this.nativeRequest(row.id) : undefined }
	nativeRequestReplay(input: { requestKey: string; requestId: string; root: string; owner: string; kind: NativeRequestKind; payload: unknown; controlRevision: number }): { result: "NONE" | "MISMATCH" } | { result: "EXACT"; request: NativeRequest } {
		const request = this.nativeRequestForKey(input.requestKey)
		if (!request) return { result: "NONE" }
		const exact = request.requestId === input.requestId && request.rootSessionId === input.root && request.ownerInstance === input.owner && request.kind === input.kind && request.controlRevision === input.controlRevision && canonicalJson(request.payload) === canonicalJson(input.payload)
		return exact ? { result: "EXACT", request } : { result: "MISMATCH" }
	}
	finishNativeAnnouncement(id: string, _delivered: boolean): boolean { const existing = this.nativeRequest(id); if (existing?.state === "OPEN") return true; return this.db.query("UPDATE native_requests SET state='OPEN',updated_at=? WHERE request_id=? AND state='ANNOUNCING'").run(new Date().toISOString(), id).changes === 1 }
	recordNativeAnswerGuard(rootSessionId: string, requestId: string): boolean {
		if (![rootSessionId, requestId].every((value) => typeof value === "string" && value.length > 0 && value.length <= 500)) return false
		return this.db.query("INSERT INTO native_answer_guards(root_session_id,request_id,created_at) SELECT root_session_id,request_id,? FROM native_requests WHERE root_session_id=? AND request_id=? AND state='OPEN' ON CONFLICT(root_session_id,request_id) DO UPDATE SET created_at=excluded.created_at").run(new Date().toISOString(), rootSessionId, requestId).changes === 1
	}
	consumeNativeAnswerGuard(rootSessionId: string, requestId: string): boolean {
		if (![rootSessionId, requestId].every((value) => typeof value === "string" && value.length > 0 && value.length <= 500)) return false
		return this.db.transaction(() => {
			const active = Boolean(this.db.query("SELECT 1 FROM native_answer_guards g JOIN native_requests r ON r.root_session_id=g.root_session_id AND r.request_id=g.request_id WHERE g.root_session_id=? AND g.request_id=? AND r.state='OPEN'").get(rootSessionId, requestId))
			this.db.query("DELETE FROM native_answer_guards WHERE root_session_id=? AND request_id=?").run(rootSessionId, requestId)
			return active
		})()
	}
	clearNativeAnswerGuard(rootSessionId: string, requestId: string): boolean { return this.db.query("DELETE FROM native_answer_guards WHERE root_session_id=? AND request_id=?").run(rootSessionId, requestId).changes > 0 }
	claimNativeResolution(id: string, inboundId: string): boolean { try { return this.db.transaction(() => { const claimed = this.db.query("UPDATE native_requests SET state='RESOLVING',inbound_id=?,updated_at=? WHERE request_id=? AND state='OPEN'").run(inboundId, new Date().toISOString(), id).changes === 1; if (claimed) this.db.query("DELETE FROM native_answer_guards WHERE request_id=?").run(id); return claimed })() } catch { return false } }
	releaseNativeResolution(id: string, inboundId: string): boolean { return this.db.query("UPDATE native_requests SET state='OPEN',inbound_id=NULL,updated_at=? WHERE request_id=? AND state='RESOLVING' AND inbound_id=?").run(new Date().toISOString(), id, inboundId).changes === 1 }
	finishNativeResolution(id: string, state: "RESOLVED" | "REJECTED" | "UNKNOWN", resolution?: unknown): boolean {
		if (!["RESOLVED", "REJECTED", "UNKNOWN"].includes(state)) return false
		const json = resolution === undefined ? null : boundedJson(resolution)
		return this.db.transaction(() => { const changed = this.db.query("UPDATE native_requests SET state=?,resolution_json=?,updated_at=? WHERE request_id=? AND state='RESOLVING'").run(state, json, new Date().toISOString(), id).changes === 1; if (changed) this.db.query("DELETE FROM native_answer_guards WHERE request_id=?").run(id); return changed })()
	}
	settleNativeTerminal(id: string, state: "RESOLVED" | "REJECTED", resolution?: unknown): boolean {
		const json = resolution === undefined ? null : boundedJson(resolution)
		return this.db.transaction(() => { const changed = this.db.query("UPDATE native_requests SET state=?,resolution_json=COALESCE(?,resolution_json),updated_at=? WHERE request_id=? AND state IN ('ANNOUNCING','OPEN','RESOLVING','UNKNOWN')").run(state, json, new Date().toISOString(), id).changes === 1; this.db.query("DELETE FROM native_answer_guards WHERE request_id=?").run(id); return changed })()
	}
	activeNativeRequests(rootSessionId: string): NativeRequest[] { return (this.db.query("SELECT request_id AS id FROM native_requests WHERE root_session_id=? AND state IN ('ANNOUNCING','OPEN','RESOLVING','UNKNOWN') ORDER BY created_at,request_id").all(rootSessionId) as Array<{ id: string }>).map((row) => this.nativeRequest(row.id)!) }
	nativeQuery(rootSessionId: string): { kind: "NONE" } | { kind: "ONE"; request: NativeRequest } | { kind: "MULTIPLE"; requests: NativeRequest[] } { const requests = this.activeNativeRequests(rootSessionId); return requests.length === 0 ? { kind: "NONE" } : requests.length === 1 ? { kind: "ONE", request: requests[0] } : { kind: "MULTIPLE", requests } }
	beginRuntimeAdmission(submissionId: string, root: string, owner: string, now = Date.now(), leaseMs = INSTANCE_TTL_MS): number | undefined {
		return this.db.transaction(() => {
			const submission = this.promptSubmission(submissionId)
			if (!submission || submission.rootSessionId !== root || submission.ownerInstance !== owner) return
			if (submission.admissionGeneration !== null) return submission.admissionGeneration
			if (submission.state !== "SUBMITTING") return
			const iso = new Date(now).toISOString()
			this.db.query("INSERT INTO root_runtime(root_session_id,owner_instance,status,generation,busy_generation,admission_count,work_pending,observed_ms,lease_expires_ms,updated_at) VALUES(?,?,'QUEUED',1,NULL,1,1,?,?,?) ON CONFLICT(root_session_id) DO UPDATE SET owner_instance=excluded.owner_instance,status=CASE WHEN status IN ('BUSY','RETRY') THEN status ELSE 'QUEUED' END,generation=generation+1,admission_count=admission_count+1,work_pending=1,observed_ms=excluded.observed_ms,lease_expires_ms=excluded.lease_expires_ms,updated_at=excluded.updated_at").run(root, owner, now, now + leaseMs, iso)
			const generation = this.runtime(root)!.generation
			const claimed = this.db.query("UPDATE prompt_submissions SET admission_generation=?,updated_at=? WHERE submission_id=? AND admission_generation IS NULL AND admission_finished=0").run(generation, iso, submissionId)
			if (claimed.changes !== 1) throw new Error("runtime admission claim race")
			return generation
		})()
	}
	finishRuntimeAdmission(submissionId: string, root: string, owner: string): boolean {
		return this.db.transaction(() => {
			const submission = this.promptSubmission(submissionId)
			if (!submission || submission.rootSessionId !== root || submission.ownerInstance !== owner || submission.admissionGeneration === null) return false
			if (submission.admissionFinished) return true
			const now = new Date().toISOString(), finished = this.db.query("UPDATE prompt_submissions SET admission_finished=1,updated_at=? WHERE submission_id=? AND admission_generation=? AND admission_finished=0").run(now, submissionId, submission.admissionGeneration)
			if (finished.changes !== 1) return false
			this.db.query("UPDATE root_runtime SET admission_count=admission_count-1,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation>=? AND admission_count>0").run(now, root, owner, submission.admissionGeneration)
			return true
		})()
	}
	rejectPromptSubmissionNoEffect(submissionId: string, root: string, owner: string, rejection?: string): boolean {
		return this.db.transaction(() => {
			const submission = this.promptSubmission(submissionId)
			if (!submission || submission.rootSessionId !== root || submission.ownerInstance !== owner || submission.admissionGeneration === null) return false
			if (submission.state === "REJECTED" && submission.admissionFinished) return true
			if (submission.state !== "SUBMITTING") return false
			const now = new Date().toISOString()
			if (this.db.query("UPDATE prompt_submissions SET state='REJECTED',rejection=?,admission_finished=1,updated_at=? WHERE submission_id=? AND state='SUBMITTING' AND admission_generation=? AND admission_finished=0").run(rejection?.slice(0, 500) ?? null, now, submissionId, submission.admissionGeneration).changes !== 1) return false
			this.db.query("UPDATE root_runtime SET admission_count=admission_count-1,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation>=? AND admission_count>0").run(now, root, owner, submission.admissionGeneration)
			this.db.query("UPDATE root_runtime SET status='IDLE',generation=generation+1,busy_generation=NULL,work_pending=0,observed_ms=0,lease_expires_ms=0,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=? AND status='QUEUED' AND admission_count=0").run(now, root, owner, submission.admissionGeneration)
			return true
		})()
	}
	observeRuntimeStatus(root: string, owner: string, status: "BUSY" | "RETRY" | "QUEUED" | "IDLE", generation: number, now = Date.now(), leaseMs = INSTANCE_TTL_MS): boolean {
		if (status === "IDLE") return this.db.query("UPDATE root_runtime SET status='IDLE',busy_generation=NULL,work_pending=0,observed_ms=?,lease_expires_ms=?,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=? AND status IN ('BUSY','RETRY') AND busy_generation=? AND admission_count=0").run(now, now + leaseMs, new Date(now).toISOString(), root, owner, generation, generation).changes === 1
		if (status === "QUEUED") return this.db.query("UPDATE root_runtime SET status='QUEUED',work_pending=1,observed_ms=?,lease_expires_ms=?,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=?").run(now, now + leaseMs, new Date(now).toISOString(), root, owner, generation).changes === 1
		return this.db.query("UPDATE root_runtime SET status=?,busy_generation=?,work_pending=1,observed_ms=?,lease_expires_ms=?,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=?").run(status, generation, now, now + leaseMs, new Date(now).toISOString(), root, owner, generation).changes === 1
	}
	syncRuntimeAuthoritative(root: string, owner: string, status: "BUSY" | "RETRY" | "QUEUED" | "IDLE", generation: number, now = Date.now(), leaseMs = INSTANCE_TTL_MS): boolean {
		if (status !== "IDLE") return this.observeRuntimeStatus(root, owner, status, generation, now, leaseMs)
		const iso = new Date(now).toISOString()
		const active = this.db.query("UPDATE root_runtime SET observed_ms=?,lease_expires_ms=?,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=? AND admission_count>0").run(now, now + leaseMs, iso, root, owner, generation)
		if (active.changes === 1) return true
		return this.db.query("UPDATE root_runtime SET status='IDLE',generation=generation+1,busy_generation=NULL,admission_count=0,work_pending=0,observed_ms=?,lease_expires_ms=?,updated_at=? WHERE root_session_id=? AND owner_instance=? AND generation=? AND admission_count=0").run(now, now + leaseMs, iso, root, owner, generation).changes === 1
	}
	reconcileRuntimeAuthoritative(root: string, owner: string, status: "BUSY" | "RETRY" | "IDLE", now = Date.now(), leaseMs = INSTANCE_TTL_MS): number {
		const current = this.runtime(root), generation = current?.generation ?? 1, iso = new Date(now).toISOString()
		if (!current) this.db.query("INSERT INTO root_runtime(root_session_id,owner_instance,status,generation,busy_generation,admission_count,work_pending,observed_ms,lease_expires_ms,updated_at) VALUES(?,?,?,?,?,0,?,?,?,?)").run(root, owner, status, generation, status === "IDLE" ? null : generation, status === "IDLE" ? 0 : 1, now, now + leaseMs, iso)
		else this.syncRuntimeAuthoritative(root, owner, status, generation, now, leaseMs)
		return generation
	}
	runtime(root: string): RootRuntime | undefined { const row = this.db.query("SELECT root_session_id AS rootSessionId,owner_instance AS ownerInstance,status,generation,busy_generation AS busyGeneration,admission_count AS admissionCount,work_pending AS workPending,observed_ms AS observedMs,lease_expires_ms AS leaseExpiresMs FROM root_runtime WHERE root_session_id=?").get(root) as any; return row ? { ...row, workPending: row.workPending === 1 } : undefined }
	activeRuntimeSnapshots(): Array<RootRuntime & { endpoint: string; instanceToken: string; directory: string }> {
		return (this.db.query("SELECT r.root_session_id AS rootSessionId,r.owner_instance AS ownerInstance,r.status,r.generation,r.busy_generation AS busyGeneration,r.admission_count AS admissionCount,r.work_pending AS workPending,r.observed_ms AS observedMs,r.lease_expires_ms AS leaseExpiresMs,i.endpoint,i.instance_token AS instanceToken,b.directory FROM root_runtime r JOIN bindings b ON b.root_session_id=r.root_session_id AND b.owner_instance=r.owner_instance AND b.active=1 JOIN instances i ON i.instance_id=r.owner_instance WHERE r.work_pending=1 AND (r.status IN ('BUSY','RETRY') OR (r.status='QUEUED' AND r.admission_count>0))").all() as any[]).map((row) => ({ ...row, workPending: row.workPending === 1 }))
	}
	expireRuntimeLeases(now = Date.now()): number { return this.db.query("UPDATE root_runtime SET status='IDLE',generation=generation+1,busy_generation=NULL,admission_count=0,work_pending=0,lease_expires_ms=0,updated_at=? WHERE work_pending=1 AND admission_count=0 AND lease_expires_ms>0 AND lease_expires_ms<=?").run(new Date(now).toISOString(), now).changes }
	desiredTyping(now = Date.now()): boolean {
		this.expireRuntimeLeases(now); return this.recomputeTypingDesired(now)
	}
	private recomputeTypingDesired(now = Date.now()): boolean { const control = this.control(), route = this.route(); const desired = Boolean(control.enabled && route.conversationId && route.contextToken && this.db.query("SELECT 1 FROM root_runtime WHERE work_pending=1 AND status IN ('BUSY','RETRY','QUEUED') AND lease_expires_ms>?").get(now)); this.db.query("UPDATE typing_state SET desired=?,updated_at=? WHERE singleton=1").run(desired ? 1 : 0, new Date(now).toISOString()); return desired }
	typingDesired(): boolean { return (this.db.query("SELECT desired FROM typing_state WHERE singleton=1").get() as { desired: number }).desired === 1 }
	typingState(): { desired: boolean; actual: boolean | null; contextHash: string | null; attemptMs: number } { const row = this.db.query("SELECT desired,actual,context_hash AS contextHash,attempt_ms AS attemptMs FROM typing_state WHERE singleton=1").get() as any; return { desired: row.desired === 1, actual: row.actual === null ? null : row.actual === 1, contextHash: row.contextHash, attemptMs: row.attemptMs } }
	setTypingActual(actual: boolean | null, contextHash: string | null, now = Date.now()): void { if (contextHash !== null && contextHash.length > 500) throw new Error("invalid typing context hash"); this.db.query("UPDATE typing_state SET actual=?,context_hash=?,attempt_ms=?,updated_at=? WHERE singleton=1").run(actual === null ? null : actual ? 1 : 0, contextHash, now, new Date(now).toISOString()) }
	checkpointForRequest(requestKey: string, rootSessionId: string): { checkpointId: string; state: string; rootSessionId: string; ownerInstance: string } | undefined { const row = this.db.query("SELECT checkpoint_id AS checkpointId,state,root_session_id AS rootSessionId,owner_instance AS ownerInstance FROM checkpoints WHERE request_key=? AND root_session_id=?").get(requestKey, rootSessionId) as { checkpointId: string; state: string; rootSessionId: string; ownerInstance: string } | null; return row ?? undefined }
	openCheckpoint(input: { checkpointId: string; requestKey: string; root: string; owner: string; alias: number; question: string; choices: string[]; revision: number }): boolean {
		const control = this.control(), binding = this.bindingForRoot(input.root); if (!control.enabled || control.revision !== input.revision || !binding?.active || binding.ownerInstance !== input.owner) return false
		const route = this.route(); if (!route.conversationId) return false
		if (this.db.query("SELECT 1 FROM checkpoints WHERE root_session_id=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").get(input.root)) return false
		try { this.db.query("INSERT INTO checkpoints(checkpoint_id,request_key,root_session_id,owner_instance,conversation_id,alias,question,choices_json,state,inbound_id,control_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'SENDING',NULL,?,?,?)").run(input.checkpointId, input.requestKey, input.root, input.owner, route.conversationId, input.alias, input.question, JSON.stringify(input.choices), input.revision, new Date().toISOString(), new Date().toISOString()); return true } catch { return false }
	}
	activateCheckpoint(checkpointId: string): boolean { return this.db.query("UPDATE checkpoints SET state='OPEN',updated_at=? WHERE checkpoint_id=? AND state='SENDING'").run(new Date().toISOString(), checkpointId).changes === 1 }
	failCheckpoint(checkpointId: string): void { this.db.query("UPDATE checkpoints SET state='UNKNOWN',updated_at=? WHERE checkpoint_id=? AND state IN ('SENDING','OPEN','ANSWERING')").run(new Date().toISOString(), checkpointId) }
	openCheckpointFor(binding: Binding): { checkpointId: string } | undefined { return this.db.query("SELECT checkpoint_id AS checkpointId FROM checkpoints WHERE root_session_id=? AND owner_instance=? AND state='OPEN'").get(binding.rootSessionId, binding.ownerInstance) as { checkpointId: string } | undefined }
	claimCheckpoint(checkpointId: string, inboundId: string, binding: Binding): boolean { return this.db.query("UPDATE checkpoints SET state='ANSWERING',inbound_id=?,updated_at=? WHERE checkpoint_id=? AND state='OPEN' AND root_session_id=? AND owner_instance=?").run(inboundId, new Date().toISOString(), checkpointId, binding.rootSessionId, binding.ownerInstance).changes === 1 }
	checkpointAnswered(checkpointId: string): void { this.db.query("UPDATE checkpoints SET state='ANSWERED',updated_at=? WHERE checkpoint_id=? AND state='ANSWERING'").run(new Date().toISOString(), checkpointId) }
	checkpointInjectionUnknown(checkpointId: string): void { this.db.query("UPDATE checkpoints SET state='UNKNOWN',updated_at=? WHERE checkpoint_id=? AND state='ANSWERING'").run(new Date().toISOString(), checkpointId) }
	checkpointState(checkpointId: string): string | undefined { return (this.db.query("SELECT state FROM checkpoints WHERE checkpoint_id=?").get(checkpointId) as { state: string } | null)?.state }
	markDirectPending(root: string, owner: string, inboundId: string, checkpointId?: string): void {
		const current = this.db.query("SELECT COALESCE(run_id,0) AS runId FROM session_activity WHERE root_session_id=?").get(root) as { runId: number } | null, control = this.control(), run = (current?.runId ?? 0) + 1, origin = checkpointId ? `CHECKPOINT:${checkpointId}` : `INBOUND:${inboundId}`
		this.db.query("INSERT INTO session_activity(root_session_id,owner_instance,running,idle,last_assistant_id,last_assistant_error,direct_assistant_id,epoch,run_id,origin,candidate_run,claimed_run,updated_at) VALUES(?,?,1,0,NULL,0,?,?,?,?,NULL,0,?) ON CONFLICT(root_session_id) DO UPDATE SET owner_instance=excluded.owner_instance,running=1,idle=0,last_assistant_id=NULL,last_assistant_error=0,direct_assistant_id=excluded.direct_assistant_id,epoch=excluded.epoch,run_id=excluded.run_id,origin=excluded.origin,candidate_run=NULL,updated_at=excluded.updated_at").run(root, owner, `pending:${inboundId}`, control.revision, run, origin, new Date().toISOString())
	}
	markDirectAssistant(root: string, owner: string, assistantId: string): void { this.db.query("UPDATE session_activity SET owner_instance=?,direct_assistant_id=?,updated_at=? WHERE root_session_id=?").run(owner, assistantId, new Date().toISOString(), root) }
	observeAssistant(root: string, owner: string, assistantId: string, failed: boolean): void { this.db.query("UPDATE session_activity SET last_assistant_id=?,last_assistant_error=?,candidate_run=run_id,updated_at=? WHERE root_session_id=? AND owner_instance=? AND running=1").run(assistantId, failed ? 1 : 0, new Date().toISOString(), root, owner) }
	observeStatus(root: string, owner: string, status: "busy" | "idle"): void {
		const existing = this.db.query("SELECT running,run_id AS runId FROM session_activity WHERE root_session_id=? AND owner_instance=?").get(root, owner) as { running: number; runId: number } | null
		if (status === "idle") { this.db.query("UPDATE session_activity SET idle=1,updated_at=? WHERE root_session_id=? AND owner_instance=? AND running=1").run(new Date().toISOString(), root, owner); return }
		if (existing?.running) return
		const control = this.control(), run = (existing?.runId ?? 0) + 1
		this.db.query("INSERT INTO session_activity(root_session_id,owner_instance,running,idle,last_assistant_id,last_assistant_error,direct_assistant_id,epoch,run_id,origin,candidate_run,claimed_run,updated_at) VALUES(?,?,1,0,NULL,0,NULL,?,?,'LOCAL',NULL,0,?) ON CONFLICT(root_session_id) DO UPDATE SET owner_instance=excluded.owner_instance,running=1,idle=0,last_assistant_id=NULL,last_assistant_error=0,direct_assistant_id=NULL,epoch=excluded.epoch,run_id=excluded.run_id,origin='LOCAL',candidate_run=NULL,updated_at=excluded.updated_at").run(root, owner, control.revision, run, new Date().toISOString())
	}
	claimCompletion(root: string, owner: string): { outboundId: string; binding: RoutedBinding; payload: string } | undefined {
		const registration = this.bindingForRoot(root), route = this.route(), control = this.control(); if (!control.enabled || !registration?.active || !route.conversationId || !route.contextToken || registration.ownerInstance !== owner) return
		const binding: RoutedBinding = { ...registration, conversationId: route.conversationId, contextToken: route.contextToken }
		const row = this.db.query("SELECT running,epoch,run_id AS runId,origin,candidate_run AS candidateRun,claimed_run AS claimedRun,idle,last_assistant_id AS assistantId,last_assistant_error AS failed,direct_assistant_id AS directId FROM session_activity WHERE root_session_id=? AND owner_instance=?").get(root, owner) as any
		if (row?.running && row.idle && row.origin !== "LOCAL") {
			const consumed = this.db.query("UPDATE session_activity SET running=0,claimed_run=CASE WHEN claimed_run<run_id THEN run_id ELSE claimed_run END,updated_at=? WHERE root_session_id=? AND owner_instance=? AND run_id=? AND running=1 AND idle=1 AND origin=?").run(new Date().toISOString(), root, owner, row.runId, row.origin)
			if (consumed.changes) this.audit(`completion-suppressed:${row.origin}:run-${row.runId}`)
			return
		}
		if (!row?.running || !row.idle || row.epoch !== control.revision || row.origin !== "LOCAL" || row.candidateRun !== row.runId || row.runId <= row.claimedRun || !row.assistantId || row.assistantId === row.directId) return
		if (this.db.query("SELECT 1 FROM checkpoints WHERE root_session_id=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").get(root)) return
		if (this.db.query("SELECT 1 FROM outbound WHERE message_id=? UNION ALL SELECT 1 FROM pending_replies WHERE assistant_message_id=? LIMIT 1").get(row.assistantId, row.assistantId)) return
		const payload = formatOutbound(binding.alias, row.failed === 1 ? COMPLETION_ERROR_TEXT : COMPLETION_TEXT), outboundId = crypto.randomUUID(), dedupeKey = `completion:${root}:${row.assistantId}`
		let claimed = false
		this.db.transaction(() => { try { this.db.query("INSERT INTO control_outbound(outbound_id,dedupe_key,root_session_id,kind,state,payload,conversation_id,context_token,updated_at) VALUES(?,?,?,'completion','SENDING',?,?,?,?)").run(outboundId, dedupeKey, root, payload, binding.conversationId, binding.contextToken, new Date().toISOString()); this.db.query("UPDATE session_activity SET claimed_run=run_id,running=0,updated_at=? WHERE root_session_id=? AND run_id=? AND claimed_run<?").run(new Date().toISOString(), root, row.runId, row.runId); claimed = true } catch {} })()
		return claimed ? { outboundId, binding, payload } : undefined
	}
	claimControlOutbound(input: { dedupeKey: string; root: string; kind: string; payload: string; logicalText: string; revision?: number }): { result: "CLAIMED"; outboundId: string; binding: RoutedBinding } | { result: "REPLAY"; state: string; payload: string } | { result: "CONFLICT" } | undefined
	claimControlOutbound(input: { dedupeKey: string; root: string; kind: string; payload: string; revision?: number }): { outboundId: string; binding: RoutedBinding } | undefined
	claimControlOutbound(input: { dedupeKey: string; root: string; kind: string; payload: string; logicalText?: string; revision?: number }): { outboundId: string; binding: RoutedBinding } | { result: "CLAIMED"; outboundId: string; binding: RoutedBinding } | { result: "REPLAY"; state: string; payload: string } | { result: "CONFLICT" } | undefined {
		const registration = this.bindingForRoot(input.root), route = this.route(), control = this.control(); if (!registration?.active || !route.conversationId || !route.contextToken || !control.enabled || (input.revision !== undefined && input.revision !== control.revision)) return
		const binding: RoutedBinding = { ...registration, conversationId: route.conversationId, contextToken: route.contextToken }
		const logicalHash = input.logicalText === undefined ? null : sha256(input.logicalText)
		const replay = (): { result: "REPLAY"; state: string; payload: string } | { result: "CONFLICT" } | undefined => {
			const existing = this.db.query("SELECT root_session_id AS root,kind,state,payload,logical_text AS logicalText,logical_hash AS logicalHash FROM control_outbound WHERE dedupe_key=?").get(input.dedupeKey) as any
			if (!existing) return
			return existing.root === input.root && existing.kind === input.kind && existing.logicalText === input.logicalText && existing.logicalHash === logicalHash ? { result: "REPLAY", state: existing.state, payload: existing.payload } : { result: "CONFLICT" }
		}
		if (input.logicalText !== undefined) {
			if (!isPlainText(input.logicalText)) throw new Error("invalid logical outbound text")
			const existing = replay(); if (existing) return existing
		}
		const outboundId = crypto.randomUUID()
		try { this.db.query("INSERT INTO control_outbound(outbound_id,dedupe_key,root_session_id,kind,state,payload,conversation_id,context_token,updated_at,logical_text,logical_hash) VALUES(?,?,?,?,'SENDING',?,?,?,?,?,?)").run(outboundId, input.dedupeKey, input.root, input.kind, input.payload, binding.conversationId, binding.contextToken, new Date().toISOString(), input.logicalText ?? null, logicalHash); return input.logicalText === undefined ? { outboundId, binding } : { result: "CLAIMED", outboundId, binding } } catch { return input.logicalText === undefined ? undefined : replay() }
	}
	claimSystemOutbound(message: WeixinInbound, kind: string, payload: string): { outboundId: string } | undefined { const outboundId = crypto.randomUUID(); try { this.db.query("INSERT INTO control_outbound(outbound_id,dedupe_key,root_session_id,kind,state,payload,conversation_id,context_token,updated_at) VALUES(?,?,?,?,'SENDING',?,?,?,?)").run(outboundId, `inbound:${message.id}:${kind}`, `inbound:${message.id}`, kind, payload, message.fromUserId, message.contextToken, new Date().toISOString()); return { outboundId } } catch { return } }
	controlOutboundState(dedupeKey: string): string | undefined { return (this.db.query("SELECT state FROM control_outbound WHERE dedupe_key=?").get(dedupeKey) as { state: string } | null)?.state }
	controlOutboundPayload(dedupeKey: string): string | undefined { return (this.db.query("SELECT payload FROM control_outbound WHERE dedupe_key=?").get(dedupeKey) as { payload: string } | null)?.payload }
	finishControlOutbound(outboundId: string, sent: boolean): void { this.db.query("UPDATE control_outbound SET state=?,updated_at=? WHERE outbound_id=? AND state='SENDING'").run(sent ? "SENT" : "UNKNOWN", new Date().toISOString(), outboundId) }
	recordEcho(conversationId: string, contextToken: string, payload: string, now = Date.now(), ttlMs = OUTBOUND_ECHO_TTL_MS): void { this.db.query("INSERT OR REPLACE INTO outbound_echoes(echo_hash,conversation_id,context_token,payload,expires_ms,created_at) VALUES(?,?,?,?,?,?)").run(sha256(`${conversationId}\0${contextToken}\0${payload}`), conversationId, contextToken, payload, now + ttlMs, new Date(now).toISOString()) }
	matchesEcho(conversationId: string, contextToken: string, payload: string, now = Date.now()): boolean { this.sweepOutboundEchoes(now); return Boolean(this.db.query("SELECT 1 FROM outbound_echoes WHERE echo_hash=? AND conversation_id=? AND context_token=? AND payload=? AND expires_ms>?").get(sha256(`${conversationId}\0${contextToken}\0${payload}`), conversationId, contextToken, payload, now)) }
	sweepOutboundEchoes(now = Date.now()): number { return this.db.query("DELETE FROM outbound_echoes WHERE expires_ms<=?").run(now).changes }
	beginInbound(message: WeixinInbound): boolean { return this.db.query("INSERT OR IGNORE INTO inbound(message_id,from_user_id,context_token,text,state,updated_at) VALUES(?,?,?,?, 'RECEIVED',?)").run(message.id, message.fromUserId, message.contextToken, message.text, new Date().toISOString()).changes > 0 }
	beginPending(messageId: string, root: string, alias: number, revision: number): void {
		this.db.transaction(() => {
			this.db.query("UPDATE inbound SET state='INJECTING',root_session_id=?,updated_at=? WHERE message_id=? AND state='RECEIVED'").run(root, new Date().toISOString(), messageId)
			this.db.query("INSERT OR IGNORE INTO pending_replies(inbound_id,root_session_id,prompt_message_id,alias,state,assistant_message_id,payload,injected_at,control_revision,updated_at) VALUES(?,?,NULL,?,'WAITING',NULL,NULL,NULL,?,?)").run(messageId, root, alias, revision, new Date().toISOString())
		})()
	}
	markUnknown(messageId: string, reason: string): void { this.db.transaction(() => { this.db.query("UPDATE inbound SET state='UNKNOWN',reason=?,updated_at=? WHERE message_id=?").run(reason.slice(0, 200), new Date().toISOString(), messageId); this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE inbound_id=? AND state='WAITING'").run(new Date().toISOString(), messageId) })() }
	completePendingAndClaim(instanceId: string, root: string, inboundId: string, promptMessageId: string, assistantId: string, text: string, revision: number): { binding: RoutedBinding; payload: string; revision: number } | undefined {
		const registration = this.bindingForRoot(root), route = this.route()
		const control = this.control(); if (!registration?.active || registration.ownerInstance !== instanceId || !route.conversationId || !route.contextToken || !control.enabled || control.revision !== revision) return
		const binding: RoutedBinding = { ...registration, conversationId: route.conversationId, contextToken: route.contextToken }
		const payload = formatOutbound(binding.alias, text)
		let claimed = false
		this.db.transaction(() => {
			const now = Date.now()
			const result = this.db.query("UPDATE pending_replies SET state='SENDING',prompt_message_id=?,assistant_message_id=?,payload=?,injected_at=?,updated_at=? WHERE inbound_id=? AND root_session_id=? AND control_revision=? AND state='WAITING' AND EXISTS(SELECT 1 FROM inbound WHERE message_id=? AND state='INJECTING') AND EXISTS(SELECT 1 FROM control_state WHERE singleton=1 AND enabled=1 AND revision=?)").run(promptMessageId, assistantId, payload, now, new Date(now).toISOString(), inboundId, root, revision, inboundId, revision)
			if (!result.changes) return
			this.db.query("UPDATE inbound SET state='INJECTED',root_session_id=?,prompt_message_id=?,updated_at=? WHERE message_id=? AND state='INJECTING'").run(root, promptMessageId, new Date(now).toISOString(), inboundId)
			this.db.query("INSERT INTO outbound(message_id,inbound_id,state,payload,updated_at) VALUES(?,?, 'SENDING',?,?)").run(assistantId, inboundId, payload, new Date().toISOString())
			claimed = true
		})()
		return claimed ? { binding, payload, revision } : undefined
	}
	controlMatches(revision: number): boolean { const control = this.control(); return control.enabled && control.revision === revision }
	sweepOrphanWaiting(now = Date.now(), ttlMs = ORPHAN_PENDING_TTL_MS): number {
		const stale = (this.db.query("SELECT inbound_id AS inboundId,updated_at AS updatedAt FROM pending_replies WHERE state='WAITING'").all() as Array<{ inboundId: string; updatedAt: string }>).filter((row) => now - Date.parse(row.updatedAt) >= ttlMs)
		if (!stale.length) return 0
		this.db.transaction(() => { for (const row of stale) { this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE inbound_id=? AND state='WAITING'").run(new Date(now).toISOString(), row.inboundId); this.db.query("UPDATE inbound SET state='UNKNOWN',reason='orphan-waiting-timeout',updated_at=? WHERE message_id=? AND state IN ('RECEIVED','INJECTING','INJECTED')").run(new Date(now).toISOString(), row.inboundId) } })()
		return stale.length
	}
	finishReply(inboundId: string, assistantId: string, sent: boolean): void {
		const state = sent ? "SENT" : "UNKNOWN"
		this.db.transaction(() => {
			this.db.query("UPDATE pending_replies SET state=?,updated_at=? WHERE inbound_id=? AND state='SENDING'").run(state, new Date().toISOString(), inboundId)
			this.db.query("UPDATE outbound SET state=?,updated_at=? WHERE message_id=? AND state='SENDING'").run(state, new Date().toISOString(), assistantId)
		})()
	}
	state(messageId: string): string | undefined { return (this.db.query("SELECT state FROM inbound WHERE message_id=?").get(messageId) as { state: string } | null)?.state }
	pendingState(inboundId: string): string | undefined { return (this.db.query("SELECT state FROM pending_replies WHERE inbound_id=?").get(inboundId) as { state: string } | null)?.state }
	audit(reason: string): void { this.db.query("INSERT INTO audit(at,reason) VALUES(?,?)").run(new Date().toISOString(), reason.slice(0, 200)) }
}

export class SerialQueue {
	private readonly tails = new Map<string, Promise<void>>()
	run<T>(key: string, action: () => Promise<T>): Promise<T> {
		const prior = this.tails.get(key) ?? Promise.resolve()
		const result = prior.then(action)
		const tail = result.then(() => undefined, () => undefined)
		this.tails.set(key, tail)
		return result.finally(() => { if (this.tails.get(key) === tail) this.tails.delete(key) })
	}
}

export function stateDirectory(): string {
	return path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "opencode", "wechat-control")
}

export async function initializeState(): Promise<{ directory: string; secret: string }> {
	const directory = stateDirectory()
	await mkdir(directory, { recursive: true, mode: 0o700 })
	const secretPath = path.join(directory, "rpc.secret")
	if (!(await Bun.file(secretPath).exists())) await writeFile(secretPath, randomBytes(48).toString("hex"), { flag: "wx", mode: 0o600 })
	return { directory, secret: (await Bun.file(secretPath).text()).trim() }
}
