import { Database } from "bun:sqlite"
import { createHash, randomBytes } from "node:crypto"
import { mkdir, writeFile } from "node:fs/promises"
import { existsSync, renameSync, unlinkSync } from "node:fs"
import * as os from "node:os"
import * as path from "node:path"

export const MAX_TEXT_LENGTH = 4000
export const MAX_ROUTE_ID_LENGTH = 500
export const MAX_CONTEXT_TOKEN_LENGTH = 4000
export const HELP_TEXT = "用法：首行 #编号，下一行输入正文。"
export const UNKNOWN_TITLE = "未命名会话"
export const OFFLINE_TEXT = "目标 OpenCode 会话当前离线，请稍后重试。"
export const CONTROL_OFF_TEXT = "微信接管当前已关闭；发送 help 查看用法。"
export const PERMISSION_DENIED_TEXT = "OpenCode 需要本地权限确认；受限微信接管已拒绝该权限请求。"
export const COMPLETION_TEXT = "任务已完成。"
export const COMPLETION_ERROR_TEXT = "任务执行失败。"
export const INSTANCE_TTL_MS = 45_000
export const ORPHAN_PENDING_TTL_MS = 12 * 60_000
export const OUTBOUND_ECHO_TTL_MS = 10 * 60_000

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
	rootSessionId: string
	directory: string
	ownerInstance: string
	title: string | null
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
		if ([...lines, line].join("\n").length <= MAX_TEXT_LENGTH) { lines.push(line); continue }
		let suffix = `……另有 ${remaining} 个会话未显示。`
		while (lines.length && [...lines, suffix].join("\n").length > MAX_TEXT_LENGTH) { lines.pop(); suffix = `……另有 ${ordered.length - lines.length} 个会话未显示。` }
		lines.push(suffix.slice(0, MAX_TEXT_LENGTH)); break
	}
	return lines.join("\n")
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
		this.sweepOrphanWaiting(Date.now(), ORPHAN_PENDING_TTL_MS)
		this.sweepOutboundEchoes()
	}
	private migrate(databasePath: string): string | undefined {
		const userVersion = (this.db.query("PRAGMA user_version").get() as any)?.user_version ?? 0
		if (userVersion > 5) throw new Error("unsupported database schema")
		const has = (table: string) => Boolean(this.db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table))
		const hasLegacyData = has("bindings") || has("inbound") || has("outbound")
		let snapshot: string | undefined
		if (databasePath !== ":memory:" && userVersion < 5 && (userVersion > 0 || hasLegacyData)) {
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
CREATE INDEX IF NOT EXISTS idx_pending_root_state ON pending_replies(root_session_id,state);
CREATE INDEX IF NOT EXISTS idx_inbound_state ON inbound(state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbound_inbound ON outbound(inbound_id);
`)
			add("pending_replies", "control_revision", "INTEGER NOT NULL DEFAULT 0")
			add("checkpoints", "request_key", "TEXT"); add("checkpoints", "control_revision", "INTEGER NOT NULL DEFAULT 0")
			this.db.exec("UPDATE checkpoints SET request_key=checkpoint_id WHERE request_key IS NULL")
			add("session_activity", "epoch", "INTEGER NOT NULL DEFAULT 0"); add("session_activity", "run_id", "INTEGER NOT NULL DEFAULT 0"); add("session_activity", "origin", "TEXT NOT NULL DEFAULT 'NONE'"); add("session_activity", "candidate_run", "INTEGER"); add("session_activity", "claimed_run", "INTEGER NOT NULL DEFAULT 0")
			this.db.exec("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL WHERE epoch=0")
			add("outbound_echoes", "context_token", "TEXT NOT NULL DEFAULT ''"); add("outbound_echoes", "expires_ms", "INTEGER NOT NULL DEFAULT 0")
			this.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_request_key ON checkpoints(request_key); CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_active_root ON checkpoints(root_session_id) WHERE state IN ('SENDING','OPEN','ANSWERING')")
			this.db.query("INSERT OR IGNORE INTO control_state(singleton,enabled,revision) VALUES(1,0,0)").run()
			if (userVersion < 5 && has("bindings") && columns("bindings").has("conversation_id")) {
				const conversations = this.db.query("SELECT DISTINCT conversation_id AS id FROM bindings WHERE conversation_id<>''").all() as Array<{ id: string }>
				this.db.exec("ALTER TABLE bindings RENAME TO bindings_pre_v5")
				this.db.exec("CREATE TABLE bindings(alias INTEGER PRIMARY KEY AUTOINCREMENT,root_session_id TEXT NOT NULL UNIQUE,directory TEXT NOT NULL,owner_instance TEXT NOT NULL,title TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
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
			this.db.query("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','5')").run()
			this.db.exec("PRAGMA user_version=5")
		})()
		return snapshot
	}
	private createConsistentSnapshot(databasePath: string): string {
		const backup = `${databasePath}.pre-v5-${Date.now()}-${crypto.randomUUID()}.bak`, temp = `${backup}.tmp`
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
			this.db.query("UPDATE inbound SET state='UNKNOWN',reason='crash-during-injection',updated_at=? WHERE state='INJECTING'").run(new Date().toISOString())
			this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='WAITING' AND inbound_id IN (SELECT message_id FROM inbound WHERE state='UNKNOWN' AND reason='crash-during-injection')").run(new Date().toISOString())
			this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE outbound SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE control_outbound SET state='UNKNOWN',updated_at=? WHERE state='SENDING'").run(new Date().toISOString())
			this.db.query("UPDATE checkpoints SET state='UNKNOWN',updated_at=? WHERE state IN ('SENDING','ANSWERING')").run(new Date().toISOString())
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
		this.db.query("DELETE FROM instances WHERE instance_id=?").run(instanceId)
		return true
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
			const existing = this.bindingForRoot(input.rootSessionId)
			if (existing) {
				if (existing.ownerInstance !== input.ownerInstance && this.instance(existing.ownerInstance)?.online) throw new Error("owner-live")
				this.db.query("UPDATE bindings SET directory=?,owner_instance=?,title=?,updated_at=? WHERE root_session_id=?").run(input.directory, input.ownerInstance, title, now, input.rootSessionId)
				if (existing.ownerInstance !== input.ownerInstance) { this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE root_session_id=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now, input.rootSessionId); this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,owner_instance=?,updated_at=? WHERE root_session_id=?").run(input.ownerInstance, now, input.rootSessionId); this.db.query("UPDATE inbound SET state='UNKNOWN',reason='owner-rebound',updated_at=? WHERE state='INJECTING' AND message_id IN (SELECT inbound_id FROM pending_replies WHERE root_session_id=? AND state='WAITING')").run(now, input.rootSessionId); this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE root_session_id=? AND state='WAITING'").run(now, input.rootSessionId) }
			} else this.db.query("INSERT INTO bindings(root_session_id,directory,owner_instance,title,created_at,updated_at) VALUES(?,?,?,?,?,?)").run(input.rootSessionId, input.directory, input.ownerInstance, title, now, now)
			this.db.query("UPDATE control_state SET enabled=1,revision=revision+1 WHERE singleton=1 AND enabled=0").run()
			binding = this.bindingForRoot(input.rootSessionId)
		})()
		return binding!
	}
	bindingForAlias(alias: number): Binding | undefined { return (this.db.query("SELECT alias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title FROM bindings WHERE alias=?").get(alias) as Binding | null) ?? undefined }
	bindingForRoot(root: string): Binding | undefined { return (this.db.query("SELECT alias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title FROM bindings WHERE root_session_id=?").get(root) as Binding | null) ?? undefined }
	bindings(): Binding[] { return this.db.query("SELECT alias,root_session_id AS rootSessionId,directory,owner_instance AS ownerInstance,title FROM bindings ORDER BY alias").all() as Binding[] }
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
			if (!enabled) { const now = new Date().toISOString(); this.db.query("UPDATE checkpoints SET state='CANCELLED',updated_at=? WHERE state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").run(now); this.db.query("UPDATE session_activity SET running=0,idle=1,origin='NONE',candidate_run=NULL,updated_at=?").run(now); this.db.query("UPDATE pending_replies SET state='UNKNOWN',updated_at=? WHERE state='WAITING'").run(now); this.db.query("UPDATE inbound SET state='UNKNOWN',reason='control-cancelled',updated_at=? WHERE state='INJECTING'").run(now) }
		})()
		return this.control()
	}
	checkpointForRequest(requestKey: string, root?: string): { checkpointId: string; state: string; rootSessionId: string; ownerInstance: string } | undefined { const row = this.db.query("SELECT checkpoint_id AS checkpointId,state,root_session_id AS rootSessionId,owner_instance AS ownerInstance FROM checkpoints WHERE request_key=?").get(requestKey) as { checkpointId: string; state: string; rootSessionId: string; ownerInstance: string } | null; return row && (!root || row.rootSessionId === root) ? row : undefined }
	openCheckpoint(input: { checkpointId: string; requestKey: string; root: string; owner: string; alias: number; question: string; choices: string[]; revision: number }): boolean {
		const control = this.control(); if (!control.enabled || control.revision !== input.revision || this.bindingForRoot(input.root)?.ownerInstance !== input.owner) return false
		const route = this.route(); if (!route.conversationId) return false
		if (this.db.query("SELECT 1 FROM checkpoints WHERE root_session_id=? AND state IN ('SENDING','OPEN','ANSWERING','UNKNOWN')").get(input.root)) return false
		try { this.db.query("INSERT INTO checkpoints(checkpoint_id,request_key,root_session_id,owner_instance,conversation_id,alias,question,choices_json,state,inbound_id,control_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'SENDING',NULL,?,?,?)").run(input.checkpointId, input.requestKey, input.root, input.owner, route.conversationId, input.alias, input.question, JSON.stringify(input.choices), input.revision, new Date().toISOString(), new Date().toISOString()); return true } catch { return false }
	}
	activateCheckpoint(checkpointId: string): boolean { return this.db.query("UPDATE checkpoints SET state='OPEN',updated_at=? WHERE checkpoint_id=? AND state='SENDING'").run(new Date().toISOString(), checkpointId).changes === 1 }
	failCheckpoint(checkpointId: string): void { this.db.query("UPDATE checkpoints SET state='UNKNOWN',updated_at=? WHERE checkpoint_id=? AND state IN ('SENDING','OPEN','ANSWERING')").run(new Date().toISOString(), checkpointId) }
	openCheckpointFor(binding: Binding): { checkpointId: string } | undefined { return this.db.query("SELECT checkpoint_id AS checkpointId FROM checkpoints WHERE root_session_id=? AND owner_instance=? AND alias=? AND state='OPEN'").get(binding.rootSessionId, binding.ownerInstance, binding.alias) as { checkpointId: string } | undefined }
	claimCheckpoint(checkpointId: string, inboundId: string, binding: Binding): boolean { return this.db.query("UPDATE checkpoints SET state='ANSWERING',inbound_id=?,updated_at=? WHERE checkpoint_id=? AND state='OPEN' AND root_session_id=? AND owner_instance=? AND alias=?").run(inboundId, new Date().toISOString(), checkpointId, binding.rootSessionId, binding.ownerInstance, binding.alias).changes === 1 }
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
		const registration = this.bindingForRoot(root), route = this.route(), control = this.control(); if (!control.enabled || !registration || !route.conversationId || !route.contextToken || registration.ownerInstance !== owner) return
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
		this.db.transaction(() => { try { this.db.query("INSERT INTO control_outbound VALUES(?,?,?,'completion','SENDING',?,?,?,?)").run(outboundId, dedupeKey, root, payload, binding.conversationId, binding.contextToken, new Date().toISOString()); this.db.query("UPDATE session_activity SET claimed_run=run_id,running=0,updated_at=? WHERE root_session_id=? AND run_id=? AND claimed_run<?").run(new Date().toISOString(), root, row.runId, row.runId); claimed = true } catch {} })()
		return claimed ? { outboundId, binding, payload } : undefined
	}
	claimControlOutbound(input: { dedupeKey: string; root: string; kind: string; payload: string; revision?: number }): { outboundId: string; binding: RoutedBinding } | undefined {
		const registration = this.bindingForRoot(input.root), route = this.route(), control = this.control(); if (!registration || !route.conversationId || !route.contextToken || !control.enabled || (input.revision !== undefined && input.revision !== control.revision)) return
		const binding: RoutedBinding = { ...registration, conversationId: route.conversationId, contextToken: route.contextToken }
		const outboundId = crypto.randomUUID()
		try { this.db.query("INSERT INTO control_outbound VALUES(?,?,?,?,'SENDING',?,?,?,?)").run(outboundId, input.dedupeKey, input.root, input.kind, input.payload, binding.conversationId, binding.contextToken, new Date().toISOString()); return { outboundId, binding } } catch { return }
	}
	claimSystemOutbound(message: WeixinInbound, kind: string, payload: string): { outboundId: string } | undefined { const outboundId = crypto.randomUUID(); try { this.db.query("INSERT INTO control_outbound VALUES(?,?,?,?,'SENDING',?,?,?,?)").run(outboundId, `inbound:${message.id}:${kind}`, `inbound:${message.id}`, kind, payload, message.fromUserId, message.contextToken, new Date().toISOString()); return { outboundId } } catch { return } }
	controlOutboundState(dedupeKey: string): string | undefined { return (this.db.query("SELECT state FROM control_outbound WHERE dedupe_key=?").get(dedupeKey) as { state: string } | null)?.state }
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
		const control = this.control(); if (!registration || registration.ownerInstance !== instanceId || !route.conversationId || !route.contextToken || !control.enabled || control.revision !== revision) return
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
