import { lstat, readdir, readFile, realpath } from "node:fs/promises"
import * as path from "node:path"
import { getConfig, sendTyping } from "weixin-mcp/dist/api.js"
import { ACCOUNTS_DIR } from "weixin-mcp/dist/paths.js"
import type { Store } from "./core"

type TypingApi = { getConfig(userId: string, token: string, baseUrl: string, contextToken?: string): Promise<unknown>; sendTyping(userId: string, ticket: string, status: 1 | 2, token: string, baseUrl: string): Promise<unknown> }
type Account = { token: string; baseUrl: string; accountId: string }
export interface TypingCoordinatorOptions { api?: TypingApi; accountId?: string; loadAccount?: () => Promise<Account>; debounceMs?: number; retryMs?: number }
interface AccountConfigOptions { accountsDir?: string }

// Account files contain only a few credentials; reject unexpectedly large input before parsing.
const MAX_ACCOUNT_FILE_BYTES = 64 * 1024

function object(value: unknown): Record<string, unknown> | undefined { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined }
function bounded(value: unknown, max: number): value is string { return typeof value === "string" && value.length > 0 && value.length <= max }

async function accountConfig(accountId = process.env.WEIXIN_ACCOUNT_ID, options: AccountConfigOptions = {}): Promise<{ token: string; baseUrl: string; accountId: string }> {
	if (accountId !== undefined && !bounded(accountId, 200)) throw new Error("invalid Weixin account ID")
	const accountsDir = await realpath(options.accountsDir ?? ACCOUNTS_DIR)
	const files = (await readdir(accountsDir)).filter((file) => file.endsWith(".json"))
	const candidates = files.filter((file) => !file.endsWith(".sync.json") && !file.endsWith(".cursor.json"))
	const selected = accountId ? candidates.find((file) => path.basename(file, ".json") === accountId) : candidates[0]
	if (!selected) throw new Error("Weixin account unavailable")
	const selectedPath = path.join(accountsDir, selected), selectedStat = await lstat(selectedPath)
	if (selectedStat.isSymbolicLink() || !selectedStat.isFile()) throw new Error("invalid Weixin account file")
	if (selectedStat.size > MAX_ACCOUNT_FILE_BYTES) throw new Error("Weixin account file too large")
	const canonicalSelectedPath = await realpath(selectedPath), relative = path.relative(accountsDir, canonicalSelectedPath)
	if (relative === "" || relative.startsWith(`..${path.sep}`) || relative === ".." || path.isAbsolute(relative)) throw new Error("invalid Weixin account path")
	const parsed = object(JSON.parse(await readFile(canonicalSelectedPath, "utf8")))
	if (!parsed || !bounded(parsed.token, 2000)) throw new Error("Weixin account token unavailable")
	const baseUrl = typeof parsed.baseUrl === "string" ? parsed.baseUrl : typeof parsed.base_url === "string" ? parsed.base_url : "https://ilinkai.weixin.qq.com"
	let url: URL
	try { url = new URL(baseUrl) } catch { throw new Error("invalid Weixin base URL") }
	if (url.protocol !== "https:" || baseUrl.length > 500) throw new Error("invalid Weixin base URL")
	return { token: parsed.token, baseUrl, accountId: path.basename(selected, ".json") }
}

export class TypingCoordinator {
	private readonly api: TypingApi
	private readonly debounceMs: number
	private readonly retryMs: number
	private readonly loadAccount: () => Promise<Account>
	private timer?: ReturnType<typeof setTimeout>
	private retryTimer?: ReturnType<typeof setTimeout>
	private stopped = false
	private shutdownPromise?: Promise<void>
	private target = false
	private epoch = 0
	private observedRoute?: string | null
	private operation: Promise<void> = Promise.resolve()
	constructor(private readonly store: Store, options: TypingCoordinatorOptions = {}) { this.api = options.api ?? { getConfig, sendTyping }; this.debounceMs = options.debounceMs ?? 50; this.retryMs = options.retryMs ?? 1000; this.loadAccount = options.loadAccount ?? (() => accountConfig(options.accountId)) }
	refresh(now = Date.now()): void { if (this.stopped) return; const next = this.store.desiredTyping(now), route = this.routeKey(); if (next !== this.target || route !== this.observedRoute) this.epoch++; this.target = next; this.observedRoute = route; this.schedule() }
	async startup(): Promise<void> { this.store.setTypingActual(null, null); await this.enqueue(false, true, ++this.epoch); this.refresh() }
	shutdown(): Promise<void> { if (this.shutdownPromise) return this.shutdownPromise; this.stopped = true; this.target = false; this.epoch++; if (this.timer) clearTimeout(this.timer); if (this.retryTimer) clearTimeout(this.retryTimer); this.timer = this.retryTimer = undefined; this.shutdownPromise = this.enqueue(false, true, this.epoch); return this.shutdownPromise }
	reassert(): void { this.refresh(); this.epoch++; this.schedule(true) }
	async flush(): Promise<void> { if (this.stopped) return this.shutdownPromise; this.target = this.store.desiredTyping(); await this.enqueue(this.target, false, ++this.epoch) }
	private routeKey(): string | null { const route = this.store.route(); return route.conversationId && route.contextToken ? `${route.conversationId}:${route.contextToken}` : null }
	private schedule(immediate = false): void { if (this.timer) clearTimeout(this.timer); const epoch = this.epoch; this.timer = setTimeout(() => { this.timer = undefined; void this.enqueue(this.target, false, epoch) }, immediate ? 0 : this.debounceMs) }
	private enqueue(desired: boolean, force: boolean, epoch: number): Promise<void> { const next = this.operation.then(() => this.apply(desired, force, epoch)); this.operation = next.catch(() => {}); return next }
	private async apply(desired: boolean, force = false, epoch = this.epoch): Promise<void> {
		if (this.stopped && desired) return
		const state = this.store.typingState(); if (!force && !desired && state.actual === false) return
		try {
			const route = this.store.route(); if (!route.conversationId || !route.contextToken) throw new Error("route unavailable")
			const routeKey = `${route.conversationId}:${route.contextToken}`
			const account = await this.loadAccount(), config = object(await this.api.getConfig(route.conversationId, account.token, account.baseUrl, route.contextToken)), ticket = config?.typing_ticket
			if (!bounded(ticket, 4000)) throw new Error("typing ticket unavailable")
			await this.api.sendTyping(route.conversationId, ticket, desired ? 1 : 2, account.token, account.baseUrl)
			if (epoch === this.epoch && desired === this.target && routeKey === this.routeKey()) this.store.setTypingActual(desired, routeKey, Date.now())
			else if (!this.stopped) { this.target = this.store.desiredTyping(); this.schedule(true) }
			if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = undefined }
		} catch {
			if (!this.stopped) { if (this.retryTimer) clearTimeout(this.retryTimer); this.retryTimer = setTimeout(() => { this.retryTimer = undefined; this.refresh() }, this.retryMs) }
		}
	}
}

export { accountConfig }
