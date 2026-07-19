import { OpencodeClient } from "@opencode-ai/sdk/v2/client"

export type ControlResult = { data: unknown; error: unknown; status: number }

export interface V2ControlClient {
	sessionGet(input: { sessionID: string; directory: string }): Promise<ControlResult>
	sessionPromptAsync(input: { sessionID: string; directory: string; messageID: string; system?: string; parts: Array<{ type: "text"; text: string }> }, signal?: AbortSignal): Promise<ControlResult>
	sessionStatus(input: { directory: string }): Promise<ControlResult>
	questionList(input: { directory: string }): Promise<ControlResult>
	questionReply(input: { requestID: string; directory: string; answers: string[][] }, signal?: AbortSignal): Promise<ControlResult>
	permissionList(input: { directory: string }): Promise<ControlResult>
	permissionReply(input: { requestID: string; directory: string; reply: "once" | "reject" }, signal?: AbortSignal): Promise<ControlResult>
}

function result(value: { data?: unknown; error?: unknown; response: Response }): ControlResult {
	return { data: value.data, error: value.error, status: value.response.status }
}

type CompatibleTransport = Record<"buildUrl" | "get" | "getConfig" | "post" | "request" | "setConfig", Function> & { interceptors: object }

// OpenCode's PluginInput client is the v1 generated facade, while the callback
// endpoints use the v2 facade. Both facades accept the same Hey API transport.
// Keep the one private-field compatibility dependency and its startup guard here.
function pluginTransport(pluginClient: unknown): CompatibleTransport {
	const value = pluginClient !== null && typeof pluginClient === "object" ? pluginClient as Record<string, unknown> : undefined
	const transport = value?._client !== null && typeof value?._client === "object" ? value._client as Record<string, unknown> : undefined
	const methods = ["buildUrl", "get", "getConfig", "post", "request", "setConfig"] as const
	if (!transport || !methods.every((name) => typeof transport[name] === "function") || transport.interceptors === null || typeof transport.interceptors !== "object") {
		throw new Error("Incompatible OpenCode plugin client: authenticated transport is unavailable")
	}
	return transport as CompatibleTransport
}

export function createV2ControlClient(pluginClient: unknown): V2ControlClient {
	const client = new OpencodeClient({ client: pluginTransport(pluginClient) as never })
	return {
		async sessionGet(input) { return result(await client.session.get(input)) },
		async sessionPromptAsync(input, signal) { return result(await client.session.promptAsync(input, { signal })) },
		async sessionStatus(input) { return result(await client.session.status(input)) },
		async questionList(input) { return result(await client.question.list(input)) },
		async questionReply(input, signal) { return result(await client.question.reply(input, { signal })) },
		async permissionList(input) { return result(await client.permission.list(input)) },
		async permissionReply(input, signal) { return result(await client.permission.reply(input, { signal })) },
	}
}

// These assignments intentionally fail compilation if the pinned v2 surface drifts.
type PromptAsyncArgs = Parameters<OpencodeClient["session"]["promptAsync"]>[0]
type QuestionReplyArgs = Parameters<OpencodeClient["question"]["reply"]>[0]
type PermissionReplyArgs = Parameters<OpencodeClient["permission"]["reply"]>[0]
type PermissionReply = NonNullable<PermissionReplyArgs["reply"]>
type RemotePermissionReply = Extract<PermissionReply, "once" | "reject">

const promptContract: PromptAsyncArgs = { sessionID: "session", directory: "directory", messageID: "message", system: "system", parts: [{ type: "text", text: "text" }] }
const questionContract: QuestionReplyArgs = { requestID: "request", directory: "directory", answers: [["answer"]] }
const permissionContract: PermissionReplyArgs & { reply: RemotePermissionReply } = { requestID: "request", directory: "directory", reply: "once" }
void [promptContract, questionContract, permissionContract]
