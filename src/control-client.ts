import { createOpencodeClient, type OpencodeClient } from "@opencode-ai/sdk/v2/client"

export type ControlResult = { data: unknown; error: unknown; status: number }

export interface V2ControlClient {
	sessionGet(input: { sessionID: string; directory: string }): Promise<ControlResult>
	sessionPromptAsync(input: { sessionID: string; directory: string; messageID: string; parts: Array<{ type: "text"; text: string }> }, signal?: AbortSignal): Promise<ControlResult>
	sessionStatus(input: { directory: string }): Promise<ControlResult>
	questionList(input: { directory: string }): Promise<ControlResult>
	questionReply(input: { requestID: string; directory: string; answers: string[][] }, signal?: AbortSignal): Promise<ControlResult>
	permissionList(input: { directory: string }): Promise<ControlResult>
	permissionReply(input: { requestID: string; directory: string; reply: "once" | "reject" }, signal?: AbortSignal): Promise<ControlResult>
}

function result(value: { data?: unknown; error?: unknown; response: Response }): ControlResult {
	return { data: value.data, error: value.error, status: value.response.status }
}

export function createV2ControlClient(serverUrl: URL | string, directory: string): V2ControlClient {
	const client = createOpencodeClient({ baseUrl: String(serverUrl), directory })
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

const promptContract: PromptAsyncArgs = { sessionID: "session", directory: "directory", messageID: "message", parts: [{ type: "text", text: "text" }] }
const questionContract: QuestionReplyArgs = { requestID: "request", directory: "directory", answers: [["answer"]] }
const permissionContract: PermissionReplyArgs & { reply: RemotePermissionReply } = { requestID: "request", directory: "directory", reply: "once" }
void [promptContract, questionContract, permissionContract]
