# WeChat Control 0.1.3：受限微信接管

这是 OpenCode 用户级插件和一个持久 broker worker。它面向**一个微信账号、一个固定聊天窗口**：用户不需要输入、配置或理解 `conversationId`，也没有配对码或联系人选择。首次仅由微信纯文本 `id` 自动认定固定聊天；之后只有同一 sender 能刷新底层 context token。

## 安装

要求 Bun 1.3.14 或更高版本：

```sh
npm install @mingzzz/ocx-wechat-control@0.1.3
```

```json
{
  "plugin": [["@mingzzz/ocx-wechat-control@0.1.3", { "enabled": true }]],
  "mcp": { "weixin": { "enabled": false } }
}
```

唯一 worker 持有排他锁和唯一 `weixin-mcp@1.7.7` poller；不要同时启用 `mcp.weixin`。首次微信登录仍须安装者在受信环境手动完成。插件不会运行安装器、`npx`、登录或 QR 命令。

## 用户操作

- 在任一 OpenCode **root session** 执行 `/leave`。插件自动永久编号为 `#1`、`#2`、`#3`……并全局开启接管。同一 root 再次执行会复用原编号，并把标题刷新为本次 `/leave` 时的最新标题。child session 会被拒绝。
- 在微信发送纯文本 `id`（trim 后必须精确为小写 `id`，不能带参数或多行），会在首次使用时自动认定这个固定聊天，并按编号列出 `#N  标题`。并发的首次 `id` 只会有一个 sender 成功。此命令在接管开启或关闭时都可用；未知标题显示“未命名会话”，不会暴露内部 session ID。回复最多 4000 字符，过长时说明未显示数量。
- 在已认定的固定微信聊天发送 `#N`、换行、正文，把正文送到对应 root。所有编号共用固定 recipient；该 sender 的合法入站会自动刷新 context。其他 sender 的 `id`、`help` 或 `#N` 均不回复、不注入且不能修改 route。`help`/`#N` 不能完成首次认定。
- 执行 `/back` 会全局关闭接管并取消现有接管检查点/活动状态，但保留全部编号、标题和全局 route。再次 `/leave` 仍复用原编号。
- `help` 在接管开关任一状态均可用。接管关闭时普通 `#N` 路由返回固定拒绝。

不存在手工绑定工具；`wechat_bind_session` 已移除。`wechat_control_status` 会报告当前 root 是否登记、alias、全局 route 是否 ready，以及 takeover on/off，不要求 `conversationId`。

## 受限接管边界

原生 `question` 不会透明转发。接管开启且 root 可路由时，模型应只调用一次 `wechat_request_input` 并结束当前回合；答案之后作为新的 user turn 注入 exact root。每个 root 同时最多一个 active checkpoint，UNKNOWN 状态禁止重放。permission 只有在 broker 确认 enabled、已登记且 route ready 时才 deny，并使用固定通知。

同步微信注入只调用一次 `client.session.prompt(...)`，等待其直接 assistant 并发送一次关联回复。callback、发送和崩溃不确定性继续按 UNKNOWN-safe/at-most-once 处理；不会自动重放。echo 在路由前按 recipient、context 和 exact payload 带 TTL 抑制。明显非法消息和已识别的 outbound echo 不会刷新全局 route。

completion 只使用 terminal assistant 与 idle/busy 元数据，不读取 history、reasoning 或消息正文。direct/checkpoint run 不产生额外 generic completion；LOCAL run 固定发送 `#alias\n任务已完成。`（或固定失败文案）。

## 持久状态和安全

SQLite schema 为 v5。root registrations 使用永久 `AUTOINCREMENT` alias，保存 root、directory、owner、标题和时间；全局 route 单独保存底层 recipient/context。v4 及更旧数据库在迁移前通过 `VACUUM INTO` 创建 WAL-consistent `pre-v5` 备份；旧 alias/root/directory/owner 保留，标题为 null。旧数据只有一个 distinct conversation 时仅迁移 recipient，context 等待下一条合法入站刷新。v5 重开不重复备份，迁移失败仍报告可打开的备份。

broker 只监听 `127.0.0.1`，RPC 使用 shared secret、instance token、heartbeat、root ownership 和 exact-root callback health。单 worker 锁仅在旧 PID 明确死亡且 authenticated health 失败时接管。上游没有稳定消息 ID，因此入站仍为 at-least-once，出站 UNKNOWN-safe 而非 exactly-once。

底层 route metadata 在任何 durable 入站或首次认定之前校验：sender ID 必须为 1–500 字符，context token 必须为 1–4000 字符。空值或超长值不回复、不落入 inbound 表，也不会阻止之后的合法 `id` 认定。

## 开发验证

```sh
bun install
bun run check
npm pack --dry-run --json
git diff --check
```

测试使用临时 SQLite、fake client/MCP 和 mock adapter，不登录、poll 或发送真实微信。发布包不包含源码测试、状态库、WAL/SHM、日志、账号或 QR/cache 数据。

## 维护者发布

Trusted Publisher 必须绑定 GitHub owner `zjm54321`、repository `ocx-wechat-control`、workflow `publish.yml`。发布 `v0.1.3` 时，先将发布提交 push 到 `main`，再创建并 push `v0.1.3` tag；workflow 校验 tag/version 与 registry 后通过 OIDC 发布。
