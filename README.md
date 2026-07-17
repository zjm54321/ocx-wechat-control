# WeChat Control v3：受限微信接管

这是用户级 OpenCode 插件加一个持久 broker worker。它提供的是**受限接管**，不是透明代理，也不能透明转发 OpenCode 原生 `question`。插件进程不 poll 微信；唯一 worker 持有排他锁和唯一 `weixin-mcp@1.7.7` stdio poller。不要同时启用 `mcp.weixin`。

## 安装

运行时要求 Bun 1.3.14 或更高版本。在 OpenCode 的用户配置目录安装公开包：

```sh
npm install @mingzzz/ocx-wechat-control@0.1.2
```

然后在 OpenCode 配置中启用插件，并关闭任何并行的 WeChat MCP 配置：

```json
{
  "plugin": [
    ["@mingzzz/ocx-wechat-control@0.1.2", { "enabled": true }]
  ],
  "mcp": {
    "weixin": { "enabled": false }
  }
}
```

默认 adapter command 从本包声明的 `weixin-mcp@1.7.7` 依赖解析，不依赖发布者机器路径。安装者也可以显式传入 `weixinCommand: ["node", "<absolute-path-to-weixin-mcp-dist-cli.js>"]`。插件不会调用安装器、`npx`、登录或 QR 命令；首次登录必须由安装者在受信环境中手动完成。不要把账号文件、token、QR 数据或状态数据库提交到项目。

OpenCode 必须加载包的默认导出；发布包入口为 `dist/index.js`，持久 worker 为 `dist/worker.js`。修改配置后重启 OpenCode。

## 明确限制

- `/leave` 开启全局接管，`/back` 关闭。命令由 config hook 动态注册；已有同名命令时插件明确报冲突。命令参数被拒绝，模板 sentinel 会被清空并通过 handled error 截断，绝不送入模型。
- 只绑定 OpenCode **root session**。工具和 permission 最多沿 parent 链解析 32 层；循环、过深或不可达都拒绝。completion 事件不解析 parent，按原始 session ID 串行上报，因此 child 事件不能触发 root 通知。
- 原生 `question` 不会转发。接管开启且 root 可路由时，system hook 会诚实要求模型改用一次 `wechat_request_input` 并结束回合。
- `wechat_request_input` 是异步检查点：只负责向已绑定会话发送问题；不等待答案。微信答案之后作为一个新的 user turn 注入 exact root，不是同一 tool call 的返回值。请求使用 tool `callID`（SDK 无 callID 时使用 message+参数的稳定保守键）去重；同一调用重放只返回既有状态。root 重绑定 owner 后仍先验证当前 owner，再返回旧请求状态，绝不重新发送。
- 每个 root 同时最多一个 active checkpoint。`UNKNOWN` 仍保持 active 和逻辑去重，直到 `/back` 或重绑定显式取消。`/back` 取消 `SENDING/OPEN/ANSWERING/UNKNOWN`，之后不接纳新微信输入、不建 checkpoint、不发 completion 或 permission 通知；它不取消 OpenCode 中已经运行的任务，也无法撤回已经进入 adapter 的 send。
- 只允许绑定关系决定 recipient；没有任意收件人发送接口。所有外发都要求该 conversation 已由入站消息提供 `context_token`。同 conversation 的所有 binding 会一起更新 token。
- permission 首先用短 RPC 确认 enabled+routable；不能确认时明确覆盖为 `ask`，确认后立即设为 `deny`，绝不 `allow`。通知先 durable claim，broker 立即返回并在后台发送，不等待慢或挂起的 adapter。
- completion 只使用 terminal assistant 和 idle/busy 元数据，不读 history、reasoning、tool output 或 assistant 内容。`session_activity` 持久化 control epoch、run 和 origin；assistant 不改变 idle，重复 busy 不清候选，idle/assistant 任一后到都可触发原子 claim。idle 的 direct/checkpoint run 会按 exact run 原子消费并写审计，不发送 generic completion，从而允许下一次 busy 建立 LOCAL run。固定发送 `#alias\n任务已完成。`，错误使用固定错误文案。summary、pending/outbound assistant 和 active checkpoint 不产生 generic completion。
- `event` 只处理 terminal、non-summary `message.updated` 以及 `session.status`/`session.idle`，向 worker 发送 ID、状态和布尔错误标记，不发送消息内容。实现禁止 `session.messages`。

## 路由和状态

入站格式是 `help`，或首字符开始的 `#NN`、换行、正文。`help` 永远可用。普通路由仅在 takeover enabled 时注入；关闭时返回固定拒绝。授权要求 alias、owner instance 和 conversation 全部精确匹配。broker 在 route 前按 conversation+context token+payload 指纹抑制 echo；记录带 TTL，TTL 内每次完全匹配都抑制而不是消费一次，过期后清理。

同步微信注入仍只调用一次 `client.session.prompt(...)`，等待其直接 assistant 返回并发送一次关联回复。checkpoint envelope 会标识异步答案，但仍注入原来的 exact root/owner/conversation。callback 成功后 checkpoint 立即成为 `ANSWERED`；随后 direct reply 的 `SENT/UNKNOWN` 只记录在 outbound，不反向污染 checkpoint。callback/注入不确定才把 checkpoint 记为 `UNKNOWN`。callback 返回后 direct outbound claim 会再次检查 enabled+revision，因 `/back` 失效且尚未进入 adapter 的回复不会发送。

## SQLite 与 at-most-once

产品仍称 WeChat Control v3；数据库内部结构版本为 `PRAGMA user_version=4`。任何已有 v2 或旧 v3 数据库在结构补丁前先用 `VACUUM INTO` 创建一致的 `pre-v4` 备份，再原子发布；迁移事务失败则拒绝启动，重复打开 v4 不重复备份。状态结构包括：

- 全局 `control_state(enabled, revision)`
- `checkpoints`
- `session_activity`
- durable `control_outbound`
- exact `outbound_echoes`

bindings 保留 alias 主键和 root 唯一约束，但不再把 conversation 设为唯一，以允许同一 conversation 绑定多个 root。启动恢复把遗留 `SENDING`（以及正在注入答案的 checkpoint）改成 `UNKNOWN`。request-input、permission、completion、help 和固定系统回复都使用 durable dedupe/claim；`SENDING` 后失败或崩溃可能已经送达，因此记为 `UNKNOWN` 且绝不重试。这不是 exactly-once。

上游 poll 没有稳定 message ID；本地 key 由 cursor、数组下标、sender、context 和原文计算。固定 poller、持久 cursor 和本地 inbound 表共同去重，但仍只能提供 at-least-once 入站与 UNKNOWN-safe 出站。

## 安全边界和运行

broker 只监听 `127.0.0.1`，RPC 使用用户私密 shared secret、instance token、heartbeat、root ownership 和 loopback callback 检查。锁接管仅在旧 PID 明确死亡且 authenticated challenge 失败时发生；PID alive/unknown 一律拒绝接管。运行时不使用 `npx`、不安装、不登录、不显示 QR、不解析人类 CLI 输出。

`weixin_send` 的 MCP envelope 必须是单一 text JSON。固定版本自己的 CLI 认可 numeric `ret: 0`、numeric `errcode: 0` 或缺失 status；本插件只把缺失 status 的精确空对象 `{}`视为成功。字符串 `"0"`、非零状态、`isError`、错误字段、畸形或未知对象全部失败并进入 `UNKNOWN`。

## 开发与验证

```sh
bun install
bun run check
npm pack --dry-run
```

测试只使用内存/临时 SQLite、fake MCP/client 和 `MockWeChatAdapter`；不会启动真实 worker、登录、poll 或发送微信。npm tarball 不包含源码测试、`node_modules`、状态库、WAL/SHM、日志、登录或 QR/cache 数据。

## 维护者发布

npm package settings 中的 Trusted Publisher 必须精确绑定 GitHub owner `zjm54321`、repository `ocx-wechat-control`、workflow filename `publish.yml`，Environment 留空。发布 `0.1.2` 时先把发布提交 push 到 `main`，再创建并 push `v0.1.2` tag；workflow 会校验 tag/version、tag commit 属于 `origin/main`，且 registry 中尚无同版本后才通过 OIDC 发布。
