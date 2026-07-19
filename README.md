# WeChat Control 0.2.5：受限微信接管

这是 OpenCode 用户级插件和持久 broker worker，面向**一个微信账号、一个固定聊天窗口**。首次由微信纯文本 `id` 认定固定 controller；之后只有同一 sender 可以使用或刷新 route。0.2 系列采用原生异步 admission、原生 Question/Permission 转发和显式回复，不自动镜像 assistant 文本。0.2.5 会在 OpenCode 实例注销以及 worker 启动/维护时完整停用失去 owner 的会话，并继续使用活动会话的紧凑动态编号，同时保留注册顺序和历史外发内容不变。

## 安装

要求 Bun 1.3.14 或更高版本：

```sh
npm install @mingzzz/ocx-wechat-control@0.2.5
```

```json
{
  "plugin": [["@mingzzz/ocx-wechat-control@0.2.5", { "enabled": true }]],
  "mcp": { "weixin": { "enabled": false } }
}
```

唯一 worker 持有排他锁和唯一 `weixin-mcp@1.7.7` poller，因此不要同时启用 `mcp.weixin`。首次微信登录须在受信环境手动完成；插件不会运行安装器、`npx`、登录或 QR 命令。

## 固定 controller 与命令

- 在 OpenCode **root session** 执行 `/leave`，登记稳定的注册顺序；当前活动会话显示为紧凑编号 `#1`、`#2`……。停用或关闭所属 OpenCode 实例后，会话及其待处理状态会完整停用，编号自动补齐；重新登记原 root 会回到其注册顺序位置，历史已发送内容不会被改写。worker 只按 owner 记录是否存在清理孤儿绑定，不使用 heartbeat 年龄或网络探测，因此不会因睡眠/唤醒误删仍登记的实例。
- 在微信发送纯文本小写 `id`。首次成功的 sender 成为固定 controller，并收到当前 `#N  标题` 列表；其他 sender 不会收到回复或修改 route。每次路由都以当前 `id` 映射为准，旧消息中的编号不代表当前映射。
- 在固定聊天发送 `#N`、换行、正文。broker 立即通过原生 `promptAsync` 将每条消息 admission 到对应 root；同一 root 的 admission 按顺序执行，但不会等待 assistant 完成，连续消息可立即进入原生队列。
- `/back` 全局关闭接管并清理活动状态，但保留编号、标题和固定 route。再次 `/leave` 可恢复。
- `help` 和 `id` 在接管开关任一状态都可用。`wechat_control_status` 报告登记、alias、route、adapter 和 takeover 状态。

## Question 与 Permission

接管开启且 route ready 时，OpenCode 原生 Question/Permission 会自动转发到微信，并带稳定的 `QXXXXXX` 或 `PXXXXXX` 编号。多个待处理请求并存时必须在答案前填写编号。

- 单个 Question：必须在一条消息中回复 `#N\nQCODE 1`，或无多个待处理请求时回复 `#N\n1`；也可使用精确 label，允许自定义时用 `=自定义内容`。
- 多选 Question：用逗号分隔，例如 `1,3`。
- 多问题：每行使用 `问题序号: 答案`，例如 `1: 2`。
- Permission：仅接受精确的 `once` 或 `reject`；不支持 `always`，也不会直接替用户 deny。
- 显式编号示例：`#2\nQABC234 2` 或 `#1\nPABC234 reject`。只发送 code、把 code 和答案拆成两条消息，或使用其他会话的 code 都不会消耗请求，并会收到重试提示。

解析失败会保持请求可回答；结果不确定时标记 UNKNOWN，禁止自动重放。

## 回复与 typing

普通本地 TUI 回合仍只在模型显式调用 `wechat_reply` 时发送回复。每个微信入站回合都会收到 per-turn system directive，要求模型作答后调用 `wechat_reply({text})`；assistant 文本仍不会由 broker 事件自动镜像到微信。该调用按 tool call ID 持久去重：相同 ID 和相同文本返回既有 SENT/UNKNOWN；相同 ID 配不同文本会冲突；UNKNOWN 不重试发送。

当已登记 root 存在 queued/busy/retry 工作时，固定聊天显示“正在输入”；全局无工作、关闭接管、启动恢复或 worker 退出时发送 OFF。typing 使用当前 recipient/context，并对 route 变化、失败重试和 shutdown 做竞态保护。

## 持久状态与安全

SQLite schema v7 保存 registrations、全局 route、admission、原生请求、runtime、typing 和 UNKNOWN-safe outbound 状态，并单独保存 `wechat_reply` 的逻辑文本身份。启动会把崩溃遗留的 runtime 工作恢复为安全 idle。broker 仅监听 `127.0.0.1`，RPC 使用 shared secret、instance token、heartbeat、root ownership 和 exact-root callback 校验。

上游没有稳定消息 ID，因此入站为 at-least-once，外发为 UNKNOWN-safe/at-most-once，而不是 exactly-once。route metadata、文本长度和 callback payload 均有边界校验；outbound echo 在入站路由前按 recipient、context 和精确 payload 带 TTL 抑制。

## 开发与发布检查

```sh
bun install
bun run check
npm pack --dry-run --json
git diff --check
```

测试使用临时 SQLite、真实本地 callback HTTP 边界、fake SDK client/MCP 和 mock adapter，不登录、poll 或发送真实微信。npm 包只包含 `dist/index.js`、`dist/worker.js`、README、LICENSE 和 package metadata；不包含源码测试、状态库、日志、账号、QR/cache 或项目编排文件。

维护者发布 `v0.2.5` 时应先完成全部检查并确认 tag 与 package version 一致，再由仓库 Trusted Publisher/OIDC workflow 发布。本项目不会从开发命令自动发布。
