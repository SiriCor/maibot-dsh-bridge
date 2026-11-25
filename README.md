# 麦麦×dsh 桥（maibot-dsh-bridge）

把 [DeepSeek Harness (dsh)](https://deepseek.com/harness/) 接入麦麦：麦麦用一句话下单，dsh 在服务器上把活干完——执行期间静默，完成后直接发送最终结果与交付文件，并记录到麦麦上下文，还能**定时自排班**。

## 功能

- **命令** `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh 在后台执行，只在提交失败时显示错误；成功结果稍后由通知通道返回。
- **工具** `dsh_run`：麦麦（Planner）在聊天中判断「这活该外包」时自主调用。
- **工具** `schedule_task` / `schedule_list` / `schedule_update` / `schedule_cancel`：让麦麦建 / 看 / 改 / 删**定时任务**。
- **交付物**：dsh 用 `present` 声明的文件，桥发送回当前聊天；图片直接发送，其他文件通过 NapCat 上传。

## 桌面模式：下单与通知解耦

MaiBot 的 `dsh_run` 或 `/dsh` 只负责提交任务。宿主将任务落盘后立即返回内部任务编号（HTTP 202），不再保持连接等待最终答案。可继续提交任务，由 DSH 顺序执行。执行超时仍限制单个任务的资源占用，与提交连接的 10 秒超时独立。

DSH 完成后把最终文字、`present` 文件和原聊天路由存入独立通知队列。MaiBot 插件每 2 秒领取一次，先发送成果文件，再通过自身 replyer 模型，使用当前人设和回复风格组织一条自然回复。DSH 的结果只作为事实依据，不会原样作为守岸人的发言；不唤醒 Planner 再规划或调用工具。文件也可以作为唯一成果。回执生成失败会保留通知并记录错误，禁止退回 DSH 原文发送；已生成回执落盘，重试不再次生成或重复已发送部分。

通知成功发送后才确认领取。MaiBot 离线、重启或提交连接断开不会取消 DSH 任务；未领取结果保留在宿主 `state/tasks/`。发送进度保存到插件数据目录的 `desktop_delivery_progress.json`，确认领取失败可续传。发送成功与本地进度写入之间若恰好崩溃，仍可能重复最后一段；不宣称跨 QQ 和本地文件实现了严格一次投递。宿主重启时，排队任务继续执行，执行中任务标记中断，避免自动重复有副作用的操作。

宿主源码在 `desktop_host/`，需将 `index.js`、`async_tasks.js`、`workspace_rules.js` 与 `AGENT.md` 一同部署到 DSH 的桥接目录，加载 `index.js`，保留原有 workspaceRoot、senderId、tokenFile、stateDir、模型配置；设置 `reasoningEffort: low`。`endpoint.json` 的协议版本为 2。升级运行中的宿主时使用独立版本的模块文件名，避免旧模块缓存；若改变辅助模块或规则资源文件名，也要同步更新入口的相对路径。MaiBot 插件升级后需重载，不能继续使用旧 `/run` 客户端。

宿主会在会话工作目录维护 `AGENT.md`，并将同一份固定规则同步到 DSH 原生读取的 `AGENTS.md` 管理区。管理区以 `MAIBOT_BRIDGE_RULES_START/END` 为界，更新时保留用户原有内容；损坏的边界直接报错，不覆盖。固定工作目录在宿主加载时准备规则，分流目录在创建会话前准备规则。每次实际提交只包含 `【MaiBot桥接任务】 delivery=...` 和用户任务，不再重复整段身份、输出和交付提示词。普通成果文本仍由 MaiBot 按人设组织，规则文件不代替这一步。

所有接口只监听 `127.0.0.1`，沿用本机 token：

- `POST /.maibot-desktop-bridge/tasks`：任务落盘后返回 `{status: "accepted", jobId}`。
- `GET /.maibot-desktop-bridge/notifications`：领取一个最终通知，领取不会删除；无结果时返回 `notification: null`。
- `POST /.maibot-desktop-bridge/notifications/<jobId>/ack`：确认已发送，删除通知；重复确认安全。

`dsh_run.delivery` 明确区分收件关系：`maibot` 返回普通任务结果；`to_assistant` 表示用户让泰缇斯对守岸人说话，回执以“泰缇斯回复我说”标明来源；`qq` 用于用户明确说“让泰提斯／泰缇斯／Tethys 系统发给我、把截图或文件发给我、回复我”，不需要用户知道或说出 notify_qq 工具名。DSH 必须用 notify_qq 发送内容与文件，随后独立通知守岸人发送状态；守岸人不再重复投递成果。只 present、未成功交给 QQ 通道发送的任务不能标成直发完成。普通桥接和回复守岸人的任务会在该任务执行期间限制 `notify_qq`、`send_to_qq`，不会影响其他 DSH 会话。

QQ 直发需要真实 QQ 对话绑定，不能把 MaiBot 会话 ID 当成绑定。宿主读取 `notify_qq` 的真实执行结果，区分已发送与已排队，MaiBot 只生成简短确认，不复述 DSH 的执行汇报和已发送正文，也不重复发送已交给 QQ 的同一文件。仅有模型声称发送成功不能当作成功凭证。

## CLI 模式：两轮协议，时长由 dsh 自己定

派活方（麦麦）给的秒数只作**参考建议**，真正决定「做什么、花多久」的是 dsh：

1. **规划轮**：桥先把任务交给 dsh，让它只做规划——解析意图、给出操作计划，并自报一个「最大秒数」（要求输出 JSON：`{"plan": "...", "max_seconds": N}`）。发起方的建议值会写进这一轮提示，**仅供它参考**。
2. **执行轮**：桥拿到 dsh 自报的时长，用它作为超时发起真正的执行。
3. 自报时长会被夹在 `[dsh.timeout_min, dsh.timeout_max]` 之间；规划轮本身用较短超时（最多 60s），失败则回退发起方建议值。

这样派活方定「愿望」、干活方定「现实」，既避免拍一个过紧的死超时，也逼 dsh「先想清楚怎么做」。

## 结果回传（只回答结果）

默认将最终回执在本机渲染为 Markdown 图片卡片，支持标题、加粗、列表、引用、表格、链接和代码块；长内容自动分页。附件仍单独发送，聊天上下文保留原始文字。图片发送或渲染失败时保留通知并记录错误，不会误标已发送。

需要复制正文时，将 `[dsh].receipt_format` 设为 `"text"`；默认值为 `"card"`。旧 `display_mode` 不控制新回执。修改插件配置后需重载插件。Windows 使用微软雅黑，Linux 需安装 Noto Sans CJK 字体；渲染不访问远程图片或执行 HTML。

此设置控制 MaiBot 桥接回执。`delivery=qq` 时 DSH 经 `notify_qq` 直接发送的正文由独立 QQ 通道控制，本插件只将发送状态确认为卡片，不重复投递正文。

- CLI 与桌面模式均静默执行，不发送接单确认、过程转发、完成通知或重复的结果文本文件。旧 `display_mode` 设置不再控制回传形态。
- 最终正文默认以 Markdown 图片卡片发送，可选纯文本；要求只回答任务结果，遵循用户指定的语言、格式和必要细节，不添加寒暄、人设台词、无关解释和结尾邀约。失败时报告实际原因。
- 当前聊天的文件交付由 MaiBot 的独立通知循环负责。DSH 生成文件后调用 `present` 并结束，不再查找 QQ 发送接口；单纯截图无需读取图片内容。宿主任务超时等已知失败保留具体原因，任务编号和 DSH 会话 ID 只用于内部排查。
- 图片直接发送；其他交付文件经 NapCat 上传。CLI 的 `present` 文件仍受工作区路径检查与数量限制。
- 工具提交结果明确标记为后台执行，并要求麦麦静默等待；结果摘要回灌时标记已发送，避免再次转述或追加闲聊。这些模型回复约束仍需实际对话验证。

> 非图片文件上传依赖 NapCat 文件接口；其他适配器不支持此上传方式。

## 定时任务（桥内置调度）

桥自带一个轻量调度器（轮询 10s），任务持久化在插件 data 目录的 `scheduled_tasks.json`，重启自动载入。

- **麦麦侧**：`schedule_task` 建任务，四种时间规则任选其一——`after_seconds`（N 秒后一次）/ `every_seconds`（每 N 秒重复）/ `at`（ISO 时刻）/ `cron`（5 字段，如 `0 9 * * 1-5`）。
- **dsh 侧自建**：dsh 在它的工作目录写 `schedule-requests/<名>.json`（`{"title","prompt",时间规则}`），桥在任务收尾时扫描并建任务，随后移入 `.done/`（幂等）。
  - 该约定通过 dsh 的全局 `AGENTS.md`（`$DSH_HOME/AGENTS.md`）告知模型；`schedule-requests` 目录**不计入交付物**。
- 到点后，桥把任务的 prompt 交给 dsh 执行，**走同一条链**回群；触发时若正忙会顺延 60s，避免与手动任务打架。

## 依赖

- 服务器安装 **Node.js 22+** 与 **dsh**（`npm i -g @deepseek-ai/dsh`）。
- 需要 **`DEEPSEEK_API_KEY`**（dsh 不产算力）。
- 若要让麦麦产出的插件自动上线，需配合 [插件安装审批器](https://github.com/ji-or-ji/maibot-plugin-installer)。

## 安装

把本仓库目录放进麦麦的 `plugins/` 下（目录名建议 `maibot-dsh-bridge`），重载麦麦。

## 配置流程（三步）

1. **装引擎**：服务器装 Node 22+ 与 dsh。
2. **给密钥**：设置环境变量 `DEEPSEEK_API_KEY`（**推荐**，插件自动读取）；也可填进 `config.toml` 的 `[dsh].api_key`（不推荐，勿提交）。
3. **启用**：把 `config.toml.example` 复制为 `config.toml`，将 `[plugin].enabled` 设为 `true`，重载麦麦。

可选：`[permission].mode` 选择 `whitelist`（默认，仅名单内可用）或 `blacklist`（名单内不可用、其余放行）。白名单模式在 `allowed_users` / `allowed_groups` 填 ID，**留空表示拒绝所有人**（fail-closed）；黑名单模式在 `blocked_users` / `blocked_groups` 填要拦的 ID。群聊里若取不到发言人 ID，用群 ID 兜底。名单同时约束 `/dsh` 命令、`dsh_run` 工具与 `schedule_task/update/cancel` 三个工具。把 `[permission].enabled` 设为 `false` 等同于**放开给所有人**（不推荐）。`dsh_run` 由麦麦的 Planner 决定调用。

> ⚠️ **隐私提示**：被外包的任务文本（以及 dsh 执行时读到的上下文）会经 **dsh 发往 DeepSeek 的推理服务**。群内内容敏感时，请启用前知晓。

## 配套 dsh 插件推荐

dsh 的能力按「plugin bundle」叠进 profile（改 `$DSH_HOME/profiles/<名>/cordis.patch.yml`，用 `- insert:` 包一层；`$DSH_HOME/profiles/<名>/package.json` 的 `dsh.profile.bundles` 也可挂 bundle 型插件）。以下是本桥实测推荐的几项，**标注了是否需要额外适配**：

| 插件 | 作用 | 是否需适配 |
|---|---|---|
| `@deepseek-ai/dsh-tool-present` | 声明交付文件，让产出能交出来 | 装上即用 |
| `@deepseek-ai/dsh-workspace-changes` | 记录每轮工作区文件变更 | 装上即用 |
| `@deepseek-ai/dsh-skill-office` | Word / PPT / Excel 读写 + 结构检查 | **需**：指定 Python 解释器（3.9+）并装 `python-docx` `python-pptx` `openpyxl` |
| `@deepseek-ai/dsh-mcp-client` | 连外部 MCP 服务器、注册其工具 | **需**：为每台服务器填一条配置（`serverName` + `transport` + `url`/`headers`） |

> 注：官方 `@deepseek-ai/dsh-experimental-schedule-bundle` 的定时能力**要求 Web Session controller，headless profile 不适用**——本桥用内置调度器替代（见「定时任务」）。

**office 的 Python 从哪里指**：office skill 会遵循「用户或 `AGENTS.md` 明确指定的环境」。所以在 `$DSH_HOME/AGENTS.md` 里写明用哪个 Python 即可，**无需官方随包 payload**：

```markdown
## Office 文档工作
用 Python：`<你的 Python 3.9+ 绝对路径>`（已装 python-docx / python-pptx / openpyxl）。
```

## 主要配置项

| 项 | 说明 |
|---|---|
| `[dsh].command` | dsh 可执行命令，如 `dsh` 或 `C:\nodejs\dsh.cmd` |
| `[dsh].timeout_default` | 发起方未给秒数时的默认**建议值**（秒） |
| `[dsh].timeout_min/max` | dsh 自报时长的**夹取范围**（秒），默认 60~1800 |
| `[dsh].heartbeat_seconds` | 无进展心跳：三信号（输出/进程CPU/子进程）全静超此秒数判卡死 |
| `[dsh].context_inject_chars` | 回灌进麦麦上下文的结果摘要上限（默认 600，越小越省上下文） |
| `[dsh].deliver_max_files` | 单次任务最多打包发送的交付物数量（默认 8） |
| `[dsh].allow_self_schedule` | 是否允许 dsh 经 schedule-requests 自建定时任务（默认开；关则禁止） |
| `[dsh].display_mode` | 旧配置保留，当前统一只发送结果 |
| `[dsh].permission_mode` | dsh 沙箱模式：`danger-full-access`（默认，git/npm 可用）/ `workspace-write` / `read-only` |
| `[permission].mode` | `whitelist`（默认，仅名单内可用）或 `blacklist`（名单内不可用，其余放行） |
| `[permission].allowed_users` | 白名单用户 ID，留空且白名单模式=拒绝所有人 |
| `[permission].allowed_groups` | 白名单群 ID，群聊里取不到发言人 ID 时兜底 |
| `[permission].blocked_users` | 黑名单用户 ID（仅 `blacklist` 模式生效） |
| `[permission].blocked_groups` | 黑名单群 ID（仅 `blacklist` 模式生效） |
| `[permission].enforce_tools` | 是否对 `dsh_run` / `schedule_*` 工具也施加名单（默认开；关则工具面放开，风险自负） |

## 安全边界

- 每个聊天流一个独立工作区（`ctx.paths.data_dir/workspace/<stream>/`），群间不串。
- dsh 自报时长被夹在上限内 + **无进展心跳** + **分层终止**（先温和，宽限后 `taskkill /T` 杀整棵进程树）。
- 任务白名单前缀（`[dsh].allowed_prefixes`，留空不限制）。
- API Key 优先走环境变量，避免明文入库。
- 若给 dsh 挂了高权限 MCP（如可执行命令的服务器），等于放宽了边界，请自行评估。

### 关于子进程、文件与网络（供审查参考）

- **子进程**：本插件以子进程方式调用 `dsh`（`subprocess`），这是它的核心用途——把任务交给 dsh 引擎执行。命令取自配置项 `[dsh].command`（默认 `dsh`），**不使用 `shell=True`、不拼接用户输入成命令**。
  - **沙箱模式**：桥默认给 dsh 注入 `DSH_PERMISSION_MODE=danger-full-access`。原因：Windows 上 dsh 的 `workspace-write` 使用 ACL 受限令牌，`git clone` 会因 `git-remote-https` 无法创建命名管道而失败（`cannot create standard input pipe ... Permission denied`）。任务已由 `[permission]` 名单限制在授权用户/群内，工作目录也按聊天流隔离；若需要更严边界，可把 `[dsh].permission_mode` 改为 `workspace-write`，代价是 git / npm 等命令不可用。
  - **注意**：`_spawn` 在 Windows 下经 `cmd.exe /c` 启动，`[dsh].command` 与 `[dsh].extra_args` 实质会被 shell 解析——**这两项属管理员可信配置，不得由聊天内容产生**。
  - **平台**：Windows 与类 Unix 均已支持。Windows 经 `cmd.exe /c` 启动、以 `taskkill /T` 终止整棵进程树；类 Unix 经 `/bin/sh -c` 启动、子进程独立进程组，终止时用 `killpg` 整体干掉。
  - 说明：最初只在 Windows 上验证过（作者手边没有可跑 WSL / Docker / Linux 的环境），所以起步阶段**没打算适配 Linux**；后来调研发现有不少用户跑在 Linux 上，于是补上了跨平台支持。**非 Windows 环境请自行评估适配性。**
- **权限**：`/dsh`、`dsh_run`、`schedule_*` 均受 `[permission]` 约束。`mode = "whitelist"`（默认）只放行 `allowed_users` / `allowed_groups`；`mode = "blacklist"` 拦截 `blocked_users` / `blocked_groups`，其余放行。用户 ID 与群 ID 会先归一化为纯数字再比较，兼容 `qq:` 前缀。白名单留空 = 拒绝所有人。若不需要对工具收紧，可将 `[permission].enforce_tools` 设为 `false`（工具面放开，风险自负）。
- **dsh 自建任务**：`schedule-requests` 通道可用 `[dsh].allow_self_schedule` 关掉（默认开）。
- **文件写入**：仅写插件数据目录 `ctx.paths.data_dir`；每个聊天流的工作区名做**白名单字符清洗**（仅 `A-Za-z0-9_-`），杜绝 `..` 越位；`schedule-requests/` 内部请求消费后移入 `.done/`。
- **交付物**：来自 dsh 通过 `present` 声明的文件，逐条校验为「**本流工作区内存在的普通文件**」（resolve 后做 containment）后才发送；`schedule-requests` 等内部路径排除。
- **网络**：本插件自身**不主动发起外部请求**，不上传聊天记录 / 用户 ID / 群 ID / 图片 / 配置 / token。dsh 是否联网取决于用户自己的 dsh 配置与任务内容。
- **结果文件 / 交付物上传**：依赖 NapCat 的 `adapter.napcat.file.*`，属 **best-effort**——非 NapCat 适配器回退为转发卡片，该接口不属插件承诺的稳定能力。
- **配置**：仓库不提交 `config.toml`（只给 `config.toml.example` + `.gitignore`）。

### dsh 自建定时任务（设计说明，供审查参考）

- dsh 可在其工作区写 `schedule-requests/*.json`，桥在任务收尾时扫描并转为定时任务（移入 `.done/`，幂等）。这是**有意提供的能力**——让 dsh 能把「稍后再做」的事排进调度。
- 该通道以「用户消息触发 dsh 任务」为前提；需要更紧时，可在 `[permission]` 收紧可用用户。

## 许可

MIT
