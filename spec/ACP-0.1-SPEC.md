# ACP 0.1 — Agent-Component-Protocol Specification

- 版本:`0.1`
- 状态:DRAFT / STABLE-CANDIDATE
- 日期:2025-08
- 组织:xi-era

**ACP(Agent-Component-Protocol)是面向远程节点、硬件设备、分布式业务元件的标准化 Agent 开放调用协议。**
它与 MCP(Model Context Protocol)互补:MCP 强于本地 stdio 子进程调用,ACP 主打远程 HTTP / WebSocket / IoT 硬件 / 跨机器分布式元件调用。二者双向桥接、互不替代。

> 命名澄清:本协议为 Agent-**Component**-Protocol,与 Zed Editor 的 Agent-**Client**-Protocol(同缩写 ACP)是两个完全不同的协议。

---

## 目录

1. [设计原则](#1-设计原则)
2. [术语](#2-术语)
3. [报文信封](#3-报文信封)
4. [操作(op)](#4-操作op)
5. [响应信封](#5-响应信封)
6. [流式分片](#6-流式分片)
7. [元件模型与元数据](#7-元件模型与元数据)
8. [错误码体系](#8-错误码体系)
9. [HTTP 传输映射](#9-http-传输映射)
10. [WebSocket 传输映射](#10-websocket-传输映射)
11. [Stdio 传输映射](#11-stdio-传输映射)
12. [版本协商](#12-版本协商)
13. [安全预留](#13-安全预留)
14. [协议示例集](#14-协议示例集)
- [附录 A:与 MCP 的映射](#附录-a与-mcp-的映射)
- [附录 B:与 OpenAI Tool-Call 的映射](#附录-b与-openai-tool-call-的映射)
- [附录 C:实现一致性级别(MUST/SHOULD/MAY 汇总)](#附录-c实现一致性级别)

本规范中的关键词 **MUST / MUST NOT / SHOULD / MAY** 按 RFC 2119 语义解释。

---

## 1. 设计原则

1. **极简纯净 JSON,拒绝 JSON-RPC**。ACP 不使用 JSON-RPC 2.0 信封(`jsonrpc`/`method`/`params`/`result`/`error` 分离),而使用**单信封双语义**:一条报文既是请求也是响应,靠字段组合区分。所有顶层字段名 ≤ 9 个字符,为嵌入式设备节省每一个字节。
2. **传输无关**。信封本身不携带任何传输信息;HTTP / WebSocket / Stdio 只定义"报文如何搬运",不改变报文结构。同一报文在三种传输之间零改写。
3. **低算力友好**。任何能做字符串拼接与 JSON parse/stringify 的设备即可实现 ACP;不要求二进制协议、不要求压缩、不要求长连接(短连接 HTTP 即可完整实现)。
4. **远程优先**。HTTP 与 WebSocket 是一等公民传输;Stdio 仅为兼容 MCP 桥接与本地调试的可选传输。
5. **schema 语言统一**。元件输入输出 schema 一律使用 **JSON Schema draft-07**——与 MCP `inputSchema`、OpenAI Tool-Call `parameters` 同款,使桥接转换接近零成本。
6. **预留而非实现**。v0.1 在报文中为鉴权、权限域、调用溯源预留字段,但实现方 MUST 忽略其语义;企业级能力(网关、SSO、审计、限流)留给 v1.0 商业层。

---

## 2. 术语

| 术语 | 含义 |
|---|---|
| **Agent(客户端)** | ACP 报文的发起方:AI Agent、编码工具、编排器、CLI 调试器等 |
| **Server(服务端)** | 承载元件、接收并处理 ACP 报文的进程或设备 |
| **Component(元件)** | 被封装为可调用单元的远程能力:API、硬件、传感器、业务模块、微服务等 |
| **Component ID** | 元件全局标识,见 [§7.1](#71-component_id-命名规范) |
| **Component Descriptor(元件描述符)** | 元件的自描述元数据,discover 操作的返回单元,见 [§7.2](#72-元件描述符component-descriptor) |
| **Envelope(信封)** | 一条完整 ACP JSON 报文,见 [§3](#3-报文信封) |
| **Chunk(分片帧)** | 流式返回中的单条报文,见 [§6](#6-流式分片) |

---

## 3. 报文信封

ACP 报文是单个 JSON 对象,编码为 **UTF-8**。`acp` 字段标识协议与版本。

### 3.1 请求信封(Request Envelope)

```json
{
  "acp": "0.1",
  "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A",
  "op": "call",
  "component": "sensor.temperature",
  "input": { "unit": "C" },
  "stream": false,
  "meta": {
    "auth": "bearer <token>",
    "scopes": ["sensor.read"],
    "traceId": "tr-9f8e7d",
    "timeoutMs": 30000
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `acp` | string | 是 | 协议标识 + 版本,合二为一。格式 `主.次`,当前 `"0.1"`。接收方以此识别 ACP 流量并做版本协商(§12) |
| `id` | string | 是 | 请求关联 ID,**客户端生成**。同一次调用的所有响应帧与分片帧 MUST 回显同一 `id`。建议 ULID / UUID;同一连接内在途请求 MUST 唯一(WebSocket 多路复用依赖它) |
| `op` | string | 是 | 操作路由,枚举:`"discover"` \| `"call"`(见 §4) |
| `component` | string | `op="call"` 时必填;`op="discover"` 时可选 | 目标元件 ID,见 §7.1 |
| `input` | any | `op="call"` 时可选,缺省视为 `null` | 调用输入,MUST 满足元件描述符的 `inputSchema` |
| `stream` | boolean | 否,缺省 `false` | 客户端是否要求流式分片返回(见 §6) |
| `meta` | object | 否 | **安全预留区**(见 §13)。v0.1 服务端 MUST 忽略其全部字段且不校验;未知键同样忽略(向前兼容) |

**保留键**:`$` 前缀是协议保留命名空间,标准 op 永不使用 `$` 前缀(见 §10)。

### 3.2 信封校验规则

接收方按顺序校验,任一失败即返回对应错误码(见 §8),**MUST NOT** 处理后续语义:

1. 报文是合法 JSON → 否则 `40000 PARSE_ERROR`;
2. 顶层 `acp`、`id`、`op` 存在且类型正确 → 否则 `40001 INVALID_ENVELOPE`;
3. `acp` 版本可支持 → 否则 `40003 UNSUPPORTED_VERSION`;
4. `op` ∈ {`discover`, `call`} → 否则 `40002 UNKNOWN_OP`;
5. `op="call"` 时 `component` 存在且合法 → 否则 `40004 INVALID_COMPONENT_ID` / `40001 INVALID_ENVELOPE`。

---

## 4. 操作(op)

v0.1 只有两个操作。**这是刻意的极简**:列表、单个查询、标签过滤全部由 `discover` 的参数变体覆盖,不引入 `list` / `get` / `info` 等多余 op。

### 4.1 `discover` — 发现元件

| 变体 | 请求形状 | 语义 |
|---|---|---|
| 全量列出 | `{"op":"discover"}`(无 `component`) | 返回服务端全部元件 |
| 单个查询 | `{"op":"discover","component":"sensor.temperature"}` | 查询单个元件;不存在时 `components` 为空数组,**不是错误** |
| 标签过滤(可选实现) | `{"op":"discover","tags":["iot","sensor"]}` | 返回同时含全部指定标签的元件;`tags` 为字符串数组,交集语义 |

`discover` 的响应 `result` 形状固定:

```json
{
  "server": { "name": "edge-node-01", "version": "0.1.0", "protocol": "0.1" },
  "components": [
    { "id": "sensor.temperature", "...": "ComponentDescriptor" }
  ]
}
```

- `server`:服务端自描述。`name`、`version`(服务端自身语义化版本)、`protocol`(服务端实现的 ACP 版本)。
- `components`:描述符数组,**即使按 id 查询也返回数组**(0 或 1 个元素),使客户端处理逻辑单一。

### 4.2 `call` — 调用元件

```json
{ "acp": "0.1", "id": "…", "op": "call", "component": "sensor.temperature", "input": { "unit": "C" } }
```

- 服务端 MUST 先按描述符 `inputSchema` 校验 `input`,失败返回 `42200 INVALID_INPUT`;
- 元件不存在返回 `40400 COMPONENT_NOT_FOUND`;
- 元件已注册但停用/离线返回 `40401 COMPONENT_UNAVAILABLE`;
- 配合 `stream: true` 走流式分片(§6);
- **流能力不匹配的处理**:
  - 非流式元件收到 `stream:true`:服务端仍按分片协议发,单帧 `{"seq":0,"end":true}` 包裹全部结果(统一处理,无特例);
  - 必需流式的元件(`descriptor.stream === true` 且服务端实现不支持降级)收到 `stream:false`:返回 `40005 STREAM_REQUIRED`。

---

## 5. 响应信封

响应信封与请求信封同为单对象,靠 `ok` 字段区分成败。

### 5.1 成功(一次性)

```json
{ "acp": "0.1", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "ok": true, "result": { "celsius": 23.5 } }
```

- `result` 是**裸输出值**——即元件 `outputSchema` 所描述的值本身,不再包一层信封字段。这是与 JSON-RPC 最直观的差异点。

### 5.2 失败

```json
{
  "acp": "0.1",
  "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A",
  "ok": false,
  "error": { "code": 42200, "message": "input validation failed", "data": { "errors": ["..."] } }
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `ok` | boolean | 是 | `false` 表示失败 |
| `error.code` | number | 是 | 5 位数字错误码,见 §8 |
| `error.message` | string | 是 | 人读错误信息 |
| `error.data` | any | 否 | 结构化详情(校验错误列表、上游状态码等) |

### 5.3 响应字段规则

- 响应 MUST 回显请求的 `acp` 与 `id`;
- `ok:true` 时 MUST 有 `result` 字段(值可为 `null`),MUST NOT 有 `error`;
- `ok:false` 时 MUST 有 `error`,MUST NOT 有 `result` / `chunk`;
- 服务端 MAY 在响应中附带额外顶层字段(如诊断信息),客户端 MUST 忽略未知顶层字段。

---

## 6. 流式分片

当请求携带 `stream: true`,服务端不回单条 `result`,而是发 **N+1 条分片帧**(N ≥ 0)。每条分片帧仍是完整信封,靠 `chunk` 字段区分:

```json
{ "acp": "0.1", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "chunk": { "seq": 0, "end": false, "data": { "line": "boot ok" } } }
{ "acp": "0.1", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "chunk": { "seq": 1, "end": false, "data": { "line": "reading..." } } }
{ "acp": "0.1", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "chunk": { "seq": 2, "end": true,  "data": { "line": "done" } } }
```

### 6.1 分片规则

- `chunk.seq`:number,从 **0** 开始严格递增。v0.1 不定义重传机制(依赖传输层自身可靠性);
- `chunk.end`:boolean。`end:true` 的帧是**终止帧,MUST 存在**,可携带最后一段 `data`(可为 `null`);
- 终止帧之后 MUST NOT 再有同 `id` 的任何帧;
- `chunk.data`:any,本分片负载;
- `chunk.bin`:可选 boolean。`bin:true` 时 `data` MUST 是 **base64 字符串**(UTF-8 之外的字节流唯一标准表达)。v0.1 不定义二进制传输帧,base64 是唯一标准路径——牺牲约 33% 体积,换取"任何 MCU 上的 JSON 解析器都能跑";
- 客户端 MUST 按 `seq` 顺序处理(传输层保证有序;若实现发现乱序,SHOULD 报 `51000 STREAM_ABORTED`);
- 中途出错:服务端直接发一条 `ok:false` 错误信封(同 `id`),**等效终止流**;客户端收到后 MUST 停止等待后续分片;
- 客户端取消:客户端 MAY 关闭连接或(在支持的传输上)发送取消信号;服务端 SHOULD 优雅停止生成并丢弃剩余分片。

### 6.2 帧大小与解析

- 推荐(非强制)单帧序列化后 ≤ **64 KB**;
- 接收方 MUST 至少接受 **1 MB** 的单帧;
- 流式 over HTTP 使用 NDJSON(§9.2),逐行解析天然免粘包。

---

## 7. 元件模型与元数据

### 7.1 component_id 命名规范

```
component_id = segment ( "." segment ){1,3}
segment      = [a-z] [a-z0-9-]{0,62}

正则: ^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$
示例: sensor.temperature、weather.current、biz.order.refund
```

- 全小写、`.` 分层、2–4 段;
- **禁止**冒号、斜杠、大写、空格、中文;
- 违反命名规范的调用 MUST 回 `40004 INVALID_COMPONENT_ID`;
- **MCP 桥接映射**:`component_id` 的 `.` ↔ `_` 互换是无损可逆映射(`sensor.temperature` ⇄ `sensor_temperature`),因 MCP 工具名仅允许 `^[a-zA-Z0-9_-]{1,64}$`。桥接器 MUST 采用此映射。

### 7.2 元件描述符(Component Descriptor)

discover 返回的最小自描述单元:

```json
{
  "id": "sensor.temperature",
  "name": "Temperature Sensor",
  "description": "Reads current temperature from a virtual sensor",
  "version": "1.0.0",
  "inputSchema": { "type": "object", "properties": { "unit": { "enum": ["C", "F"] } }, "required": [] },
  "outputSchema": { "type": "object", "properties": { "celsius": { "type": "number" } }, "required": ["celsius"] },
  "stream": false,
  "tags": ["iot", "sensor"],
  "meta": {}
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | string | 是 | component_id,见 §7.1 |
| `name` | string | 是 | 人读名称 |
| `description` | string | 是 | 用途描述(LLM 选择元件的主要依据,建议写清楚) |
| `version` | string | 是 | 元件自身语义化版本,**独立于协议版本** |
| `inputSchema` | object | 否 | JSON Schema **draft-07**;缺省等价 `true`(任意输入) |
| `outputSchema` | object | 否 | JSON Schema draft-07;缺省等价 `true` |
| `stream` | boolean | 是 | 是否具备流式输出能力 |
| `tags` | string[] | 否 | 标签,供 discover 过滤 |
| `meta` | object | 否 | 自由扩展点 |

`endpoint` 字段为**预留**(注册中心 / 网关场景 v0.2+ 使用);v0.1 元件描述符由承载它的服务端自述,无需端点。

---

## 8. 错误码体系

5 位数字,**前两位对齐 HTTP 语义类**(便于 HTTP 映射:`floor(code/1000)`,51000 段例外映射 500)。

### 8.1 标准错误码表

| code | 名称 | 含义 | HTTP |
|---|---|---|---|
| 40000 | `PARSE_ERROR` | 报文不是合法 JSON | 400 |
| 40001 | `INVALID_ENVELOPE` | 缺少 `acp` / `id` / `op` 等必填字段,或类型错误 | 400 |
| 40002 | `UNKNOWN_OP` | `op` 不在枚举内 | 400 |
| 40003 | `UNSUPPORTED_VERSION` | 版本不支持;`error.data` 携带 `{"supported":["0.1"]}` | 400 |
| 40004 | `INVALID_COMPONENT_ID` | component_id 违反命名规范(§7.1) | 400 |
| 40005 | `STREAM_REQUIRED` | 元件必需流式但请求未带 `stream:true` | 400 |
| 40100 | `UNAUTHORIZED` | **预留**(v0.1 不实现鉴权) | 401 |
| 40101 | `FORBIDDEN` | **预留** | 403 |
| 40400 | `COMPONENT_NOT_FOUND` | 元件不存在 | 404 |
| 40401 | `COMPONENT_UNAVAILABLE` | 已注册但停用 / 离线 | 404 |
| 40500 | `METHOD_NOT_ALLOWED` | HTTP 动词误用(如 GET 调用端点) | 405 |
| 41500 | `UNSUPPORTED_MEDIA_TYPE` | Content-Type 不符 | 415 |
| 42200 | `INVALID_INPUT` | input 未通过 inputSchema 校验;`error.data` 建议携带结构化校验错误 | 422 |
| 42201 | `INVALID_OUTPUT` | 输出未通过 outputSchema 校验(开发期自检用) | 422 |
| 42900 | `RATE_LIMITED` | **预留**(v1.0 网关实现) | 429 |
| 42901 | `CONCURRENCY_LIMIT` | 并发超限 | 429 |
| 50000 | `INTERNAL_ERROR` | 服务端未分类错误 | 500 |
| 50001 | `COMPONENT_ERROR` | 元件 handler 抛出异常 | 500 |
| 50002 | `UPSTREAM_ERROR` | 元件包装的后端 / 设备返回失败;`error.data` 建议携带上游信息 | 502 |
| 50300 | `SHUTTING_DOWN` | 服务关闭中 | 503 |
| 50400 | `TIMEOUT` | 调用超时 | 504 |
| 51000 | `STREAM_ABORTED` | 流被中断(客户端断开 / 取消 / 乱序) | 500 |
| 51001 | `CHUNK_OVERFLOW` | 分片帧超过大小上限 | 500 |

### 8.2 扩展规则

- `42200` 之后至 `43xxx` 起、`51000` 之前为未来语义保留段,**不允许私有占用**;
- 私有/实验扩展码只允许 **59000–59999**;
- 客户端 MUST 将未知错误码按其千位段语义处理(如 `42xxx` 当 4xx 客户端错误,`50xxx`/`51xxx` 当 5xx 服务端错误)。

---

## 9. HTTP 传输映射

### 9.1 端点

| 端点 | 动词 | 级别 | 说明 |
|---|---|---|---|
| `/acp` | POST | **MUST** | 唯一必实现端点。请求体 = 请求信封,响应体 = 响应/分片信封。单路由对嵌入式(固件只挂一个 handler)最友好 |
| `/acp/discover` | GET | SHOULD | 浏览器 / curl 便捷入口,返回与 `discover` op 相同的 `result` |
| `/acp/health` | GET | MAY | 探活,返回 `{"ok":true}`;非协议层 |

- 请求 `Content-Type`: `application/json`;
- 服务器对 `POST /acp` 之外的动词应回 `40500 METHOD_NOT_ALLOWED`(HTTP 状态 405);
- Content-Type 不符回 `41500 UNSUPPORTED_MEDIA_TYPE`(HTTP 415);
- MAY 约定请求头 `ACP-Version: 0.1`;但服务端 **MUST 以报文内 `acp` 字段为准**(报文自洽,不依赖头)。

### 9.2 流式:NDJSON

`stream:true` 的调用在 HTTP 上以 **NDJSON**(`Content-Type: application/x-ndjson`,chunked transfer)返回:每个分片信封序列化为一行 JSON,以 `\n` 分隔,末行为 `end:true` 终止帧。

**决策理由**:NDJSON 与 WebSocket 文本帧一一对应——同一组分片信封在两种传输间零改写。SSE 仅作可选附加(客户端 `Accept: text/event-stream` 时以 `data: <同样的 JSON>\n\n` 输出;级别 MAY,v0.1 官方 SDK 不实现)。

### 9.3 状态码映射

- 成功:HTTP 200,响应体为 ACP 信封;
- 失败:HTTP 状态按 §8.1 表映射(`51000`/`51001` 段映射 500),**响应体仍是完整 ACP 错误信封**(语义双通道:HTTP 状态给基础设施,信封给客户端);
- 报文级解析失败(无法取得 `id`)时,响应信封 `id` 可为 `null`。

---

## 10. WebSocket 传输映射

### 10.1 连接

- 端点与 HTTP 同端口同进程:`ws://host:port/acp`(路径 `/acp`,Upgrade 升级);
- **一个文本帧 = 一个完整信封**(UTF-8 JSON),无分隔符问题;
- **二进制帧 v0.1 保留不定义**(v0.2 报文压缩预留);收到二进制帧 MUST 回 `40001 INVALID_ENVELOPE`。

### 10.2 生命周期与协商

- 连接后客户端直接发业务信封,**无握手包**——版本协商随每条报文的 `acp` 字段逐条进行,不匹配回 `40003`。这是刻意的无状态设计:嵌入式设备断线重连零成本;
- 保活使用 WebSocket 协议层原生 ping/pong;协议不定义应用层心跳(v0.2 再议)。

### 10.3 并发与推送

- 同一连接可同时在途多个请求,靠 `id` 关联;服务端**乱序返回合法**(先完成的先回);
- 服务端主动推送:v0.1 **没有**独立推送——所有服务端帧(含分片)都关联某个客户端 `id`;
- `"$event"` 等以 `$` 前缀 op 保留的命名空间留给 v0.2 的服务端事件 / 心跳,标准实现收到应回 `40002 UNKNOWN_OP`。

---

## 11. Stdio 传输映射

Stdio 是**可选兼容传输**,主要用于 MCP 桥接与本地调试:

- 进程 `stdin` / `stdout` 按行读写:一行 = 一个完整信封(NDJSON 子集);
- 进程 `stderr` 保留给日志,不属于协议;
- 其余语义(信封、流式、错误码)与通用规则完全一致;
- Stdio 下并发多路复用可选实现;`meta.timeoutMs` 由服务端自行解释。

---

## 12. 版本协商

- `acp` 字段格式为 `主.次`(如 `"0.1"`);
- 兼容规则:**主版本必须相等;次版本服务端 ≥ 客户端即可**(服务端 0.2 可以服务 0.1 客户端);
- 不匹配 → `40003 UNSUPPORTED_VERSION`,`error.data.supported` 列出服务端支持的全部版本;
- 报文中出现未知顶层字段、未知 `meta` 键、未知 `chunk` 键,实现方 MUST 忽略(向前兼容),不视为版本问题。

---

## 13. 安全预留

v0.1 **只预留字段、不实现语义**。请求信封 `meta` 对象的预留键:

| 键 | 类型 | 预留语义(v0.1 不实现) |
|---|---|---|
| `auth` | string | 鉴权凭证(如 `bearer <token>`) |
| `scopes` | string[] | 权限域声明 |
| `traceId` | string | 调用溯源 / 分布式追踪 ID |
| `timeoutMs` | number | 客户端期望超时 |

- 服务端 v0.1 MUST 忽略 `meta` 全部内容(包括预留键与未知键),不做校验、不做鉴权;
- `meta` 结构设计为开放对象:企业网关(v1.0)可在不破坏 v0.1 兼容性的前提下注入审计、限流、SSO 等字段;
- HTTP 传输层 MAY 另行使用标准 HTTP 头(如 `Authorization`)承载凭证,这不属于协议层。

---

## 14. 协议示例集

### 14.1 全量发现

请求:
```json
{ "acp": "0.1", "id": "req-001", "op": "discover" }
```

响应:
```json
{
  "acp": "0.1",
  "id": "req-001",
  "ok": true,
  "result": {
    "server": { "name": "edge-node-01", "version": "0.1.0", "protocol": "0.1" },
    "components": [
      {
        "id": "sensor.temperature",
        "name": "Temperature Sensor",
        "description": "Reads current temperature from a virtual sensor",
        "version": "1.0.0",
        "inputSchema": { "type": "object", "properties": { "unit": { "enum": ["C", "F"] } }, "required": [] },
        "outputSchema": { "type": "object", "properties": { "celsius": { "type": "number" } }, "required": ["celsius"] },
        "stream": false,
        "tags": ["iot", "sensor"],
        "meta": {}
      },
      {
        "id": "log.tail",
        "name": "Log Tail",
        "description": "Streams the last n log entries",
        "version": "1.2.0",
        "inputSchema": { "type": "object", "properties": { "n": { "type": "integer", "minimum": 1, "maximum": 1000 } }, "required": ["n"] },
        "stream": true,
        "tags": ["log"],
        "meta": {}
      }
    ]
  }
}
```

### 14.2 单次调用(成功)

请求:
```json
{ "acp": "0.1", "id": "req-002", "op": "call", "component": "sensor.temperature", "input": { "unit": "C" } }
```

响应:
```json
{ "acp": "0.1", "id": "req-002", "ok": true, "result": { "celsius": 23.5 } }
```

### 14.3 流式调用

请求:
```json
{ "acp": "0.1", "id": "req-003", "op": "call", "component": "log.tail", "input": { "n": 3 }, "stream": true }
```

响应(NDJSON 三行 / WS 三帧):
```json
{ "acp": "0.1", "id": "req-003", "chunk": { "seq": 0, "end": false, "data": { "line": "entry-0" } } }
{ "acp": "0.1", "id": "req-003", "chunk": { "seq": 1, "end": false, "data": { "line": "entry-1" } } }
{ "acp": "0.1", "id": "req-003", "chunk": { "seq": 2, "end": true,  "data": { "line": "entry-2" } } }
```

### 14.4 二进制分片

```json
{ "acp": "0.1", "id": "req-004", "chunk": { "seq": 0, "end": true, "bin": true, "data": "iVBORw0KGgoAAAANSUhEUg==" } }
```

### 14.5 流式中途出错

```json
{ "acp": "0.1", "id": "req-005", "ok": false, "error": { "code": 50002, "message": "device disconnected", "data": { "device": "uart-1" } } }
```

### 14.6 错误示例(每种典型错误码一例)

```json
{ "acp": "0.1", "id": null, "ok": false, "error": { "code": 40000, "message": "request body is not valid JSON" } }
{ "acp": "0.1", "id": "req-006", "ok": false, "error": { "code": 40001, "message": "missing required field: id" } }
{ "acp": "0.1", "id": "req-007", "ok": false, "error": { "code": 40002, "message": "unknown op: exec" } }
{ "acp": "0.1", "id": "req-008", "ok": false, "error": { "code": 40003, "message": "unsupported protocol version", "data": { "supported": ["0.1"] } } }
{ "acp": "0.1", "id": "req-009", "ok": false, "error": { "code": 40004, "message": "invalid component id: Sensor/Temp" } }
{ "acp": "0.1", "id": "req-010", "ok": false, "error": { "code": 40005, "message": "component requires stream:true" } }
{ "acp": "0.1", "id": "req-011", "ok": false, "error": { "code": 40400, "message": "component not found: sensor.humidity" } }
{ "acp": "0.1", "id": "req-012", "ok": false, "error": { "code": 40401, "message": "component unavailable: sensor.temperature (disabled)" } }
{ "acp": "0.1", "id": "req-013", "ok": false, "error": { "code": 41500, "message": "unsupported content-type: text/plain" } }
{ "acp": "0.1", "id": "req-014", "ok": false, "error": { "code": 42200, "message": "input validation failed", "data": { "errors": ["input/n must be integer"] } } }
{ "acp": "0.1", "id": "req-015", "ok": false, "error": { "code": 42901, "message": "concurrency limit reached" } }
{ "acp": "0.1", "id": "req-016", "ok": false, "error": { "code": 50001, "message": "component handler threw: ECONNREFUSED" } }
{ "acp": "0.1", "id": "req-017", "ok": false, "error": { "code": 50400, "message": "call timed out after 30000ms" } }
{ "acp": "0.1", "id": "req-018", "ok": false, "error": { "code": 59000, "message": "vendor-specific error (private range)" } }
```

---

## 附录 A:与 MCP 的映射

| MCP 概念 | ACP 对应 | 转换规则 |
|---|---|---|
| `tools/list` | `discover` | descriptor → tool 定义:`{"name": id.replaceAll(".","_"), "description", "inputSchema"}`(`.` → `_` 无损可逆) |
| `tools/call` | `call` | MCP `arguments`(object)直接作为 ACP `input`;ACP `result` 包装为 MCP `content`(JSON 序列化) |
| MCP stdio 传输 | ACP Stdio 传输(§11) | 同为按行 JSON,桥接器做信封转换 |
| MCP 无流式 | ACP `stream:true` | 桥接时把分片依次拼接为最终 `result`(文本分片字符串串接;`bin` 分片 base64 解码后拼接);错误信封 → MCP `isError: true` |

**方向一(ACP → MCP)**:把 ACP server / client 暴露为 MCP stdio server,供 Claude Desktop、Cursor 等挂载。
**方向二(MCP → ACP)**:把 MCP server 的工具列表包装为 ACP 元件描述符,供 ACP 客户端调用本地 MCP 工具。

## 附录 B:与 OpenAI Tool-Call 的映射

ACP 描述符 → OpenAI tool 定义(纯函数可转换):

```json
{
  "type": "function",
  "function": {
    "name": "sensor_temperature",
    "description": "Reads current temperature from a virtual sensor",
    "parameters": { "type": "object", "properties": { "unit": { "enum": ["C", "F"] } }, "required": [] }
  }
}
```

- `function.name` = `component_id` 的 `.` → `_`;
- `function.parameters` = 描述符 `inputSchema`(同为 draft-07,直接搬运);
- 模型产出的 `tool_calls[].function.arguments` 是 **JSON 字符串**,需 `JSON.parse` 后作为 ACP `input`;
- ACP `result` 以 `JSON.stringify` 放入 `role:"tool"` 消息的 `content`。

## 附录 C:实现一致性级别

**MUST**:POST `/acp`;信封校验顺序(§3.2);discover/call 语义与 result 形状;分片终止帧;错误码表中标注非预留的全部错误码;`meta` 忽略;未知字段忽略;错误时回显 `id`。
**SHOULD**:`GET /acp/discover`;标签过滤;乱序检测;优雅停机(`50300`)。
**MAY**:`/acp/health`;SSE;标签过滤;私有扩展码 59000–59999;二进制 WS 帧(回 40001 即可)。

---

*本规范是 ACP 项目最重要的资产。欢迎通过开源社区反馈迭代:https://github.com/xi-era/acp-protocol*
