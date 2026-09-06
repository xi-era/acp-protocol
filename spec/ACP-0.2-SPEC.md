# ACP 0.2 — Agent-Component-Protocol Specification

- 版本:`0.2`
- 状态:STABLE-CANDIDATE
- 日期:2026-09
- 组织:xi-era
- 前代版本:[ACP-0.1-SPEC.md](ACP-0.1-SPEC.md)(存档,语义完全兼容)

**ACP(Agent-Component-Protocol)是面向远程节点、硬件设备、分布式业务元件的标准化 Agent 开放调用协议。**
它与 MCP(Model Context Protocol)互补:MCP 强于本地 stdio 子进程调用,ACP 主打远程 HTTP / WebSocket / IoT 硬件 / 跨机器分布式元件调用。二者双向桥接、互不替代。

> 命名澄清:本协议为 Agent-**Component**-Protocol,与 Zed Editor 的 Agent-**Client**-Protocol(同缩写 ACP)是两个完全不同的协议。

---

## Changes from 0.1

| 变更 | 章节 | 兼容性 |
|---|---|---|
| 新增保留 op `$ping`(双向应用层心跳) | §4.3 | 0.1 服务端回 40002,客户端据此禁用保活 |
| 新增保留 op `$subscribe` / `$unsubscribe` 与 `$event` 推送帧 | §4.4 / §6.2 | 仅有状态传输;HTTP 回 50100 |
| 报文压缩(标准机制:HTTP gzip + WS permessage-deflate) | §9.4 / §10.1 | 可选优化,未压缩路径永远可用 |
| 错误码新增 `42902 SUBSCRIPTION_LIMIT`、`50100 EVENT_UNSUPPORTED` | §8 | 纯增量 |
| 修正:响应 `acp` 字段**回显请求的 acp 值**(0.1 spec 如此表述,本版强化为 MUST) | §5.3 | 对 0.1 实现通常是诚实化 |
| `$` 前缀 op 命名空间正式启用(0.1 仅预留) | §4.3-4.4 | 0.1 服务端对未知 op 回 40002,符合 0.1 行为 |

---

## 目录

1. [设计原则](#1-设计原则)
2. [术语](#2-术语)
3. [报文信封](#3-报文信封)
4. [操作(op)](#4-操作op)
5. [响应信封](#5-响应信封)
6. [流式分片与事件推送](#6-流式分片与事件推送)
7. [元件模型与元数据](#7-元件模型与元数据)
8. [错误码体系](#8-错误码体系)
9. [HTTP 传输映射](#9-http-传输映射)
10. [WebSocket 传输映射](#10-websocket-传输映射)
11. [Stdio 传输映射](#11-stdio-传输映射)
12. [版本协商与兼容矩阵](#12-版本协商与兼容矩阵)
13. [安全预留](#13-安全预留)
14. [协议示例集](#14-协议示例集)
- [附录 A:与 MCP 的映射](#附录-a与-mcp-的映射)
- [附录 B:与 OpenAI Tool-Call 的映射](#附录-b与-openai-tool-call-的映射)
- [附录 C:实现一致性级别](#附录-c实现一致性级别)

关键词 **MUST / MUST NOT / SHOULD / MAY** 按 RFC 2119 语义解释。

---

## 1. 设计原则

1. **极简纯净 JSON,拒绝 JSON-RPC**。单信封双语义:一条报文既是请求也是响应,靠字段组合区分。所有顶层字段名 ≤ 9 个字符。
2. **传输无关**。信封不携带传输信息;HTTP / WebSocket / Stdio 只定义"报文如何搬运"。同一报文跨传输零改写。
3. **低算力友好**。不要求二进制协议;不要求压缩(压缩是可选项,见 §9.4);短连接 HTTP 即可完整实现。
4. **远程优先**。HTTP 与 WebSocket 是一等公民;Stdio 为兼容 MCP 桥接与本地调试的可选传输。
5. **schema 语言统一**。元件输入输出 schema 一律使用 **JSON Schema draft-07**。
6. **预留而非实现**。安全字段(auth/scopes/traceId)仍是预留区;企业级能力留给商业层。
7. **可降级**。所有 v0.2 新能力(心跳、事件、压缩)都是可选增强;不支持它们的 0.1 实现依然是合法的 0.2 生态成员(见 §12)。

---

## 2. 术语

| 术语 | 含义 |
|---|---|
| **Agent(客户端)** | ACP 报文的发起方:AI Agent、编码工具、编排器、CLI 调试器等 |
| **Server(服务端)** | 承载元件、接收并处理 ACP 报文的进程或设备 |
| **Component(元件)** | 被封装为可调用单元的远程能力 |
| **Connection(连接)** | 客户端与服务端之间的一次有状态会话(WS 连接、Stdio 进程对、Memory 绑定);HTTP 每个请求是瞬时连接 |
| **Subscription(订阅)** | 连接级的事件注册,见 §4.4 |
| **Reserved op($ op)** | `$` 前缀的协议保留操作:`$ping`、`$subscribe`、`$unsubscribe` |

---

## 3. 报文信封

ACP 报文是单个 JSON 对象,编码为 UTF-8。

### 3.1 请求信封(Request Envelope)

```json
{
  "acp": "0.2",
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
| `acp` | string | 是 | 协议标识 + 版本,格式 `主.次`,当前 `"0.2"` |
| `id` | string | 是 | 请求关联 ID,客户端生成;同一次调用的所有响应帧与分片帧 MUST 回显 |
| `op` | string | 是 | 操作路由,枚举:`"discover"` \| `"call"` \| `"$ping"` \| `"$subscribe"` \| `"$unsubscribe"` |
| `component` | string | `op="call"` 必填;`op="discover"` 可选 | 目标元件 ID |
| `tags` | string[] | 可选 | discover 标签过滤 |
| `input` | any | 可选 | 调用输入,须满足元件 `inputSchema` |
| `stream` | boolean | 否,缺省 false | 是否要求流式分片返回 |
| `meta` | object | 否 | 安全预留区,服务端 MUST 忽略(v0.1 语义不变) |

### 3.2 信封校验规则

接收方按顺序校验,任一失败即返回对应错误码:

1. 报文是合法 JSON → 否则 `40000`;
2. `acp`/`id`/`op` 存在且类型正确 → 否则 `40001`;
3. `acp` 版本可支持 → 否则 `40003`;
4. `op` ∈ 枚举(标准 op + 保留 op)→ 否则 `40002`;
5. `op="call"` 时 `component` 存在且合法 → 否则 `40004` / `40001`;
6. 保留 op 的 `input` 形状(§4.3-4.4)→ 否则 `40001`。

---

## 4. 操作(op)

### 4.1 `discover` — 发现元件(0.1 语义不变)

全量列出 / 单个查询 / 标签过滤三种变体;`result` 固定形状:

```json
{ "server": { "name": "edge-node-01", "version": "1.0.0", "protocol": "0.2" }, "components": [] }
```

### 4.2 `call` — 调用元件(0.1 语义不变)

inputSchema 校验 → 42200;流能力不匹配 → 40005;非流式元件 + `stream:true` → 单帧 `{seq:0,end:true}` 包裹。全部与 0.1 相同。

### 4.3 `$ping` — 应用层心跳(v0.2 新增)

双向保活:客户端与服务端**均可发起**,接收方 MUST 立即应答。

请求:
```json
{ "acp": "0.2", "id": "ka-01", "op": "$ping", "input": { "ts": 1794000000000 } }
```

响应:
```json
{ "acp": "0.2", "id": "ka-01", "ok": true, "result": { "pong": 1794000000042, "ts": 1794000000000 } }
```

- `input` 可选;`input.ts` 为发送方 unix 毫秒,存在时原样回显到 `result.ts`;
- `result.pong` 必填 = 响应方当前 unix 毫秒(供 RTT 估算);
- `$ping` 与业务调用共用 `id` 多路复用,不影响在途请求;
- `$ping` 在所有传输上均可用(包括无状态 HTTP:就是一次普通 POST);
- **0.1 服务端收到 `$ping` 回 `40002 UNKNOWN_OP`** —— 客户端据此永久禁用本连接的保活与订阅(见 §12.2),MUST NOT 重试。

**建议参数**(SDK 默认值,spec 不强制):

| 参数 | 默认 | 说明 |
|---|---|---|
| `keepAliveMs` | 30000 | 连接空闲时每 N ms 发一次 `$ping`;0 = 关闭 |
| `pongTimeoutMs` | 10000 | 超时未收到同 id 应答 → 判定连接死亡:关闭、fail 所有在途请求 |

谁发谁收:**客户端为主发起方**(客户端最需要感知死连接);服务端发起为 MAY。v0.2 服务端默认不因应用层空闲断连;WS 协议层 ping/pong 仍然可用且推荐。

### 4.4 `$subscribe` / `$unsubscribe` — 事件订阅(v0.2 新增)

订阅绑定在**连接**上。`input` 必须恰含 `component` **或** `tags` 之一(都给或都不给 → `40001`):

```json
{ "acp": "0.2", "id": "sub-01", "op": "$subscribe", "input": { "component": "sensor.temperature" } }
{ "acp": "0.2", "id": "sub-02", "op": "$subscribe", "input": { "tags": ["iot"] } }
```

订阅成功响应:
```json
{ "acp": "0.2", "id": "sub-01", "ok": true, "result": { "subscription": "s-7f3a" } }
```

退订(`input` 缺省 / null = 退订本连接全部订阅):
```json
{ "acp": "0.2", "id": "sub-03", "op": "$unsubscribe", "input": { "component": "sensor.temperature" } }
```
退订响应:`result` 固定为 `null`。

**约束**:
- 仅在有状态传输(WS / Stdio / Memory)上可用;HTTP 上服务端 MUST 回 `50100 EVENT_UNSUPPORTED`;
- 每连接订阅数上限(建议 64)由实现自定,超限回 `42902 SUBSCRIPTION_LIMIT`(MAY 实现);
- 事件投递语义见 §6.2。

---

## 5. 响应信封

### 5.1 成功(一次性)

```json
{ "acp": "0.2", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "ok": true, "result": { "celsius": 23.5 } }
```

`result` 是裸输出值(即元件 outputSchema 描述的值本身)。

### 5.2 失败

```json
{ "acp": "0.2", "id": "01J8ZK3M9Q2W5R7T9V0X2Y4Z6A", "ok": false,
  "error": { "code": 42200, "message": "input validation failed", "data": { "errors": ["..."] } } }
```

### 5.3 响应字段规则

- 响应 **MUST 回显请求的 `acp` 值**(按客户端声明版本应答,即使服务端能力更高);
- 响应 MUST 回显请求的 `id`;
- `ok:true` 时 MUST 有 `result`(可为 null),MUST NOT 有 `error`/`chunk`;
- `ok:false` 时 MUST 有 `error`,MUST NOT 有 `result`/`chunk`;
- 客户端 MUST 忽略未知顶层字段。

---

## 6. 流式分片与事件推送

### 6.1 流式分片(0.1 语义不变)

`stream:true` 的调用产生 N+1 条分片帧(N≥0):

```json
{ "acp": "0.2", "id": "…", "chunk": { "seq": 0, "end": false, "data": { "line": "boot ok" } } }
{ "acp": "0.2", "id": "…", "chunk": { "seq": 1, "end": true,  "data": null } }
```

规则(seq 从 0 严格递增、终止帧必须存在、错误信封等效终止、`bin:true` 时 data 为 base64、单帧 ≤64KB 推荐 / ≥1MB 必须接受)与 0.1 完全一致,此处不再重复。

### 6.2 `$event` — 服务端事件推送(v0.2 新增)

服务端 → 客户端,**无请求关联**:

```json
{ "acp": "0.2", "id": null, "event": { "component": "sensor.temperature", "tags": ["iot", "sensor"], "data": { "celsius": 23.5 }, "ts": 1794000000042 } }
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `event.component` | 与 tags 至少一项 | 事件来源元件 id |
| `event.tags` | 与 component 至少一项 | 来源元件标签(默认取描述符 tags) |
| `event.data` | 是(可为 null) | 事件负载 |
| `event.ts` | 否 | 服务端 unix 毫秒 |

- 顶层 `event` 字段与 `chunk` 对称;`id` 必为 `null`,客户端 MUST NOT 用 id 关联事件;
- **匹配规则**:按 `component` 订阅 → `event.component === 订阅值`;按 `tags` 订阅 → `订阅.tags ⊆ event.tags`;
- **投递语义:best-effort, at-most-once**。无 ack、无重放;每连接事件队列有界(建议 256),溢出丢弃新事件;断线即失去订阅(重连后由客户端 SDK 自动重订,协议不定义);
- `$event` 仅在有状态传输上投递;事件与服务端流式分片共用连接,互不干扰。

---

## 7. 元件模型与元数据(0.1 语义不变)

### 7.1 component_id 命名规范

```
component_id = segment ( "." segment ){1,3}
segment      = [a-z] [a-z0-9-]{0,62}
正则: ^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$
```
MCP 桥接映射:`.` ↔ `_` 无损可逆。

### 7.2 元件描述符

```json
{
  "id": "sensor.temperature", "name": "Temperature Sensor", "description": "…",
  "version": "1.0.0",
  "inputSchema": {}, "outputSchema": {},
  "stream": false, "tags": ["iot", "sensor"], "meta": {}
}
```
字段与 0.1 完全一致(draft-07、version 独立于协议版本、endpoint 预留)。

---

## 8. 错误码体系

5 位数字,前两位对齐 HTTP 语义类(`floor(code/100)`;例外:50002→502,51xxx→500)。

### 8.1 错误码表(v0.1 全表保留,新增两码)

| code | 名称 | 含义 | HTTP | 变更 |
|---|---|---|---|---|
| 40000 | `PARSE_ERROR` | 报文不是合法 JSON | 400 | |
| 40001 | `INVALID_ENVELOPE` | 必填字段缺失/类型错误/保留 op input 形状错误 | 400 | |
| 40002 | `UNKNOWN_OP` | op 不在枚举内(0.1 服务端收到 $ op 也回此码) | 400 | |
| 40003 | `UNSUPPORTED_VERSION` | 版本不支持,data.supported 列出支持版本 | 400 | |
| 40004 | `INVALID_COMPONENT_ID` | component_id 违反命名规范 | 400 | |
| 40005 | `STREAM_REQUIRED` | 元件必需流式但请求未带 stream:true | 400 | |
| 40100 | `UNAUTHORIZED` | 预留 | 401 | |
| 40101 | `FORBIDDEN` | 预留 | 403 | |
| 40400 | `COMPONENT_NOT_FOUND` | 元件不存在 | 404 | |
| 40401 | `COMPONENT_UNAVAILABLE` | 已注册但停用/离线 | 404 | |
| 40500 | `METHOD_NOT_ALLOWED` | HTTP 动词误用 | 405 | |
| 41500 | `UNSUPPORTED_MEDIA_TYPE` | Content-Type / Content-Encoding 不符 | 415 | |
| 42200 | `INVALID_INPUT` | input 未过 inputSchema | 422 | |
| 42201 | `INVALID_OUTPUT` | 输出未过 outputSchema(开发期自检) | 422 | |
| 42900 | `RATE_LIMITED` | 预留 | 429 | |
| 42901 | `CONCURRENCY_LIMIT` | 并发超限 | 429 | |
| **42902** | **`SUBSCRIPTION_LIMIT`** | **每连接订阅数超上限**(MAY 实现) | 429 | **v0.2 新增** |
| 50000 | `INTERNAL_ERROR` | 服务端未分类错误 | 500 | |
| 50001 | `COMPONENT_ERROR` | 元件 handler 抛异常 | 500 | |
| 50002 | `UPSTREAM_ERROR` | 上游后端/设备失败 | 502 | |
| **50100** | **`EVENT_UNSUPPORTED`** | **事件推送在当前传输上不可用(HTTP)** | 501 | **v0.2 新增** |
| 50300 | `SHUTTING_DOWN` | 服务关闭中 | 503 | |
| 50400 | `TIMEOUT` | 调用超时 | 504 | |
| 51000 | `STREAM_ABORTED` | 流被中断 | 500 | |
| 51001 | `CHUNK_OVERFLOW` | 分片帧超限 | 500 | |

### 8.2 扩展规则

`43xxx`–`50xxx` 中未标注段为未来保留;私有扩展码仅 **59000–59999**;客户端 MUST 按千位段语义处理未知错误码。

---

## 9. HTTP 传输映射

### 9.1 端点(0.1 不变)

| 端点 | 动词 | 级别 |
|---|---|---|
| `/acp` | POST | MUST |
| `/acp/discover` | GET | SHOULD |
| `/acp/health` | GET | MAY |

### 9.2 流式:NDJSON(0.1 不变)

### 9.3 状态码映射(0.1 不变)

成功 200;失败按 §8.1 映射,响应体仍是完整 ACP 错误信封。

### 9.4 报文压缩(v0.2 新增)

**原则:压缩是传输层优化,报文语义零变化;未压缩路径必须永远可用。**

请求体:
- 客户端 MAY 以 `Content-Encoding: gzip` 发送压缩请求体;
- 服务端 MUST 支持 `gzip` 与未压缩;其他 Content-Encoding 值 → `41500 UNSUPPORTED_MEDIA_TYPE`;
- `Content-Length` 为压缩后字节数。

响应体:
- 客户端以 `Accept-Encoding: gzip` 声明;
- 服务端 MAY 对**缓冲型**响应(一次性 JSON / 错误信封)在序列化字节数 ≥ 阈值(建议 **1024**)时 gzip,附 `Content-Encoding: gzip` 与 `Vary: Accept-Encoding`;
- NDJSON 流式响应:参考实现不压缩;实现 MAY 自行压缩但 MUST 逐行 flush;
- HTTP 客户端 MUST 透明处理压缩与未压缩两种响应。

---

## 10. WebSocket 传输映射

### 10.1 连接与压缩(v0.2 修订)

- 端点同端口 `/acp` Upgrade;一个文本帧 = 一个信封 —— 0.1 不变;
- **`permessage-deflate` 扩展**:服务端与客户端 SHOULD 支持;由 WS 库自动协商,协议层零感知;MUST 接受不带该扩展的连接(自然降级);
- 二进制帧仍保留不定义(收到回 40001)。

### 10.2 生命周期(0.1 不变,补充心跳)

- 无握手包,逐条版本协商;保活除 WS 原生 ping/pong 外,**MAY 使用应用层 `$ping`**(§4.3)——对需要穿越不透传 WS 控制帧的代理/网关时有用。

### 10.3 并发与事件(修订)

- id 多路复用、乱序返回合法 —— 0.1 不变;
- **v0.2:服务端 MAY 主动推送 `$event` 帧(§6.2)**——这是 0.2 服务端唯一的非请求关联帧;`$` 前缀其余用途仍保留。

---

## 11. Stdio 传输映射(0.1 不变,补充)

- 按行读写,一行一个信封;stderr 留给日志;
- v0.2:进程存活期间连接持续,`$ping` 与 `$event`(SHOULD)可用。

---

## 12. 版本协商与兼容矩阵

### 12.1 协商规则(0.1 不变)

`acp` 格式 `主.次`;**主版本相等 + 服务端次版本 ≥ 客户端次版本** 即兼容;不匹配回 `40003`,`data.supported` 列出服务端支持的版本。

### 12.2 兼容矩阵与降级阶梯(v0.2 新增)

| 服务端 \ 客户端声明 | `"0.1"` | `"0.2"` |
|---|---|---|
| 0.1 服务端 | 正常 | `40003`,data.supported=["0.1"] |
| 0.2 服务端 | 正常 | 正常 |

> 注意:0.1 服务端会在版本检查处直接拒绝声明 "0.2" 的客户端(次版本 1 < 2),不会等到 $ op 才回 40002。

**客户端降级阶梯(SDK SHOULD 实现)**:
1. 客户端声明 `"0.2"` 收到 `40003` 且 `data.supported` 含可兼容版本 → 以其中的最高兼容版本重试一次,并锁定该版本用于本连接;
2. 锁定 `"0.1"` 后发出 `$ping` 收到 `40002` → 服务端不认识保留 op:**永久禁用本连接的保活与订阅**,MUST NOT 重试。

**服务端兼容义务**:0.2 服务端 MUST 完整服务 0.1 客户端(discover/call/流式/错误码全兼容);0.2 服务端收到 `$ping` MUST 应答(§4.3)。

---

## 13. 安全预留(0.1 语义不变)

`meta` 预留键:`auth`、`scopes`、`traceId`、`timeoutMs`。服务端 MUST 忽略全部内容。企业网关(v1.0 商业层)可在不破坏兼容性的前提下注入字段。

---

## 14. 协议示例集

### 14.1 全量发现(0.2 服务端)

请求:`{ "acp": "0.2", "id": "req-001", "op": "discover" }`

响应:
```json
{
  "acp": "0.2",
  "id": "req-001",
  "ok": true,
  "result": {
    "server": { "name": "edge-node-01", "version": "1.0.0", "protocol": "0.2" },
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
      }
    ]
  }
}
```

### 14.2 单次调用(0.1 客户端,0.2 服务端——注意响应回显 "0.1")

请求:`{ "acp": "0.1", "id": "req-002", "op": "call", "component": "sensor.temperature", "input": {} }`

响应:`{ "acp": "0.1", "id": "req-002", "ok": true, "result": { "celsius": 23.5 } }`

### 14.3 流式调用(0.1 不变,略——见 ACP-0.1-SPEC §14.3)

### 14.4 心跳

请求:`{ "acp": "0.2", "id": "ka-01", "op": "$ping", "input": { "ts": 1794000000000 } }`
响应:`{ "acp": "0.2", "id": "ka-01", "ok": true, "result": { "pong": 1794000000042, "ts": 1794000000000 } }`

### 14.5 订阅与事件

订阅:`{ "acp": "0.2", "id": "sub-01", "op": "$subscribe", "input": { "tags": ["iot"] } }`
订阅响应:`{ "acp": "0.2", "id": "sub-01", "ok": true, "result": { "subscription": "s-7f3a" } }`

事件推送(服务端主动,无 id 关联):
```json
{ "acp": "0.2", "id": null, "event": { "component": "sensor.temperature", "tags": ["iot", "sensor"], "data": { "celsius": 23.5 }, "ts": 1794000000042 } }
```

退订:`{ "acp": "0.2", "id": "sub-02", "op": "$unsubscribe", "input": null }`
退订响应:`{ "acp": "0.2", "id": "sub-02", "ok": true, "result": null }`

### 14.6 HTTP 上的保留 op

```json
{ "acp": "0.2", "id": "sub-03", "ok": false, "error": { "code": 50100, "message": "events unsupported on connectionless transport" } }
```

### 14.7 错误示例(v0.2 新增两码)

```json
{ "acp": "0.2", "id": "req-019", "ok": false, "error": { "code": 42902, "message": "subscription limit reached (64)" } }
{ "acp": "0.2", "id": "ka-02", "ok": false, "error": { "code": 40002, "message": "unknown op: $ping" } }
```

---

## 附录 A:与 MCP 的映射(0.1 不变)

tools/list ↔ discover;tools/call ↔ call;`.` ↔ `_`;MCP 无流式/无事件,分片拼接为 result、$event 不映射。

## 附录 B:与 OpenAI Tool-Call 的映射(0.1 不变)

## 附录 C:实现一致性级别

**MUST**:0.1 全部 MUST 项;$ping 应答;响应 `acp` 回显请求版本;接受未压缩报文(§9.4 原则)。
**SHOULD**:permessage-deflate;客户端降级阶梯;`GET /acp/discover`;事件队列管理。
**MAY**:$subscribe/$unsubscribe 实现(不实现则回 50100 之外的 40002 亦可,但 0.2 参考实现实现之);42902;NDJSON 压缩;服务端主动 $ping;二进制 WS 帧(回 40001)。

---

*本规范欢迎通过开源社区反馈迭代:https://github.com/xi-era/acp-protocol*
