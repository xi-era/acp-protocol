# ACP (Agent-Component-Protocol)

> Light-weight open protocol for AI Agent invoking remote services, IoT hardware and distributed business components.
> 轻量中立 JSON 协议,专为 AI Agent 调用**远程服务、硬件设备、分布式业务元件**设计。

Open-source project under **xi-era** organization. xi-era is the open-source community arm of Stellxis.

⚠️ **Name Clarification**: This is **Agent-Component-Protocol (ACP)**. Not to be confused with Zed Editor's **Agent-Client-Protocol (also abbreviated ACP)**. These are two completely different protocols.

MCP 聚焦本地子进程工具调用;
**ACP 补齐远程 HTTP / WebSocket / IoT 硬件 / 跨机器分布式场景空白。**

无需模型原生适配,通过适配器即可无缝兼容 Cursor、Claude-Code、Aider、LangChain 等全主流 Agent 生态。

✅ **轻量极简**:无 JSON-RPC 冗余包袱,纯净报文,适配边缘嵌入式设备
✅ **远程优先**:原生 HTTP/WebSocket 设计,天然支持跨机器、跨网络、跨设备元件调用
✅ **全生态适配**:一键转换 OpenAI Tool-Call / MCP 协议,存量工具零改造接入
✅ **硬件原生友好**:针对 IoT、嵌入式、工业硬件、边缘设备深度优化
✅ **企业级版本可控**:严格语义化版本,向前兼容,可直接落地生产环境

> 重要声明:**ACP 不替代 MCP,双向桥接、生态互通、能力互补。**

- Repo: <https://github.com/xi-era/acp-protocol>
- NPM: `@xi-era/acp-sdk` / `@xi-era/acp-cli` · PyPI: `acp-protocol-sdk` · Go: `go get github.com/xi-era/acp-protocol/go`
- 协议规范: [spec/ACP-0.2-SPEC.md](spec/ACP-0.2-SPEC.md)(当前)· [spec/ACP-0.1-SPEC.md](spec/ACP-0.1-SPEC.md)(存档)
- 企业版与商业合作: [COMMERCIAL.md](COMMERCIAL.md)

---

## 一、项目核心定位

**ACP 不是新一代 AI 编码 Agent,而是面向远程节点、硬件设备、分布式业务元件的标准化 Agent 开放调用协议。**

精准补位 MCP 生态短板:
MCP 强于**本地 stdio 子进程**调用,弱于**远程 HTTP、WebSocket、IoT 硬件、跨机器分布式组件**场景。
ACP 主打**远程优先、硬件友好、分布式原生**,与 MCP 完全互补、双向桥接、互不替代。

## 二、官方 Slogan

1. **主 Slogan(官网 / 首页 / 海报)**
   ACP — 让 AI Agent 轻松调用远程、硬件与分布式业务元件

2. **极简短版(标题 / Logo 副标题)**
   面向分布式组件的轻量 Agent 元件协议

3. **对比记忆版(社群 / 介绍 / PPT 金句)**
   MCP 擅长本地进程,ACP 擅长远程硬件与分布式元件

## 三、三秒快速简介(访客极速理解)

现有 AI Agent 工具调用体系,绝大多数基于**本地进程**设计。
一旦需要控制远程服务器、操作硬件设备、调用跨机器微服务、联动边缘 IoT 模块,传统本地协议会臃肿、低效、适配成本极高。

**ACP 的核心价值:**
将全网分布式能力(API、硬件、传感器、业务模块、微服务)统一封装为标准化「元件」,
一套协议、一次封装,**所有 AI Agent 直接通用**,彻底解决多模型、多格式、多设备适配碎片化问题。

## 四、Problem Statement・行业现状与痛点

### 现有生态痛点

1. MCP 以本地 stdio 子进程为核心,远程网络场景需大量额外封装,对硬件、IoT 设备极不友好;
2. 各大模型工具调用格式割裂,同一套业务组件需要重复编写多端适配代码;
3. 传统协议冗余沉重,边缘、嵌入式、低端硬件设备无法承载;
4. 行业缺少**面向分布式、跨机器、硬件互联**的 Agent 统一元件调用规范。

### ACP 核心解决

1. **统一元件标准**:一套元件描述、发现、调用、流式、报错规范,全 Agent 生态通用;
2. **远程原生架构**:优先 HTTP/WebSocket,天生适配分布式服务与硬件互联;
3. **极致轻量化**:自定义纯净 JSON 报文,无冗余开销,适配全量级边缘设备;
4. **生态无损兼容**:适配器层隔离差异,存量 AI 工具无需改造即可接入;
5. **完整工程工具链**:多语言 SDK + 官方 CLI,极速开发自定义业务 / 硬件元件。

## 五、核心特性(官网 / NPM / 文档置顶版)

- 📡 **远程优先架构** — 原生 HTTP / WebSocket 双协议,支持跨主机、跨网段、跨设备分布式元件调用,彻底突破本地进程限制。
- ⚡ **极简轻量报文** — 摒弃笨重 JSON-RPC 范式,自定义精简 JSON 协议,低算力、低内存硬件设备可稳定运行。
- 🔌 **双向全生态兼容** — 内置 ACP ↔ MCP、ACP ↔ OpenAI Tool-Call 双向适配器,无缝对接 Cursor、Claude、LangChain 主流生态。
- 🧩 **标准化元件模型** — 统一 component_id、元数据描述、自动发现、单次调用、流式分片、异常捕获完整规范。
- 🛠 **完备工程工具链** — 提供多语言 acp-sdk + acp-cli 调试工具,支持本地调试、远程部署、元件快速发布。
- 🔐 **企业级安全预留** — 原生预留鉴权、权限域、访问控制、审计日志、调用溯源字段,适配私有化企业场景。
- 📜 **稳定版本策略** — 严格语义化版本迭代,保证向前兼容,支持长期生产环境落地。
- 🔁 **MCP 生态互通** — ACP 元件可快速暴露为 MCP 标准接口,零成本融入现有成熟 MCP 客户端生态。[指南:Cursor / Claude Desktop 桥接远程元件 →](docs/guides/cursor-mcp-bridge.md)

> 定位重申:**ACP 与 MCP 互补共生,非替代竞争关系**

## 六、Monorepo 结构

```text
acp-protocol/
├─ spec/                  # 最重要:ACP 协议规范 ACP-0.1-SPEC.md
├─ packages/
│  ├─ acp-sdk-ts/         # TypeScript SDK (npm: @xi-era/acp-sdk)
│  ├─ acp-cli/            # CLI 工具 (npm: @xi-era/acp-cli)
│  ├─ acp-adapter-mcp/    # ACP ↔ MCP 双向桥接适配器
│  └─ acp-adapter-openai/ # ACP → OpenAI Tool-Call 转换
├─ examples/              # Demo、硬件元件示例、http 业务元件示例
└─ docs/                  # 使用文档、路线图、决策记录
```

## 七、快速上手

```bash
pnpm install && pnpm build
```

定义一个元件并启动服务:

```ts
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";

const temperature = defineComponent({
  id: "sensor.temperature",
  name: "Temperature Sensor",
  description: "Reads current temperature from a virtual sensor",
  async handle() {
    return { celsius: 23.5 };
  },
});

const server = new AcpServer({ name: "edge-node-01", version: "0.1.0" });
server.register(temperature);
await server.listen({ port: 8080 });
```

客户端发现并调用:

```ts
import { AcpClient } from "@xi-era/acp-sdk/client";

const client = new AcpClient({ url: "http://localhost:8080/acp" });
const components = await client.discover();
const result = await client.call("sensor.temperature", { unit: "C" });
```

详见 [spec/ACP-0.1-SPEC.md](spec/ACP-0.1-SPEC.md) 与 [docs/](docs/)。

## 八、MCP & ACP 官方中立对比表

| 维度 | MCP | ACP |
|---|---|---|
| 核心场景 | 本地子进程、本机工具调用 | 远程 HTTP/WS、IoT 硬件、分布式组件 |
| 传输范式 | Stdio 优先,WebSocket 为辅 | HTTP / WebSocket 原生优先,可兼容 Stdio |
| 协议基础 | JSON-RPC 2.0 | 自定义极简纯净 JSON 报文 |
| 生态接入 | 客户端原生支持度高 | 适配器无损兼容全主流 Agent |
| 设备适配 | 适配 PC / 服务器本地程序 | 适配边缘、嵌入式、硬件、跨机器集群 |

> 官方中立结论:**二者场景互补、架构兼容、可双向桥接,无替代竞争关系**

## 九、精准适用场景 & 不适用场景

### 🎯 最佳适用场景

1. **AI + IoT / 嵌入式硬件** — Agent 远程控制传感器、执行器、工业模块、边缘硬件设备;
2. **分布式业务智能体** — 将后端微服务、业务能力封装为元件,供 AI 智能体自动化调用;
3. **边缘端轻量化 Agent** — 资源受限设备,需要低开销、高稳定的跨设备 Agent 通信协议;
4. **企业统一 Agent 中台** — 批量管理内部业务组件、硬件资源,实现多大模型统一接入治理;
5. **开发者自定义 Agent 生态** — 一次封装通用元件,全平台 AI 工具直接复用,告别重复适配。

### ❌ 非适用场景

纯本地子进程、本机工具调用场景,**优先使用 MCP**,无需引入 ACP。

## 十、商业定位

ACP 不做重复的 AI 对话工具、编码工具。
**ACP 定位:分布式 AI Agent 底层元件基础设施**

- 开源层:协议规范、SDK、CLI、适配器生态 **永久免费开源**,赋能全开发者生态
- 企业商业层:ACP 智能网关、细粒度权限管控、调用审计溯源、流量限流、SSO 集成、私有化部署、定制化元件开发、MCP-ACP 桥接服务

**核心商业逻辑:开发者用开源做大生态,企业用私有化方案做稳定生产级落地**

## 十一、路线图

- ✅ **v0.1(MVP)**:协议规范、TypeScript SDK、CLI、MCP/OpenAI 适配器、Demo — 详见 [docs/roadmap.md](docs/roadmap.md)
- ✅ **v0.2(生态扩展)**:心跳保活(`$ping`)、事件推送(`$event`)、报文压缩(gzip / permessage-deflate)、[Python SDK](sdk/python/README.md)(含 LangChain 集成)、[Go SDK](go/)、[元件模板库](templates/)、[Cursor 桥接指南](docs/guides/cursor-mcp-bridge.md)、三方互操作测试 — 变更清单见 [CHANGELOG](CHANGELOG.md)
- ⬜ **v0.2.1**:Python `[aio]` extras(aiohttp `AsyncAcpClient`,在 CI 上验证 asyncio 路径)
- ⬜ **v1.0(商业化)**:ACP-Gateway、权限管控、审计溯源、SSO、私有化部署

## 十二、License

本仓库(协议规范、SDK、CLI、适配器、示例)以 [MIT](LICENSE) 许可发布,**永久免费开源**,允许任意商业与非商业使用。

> **Open-core 边界说明**:面向企业的 ACP-Gateway(注册中心、权限管控、审计溯源、限流、SSO)、私有化部署包等商业闭源产品是**独立于本仓库的单独产品**,不适用本 MIT 许可,由 xi-era / Stellxis 另行授权。详见 [COMMERCIAL.md](COMMERCIAL.md) 与上方「商业定位」。



