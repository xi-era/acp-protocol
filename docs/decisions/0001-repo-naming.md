# 0001 - Git 仓库命名方案

> 状态:已采纳
> 背景:ACP = Agent-Component-Protocol

## 决策

**主仓库名称:`acp-protocol`**

注意:**不要单一仓库只叫 `acp-sdk`**。如果只叫 acp-sdk,会把协议规范、demo、cli、适配器全部塞到 SDK 仓库,后期臃肿混乱,开源者找不到 spec 文档。

✅ 含义:代表这是协议本体仓库,不是单纯 SDK,包含规范、cli、sdk、适配器、demo 全部内容。
> 对外认知:访客一看就知道这是一套协议标准,而不是某一个语言的 SDK 包。

## monorepo 内部目录结构(pnpm workspace)

```text
acp-protocol/
├─ spec/                  # 最重要:ACP 协议规范 ACP-0.1-SPEC.md
├─ packages/
│  ├─ acp-sdk-ts/         # TypeScript SDK (npm: @xi-era/acp-sdk)
│  ├─ acp-cli/            # CLI 工具 (npm: @xi-era/acp-cli)
│  ├─ acp-adapter-mcp/    # ACP ↔ MCP 双向桥接适配器
│  └─ acp-adapter-openai/ # ACP → OpenAI Tool-Call 转换
├─ examples/              # Demo、硬件元件示例、http 业务元件示例
└─ docs/                  # 使用文档、README、官网 md 源
```

## NPM 包名约定

统一采用 `@xi-era` scope(与 GitHub org 一致):

- `@xi-era/acp-sdk`
- `@xi-era/acp-cli`
- `@xi-era/acp-adapter-mcp`
- `@xi-era/acp-adapter-openai`
