# 指南:让 Cursor / Claude Desktop 调用远程 ACP 元件

> 路线图 v0.2 的目标:「让 Cursor 通过 MCP-ACP 桥接调用远程 ACP 元件」。所需的全部能力 v0.1 已内置——`@xi-era/acp-adapter-mcp` 的 `acp-mcp` bin 会把任意 ACP 服务器的元件暴露为 MCP 工具。

## 原理

```text
Cursor / Claude Desktop ──MCP(stdio)──> acp-mcp 桥 ──ACP(HTTP/WS)──> 远程元件服务器
```

- MCP 客户端(Cursor、Claude Desktop)原生只会讲 MCP;
- `acp-mcp` 以 MCP stdio server 的身份被它们挂载,同时作为 ACP 客户端连接你的元件服务器;
- `tools/list` ↔ `discover`,`tools/call` ↔ `call`(元件 id 的 `.` 自动映射为 `_`,如 `sensor.temperature` → `sensor_temperature`)。

## 步骤一:启动一个 ACP 元件服务器

用仓库里的虚拟 IoT 传感器:

```bash
cd acp-protocol
ACP_PORT=8082 node examples/iot-sensor/dist/index.js
# 输出:virtual IoT sensor ACP endpoint: http://localhost:8082/acp
```

验证:`acp discover http://localhost:8082`(或浏览器打开 `http://localhost:8082/acp/discover`)。

## 步骤二:在 Cursor 中挂载桥

编辑(或新建)`~/.cursor/mcp.json`(项目级则在项目根建 `.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "acp": {
      "command": "npx",
      "args": ["-y", "@xi-era/acp-adapter-mcp", "http://localhost:8082"]
    }
  }
}
```

重启 Cursor(或点击 MCP 面板的刷新)。打开 **Settings → MCP & Integrations**,应看到 `acp` 服务器下列出:

- `sensor_temperature` — Reads the current temperature
- `sensor_temperature_stream` — Streams n live temperature readings
- `sensor_ping` — Health check for the device link

## 步骤三:在 Agent 对话中直接调用

在 Cursor 的 Agent 聊天里输入:

> 帮我读一下当前的温度,连续读 3 次看变化

Cursor 会调用 `sensor_temperature` / `sensor_temperature_stream`,数据来自你的(远程/硬件)元件服务器——对模型而言与本地工具无差别。

## Claude Desktop 配置(同款)

`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "acp": {
      "command": "npx",
      "args": ["-y", "@xi-era/acp-adapter-mcp", "http://localhost:8082"]
    }
  }
}
```

## 事件订阅说明(v0.2)

MCP 协议没有事件流,因此桥接器对 `$subscribe`/`$event` 的映射是「拉取式」:订阅类元件请通过流式调用(`tools/call` 内部拼接分片)消费。原生事件订阅请直接使用 ACP SDK。

## 常见问题

| 现象 | 处理 |
|---|---|
| MCP 面板里没有 acp 服务器 | `npx -y @xi-era/acp-adapter-mcp http://localhost:8082` 手动跑一次看报错(通常是 ACP 服务器没起) |
| 工具列出但调用超时 | ACP 服务器地址写错,或防火墙拦了端口 |
| 元件名带下划线 | 这是 `.`→`_` 的无损映射(规范 §7.1),属预期行为 |
