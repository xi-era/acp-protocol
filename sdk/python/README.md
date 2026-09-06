# acp-protocol-sdk (Python)

> ACP(Agent-Component-Protocol)v0.2 的 Python SDK。pip 包名 `acp-protocol-sdk`,import 名 `acp`。
> TypeScript 参考实现见 [packages/acp-sdk-ts](../../packages/acp-sdk-ts);协议规范见 [spec/ACP-0.2-SPEC.md](../../spec/ACP-0.2-SPEC.md)。

## 安装

```bash
pip install acp-protocol-sdk
# LangChain 集成(ACP 元件 → StructuredTool):
pip install "acp-protocol-sdk[langchain]"
```

## 快速上手

### 服务端

```python
from acp import AcpServer
from acp.component import ComponentDef

server = AcpServer(name="edge-node-01", version="1.0.0")

@server.component(id="sensor.temperature", name="Temperature Sensor",
                  description="Reads current temperature", tags=["iot", "sensor"],
                  input_schema={"type": "object"})
def read_temp(inp, ctx):
    return {"celsius": 23.5}

@server.component(id="log.tail", stream=True, description="Streams n log lines")
async def tail(inp, ctx):          # async generator(或同步 generator)= 流式
    for i in range(inp["n"]):
        yield {"line": f"entry-{i}"}

ws_port = server.listen(port=8080)   # 非阻塞;WS 端口
print(server.http_port)              # HTTP 端口(见下方偏差说明)
server.serve_forever()               # 或不调用,自行管理生命周期
```

### 客户端

```python
from acp import AcpClient

client = AcpClient("http://127.0.0.1:8081/acp", timeout_ms=30_000)
client.connect()                                   # WS 必需;HTTP 可省
comps = client.discover()                          # list[ComponentDescriptor]
result = client.call("sensor.temperature", {"unit": "C"})
for chunk in client.call_stream("log.tail", {"n": 3}):
    print(chunk.data)

sub = client.subscribe(component="sensor.temperature", handler=lambda ev: print(ev.data))
sub.unsubscribe()
client.close()
```

低层逃生口:`client.request({...})` 直接发送完整信封;`$ping` 保活由 WS 传输自动处理(`keep_alive_ms` / `pong_timeout_ms` 参数,收到 40002 自动永久禁用以兼容 0.1 服务端)。

## 已知偏差(相对 TypeScript SDK)

1. **HTTP 与 WS 不共端口**。`websockets` 库在 `process_request` 之前就硬拒绝非 GET 请求,无法在同端口同时服务 WS 升级与 HTTP POST。因此:
   - `listen(port)` 返回 **WS 端口**;
   - HTTP(标准库 `ThreadingHTTPServer`)监听在 **`server.http_port`**(默认 WS 端口 + 1,冲突时自动重试);
   - 客户端用 `ws://host:<ws_port>/acp` 与 `http://host:<http_port>/acp` 分别连接。
2. **线程化传输**:服务端 dispatch 为同步;`async def` / 异步 generator handler 通过后台事件循环线程执行(`run_coroutine_threadsafe`)。对外语义与 TS SDK 一致。

## 延期项

- **`[aio]` extras(aiohttp `AsyncAcpClient`)**:计划于 **v0.2.1** 交付,届时在 CI(Linux)上验证 asyncio 路径。

## 测试

```bash
pip install -e "sdk/python[langchain]" pytest
python -m pytest sdk/python -q          # 79 个用例,含四传输一致性套件
```

一致性套件(`tests/conformance/suite.py`)与 TypeScript SDK 的
`packages/acp-sdk-ts/test/conformance/suite.ts` 断言一一对应,是跨语言 SDK 的验收标准。

## License

MIT — 见仓库根 [LICENSE](../../LICENSE)。
