# ACP 元件模板库

> 直接复制源码改两个标注点(★ 改这里)就能跑的生产级元件模板。教学定位,不是可安装库。

## TypeScript 模板(`src/`,依赖 `@xi-era/acp-sdk`)

| 模板 | 文件 | 说明 | 启动 |
|---|---|---|---|
| **HTTP 代理** | [src/http-proxy.ts](src/http-proxy.ts) | 把任意 HTTP JSON API 封装为元件:在 `ENDPOINTS` 数组声明 id→URL/方法即可 | `pnpm start:http-proxy`(端口 8091) |
| **SQLite 查询** | [src/sqlite-query.ts](src/sqlite-query.ts) | `node:sqlite` 零依赖只读查询元件,内置 SELECT/WITH 白名单防护 | `pnpm start:sqlite-query`(端口 8092) |
| **模拟 IoT** | [src/mock-iot.ts](src/mock-iot.ts) | 多传感器:一次性读取 + 流式读数 + `$event` 事件推送(v0.2) | `pnpm start:mock-iot`(端口 8093) |

## Python 模板(`python/`,依赖 `acp-protocol-sdk`)

| 模板 | 文件 | 说明 | 启动 |
|---|---|---|---|
| **HTTP 代理** | [python/http_proxy.py](python/http_proxy.py) | 同 TS 版,urllib 标准库 | `python python/http_proxy.py` |
| **SQLite 查询** | [python/sqlite_query.py](python/sqlite_query.py) | 同 TS 版,sqlite3 标准库 | `python python/sqlite_query.py` |
| **模拟 IoT** | [python/mock_iot.py](python/mock_iot.py) | 同 TS 版,含 `$event` 推送 | `python python/mock_iot.py` |

## 使用方式

1. **直接体验**:启动任一模板后用官方 CLI 走查:

   ```bash
   acp discover http://localhost:8093
   acp call http://localhost:8093 sensor.iot.temperature --raw
   acp call ws://localhost:8093/acp sensor.iot.watch '{"n":5}' --stream
   ```

2. **复制改造**:把模板文件拷进你的项目,搜索 `★ 改这里` 修改配置;替换 id 前缀(如 `sensor.iot` → `acme.robot`)避免冲突。

3. **事件订阅**(v0.2):用 SDK 订阅模拟 IoT 的读数事件:

   ```ts
   const client = new AcpClient({ url: "ws://localhost:8093/acp" });
   await client.subscribe({ component: "sensor.iot.watch" }, (ev) => console.log(ev.data));
   ```

## 测试

- TypeScript:`pnpm e2e`(根目录)
- Python:`pytest templates/python`(需先 `pip install -e sdk/python`)
