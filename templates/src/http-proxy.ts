/**
 * 模板:HTTP API → ACP 元件(通用代理)
 *
 * 把任意 HTTP JSON API 封装为 ACP 元件:只需在 ENDPOINTS 数组里声明
 * 「元件 id → 上游 URL/方法」,零业务代码。生产中把这里换成你的内网服务即可。
 *
 * 启动:pnpm start:http-proxy(或 node --experimental-strip-types src/http-proxy.ts)
 */
import { AcpServer, AcpError, AcpErrorCode, defineComponent } from "@xi-era/acp-sdk/server";
import type { ComponentDef } from "@xi-era/acp-sdk/server";

interface EndpointConfig {
  /** ACP 元件 id(必须匹配 ^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$) */
  id: string;
  description: string;
  /** 上游 HTTP 接口 */
  url: string;
  method: "GET" | "POST";
  /** 输入 JSON Schema(draft-07);省略 = 任意输入 */
  inputSchema?: object;
}

// ★ 改这里:声明你要暴露的上游 API
const ENDPOINTS: EndpointConfig[] = [
  {
    id: "http.jsonplaceholder.user",
    description: "Fetches a user from JSONPlaceholder by id",
    url: "https://jsonplaceholder.typicode.com/users/{id}",
    method: "GET",
    inputSchema: { type: "object", properties: { id: { type: "integer", minimum: 1 } }, required: ["id"] },
  },
  {
    id: "http.jsonplaceholder.post",
    description: "Creates a post on JSONPlaceholder",
    url: "https://jsonplaceholder.typicode.com/posts",
    method: "POST",
    inputSchema: {
      type: "object",
      properties: { title: { type: "string" }, body: { type: "string" } },
      required: ["title"],
    },
  },
];

/** 把 input 里的 {key} 占位符替换进 URL(仅路径参数) */
function fillUrl(url: string, input: Record<string, unknown>): string {
  return url.replace(/\{(\w+)\}/g, (_, key: string) => encodeURIComponent(String(input[key] ?? "")));
}

export function buildComponents(): ComponentDef[] {
  return ENDPOINTS.map((ep) =>
    defineComponent({
      id: ep.id,
      name: ep.id,
      description: ep.description,
      version: "1.0.0",
      ...(ep.inputSchema !== undefined ? { inputSchema: ep.inputSchema } : {}),
      tags: ["http-proxy"],
      async handle(input) {
        const record = (input ?? {}) as Record<string, unknown>;
        const res = await fetch(fillUrl(ep.url, record), {
          method: ep.method,
          headers: { "content-type": "application/json" },
          ...(ep.method === "POST" ? { body: JSON.stringify(input ?? {}) } : {}),
        });
        const body = (await res.json()) as unknown;
        if (!res.ok) {
          // 上游失败 → ACP 语义(spec §8:50002 UPSTREAM_ERROR)
          throw new AcpError(AcpErrorCode.UPSTREAM_ERROR, `upstream ${res.status}`, body);
        }
        return body;
      },
    })
  );
}

const isMain =
  process.argv[1]?.replace(/\\/g, "/").endsWith("http-proxy.ts") ||
  process.argv[1]?.replace(/\\/g, "/").endsWith("http-proxy.js");
if (isMain) {
  const server = new AcpServer({ name: "http-proxy-node", version: "1.0.0" });
  for (const c of buildComponents()) server.register(c);
  server
    .listen({ port: Number(process.env["ACP_PORT"] ?? 8091) })
    .then(({ port }) => console.log(`HTTP-proxy ACP endpoint: http://localhost:${port}/acp`));
}
