/**
 * acp-mcp — CLI: bridges a remote ACP server to MCP stdio, mountable by
 * Claude Desktop / Cursor:
 *   { "mcpServers": { "acp": { "command": "npx", "args": ["-y", "@xi-era/acp-adapter-mcp", "http://host:8080"] } } }
 */
import { AcpClient, AcpError } from "@xi-era/acp-sdk/client";
import { serveMcpFromAcp } from "./index.js";

const url = process.argv[2];
if (!url) {
  process.stderr.write("usage: acp-mcp <acp-url>   e.g. acp-mcp http://localhost:8080\n");
  process.exit(1);
}

const normalized = url.replace(/\/+$/, "").endsWith("/acp") ? url : `${url.replace(/\/+$/, "")}/acp`;

try {
  const client = new AcpClient({ url: normalized, timeoutMs: 60_000 });
  await client.connect();
  await serveMcpFromAcp(client, { name: "acp-bridge" });
} catch (e) {
  if (e instanceof AcpError) {
    process.stderr.write(`acp-mcp: ACP error ${e.code}: ${e.message}\n`);
  } else {
    process.stderr.write(`acp-mcp: ${e instanceof Error ? e.message : String(e)}\n`);
  }
  process.exit(1);
}
