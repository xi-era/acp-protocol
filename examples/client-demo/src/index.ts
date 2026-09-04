/**
 * Demo 3 — call the other demos as an ACP client, three ways:
 *
 *   1. SDK directly:   discover / call / callStream
 *   2. OpenAI adapter: componentsToOpenaiTools + createToolCallHandler
 *                      (exactly what a model's tool_calls loop consumes)
 *   3. MCP bridge:     expose the ACP components as MCP tools
 *                      (what Claude Desktop / Cursor would mount)
 */
import { AcpClient } from "@xi-era/acp-sdk/client";
import { componentsToOpenaiTools, createToolCallHandler } from "@xi-era/acp-adapter-openai";
import { createMcpBridgeServer } from "@xi-era/acp-adapter-mcp";
import { Client as McpClient } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";

export async function run(bizUrl: string, iotUrl: string): Promise<string[]> {
  const log: string[] = [];
  const say = (m: string) => {
    log.push(m);
    console.log(m);
  };

  // --- 1. SDK ------------------------------------------------------------
  const biz = new AcpClient({ url: bizUrl });
  const iot = new AcpClient({ url: iotUrl });
  await biz.connect();
  await iot.connect();

  const iotComponents = await iot.discover();
  say(`[sdk] iot components: ${iotComponents.map((c) => c.id).join(", ")}`);

  const reading = await iot.call<{ celsius: number }>("sensor.temperature", {});
  say(`[sdk] sensor.temperature -> ${reading.celsius}°C`);

  const streamed: number[] = [];
  for await (const chunk of iot.callStream("sensor.temperature.stream", { n: 3 })) {
    if (chunk.data && typeof chunk.data === "object") streamed.push((chunk.data as { celsius: number }).celsius);
  }
  say(`[sdk] streamed ${streamed.length} readings: ${streamed.join(", ")}`);

  const created = await biz.call<{ id: number }>("biz.order.create", { item: "acp-starter-kit", qty: 2 });
  say(`[sdk] biz.order.create -> order #${created.id}`);

  // --- 2. OpenAI Tool-Call adapter ---------------------------------------
  const tools = componentsToOpenaiTools(await biz.discover());
  say(`[openai] tools for the model: ${tools.map((t) => t.function.name).join(", ")}`);

  const handler = createToolCallHandler(biz);
  // This is the tool_calls array an OpenAI chat completion would produce:
  const toolMessages = await handler([
    {
      id: "call_demo_1",
      function: { name: "biz_order_create", arguments: JSON.stringify({ item: "sensor-cable", qty: 10 }) },
    },
  ]);
  say(`[openai] tool message: ${toolMessages[0]!.content}`);

  // --- 3. MCP bridge ------------------------------------------------------
  const mcpBridge = createMcpBridgeServer(iot);
  const mcpClient = new McpClient({ name: "demo-mcp-client", version: "0.1.0" });
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  await mcpBridge.connect(serverTransport);
  await mcpClient.connect(clientTransport);
  const { tools: mcpTools } = await mcpClient.listTools();
  say(`[mcp] tools a Claude/Cursor client sees: ${mcpTools.map((t) => t.name).join(", ")}`);
  const ping = await mcpClient.callTool({ name: "sensor_ping", arguments: {} });
  say(`[mcp] sensor_ping -> ${(ping.content as { text: string }[])[0]!.text}`);

  await mcpClient.close();
  await biz.close();
  await iot.close();
  return log;
}

const isMain = process.argv[1]?.replace(/\\/g, "/").endsWith("index.js");
if (isMain) {
  const biz = process.env["BIZ_URL"] ?? "http://localhost:8081/acp";
  const iot = process.env["IOT_URL"] ?? "http://localhost:8082/acp";
  run(biz, iot).catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
