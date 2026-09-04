import { describe, expect, it } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { McpClientLike } from "../src/index.js";
import { createMcpBridgeServer, mcpToolsToComponents } from "../src/index.js";
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";
import type { AcpClient } from "@xi-era/acp-sdk/client";

function makeAcpServer(): AcpServer {
  const server = new AcpServer({ name: "mcp-bridge-test" });
  server.register(
    defineComponent({
      id: "math.add",
      name: "Add",
      description: "Adds numbers",
      inputSchema: { type: "object", properties: { a: { type: "number" }, b: { type: "number" } }, required: ["a", "b"] },
      handle: (input: { a: number; b: number }) => ({ sum: input.a + input.b }),
    })
  );
  server.register(
    defineComponent({
      id: "log.stream",
      name: "LogStream",
      description: "Streams n lines",
      stream: true,
      handle: async function* (input: { n: number }) {
        for (let i = 0; i < input.n; i++) yield `line-${i}`;
      },
    })
  );
  server.register(
    defineComponent({
      id: "boom.always",
      name: "Boom",
      description: "Always fails",
      handle: () => {
        throw new Error("kaput");
      },
    })
  );
  return server;
}

async function makeMcpClientPair(acp: AcpServer | AcpClient) {
  const mcpServer = createMcpBridgeServer(acp);
  const mcpClient = new Client({ name: "test-client", version: "0.0.1" });
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  await mcpServer.connect(serverTransport);
  await mcpClient.connect(clientTransport);
  return { mcpClient, mcpServer };
}

describe("ACP -> MCP (createMcpBridgeServer)", () => {
  it("lists ACP components as MCP tools (dots -> underscores)", async () => {
    const { mcpClient, mcpServer } = await makeMcpClientPair(makeAcpServer());
    const { tools } = await mcpClient.listTools();
    const names = tools.map((t) => t.name).sort();
    expect(names).toEqual(["boom_always", "log_stream", "math_add"]);
    const add = tools.find((t) => t.name === "math_add")!;
    expect(add.inputSchema).toEqual({
      type: "object",
      properties: { a: { type: "number" }, b: { type: "number" } },
      required: ["a", "b"],
    });
    await mcpServer.close();
  });

  it("calls an ACP component via tools/call", async () => {
    const { mcpClient, mcpServer } = await makeMcpClientPair(makeAcpServer());
    const result = await mcpClient.callTool({ name: "math_add", arguments: { a: 2, b: 3 } });
    expect(result.isError).toBe(false);
    expect(result.content).toEqual([{ type: "text", text: '{"sum":5}' }]);
    await mcpServer.close();
  });

  it("concatenates stream-required components (40005 retry)", async () => {
    const { mcpClient, mcpServer } = await makeMcpClientPair(makeAcpServer());
    const result = await mcpClient.callTool({ name: "log_stream", arguments: { n: 3 } });
    expect(result.isError).toBe(false);
    expect(result.content).toEqual([{ type: "text", text: "line-0line-1line-2" }]);
    await mcpServer.close();
  });

  it("reports ACP failures as isError tool results", async () => {
    const { mcpClient, mcpServer } = await makeMcpClientPair(makeAcpServer());
    const result = await mcpClient.callTool({ name: "boom_always", arguments: {} });
    expect(result.isError).toBe(true);
    await mcpServer.close();
  });
});

describe("MCP -> ACP (mcpToolsToComponents)", () => {
  it("wraps MCP tools as ACP component defs and invokes them", async () => {
    const fakeMcp: McpClientLike = {
      listTools: async () => ({
        tools: [
          {
            name: "local_echo",
            description: "Echoes text",
            inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
          },
        ],
      }),
      callTool: async ({ name, arguments: args }) => {
        expect(name).toBe("local_echo");
        expect(args).toEqual({ text: "hi" });
        return { content: [{ type: "text", text: `echo: ${(args as { text: string }).text}` }], isError: false };
      },
    };
    const defs = await mcpToolsToComponents(fakeMcp);
    expect(defs).toHaveLength(1);
    expect(defs[0]!.id).toBe("local.echo");
    const out = await defs[0]!.handle({ text: "hi" }, {} as never);
    expect((out as { text: string }).text).toBe("echo: hi");
  });

  it("throws a clear error for tool names that cannot map to component ids", async () => {
    const fakeMcp: McpClientLike = {
      listTools: async () => ({ tools: [{ name: "9bad_name" }] }),
      callTool: async () => ({ content: [] }),
    };
    await expect(mcpToolsToComponents(fakeMcp)).rejects.toThrow(/invalid ACP component id/);
  });
});
