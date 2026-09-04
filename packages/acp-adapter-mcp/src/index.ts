/**
 * ACP <-> MCP bidirectional bridge (spec 附录 A).
 *
 * Direction 1 (ACP -> MCP): {@link serveMcpFromAcp} exposes an ACP server or
 * client as an MCP stdio server, mountable by Claude Desktop / Cursor etc.
 *
 * Direction 2 (MCP -> ACP): {@link mcpToolsToComponents} wraps an MCP client's
 * tools as ACP component definitions.
 */
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport as McpStdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import type { ComponentDescriptor } from "@xi-era/acp-sdk/client";
import type { AcpClient } from "@xi-era/acp-sdk/client";
import { AcpError, AcpErrorCode } from "@xi-era/acp-sdk/client";
import { componentIdToToolName, toolNameToComponentId, isValidComponentId } from "@xi-era/acp-sdk/client";
import type { ComponentDef } from "@xi-era/acp-sdk/server";
import { AcpServer } from "@xi-era/acp-sdk/server";
import { AcpClient as AcpClientImpl } from "@xi-era/acp-sdk/client";
import { createMemoryClient } from "@xi-era/acp-sdk/server";

// ---------------------------------------------------------------------------
// Internal unified invoker for either an in-process AcpServer or a remote client
// ---------------------------------------------------------------------------

interface AcpInvoker {
  discover(): Promise<ComponentDescriptor[]>;
  call(componentId: string, input: unknown): Promise<unknown>;
  /** Concatenates chunks (spec 附录 A: text concat / base64 concat) into one value. */
  callConcatenated(componentId: string, input: unknown): Promise<unknown>;
}

function invokerFromClient(client: AcpClient): AcpInvoker {
  return {
    discover: () => client.discover(),
    call: (id, input) => client.call(id, input),
    callConcatenated: async (id, input) => {
      let text = "";
      let binary = Buffer.alloc(0);
      let sawBinary = false;
      for await (const chunk of client.callStream(id, input)) {
        if (chunk.bin === true) {
          sawBinary = true;
          binary = Buffer.concat([binary, Buffer.from(String(chunk.data), "base64")]);
        } else if (typeof chunk.data === "string") {
          text += chunk.data;
        } else if (chunk.data !== null) {
          text += JSON.stringify(chunk.data);
        }
      }
      return sawBinary ? binary.toString("base64") : text;
    },
  };
}

function invokerFromServer(server: AcpServer): AcpInvoker {
  const client = new AcpClientImpl({ transport: createMemoryClient(server) });
  return invokerFromClient(client);
}

// ---------------------------------------------------------------------------
// Direction 1: ACP -> MCP
// ---------------------------------------------------------------------------

export interface ServeMcpOptions {
  /** MCP server name advertised to clients; default "acp-bridge". */
  name?: string;
  /** MCP server version; default "0.1.0". */
  version?: string;
}

function descriptorToMcpTool(d: ComponentDescriptor) {
  return {
    name: componentIdToToolName(d.id),
    description: d.description,
    inputSchema: d.inputSchema ?? { type: "object", properties: {}, required: [] },
  };
}

/** Concatenates streamed chunks for MCP consumption (text concat / base64 concat). */
function chunkDataToString(data: unknown): string {
  if (data === null || data === undefined) return "";
  if (typeof data === "string") return data;
  return JSON.stringify(data);
}

/**
 * Builds the MCP server that exposes the target's ACP components as MCP tools.
 * tools/list <- discover; tools/call <- call (retrying with stream and
 * concatenating chunks when the component requires streaming).
 */
export function createMcpBridgeServer(
  target: AcpServer | AcpClient,
  options: ServeMcpOptions = {}
): Server {
  const invoker =
    target instanceof AcpServer ? invokerFromServer(target) : invokerFromClient(target);

  const mcpServer = new Server(
    { name: options.name ?? "acp-bridge", version: options.version ?? "0.1.0" },
    { capabilities: { tools: {} } }
  );

  mcpServer.setRequestHandler(ListToolsRequestSchema, async () => {
    const descriptors = await invoker.discover();
    return { tools: descriptors.map(descriptorToMcpTool) };
  });

  mcpServer.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    const componentId = toolNameToComponentId(name);
    try {
      let result: unknown;
      try {
        result = await invoker.call(componentId, args ?? null);
      } catch (e) {
        if (e instanceof AcpError && e.code === AcpErrorCode.STREAM_REQUIRED) {
          result = await invoker.callConcatenated(componentId, args ?? null);
        } else {
          throw e;
        }
      }
      return {
        content: [{ type: "text" as const, text: chunkDataToString(result) || JSON.stringify(result) }],
        isError: false,
      };
    } catch (e) {
      const message =
        e instanceof AcpError
          ? `ACP error ${e.code}: ${e.message}`
          : e instanceof Error
            ? e.message
            : String(e);
      return { content: [{ type: "text" as const, text: message }], isError: true };
    }
  });

  return mcpServer;
}

/**
 * Serves {@link createMcpBridgeServer} over MCP stdio (Claude Desktop / Cursor).
 */
export async function serveMcpFromAcp(
  target: AcpServer | AcpClient,
  options: ServeMcpOptions = {}
): Promise<void> {
  const mcpServer = createMcpBridgeServer(target, options);
  await mcpServer.connect(new McpStdioServerTransport());
}

// ---------------------------------------------------------------------------
// Direction 2: MCP -> ACP
// ---------------------------------------------------------------------------

/** Minimal structural subset of the MCP Client class used by the bridge. */
export interface McpClientLike {
  listTools(): Promise<{ tools: { name: string; description?: string; inputSchema?: object }[] }>;
  callTool(request: { name: string; arguments?: unknown }): Promise<{
    content?: { type: string; text?: string }[];
    isError?: boolean;
  }>;
}

interface McpToolContentText {
  type: string;
  text?: string;
}

/**
 * Wraps an MCP client's tools as ACP component definitions. Tool names map
 * back via "_" -> "." (spec §7.1); names that do not form a valid
 * component_id are rejected with a clear error.
 */
export async function mcpToolsToComponents(
  mcpClient: McpClientLike
): Promise<ComponentDef[]> {
  const { tools } = await mcpClient.listTools();
  return tools.map((tool) => {
    const id = toolNameToComponentId(tool.name);
    if (!isValidComponentId(id)) {
      throw new Error(
        `MCP tool name "${tool.name}" maps to invalid ACP component id "${id}" (spec §7.1)`
      );
    }
    return {
      id,
      name: tool.name,
      description: tool.description ?? tool.name,
      version: "0.0.0",
      inputSchema: tool.inputSchema,
      stream: false,
      meta: { viaMcpBridge: true },
      handle: async (input: unknown) => {
        const result = await mcpClient.callTool({ name: tool.name, arguments: input ?? {} });
        const parts: McpToolContentText[] = result.content ?? [];
        const text = parts
          .map((p) => (p.type === "text" ? (p.text ?? "") : JSON.stringify(p)))
          .join("\n");
        if (result.isError) {
          throw new AcpError(AcpErrorCode.UPSTREAM_ERROR, text || "MCP tool error");
        }
        return { mcp: { content: parts }, text };
      },
    };
  });
}
