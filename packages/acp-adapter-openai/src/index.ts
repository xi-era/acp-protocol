/**
 * ACP -> OpenAI Tool-Call adapter (spec 附录 B).
 * Zero runtime dependencies besides the SDK: pure functions plus a handler
 * factory that executes model tool_calls against an ACP client.
 */
import type { ComponentDescriptor } from "@xi-era/acp-sdk/client";
import type { AcpClient } from "@xi-era/acp-sdk/client";
import { componentIdToToolName } from "@xi-era/acp-sdk/client";

/** Minimal OpenAI tool-call types (deliberately inline — no OpenAI SDK needed). */
export interface OpenAiToolDef {
  type: "function";
  function: {
    name: string;
    description: string;
    parameters: object;
  };
}

export interface OpenAiToolCall {
  id: string;
  type?: "function";
  function: { name: string; arguments: string };
}

export interface OpenAiToolMessage {
  role: "tool";
  tool_call_id: string;
  content: string;
}

/**
 * Converts ACP component descriptors to OpenAI function tool definitions.
 * name = component_id with "." -> "_" (spec §7.1 lossless mapping);
 * parameters = the descriptor's draft-07 inputSchema, passed through as-is.
 */
export function componentsToOpenaiTools(descriptors: ComponentDescriptor[]): OpenAiToolDef[] {
  return descriptors.map((d) => ({
    type: "function" as const,
    function: {
      name: componentIdToToolName(d.id),
      description: d.description,
      parameters: d.inputSchema ?? { type: "object", properties: {}, required: [] },
    },
  }));
}

export interface ToolCallHandlerOptions {
  /** Max tool messages produced per invocation; default unlimited. */
  limit?: number;
}

/**
 * Creates a handler for the `tool_calls` array of an OpenAI assistant message.
 * Each call's JSON arguments are parsed and used as the ACP `input`;
 * the ACP result is returned serialized as the tool message content.
 */
export function createToolCallHandler(
  client: AcpClient,
  options: ToolCallHandlerOptions = {}
): (toolCalls: OpenAiToolCall[]) => Promise<OpenAiToolMessage[]> {
  return async (toolCalls) => {
    const calls = options.limit ? toolCalls.slice(0, options.limit) : toolCalls;
    return Promise.all(
      calls.map(async (toolCall): Promise<OpenAiToolMessage> => {
        const componentId = toolCall.function.name.replaceAll("_", ".");
        let input: unknown;
        try {
          input = toolCall.function.arguments ? JSON.parse(toolCall.function.arguments) : null;
        } catch {
          return {
            role: "tool",
            tool_call_id: toolCall.id,
            content: `error: arguments is not valid JSON: ${toolCall.function.arguments}`,
          };
        }
        try {
          const result = await client.call(componentId, input);
          return { role: "tool", tool_call_id: toolCall.id, content: JSON.stringify(result) };
        } catch (e) {
          const message = e instanceof Error ? e.message : String(e);
          return { role: "tool", tool_call_id: toolCall.id, content: `error: ${message}` };
        }
      })
    );
  };
}
