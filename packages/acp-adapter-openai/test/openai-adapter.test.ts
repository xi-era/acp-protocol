import { describe, expect, it } from "vitest";
import { componentsToOpenaiTools, createToolCallHandler } from "../src/index.js";
import type { ComponentDescriptor } from "@xi-era/acp-sdk/client";
import { AcpClient } from "@xi-era/acp-sdk/client";
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";
import { createMemoryClient } from "@xi-era/acp-sdk/server";

const descriptors: ComponentDescriptor[] = [
  {
    id: "sensor.temperature",
    name: "Temperature Sensor",
    description: "Reads current temperature",
    version: "1.0.0",
    inputSchema: { type: "object", properties: { unit: { enum: ["C", "F"] } }, required: [] },
    stream: false,
    tags: ["iot"],
  },
  {
    id: "no.schema",
    name: "No Schema",
    description: "Component without inputSchema",
    version: "0.1.0",
    stream: false,
  },
];

describe("componentsToOpenaiTools", () => {
  it("maps component_id dots to underscores and passes inputSchema through", () => {
    const tools = componentsToOpenaiTools(descriptors);
    expect(tools).toHaveLength(2);
    expect(tools[0]).toEqual({
      type: "function",
      function: {
        name: "sensor_temperature",
        description: "Reads current temperature",
        parameters: { type: "object", properties: { unit: { enum: ["C", "F"] } }, required: [] },
      },
    });
  });

  it("defaults missing inputSchema to an empty object schema", () => {
    const tools = componentsToOpenaiTools(descriptors);
    expect(tools[1]!.function.parameters).toEqual({ type: "object", properties: {}, required: [] });
  });
});

describe("createToolCallHandler", () => {
  function makeClient(): AcpClient {
    const server = new AcpServer({ name: "t" });
    server.register(
      defineComponent({
        id: "math.add",
        name: "Add",
        description: "Adds numbers",
        inputSchema: { type: "object", properties: { a: { type: "number" }, b: { type: "number" } }, required: ["a", "b"] },
        handle: (input: { a: number; b: number }) => ({ sum: input.a + input.b }),
      })
    );
    return new AcpClient({ transport: createMemoryClient(server) });
  }

  it("parses JSON arguments, calls ACP, returns tool messages", async () => {
    const handler = createToolCallHandler(makeClient());
    const messages = await handler([
      { id: "call_1", function: { name: "math_add", arguments: '{"a":1,"b":2}' } },
    ]);
    expect(messages).toEqual([
      { role: "tool", tool_call_id: "call_1", content: '{"sum":3}' },
    ]);
  });

  it("returns an error tool message for invalid JSON arguments", async () => {
    const handler = createToolCallHandler(makeClient());
    const messages = await handler([
      { id: "call_2", function: { name: "math_add", arguments: "{not json" } },
    ]);
    expect(messages[0]!.content).toContain("error");
    expect(messages[0]!.tool_call_id).toBe("call_2");
  });

  it("returns an error tool message when ACP rejects (42200)", async () => {
    const handler = createToolCallHandler(makeClient());
    const messages = await handler([
      { id: "call_3", function: { name: "math_add", arguments: '{"a":"x"}' } },
    ]);
    expect(messages[0]!.content).toContain("error");
    expect(messages[0]!.tool_call_id).toBe("call_3");
  });
});
