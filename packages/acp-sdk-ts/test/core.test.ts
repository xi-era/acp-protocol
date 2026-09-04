import { describe, expect, it } from "vitest";
import { AcpServer, binaryChunk } from "../src/server.js";
import { defineComponent } from "../src/component.js";
import { AcpClient } from "../src/client.js";
import { createMemoryClient } from "../src/memory-transport.js";
import { AcpError, AcpErrorCode } from "../src/errors.js";
import { PROTOCOL_VERSION } from "../src/codec.js";

function makeServer() {
  const echo = defineComponent({
    id: "test.echo",
    name: "Echo",
    description: "Returns the input back",
    inputSchema: { type: "object", properties: { msg: { type: "string" } } },
    outputSchema: { type: "object", properties: { msg: { type: "string" } }, required: ["msg"] },
    tags: ["test"],
    handle: (input: { msg?: string }) => ({ msg: input.msg ?? "empty" }),
  });

  const counter = defineComponent({
    id: "test.counter",
    name: "Counter",
    description: "Streams n numbers",
    stream: true,
    tags: ["test", "stream"],
    handle: async function* (input: { n: number }) {
      for (let i = 0; i < input.n; i++) yield { i };
    },
  });

  const failing = defineComponent({
    id: "test.failing",
    name: "Failing",
    description: "Always throws",
    handle: () => {
      throw new Error("boom");
    },
  });

  const acpFailing = defineComponent({
    id: "test.acp-failing",
    name: "AcpFailing",
    description: "Throws AcpError with upstream code",
    handle: () => {
      throw new AcpError(AcpErrorCode.UPSTREAM_ERROR, "device offline", { device: "uart-1" });
    },
  });

  const binary = defineComponent({
    id: "test.binary",
    name: "Binary",
    description: "Streams one binary chunk",
    stream: true,
    handle: async function* () {
      yield binaryChunk(Buffer.from("hello").toString("base64"));
    },
  });

  const server = new AcpServer({ name: "test-node", version: "1.2.3" });
  server.register(echo).register(counter).register(failing).register(acpFailing).register(binary);
  return server;
}

function makeClient(server: AcpServer): AcpClient {
  return new AcpClient({ transport: createMemoryClient(server), timeoutMs: 5_000 });
}

describe("AcpServer over memory transport (spec conformance, core)", () => {
  it("handles discover with server info and descriptors", async () => {
    const client = makeClient(makeServer());
    await client.connect();
    const { server, components } = (await client.request({ op: "discover" })).result as any;
    expect(server).toEqual({ name: "test-node", version: "1.2.3", protocol: "0.1" });
    const ids = components.map((c: any) => c.id);
    expect(ids).toContain("test.echo");
    expect(ids).toContain("test.counter");
    const echo = components.find((c: any) => c.id === "test.echo");
    expect(echo.version).toBe("0.0.0");
    expect(echo.stream).toBe(false);
    expect(echo.inputSchema.type).toBe("object");
    await client.close();
  });

  it("discovers a single component (array shape)", async () => {
    const client = makeClient(makeServer());
    const found = await client.discover("test.echo");
    expect(found).toHaveLength(1);
    expect(found[0]!.id).toBe("test.echo");
    expect(await client.discover("nope.missing")).toHaveLength(0);
    await client.close();
  });

  it("filters by tags (intersection)", async () => {
    const client = makeClient(makeServer());
    const both = await client.discover();
    expect(both.length).toBe(5);
    const streamOnly = await (async () => {
      const reply = await client.request({ op: "discover", tags: ["test", "stream"] });
      return (reply.result as any).components;
    })();
    expect(streamOnly).toHaveLength(1);
    expect(streamOnly[0].id).toBe("test.counter");
    await client.close();
  });

  it("calls a component and returns the bare result", async () => {
    const client = makeClient(makeServer());
    const result = await client.call<{ msg: string }>("test.echo", { msg: "hi" });
    expect(result).toEqual({ msg: "hi" });
    await client.close();
  });

  it("rejects invalid input with 42200 + structured errors", async () => {
    const client = makeClient(makeServer());
    const err = (await client.call("test.echo", { msg: 42 }).catch((e) => e)) as AcpError;
    expect(err).toBeInstanceOf(AcpError);
    expect(err.code).toBe(AcpErrorCode.INVALID_INPUT);
    expect((err.data as { errors: unknown[] }).errors).toBeDefined();
    await client.close();
  });

  it("returns 40400 for unknown components", async () => {
    const client = makeClient(makeServer());
    await expect(client.call("nope.missing", {})).rejects.toMatchObject({
      code: AcpErrorCode.COMPONENT_NOT_FOUND,
    });
    await client.close();
  });

  it("returns 40003 with supported list on version mismatch", async () => {
    const server = makeServer();
    const transport = createMemoryClient(server);
    await transport.connect();
    const reply = await transport.request({
      acp: "9.9",
      id: "v1",
      op: "discover",
    } as any);
    expect(reply).toMatchObject({
      ok: false,
      id: "v1",
      error: { code: AcpErrorCode.UNSUPPORTED_VERSION, data: { supported: ["0.1"] } },
    });
    await transport.close();
  });

  it("streams chunks with sequential seq and a terminal end frame", async () => {
    const client = makeClient(makeServer());
    const chunks: any[] = [];
    for await (const chunk of client.callStream("test.counter", { n: 3 })) {
      chunks.push(chunk);
    }
    expect(chunks).toHaveLength(4); // 3 data + 1 end
    expect(chunks.map((c) => c.seq)).toEqual([0, 1, 2, 3]);
    expect(chunks[3]!.end).toBe(true);
    expect(chunks[3]!.data).toBeNull();
    expect(chunks.slice(0, 3).map((c) => c.data)).toEqual([{ i: 0 }, { i: 1 }, { i: 2 }]);
    await client.close();
  });

  it("returns 40005 STREAM_REQUIRED when calling a stream component without stream:true", async () => {
    const client = makeClient(makeServer());
    await expect(client.call("test.counter", { n: 1 })).rejects.toMatchObject({
      code: AcpErrorCode.STREAM_REQUIRED,
    });
    await client.close();
  });

  it("wraps a non-streaming component called with stream:true into one chunk", async () => {
    const client = makeClient(makeServer());
    const chunks: any[] = [];
    for await (const chunk of client.callStream("test.echo", { msg: "x" })) {
      chunks.push(chunk);
    }
    expect(chunks).toHaveLength(1);
    expect(chunks[0]).toMatchObject({ seq: 0, end: true, data: { msg: "x" } });
    await client.close();
  });

  it("streams base64 binary chunks (spec §6.1 bin)", async () => {
    const client = makeClient(makeServer());
    const chunks: any[] = [];
    for await (const chunk of client.callStream("test.binary")) chunks.push(chunk);
    expect(chunks[0]!.bin).toBe(true);
    expect(Buffer.from(chunks[0]!.data, "base64").toString()).toBe("hello");
    expect(chunks[1]!.end).toBe(true);
    await client.close();
  });

  it("maps handler exceptions to 50001", async () => {
    const client = makeClient(makeServer());
    await expect(client.call("test.failing", {})).rejects.toMatchObject({
      code: AcpErrorCode.COMPONENT_ERROR,
      message: expect.stringContaining("boom"),
    });
    await client.close();
  });

  it("passes AcpError codes through (50002 with data)", async () => {
    const client = makeClient(makeServer());
    const err = (await client.call("test.acp-failing", {}).catch((e) => e)) as AcpError;
    expect(err).toBeInstanceOf(AcpError);
    expect(err.code).toBe(AcpErrorCode.UPSTREAM_ERROR);
    expect(err.data).toEqual({ device: "uart-1" });
    await client.close();
  });

  it("ignores unknown meta and unknown top-level fields (forward compat)", async () => {
    const client = makeClient(makeServer());
    const result = await client.request({
      op: "call",
      component: "test.echo",
      input: { msg: "ok" },
      meta: { auth: "bearer x", scopes: ["t"], traceId: "tr", timeoutMs: 100, vendor: "z" },
      vendorField: { anything: true },
    });
    expect(result.ok).toBe(true);
    expect((result.result as any).msg).toBe("ok");
    await client.close();
  });

  it("responds 40002/40001 for malformed envelopes via low-level request", async () => {
    const server = makeServer();
    const transport = createMemoryClient(server);
    await transport.connect();
    const bad1 = await transport.request({ acp: PROTOCOL_VERSION, id: "x", op: "exec" } as any);
    expect(bad1).toMatchObject({ ok: false, error: { code: AcpErrorCode.UNKNOWN_OP } });
    const bad2 = await transport.request({ acp: PROTOCOL_VERSION, op: "discover" } as any);
    expect(bad2).toMatchObject({ ok: false, error: { code: AcpErrorCode.INVALID_ENVELOPE } });
    await transport.close();
  });

  it("times out a hung call with 50400", async () => {
    const slow = defineComponent({
      id: "test.slow",
      name: "Slow",
      description: "Never resolves",
      handle: () => new Promise(() => {}),
    });
    const server = new AcpServer({ name: "slow-node" });
    server.register(slow);
    const client = new AcpClient({ transport: createMemoryClient(server), timeoutMs: 50 });
    await client.connect();
    await expect(client.call("test.slow")).rejects.toMatchObject({ code: AcpErrorCode.TIMEOUT });
    await client.close();
  });
});
