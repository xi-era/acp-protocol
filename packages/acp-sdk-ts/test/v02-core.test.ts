import { describe, expect, it } from "vitest";
import { AcpServer } from "../src/server.js";
import { defineComponent } from "../src/component.js";
import { AcpClient } from "../src/client.js";
import { createMemoryClient } from "../src/memory-transport.js";
import { AcpErrorCode } from "../src/errors.js";
import { PROTOCOL_VERSION } from "../src/codec.js";

function makeServer() {
  const server = new AcpServer({ name: "v02-node" });
  server.register(
    defineComponent({
      id: "v2.sensor",
      name: "Sensor",
      description: "Emits readings",
      tags: ["iot", "sensor"],
      handle: () => ({ celsius: 21 }),
    })
  );
  return server;
}

async function makeClient(server = makeServer()) {
  const client = new AcpClient({ transport: createMemoryClient(server), timeoutMs: 5_000 });
  await client.connect();
  return client;
}

describe("v0.2 reserved ops over memory transport", () => {
  it("$ping roundtrips with pong + echoed ts (spec v0.2 §4.3)", async () => {
    const server = makeServer();
    const transport = createMemoryClient(server);
    await transport.connect();
    const before = Date.now();
    const reply = await transport.request({
      acp: PROTOCOL_VERSION,
      id: "ka-1",
      op: "$ping",
      input: { ts: 12345 },
    } as any);
    const after = Date.now();
    expect((reply as { ok?: boolean }).ok).toBe(true);
    const result = (reply as unknown as { result: { pong: number; ts: number } }).result;
    expect(result.ts).toBe(12345);
    expect(result.pong).toBeGreaterThanOrEqual(before);
    expect(result.pong).toBeLessThanOrEqual(after);
    await transport.close();
  });

  it("responses echo the request's acp value (spec v0.2 §5.3)", async () => {
    const server = makeServer();
    const transport = createMemoryClient(server);
    await transport.connect();
    const reply = await transport.request({ acp: "0.2", id: "e1", op: "discover" } as any);
    expect((reply as { acp: string }).acp).toBe("0.2");
    const bad = await transport.request({ acp: "9.9", id: "e2", op: "discover" } as any);
    expect((bad as { acp: string }).acp).toBe("9.9");
    await transport.close();
  });

  it("subscribe by component -> emit -> receive event -> unsubscribe (spec v0.2 §4.4/§6.2)", async () => {
    const server = makeServer();
    const client = await makeClient(server);
    const events: unknown[] = [];
    const sub = await client.subscribe({ component: "v2.sensor" }, (ev) => events.push(ev));

    server.emit({ component: "v2.sensor", data: { celsius: 22 } });
    expect(events).toEqual([{ component: "v2.sensor", tags: ["iot", "sensor"], data: { celsius: 22 } }]);

    await sub.unsubscribe();
    server.emit({ component: "v2.sensor", data: { celsius: 23 } });
    expect(events).toHaveLength(1);
    await client.close();
  });

  it("subscribe by tags matches subset of event tags", async () => {
    const server = makeServer();
    const client = await makeClient(server);
    const events: unknown[] = [];
    await client.subscribe({ tags: ["iot"] }, (ev) => events.push(ev));
    server.emit({ component: "v2.sensor", data: 1 });
    expect(events).toHaveLength(1);
    server.emit({ tags: ["other"], data: 2 }); // no matching subscription
    expect(events).toHaveLength(1);
    await client.close();
  });

  it("ctx.emit attributes events to the component (spec v0.2 §6.2)", async () => {
    const server = new AcpServer({ name: "emit-ctx" });
    const received: unknown[] = [];
    server.register(
      defineComponent({
        id: "v2.emitter",
        name: "Emitter",
        description: "Emits from handler",
        tags: ["tag-x"],
        handle: (_input: unknown, ctx: { emit: (e: { data: unknown }) => void }) => {
          ctx.emit({ data: "hello" });
          return "ok";
        },
      })
    );
    const client = await makeClient(server);
    await client.subscribe({ component: "v2.emitter" }, (ev) => received.push(ev));
    await client.call("v2.emitter", {});
    expect(received).toEqual([{ component: "v2.emitter", tags: ["tag-x"], data: "hello" }]);
    await client.close();
  });

  it("rejects malformed reserved-op input with 40001", async () => {
    const server = makeServer();
    const transport = createMemoryClient(server);
    await transport.connect();
    const both = await transport.request({
      acp: PROTOCOL_VERSION,
      id: "b1",
      op: "$subscribe",
      input: { component: "v2.sensor", tags: ["iot"] },
    } as any);
    expect(both).toMatchObject({ ok: false, error: { code: AcpErrorCode.INVALID_ENVELOPE } });
    const neither = await transport.request({
      acp: PROTOCOL_VERSION,
      id: "b2",
      op: "$subscribe",
      input: {},
    } as any);
    expect(neither).toMatchObject({ ok: false, error: { code: AcpErrorCode.INVALID_ENVELOPE } });
    await transport.close();
  });

  it("enforces per-connection subscription limit (42902)", async () => {
    const server = new AcpServer({ name: "limit", events: { maxSubscriptionsPerConn: 2 } });
    server.register(
      defineComponent({
        id: "v2.sensor",
        name: "Sensor",
        description: "Emits readings",
        tags: ["iot", "sensor"],
        handle: () => ({ celsius: 21 }),
      })
    );
    const client = await makeClient(server);
    await client.subscribe({ component: "v2.sensor" }, () => {});
    await client.subscribe({ tags: ["iot"] }, () => {});
    await expect(client.subscribe({ tags: ["sensor"] }, () => {})).rejects.toMatchObject({
      code: AcpErrorCode.SUBSCRIPTION_LIMIT,
    });
    await client.close();
  });

  it("unsubscribe-all with null input clears every subscription", async () => {
    const server = makeServer();
    const client = await makeClient(server);
    const events: unknown[] = [];
    await client.subscribe({ tags: ["iot"] }, (ev) => events.push(ev));
    await client.request({ op: "$unsubscribe", input: null });
    server.emit({ component: "v2.sensor", data: 1 });
    expect(events).toHaveLength(0);
    await client.close();
  });

  it("fallback ladder: 0.2 client retries with 0.1 against a 0.1 server (spec v0.2 §12.2)", async () => {
    const server = new AcpServer({ name: "legacy-node", protocolVersion: "0.1" });
    server.register(
      defineComponent({
        id: "v2.sensor",
        name: "Sensor",
        description: "Emits readings",
        handle: () => ({ ok: true }),
      })
    );
    const client = await makeClient(server); // declares "0.2"
    // First request gets 40003 -> retry with "0.1" -> success, version locked
    const result = await client.call<{ ok: boolean }>("v2.sensor", {});
    expect(result).toEqual({ ok: true });
    // Subsequent requests keep using the locked version (server acp echo proves it)
    const reply = await client.request({ op: "discover" });
    expect((reply as { acp: string }).acp).toBe("0.1");
    await client.close();
  });
});
