/** Conformance over HTTP and WebSocket (both served by one server.listen() port). */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AcpServer } from "../../src/server.js";
import { defineComponent } from "../../src/component.js";
import { AcpClient } from "../../src/client.js";
import { runConformanceSuite, type ConformanceContext } from "./suite.js";

function makeServer(): AcpServer {
  const server = new AcpServer({ name: "conf-node", version: "1.0.0" });
  server.register(
    defineComponent({
      id: "conf.echo",
      name: "Echo",
      description: "Echoes msg",
      inputSchema: { type: "object", properties: { msg: { type: "string" } } },
      tags: ["conf"],
      handle: (input: { msg?: string }) => ({ msg: input.msg ?? "" }),
    })
  );
  server.register(
    defineComponent({
      id: "conf.counter",
      name: "Counter",
      description: "Streams n items",
      stream: true,
      handle: async function* (input: { n: number }) {
        for (let i = 0; i < input.n; i++) yield { i };
      },
    })
  );
  server.register(
    defineComponent({
      id: "conf.failing",
      name: "Failing",
      description: "Always throws",
      handle: () => {
        throw new Error("conf boom");
      },
    })
  );
  return server;
}

describe("conformance: HTTP", () => {
  let server: AcpServer;
  let port: number;

  beforeAll(async () => {
    server = makeServer();
    ({ port } = await server.listen({ port: 0 }));
  });

  afterAll(async () => {
    await server.shutdown();
  });

  it("passes the conformance suite", async () => {
    const client = new AcpClient({ url: `http://127.0.0.1:${port}/acp`, timeoutMs: 5_000 });
    const ctx: ConformanceContext = {
      client,
      sendRaw: async (text) => {
        const res = await fetch(`http://127.0.0.1:${port}/acp`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: text,
        });
        return (await res.json()) as { ok: false; error: { code: number } };
      },
    };
    await runConformanceSuite(ctx);
    await client.close();
  });

  it("serves GET /acp/discover (spec §9.1 SHOULD)", async () => {
    const res = await fetch(`http://127.0.0.1:${port}/acp/discover`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { server: { name: string }; components: unknown[] };
    expect(body.server.name).toBe("conf-node");
    expect(body.components.length).toBe(3);
  });

  it("serves GET /acp/health (spec §9.1 MAY)", async () => {
    const res = await fetch(`http://127.0.0.1:${port}/acp/health`);
    expect(await res.json()).toEqual({ ok: true });
  });

  it("maps error codes to HTTP status (spec §9.3)", async () => {
    const res = await fetch(`http://127.0.0.1:${port}/acp`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ acp: "0.1", id: "h1", op: "call", component: "absent.component", input: {} }),
    });
    expect(res.status).toBe(404);
    const body = (await res.json()) as { ok: boolean; error: { code: number } };
    expect(body.ok).toBe(false);
    expect(body.error.code).toBe(40400);
  });
});

describe("conformance: WebSocket", () => {
  let server: AcpServer;
  let port: number;

  beforeAll(async () => {
    server = makeServer();
    ({ port } = await server.listen({ port: 0 }));
  });

  afterAll(async () => {
    await server.shutdown();
  });

  it("passes the conformance suite", async () => {
    const client = new AcpClient({ url: `ws://127.0.0.1:${port}/acp`, timeoutMs: 5_000 });
    await client.connect();
    await runConformanceSuite({ client });
    await client.close();
  });

  it("multiplexes concurrent calls by id (spec §10.3)", async () => {
    const client = new AcpClient({ url: `ws://127.0.0.1:${port}/acp`, timeoutMs: 5_000 });
    await client.connect();
    const calls = await Promise.all([
      client.call("conf.echo", { msg: "a" }),
      client.call("conf.echo", { msg: "b" }),
      client.call("conf.echo", { msg: "c" }),
    ]);
    expect(calls).toEqual([{ msg: "a" }, { msg: "b" }, { msg: "c" }]);
    await client.close();
  });
});
