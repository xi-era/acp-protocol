import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AcpClient } from "@xi-era/acp-sdk/client";
import { buildComponents } from "../src/http-proxy.js";
import { sqliteQuery } from "../src/sqlite-query.js";
import { buildServer } from "../src/mock-iot.js";
import { AcpServer } from "@xi-era/acp-sdk/server";

describe("template: mock-iot", () => {
  let server: AcpServer;
  let client: AcpClient;

  beforeAll(async () => {
    server = buildServer();
    const { port } = await server.listen({ port: 0 });
    // WS client: events require a stateful transport (HTTP has none per spec)
    client = new AcpClient({ url: `ws://127.0.0.1:${port}/acp`, timeoutMs: 10_000 });
    await client.connect();
  });

  afterAll(async () => {
    await client.close();
    await server.shutdown();
  });

  it("reads temperature and humidity", async () => {
    const t = await client.call<{ celsius: number }>("sensor.iot.temperature", {});
    expect(t.celsius).toBeGreaterThan(10);
    const h = await client.call<{ humidity: number }>("sensor.iot.humidity", {});
    expect(h.humidity).toBeGreaterThan(30);
  });

  it("streams readings and pushes $event to subscribers (v0.2)", async () => {
    const events: number[] = [];
    const sub = await client.subscribe({ component: "sensor.iot.watch" }, (ev) => {
      events.push((ev.data as { seq: number }).seq);
    });
    const chunks: number[] = [];
    for await (const chunk of client.callStream("sensor.iot.watch", { n: 3 })) {
      if (!chunk.end) chunks.push((chunk.data as { seq: number }).seq);
    }
    expect(chunks).toEqual([0, 1, 2]);
    // events include the ones from this stream (seq 0..2), plus none after unsubscribe
    expect(events).toContain(0);
    expect(events).toContain(2);
    await sub.unsubscribe();
  });
});

describe("template: sqlite-query", () => {
  let server: AcpServer;
  let client: AcpClient;

  beforeAll(async () => {
    server = new AcpServer({ name: "sqlite-test" });
    server.register(sqliteQuery);
    const { port } = await server.listen({ port: 0 });
    client = new AcpClient({ url: `http://127.0.0.1:${port}/acp`, timeoutMs: 10_000 });
  });

  afterAll(async () => {
    await client.close();
    await server.shutdown();
  });

  it("runs read-only queries", async () => {
    const result = await client.call<{ rows: { title: string }[]; count: number }>("db.query", {
      sql: "SELECT title FROM books ORDER BY year",
    });
    expect(result.count).toBe(2);
    expect(result.rows[0]!.title).toBe("The Pragmatic Programmer");
  });

  it("rejects write statements", async () => {
    await expect(client.call("db.query", { sql: "DELETE FROM books" })).rejects.toMatchObject({
      code: 50001,
    });
  });
});

describe("template: http-proxy", () => {
  it("builds one component per endpoint config", () => {
    const components = buildComponents();
    expect(components.map((c) => c.id)).toEqual(["http.jsonplaceholder.user", "http.jsonplaceholder.post"]);
    expect(components[0]!.inputSchema).toBeDefined();
  });
});
