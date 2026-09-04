import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AcpClient } from "@xi-era/acp-sdk/client";
import { start } from "../src/index.js";

describe("demo: http-backend (express API wrapped as ACP components)", () => {
  let handle: { acpPort: number; close(): Promise<void> };
  let client: AcpClient;

  beforeAll(async () => {
    handle = await start();
    client = new AcpClient({ url: `http://127.0.0.1:${handle.acpPort}/acp`, timeoutMs: 10_000 });
  });

  afterAll(async () => {
    await client.close();
    await handle.close();
  });

  it("discovers biz.order.* components", async () => {
    const components = await client.discover();
    expect(components.map((c) => c.id)).toEqual(
      expect.arrayContaining(["biz.order.create", "biz.order.get", "biz.order.ship"])
    );
  });

  it("creates, reads and ships an order through ACP", async () => {
    const created = await client.call<{ id: number; status: string }>("biz.order.create", {
      item: "acp-starter-kit",
      qty: 2,
    });
    expect(created.status).toBe("created");

    const fetched = await client.call<{ id: number }>("biz.order.get", { id: created.id });
    expect(fetched.id).toBe(created.id);

    const shipped = await client.call<{ status: string }>("biz.order.ship", { id: created.id });
    expect(shipped.status).toBe("shipped");
  });

  it("surfaces backend errors as ACP errors", async () => {
    const missing = await client.call("biz.order.get", { id: 99999 }).catch((e) => e);
    expect(missing).toMatchObject({ code: 50002 });
  });

  it("validates input against inputSchema (42200)", async () => {
    await expect(client.call("biz.order.create", { item: 42 })).rejects.toMatchObject({ code: 42200 });
  });
});
