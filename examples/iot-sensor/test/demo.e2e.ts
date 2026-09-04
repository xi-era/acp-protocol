import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AcpClient } from "@xi-era/acp-sdk/client";
import { start } from "../src/index.js";

describe("demo: iot-sensor (virtual hardware as ACP components)", () => {
  let handle: { acpPort: number; close(): Promise<void> };
  let client: AcpClient;

  beforeAll(async () => {
    handle = await start({ intervalMs: 5 });
    client = new AcpClient({ url: `http://127.0.0.1:${handle.acpPort}/acp`, timeoutMs: 10_000 });
  });

  afterAll(async () => {
    await client.close();
    await handle.close();
  });

  it("reads the temperature once", async () => {
    const reading = await client.call<{ celsius: number }>("sensor.temperature", {});
    expect(reading.celsius).toBeGreaterThan(10);
    expect(reading.celsius).toBeLessThan(30);
  });

  it("streams live readings with seq + terminal end frame", async () => {
    const chunks: { seq: number; end: boolean }[] = [];
    for await (const chunk of client.callStream("sensor.temperature.stream", { n: 5 })) {
      chunks.push({ seq: chunk.seq, end: chunk.end });
      if (chunk.end) expect(chunk.data).toBeNull();
      else expect((chunk.data as { celsius: number }).celsius).toBeGreaterThan(10);
    }
    expect(chunks.map((c) => c.seq)).toEqual([0, 1, 2, 3, 4, 5]);
    expect(chunks.at(-1)!.end).toBe(true);
  });

  it("pings the device", async () => {
    const ping = await client.call<{ alive: boolean }>("sensor.ping", {});
    expect(ping.alive).toBe(true);
  });
});
