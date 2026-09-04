import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { start as startBiz } from "../../http-backend/src/index.js";
import { start as startIot } from "../../iot-sensor/src/index.js";
import { run } from "../src/index.js";

describe("demo: client-demo (SDK + OpenAI + MCP calling the other demos)", () => {
  let biz: { acpPort: number; close(): Promise<void> };
  let iot: { acpPort: number; close(): Promise<void> };

  beforeAll(async () => {
    biz = await startBiz();
    iot = await startIot({ intervalMs: 5 });
  });

  afterAll(async () => {
    await biz.close();
    await iot.close();
  });

  it("calls both demo servers through all three integration paths", async () => {
    const log = await run(`http://127.0.0.1:${biz.acpPort}/acp`, `http://127.0.0.1:${iot.acpPort}/acp`);

    // path 1: SDK
    expect(log.some((l) => l.startsWith("[sdk] iot components:"))).toBe(true);
    expect(log.some((l) => /^\[sdk\] sensor\.temperature -> \d/.test(l))).toBe(true);
    expect(log.some((l) => l.includes("streamed 3 readings"))).toBe(true);
    expect(log.some((l) => /\[sdk\] biz\.order\.create -> order #\d+/.test(l))).toBe(true);

    // path 2: OpenAI Tool-Call adapter
    expect(log.some((l) => l.includes("biz_order_create"))).toBe(true);
    expect(log.some((l) => l.includes("[openai] tool message:") && l.includes('"id"'))).toBe(true);

    // path 3: MCP bridge
    expect(log.some((l) => l.includes("sensor_temperature"))).toBe(true);
    expect(log.some((l) => l.includes("[mcp] sensor_ping ->"))).toBe(true);
  });
});
