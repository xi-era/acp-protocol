/**
 * 模板:模拟 IoT 硬件 → ACP 元件(多传感器 + $event 推送)
 *
 * 展示 ACP v0.2 的硬件场景三件套:一次性读取、流式读数、$event 事件推送。
 * 替换成真实硬件时,把 reading() 换成你的传感器 SDK 调用即可。
 *
 * 启动:pnpm start:mock-iot(或 node --experimental-strip-types src/mock-iot.ts)
 * 订阅事件演示:acp call ws://localhost:8093/acp sensor.iot.watch '{"n":5}' --stream
 */
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";

function reading(seed: number, base: number): number {
  return Math.round((base + Math.sin(seed / 7) * 5 + (seed % 3)) * 10) / 10;
}

let reads = 0;

export function buildServer(): AcpServer {
  const server = new AcpServer({ name: "mock-iot-node", version: "1.0.0" });

  // 一次性读取
  server.register(
    defineComponent({
      id: "sensor.iot.temperature",
      name: "Temperature",
      description: "Reads the current temperature (°C)",
      tags: ["iot", "template", "sensor"],
      handle: () => ({ celsius: reading(reads++, 21) }),
    })
  );

  server.register(
    defineComponent({
      id: "sensor.iot.humidity",
      name: "Humidity",
      description: "Reads the current relative humidity (%)",
      tags: ["iot", "template", "sensor"],
      handle: () => ({ humidity: reading(reads++, 55) }),
    })
  );

  // 流式 + $event:每次读数既进流,也推送给订阅者(spec v0.2 §6.2)
  server.register(
    defineComponent({
      id: "sensor.iot.watch",
      name: "Sensor Watch",
      description: "Streams n live readings and pushes $event to subscribers",
      stream: true,
      inputSchema: {
        type: "object",
        properties: { n: { type: "integer", minimum: 1, maximum: 1000 } },
        required: ["n"],
      },
      tags: ["iot", "template", "stream"],
      async *handle(input: { n: number }, ctx) {
        for (let i = 0; i < input.n; i++) {
          const celsius = reading(reads++, 21);
          ctx.emit({ data: { seq: i, celsius } });
          yield { seq: i, celsius };
          await new Promise((r) => setTimeout(r, 100));
        }
      },
    })
  );

  return server;
}

const isMain =
  process.argv[1]?.replace(/\\/g, "/").endsWith("mock-iot.ts") ||
  process.argv[1]?.replace(/\\/g, "/").endsWith("mock-iot.js");
if (isMain) {
  buildServer()
    .listen({ port: Number(process.env["ACP_PORT"] ?? 8093) })
    .then(({ port }) => console.log(`Mock-IoT ACP endpoint: http://localhost:${port}/acp`));
}
