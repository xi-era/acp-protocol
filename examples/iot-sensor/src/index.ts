/**
 * Demo 2 — a virtual IoT sensor as ACP components.
 *
 * Stands in for real hardware: sensor.temperature reads once,
 * sensor.temperature.stream pushes N readings with intervals — the streaming
 * path an Agent would consume to watch hardware live.
 */
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";

/** Deterministic pseudo-random walk so tests can assert ranges. */
function reading(seed: number, base: number): number {
  return Math.round((base + Math.sin(seed / 7) * 5 + (seed % 3)) * 10) / 10;
}

export function createSensorServer(opts: { intervalMs?: number } = {}): AcpServer {
  const intervalMs = opts.intervalMs ?? 10;
  let reads = 0;

  const server = new AcpServer({ name: "virtual-iot-node", version: "1.0.0" });

  server.register(
    defineComponent({
      id: "sensor.temperature",
      name: "Temperature Sensor",
      description: "Reads the current temperature in Celsius",
      version: "1.0.0",
      inputSchema: { type: "object", properties: {} },
      outputSchema: {
        type: "object",
        properties: { celsius: { type: "number" } },
        required: ["celsius"],
      },
      tags: ["iot", "sensor"],
      handle() {
        return { celsius: reading(reads++, 21) };
      },
    })
  );

  server.register(
    defineComponent({
      id: "sensor.temperature.stream",
      name: "Temperature Stream",
      description: "Streams n live temperature readings (and pushes $event to subscribers)",
      version: "1.0.0",
      inputSchema: {
        type: "object",
        properties: { n: { type: "integer", minimum: 1, maximum: 1000 } },
        required: ["n"],
      },
      stream: true,
      tags: ["iot", "sensor", "stream"],
      async *handle(input: { n: number }, ctx) {
        for (let i = 0; i < input.n; i++) {
          const celsius = reading(reads++, 21);
          // v0.2 demo: every reading is also pushed as an $event to subscribers
          ctx.emit({ data: { seq: i, celsius } });
          yield { seq: i, celsius };
          await new Promise((r) => setTimeout(r, intervalMs));
        }
      },
    })
  );

  server.register(
    defineComponent({
      id: "sensor.ping",
      name: "Device Ping",
      description: "Health check for the device link",
      version: "1.0.0",
      inputSchema: { type: "object", properties: {} },
      tags: ["iot"],
      handle() {
        return { alive: true, uptimeMs: Math.round(process.uptime() * 1000) };
      },
    })
  );

  return server;
}

export async function start(opts: { acpPort?: number; intervalMs?: number } = {}) {
  const server = createSensorServer(opts);
  const { port } = await server.listen({ port: opts.acpPort ?? 0 });
  return { acpPort: port, close: () => server.shutdown() };
}

const isMain = process.argv[1]?.replace(/\\/g, "/").endsWith("index.js");
if (isMain) {
  start({ acpPort: Number(process.env["ACP_PORT"] ?? 8082) }).then(({ acpPort }) => {
    console.log(`virtual IoT sensor ACP endpoint: http://localhost:${acpPort}/acp`);
  });
}
