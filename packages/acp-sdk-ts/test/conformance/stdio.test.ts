/** Conformance over Stdio, using in-process PassThrough streams (spec §11). */
import { PassThrough } from "node:stream";
import { afterAll, beforeAll, describe, it } from "vitest";
import { AcpServer } from "../../src/server.js";
import { defineComponent } from "../../src/component.js";
import { AcpClient } from "../../src/client.js";
import { StdioServerTransport, StdioClientTransport } from "../../src/stdio-transport.js";
import { runConformanceSuite } from "./suite.js";

function makeServer(): AcpServer {
  const server = new AcpServer({ name: "conf-stdio", version: "1.0.0" });
  server.register(
    defineComponent({
      id: "conf.echo",
      name: "Echo",
      description: "Echoes msg",
      inputSchema: { type: "object", properties: { msg: { type: "string" } } },
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

describe("conformance: Stdio", () => {
  let server: AcpServer;
  let client: AcpClient;

  beforeAll(async () => {
    server = makeServer();
    const serverInput = new PassThrough();
    const serverOutput = new PassThrough();
    await new StdioServerTransport({ input: serverInput, output: serverOutput }).start(server.handle);
    client = new AcpClient({
      transport: new StdioClientTransport({ input: serverOutput, output: serverInput }),
      timeoutMs: 5_000,
    });
    await client.connect();
  });

  afterAll(async () => {
    await client.close();
    await server.shutdown();
  });

  it("passes the conformance suite", async () => {
    await runConformanceSuite({
      client,
      sendRaw: async (text) => {
        // One-off stream pair; read the reply line directly to observe the
        // PARSE_ERROR frame (id: null) that pending-by-id routing would drop.
        const inOnce = new PassThrough();
        const outOnce = new PassThrough();
        await new StdioServerTransport({ input: inOnce, output: outOnce }).start(server.handle);
        inOnce.write(text + "\n");
        const line = await new Promise<string>((resolve) =>
          outOnce.once("data", (d) => resolve(String(d).trim()))
        );
        return JSON.parse(line) as { ok: false; error: { code: number } };
      },
    });
  });
});
