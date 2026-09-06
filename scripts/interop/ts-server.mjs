// Shared TS ACP server for interop tests. Usage: node ts-server.mjs <port>
import { AcpServer, defineComponent } from "../../packages/acp-sdk-ts/dist/index.js";

const port = Number(process.argv[2] ?? 8611);
const server = new AcpServer({ name: "interop-ts-node", version: "1.0.0" });

server.register(
  defineComponent({
    id: "interop.echo",
    name: "Echo",
    description: "Echoes msg",
    inputSchema: { type: "object", properties: { msg: { type: "string" } }, required: ["msg"] },
    tags: ["interop"],
    handle: (input) => ({ msg: input.msg }),
  })
);
server.register(
  defineComponent({
    id: "interop.counter",
    name: "Counter",
    description: "Streams n items",
    stream: true,
    tags: ["interop", "stream"],
    handle: async function* (input) {
      for (let i = 0; i < input.n; i++) yield { i };
    },
  })
);

await server.listen({ port });
console.log(`ready:${port}`);
setInterval(() => {}, 60_000); // keep alive
