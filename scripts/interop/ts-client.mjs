// TS ACP client for interop tests. Usage: node ts-client.mjs <port>
// Asserts discover / call / stream / $ping against whichever server is up.
import { AcpClient } from "../../packages/acp-sdk-ts/dist/client/index.js";

const port = process.argv[2] ?? "8612";
const client = new AcpClient({ url: `http://127.0.0.1:${port}/acp`, timeoutMs: 10_000 });

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

try {
  await client.connect();

  const comps = await client.discover();
  assert(comps.some((c) => c.id === "interop.echo"), `missing interop.echo in ${comps.map((c) => c.id)}`);

  const result = await client.call("interop.echo", { msg: "hola" });
  assert(JSON.stringify(result) === JSON.stringify({ msg: "hola" }), `unexpected result ${JSON.stringify(result)}`);

  const seqs = [];
  for await (const chunk of client.callStream("interop.counter", { n: 3 })) seqs.push(chunk.seq);
  assert(JSON.stringify(seqs) === JSON.stringify([0, 1, 2, 3]), `unexpected seq ${seqs}`);

  const ping = await client.request({ op: "$ping", input: { ts: 42 } });
  assert(ping.ok === true && ping.result.ts === 42, `bad ping ${JSON.stringify(ping)}`);
  assert(ping.acp === "0.2", `expected acp echo, got ${ping.acp}`);

  await client.close();
  process.exit(0);
} catch (e) {
  console.error(String(e));
  process.exit(1);
}
