/**
 * Conformance suite (spec executable annotation): the same set of behaviors
 * must hold over every transport. New language SDKs should pass this suite too.
 */
import { expect } from "vitest";
import type { AcpClient } from "../../src/client.js";
import { AcpError, AcpErrorCode } from "../../src/errors.js";

export interface ConformanceContext {
  client: AcpClient;
  /** Protocol version the server reports; default "0.2". */
  protocolVersion?: string;
  /** Sends raw non-JSON text; used to assert PARSE_ERROR. Optional per transport. */
  sendRaw?: (text: string) => Promise<{ ok: false; error: { code: number } }>;
}

export async function runConformanceSuite(ctx: ConformanceContext): Promise<void> {
  const protocolVersion = ctx.protocolVersion ?? "0.2";
  const { client } = ctx;

  // spec §4.1 discover: fixed result shape with server info
  const reply = await client.request({ op: "discover" });
  expect(reply.ok).toBe(true);
  const result = reply.result as { server: { name: string; protocol: string }; components: { id: string }[] };
  expect(result.server.protocol).toBe(protocolVersion);
  expect(result.components.map((c) => c.id)).toEqual(
    expect.arrayContaining(["conf.echo", "conf.counter", "conf.failing"])
  );

  // spec §4.1 single lookup: still an array
  expect(await client.discover("conf.echo")).toHaveLength(1);
  expect(await client.discover("absent.component")).toHaveLength(0);

  // spec §5.1 bare result
  expect(await client.call<{ msg: string }>("conf.echo", { msg: "ping" })).toEqual({ msg: "ping" });

  // spec §8 42200
  const invalid = (await client.call("conf.echo", { msg: 42 }).catch((e) => e)) as AcpError;
  expect(invalid).toBeInstanceOf(AcpError);
  expect(invalid.code).toBe(AcpErrorCode.INVALID_INPUT);

  // spec §8 40400
  await expect(client.call("absent.component", {})).rejects.toMatchObject({
    code: AcpErrorCode.COMPONENT_NOT_FOUND,
  });

  // spec §6 streaming: seq order + terminal end frame
  const chunks: { seq: number; end: boolean; data: unknown }[] = [];
  for await (const chunk of client.callStream("conf.counter", { n: 3 })) chunks.push(chunk);
  expect(chunks.map((c) => c.seq)).toEqual([0, 1, 2, 3]);
  expect(chunks[3]!.end).toBe(true);
  expect(chunks.slice(0, 3).map((c) => c.data)).toEqual([{ i: 0 }, { i: 1 }, { i: 2 }]);

  // spec §4.2 40005 STREAM_REQUIRED
  await expect(client.call("conf.counter", { n: 1 })).rejects.toMatchObject({
    code: AcpErrorCode.STREAM_REQUIRED,
  });

  // spec §8 50001 handler exception
  await expect(client.call("conf.failing", {})).rejects.toMatchObject({
    code: AcpErrorCode.COMPONENT_ERROR,
  });

  // spec §12 40003 version negotiation
  const badVersion = (await client.request({ acp: "9.9", op: "discover" })) as unknown as {
    ok: false;
    error: { code: number; data?: { supported?: string[] } };
  };
  expect(badVersion.ok).toBe(false);
  expect(badVersion.error.code).toBe(AcpErrorCode.UNSUPPORTED_VERSION);
  expect(badVersion.error.data?.supported).toContain(protocolVersion);

  // spec v0.2 §5.3: responses echo the request's acp value
  const echo = await client.request({ op: "discover" });
  expect((echo as { acp?: string }).acp ?? "absent").toBe("0.2");

  // spec §13 meta ignored
  const withMeta = await client.request({
    op: "call",
    component: "conf.echo",
    input: { msg: "meta" },
    meta: { auth: "bearer x", traceId: "tr-1", vendor: "ignored" },
  });
  expect(withMeta.ok).toBe(true);

  // spec §3.2 40000 parse error (transport-dependent hook)
  if (ctx.sendRaw) {
    const parseError = await ctx.sendRaw("this is not json");
    expect(parseError.ok).toBe(false);
    expect(parseError.error.code).toBe(AcpErrorCode.PARSE_ERROR);
  }
}
