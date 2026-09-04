/**
 * acp — CLI for discovering, invoking and serving ACP components.
 */
import { Command } from "commander";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { spawn } from "node:child_process";
import { AcpClient } from "@xi-era/acp-sdk/client";
import type { AcpReply, ComponentDescriptor } from "@xi-era/acp-sdk/client";
import { AcpError } from "@xi-era/acp-sdk/client";
import type { AcpRequest, ComponentDef } from "@xi-era/acp-sdk/server";
import { AcpServer } from "@xi-era/acp-sdk/server";

const program = new Command();

program
  .name("acp")
  .description("ACP (Agent-Component-Protocol) — discover, invoke and serve remote components")
  .version("0.1.0");

interface CommonOpts {
  header?: string[];
  timeout?: string;
}

/** Ensures the URL points at the POST /acp endpoint. */
function normalizeUrl(url: string): string {
  const trimmed = url.replace(/\/+$/, "");
  return trimmed.endsWith("/acp") ? trimmed : `${trimmed}/acp`;
}

function makeClient(url: string, opts: CommonOpts): AcpClient {
  const headers: Record<string, string> = {};
  for (const h of opts.header ?? []) {
    const idx = h.indexOf("=");
    if (idx <= 0) throw new Error(`bad header (expected k=v): ${h}`);
    headers[h.slice(0, idx)] = h.slice(idx + 1);
  }
  return new AcpClient({
    url: normalizeUrl(url),
    headers,
    timeoutMs: opts.timeout ? Number(opts.timeout) : 30_000,
  });
}

function printEnvelope(kind: string, envelope: unknown): void {
  process.stderr.write(`--> ${kind}: ${JSON.stringify(envelope)}\n`);
}

/** Unwraps a reply; the static type says ok:true but the wire can say otherwise. */
function assertOk(reply: AcpReply): { result: unknown } {
  if (reply.ok !== true) {
    const err = reply as unknown as { error: { code: number; message: string } };
    throw new AcpError(err.error.code, err.error.message);
  }
  return reply;
}

function fail(error: unknown): never {
  if (error instanceof AcpError) {
    process.stderr.write(`acp: error ${error.code}: ${error.message}\n`);
    if (error.data !== undefined) process.stderr.write(`${JSON.stringify(error.data, null, 2)}\n`);
  } else {
    process.stderr.write(`acp: ${error instanceof Error ? error.message : String(error)}\n`);
  }
  process.exit(1);
}

function printDescriptorTable(components: ComponentDescriptor[]): void {
  if (components.length === 0) {
    console.log("(no components)");
    return;
  }
  const pad = (s: string, n: number) => (s.length > n ? s.slice(0, n - 1) + "…" : s.padEnd(n));
  console.log(`${pad("ID", 32)}${pad("VERSION", 10)}${pad("STREAM", 8)}DESCRIPTION`);
  for (const c of components) {
    console.log(`${pad(c.id, 32)}${pad(c.version, 10)}${pad(String(c.stream), 8)}${c.description}`);
  }
}

function parseInput(inputJson?: string, file?: string): unknown {
  if (file) {
    try {
      return JSON.parse(readFileSync(file, "utf8"));
    } catch (e) {
      throw new Error(`cannot read/parse input file ${file}: ${e instanceof Error ? e.message : e}`);
    }
  }
  if (!inputJson) return undefined;
  try {
    return JSON.parse(inputJson);
  } catch {
    throw new Error("inputJson must be valid JSON (or use -f <file>)");
  }
}

// ---------------------------------------------------------------------------
// acp discover
// ---------------------------------------------------------------------------
program
  .command("discover")
  .description("list components exposed by an ACP server")
  .argument("<url>", "server base URL, e.g. http://localhost:8080")
  .option("--tags <tags>", "comma-separated tag filter")
  .option("--json", "print raw JSON envelopes")
  .action(async (url: string, opts: CommonOpts & { tags?: string; json?: boolean }) => {
    try {
      const client = makeClient(url, opts);
      const tags = opts.tags
        ? opts.tags.split(",").map((t) => t.trim()).filter(Boolean)
        : undefined;
      if (opts.json) {
        const reply = await client.request({ op: "discover", ...(tags ? { tags } : {}) });
        console.log(JSON.stringify(reply, null, 2));
      } else {
        const reply = assertOk(await client.request({ op: "discover", ...(tags ? { tags } : {}) }));
        printDescriptorTable((reply.result as { components: ComponentDescriptor[] }).components);
      }
      await client.close();
    } catch (e) {
      fail(e);
    }
  });

// ---------------------------------------------------------------------------
// acp describe
// ---------------------------------------------------------------------------
program
  .command("describe")
  .description("show one component's descriptor")
  .argument("<url>", "server base URL")
  .argument("<componentId>", "component id, e.g. sensor.temperature")
  .option("--json", "print raw JSON")
  .action(async (url: string, componentId: string, opts: CommonOpts & { json?: boolean }) => {
    try {
      const client = makeClient(url, opts);
      const components = await client.discover(componentId);
      if (components.length === 0) {
        process.stderr.write(`acp: component not found: ${componentId}\n`);
        process.exit(1);
      }
      console.log(JSON.stringify(components[0], null, 2));
      await client.close();
    } catch (e) {
      fail(e);
    }
  });

// ---------------------------------------------------------------------------
// acp call
// ---------------------------------------------------------------------------
program
  .command("call")
  .description("invoke a component")
  .argument("<url>", "server base URL")
  .argument("<componentId>", "component id")
  .argument("[inputJson]", "input as JSON string")
  .option("-f, --file <path>", "read input from a JSON file")
  .option("--stream", "request streamed chunked reply")
  .option("--raw", "print only the result value (no envelope)")
  .option("--trace", "print request/reply envelopes to stderr")
  .option("-H, --header <k=v>", "extra HTTP headers (repeatable)", collect, undefined)
  .option("--timeout <ms>", "call timeout in milliseconds")
  .action(
    async (
      url: string,
      componentId: string,
      inputJson: string | undefined,
      opts: CommonOpts & {
        file?: string;
        stream?: boolean;
        raw?: boolean;
        trace?: boolean;
      }
    ) => {
      try {
        const client = makeClient(url, opts);
        const input = parseInput(inputJson, opts.file) ?? {};
        if (opts.stream) {
          const reqId = `cli-${Date.now()}`;
          const request: AcpRequest = {
            acp: "0.1",
            id: reqId,
            op: "call",
            component: componentId,
            input,
            stream: true,
          };
          if (opts.trace) printEnvelope("request", request);
          for await (const chunk of client.callStream(componentId, input)) {
            const out = opts.raw ? chunk.data : { chunk };
            console.log(JSON.stringify(out));
          }
        } else {
          if (opts.trace) {
            printEnvelope("request", { op: "call", component: componentId, input });
          }
          const result = await client.call(componentId, input);
          console.log(opts.raw ? JSON.stringify(result) : JSON.stringify({ ok: true, result }, null, 2));
        }
        await client.close();
      } catch (e) {
        fail(e);
      }
    }
  );

function collect(value: string, previous: string[]): string[] {
  return [...(previous ?? []), value];
}

// ---------------------------------------------------------------------------
// acp info
// ---------------------------------------------------------------------------
program
  .command("info")
  .description("show server info (name / version / protocol)")
  .argument("<url>", "server base URL")
  .action(async (url: string, opts: CommonOpts) => {
    try {
      const client = makeClient(url, opts);
      const reply = assertOk(await client.request({ op: "discover" }));
      const { server, components } = reply.result as {
        server: { name: string; version: string; protocol: string };
        components: ComponentDescriptor[];
      };
      console.log(`name:     ${server.name}`);
      console.log(`version:  ${server.version}`);
      console.log(`protocol: ACP ${server.protocol}`);
      console.log(`components: ${components.length}`);
      await client.close();
    } catch (e) {
      fail(e);
    }
  });

// ---------------------------------------------------------------------------
// acp serve
// ---------------------------------------------------------------------------
interface ServeModule {
  name?: string;
  version?: string;
  components?: ComponentDef[];
  default?: { name?: string; version?: string; components?: ComponentDef[] } | ComponentDef[];
}

program
  .command("serve")
  .description("serve a module that exports components (named export `components`)")
  .argument("<modulePath>", "path to a JS/TS module exporting `components`")
  .option("--port <port>", "HTTP+WS port", "8080")
  .option("--stdio", "serve over stdin/stdout instead of HTTP")
  .option("--watch", "restart on file changes (node --watch)")
  .action(async (modulePath: string, opts: { port: string; stdio?: boolean; watch?: boolean }) => {
    if (opts.watch && !process.env["ACP_WATCH_CHILD"]) {
      // Re-exec under node --watch; flag must not loop.
      const child = spawn(
        process.execPath,
        ["--watch", process.argv[1]!, ...process.argv.slice(2).filter((a) => a !== "--watch")],
        { stdio: "inherit", env: { ...process.env, ACP_WATCH_CHILD: "1" } }
      );
      child.on("exit", (code) => process.exit(code ?? 0));
      return;
    }

    const abs = resolve(process.cwd(), modulePath);
    let mod: ServeModule;
    try {
      mod = (await import(pathToFileURL(abs).href)) as ServeModule;
    } catch (e) {
      fail(new Error(`cannot load module ${abs}: ${e instanceof Error ? e.message : e}`));
    }
    const components = mod.components ?? (Array.isArray(mod.default) ? mod.default : mod.default?.components);
    if (!components || components.length === 0) {
      fail(new Error(`module ${abs} exports no \`components\` array`));
    }
    const server = new AcpServer({
      name: mod.name ?? "acp-serve",
      version: mod.version ?? "0.0.0",
    });
    for (const c of components) server.register(c);

    if (opts.stdio) {
      await server.serveStdio();
    } else {
      const { port } = await server.listen({ port: Number(opts.port) });
      process.stderr.write(`ACP server listening on http://localhost:${port}/acp (ws://localhost:${port}/acp)\n`);
      process.stderr.write(`  discover: curl http://localhost:${port}/acp/discover\n`);
    }
  });

program.parseAsync(process.argv).catch(fail);
