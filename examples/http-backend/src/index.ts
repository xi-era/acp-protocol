/**
 * Demo 1 — wrap a plain HTTP backend API as ACP components.
 *
 * The "legacy backend" is a tiny express service with /api/orders endpoints.
 * ACP components (biz.order.*) wrap its HTTP API with fetch, so any AI Agent
 * can call the business system without knowing HTTP details.
 */
import express from "express";
import { AcpError, AcpErrorCode } from "@xi-era/acp-sdk/server";
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";

interface Order {
  id: number;
  item: string;
  qty: number;
  status: "created" | "shipped";
}

// ---------------------------------------------------------------------------
// The "legacy" HTTP backend
// ---------------------------------------------------------------------------

const orders = new Map<number, Order>();
let nextId = 1;

export function createBackendApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.get("/api/orders/:id", (req, res) => {
    const order = orders.get(Number(req.params["id"]));
    if (!order) res.status(404).json({ error: "order not found" });
    else res.json(order);
  });
  app.post("/api/orders", (req, res) => {
    const { item, qty } = req.body as { item?: string; qty?: number };
    if (!item || !qty) res.status(400).json({ error: "item and qty are required" });
    else {
      const order: Order = { id: nextId++, item, qty, status: "created" };
      orders.set(order.id, order);
      res.status(201).json(order);
    }
  });
  app.post("/api/orders/:id/ship", (req, res) => {
    const order = orders.get(Number(req.params["id"]));
    if (!order) res.status(404).json({ error: "order not found" });
    else {
      order.status = "shipped";
      res.json(order);
    }
  });
  return app;
}

// ---------------------------------------------------------------------------
// ACP components wrapping the backend API
// ---------------------------------------------------------------------------

async function backendFetch(port: number, path: string, init?: RequestInit): Promise<unknown> {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, init);
  const body = (await res.json()) as unknown;
  if (!res.ok) {
    // Translate backend failures into ACP semantics (spec §8: 50002 UPSTREAM_ERROR).
    throw new AcpError(AcpErrorCode.UPSTREAM_ERROR, `backend returned ${res.status}`, body);
  }
  return body;
}

export function createAcpServer(backendPort: number): AcpServer {
  const server = new AcpServer({ name: "biz-backend-acp", version: "1.0.0" });

  server.register(
    defineComponent({
      id: "biz.order.create",
      name: "Create Order",
      description: "Creates an order in the business backend",
      version: "1.0.0",
      inputSchema: {
        type: "object",
        properties: { item: { type: "string" }, qty: { type: "integer", minimum: 1 } },
        required: ["item", "qty"],
      },
      outputSchema: {
        type: "object",
        properties: { id: { type: "number" }, item: { type: "string" } },
        required: ["id"],
      },
      tags: ["biz", "orders"],
      async handle(input: { item: string; qty: number }) {
        return backendFetch(backendPort, "/api/orders", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(input),
        });
      },
    })
  );

  server.register(
    defineComponent({
      id: "biz.order.get",
      name: "Get Order",
      description: "Fetches one order by id",
      version: "1.0.0",
      inputSchema: {
        type: "object",
        properties: { id: { type: "integer", minimum: 1 } },
        required: ["id"],
      },
      tags: ["biz", "orders"],
      async handle(input: { id: number }) {
        return backendFetch(backendPort, `/api/orders/${input.id}`);
      },
    })
  );

  server.register(
    defineComponent({
      id: "biz.order.ship",
      name: "Ship Order",
      description: "Marks an order as shipped",
      version: "1.0.0",
      inputSchema: {
        type: "object",
        properties: { id: { type: "integer", minimum: 1 } },
        required: ["id"],
      },
      tags: ["biz", "orders"],
      async handle(input: { id: number }) {
        return backendFetch(backendPort, `/api/orders/${input.id}/ship`, { method: "POST" });
      },
    })
  );

  return server;
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

export async function start(opts: { backendPort?: number; acpPort?: number } = {}) {
  const app = createBackendApp();
  const backendPort = opts.backendPort ?? 0;
  const backendHttp = app.listen(backendPort);
  await new Promise<void>((resolve) => backendHttp.once("listening", resolve));
  const actualBackendPort = (backendHttp.address() as { port: number }).port;

  const acp = createAcpServer(actualBackendPort);
  const { port } = await acp.listen({ port: opts.acpPort ?? 0 });
  return {
    backendPort: actualBackendPort,
    acpPort: port,
    close: async () => {
      await acp.shutdown();
      await new Promise<void>((resolve) => backendHttp.close(() => resolve()));
    },
  };
}

const isMain = process.argv[1]?.replace(/\\/g, "/").endsWith("index.js");
if (isMain) {
  start({ backendPort: Number(process.env["BACKEND_PORT"] ?? 0), acpPort: Number(process.env["ACP_PORT"] ?? 8081) })
    .then(({ acpPort, backendPort }) => {
      console.log(`backend API:  http://localhost:${backendPort}/api/orders`);
      console.log(`ACP endpoint: http://localhost:${acpPort}/acp`);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
