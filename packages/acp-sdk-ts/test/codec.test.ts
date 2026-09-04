import { describe, expect, it } from "vitest";
import {
  PROTOCOL_VERSION,
  errorEnvelope,
  isVersionSupported,
  isValidComponentId,
  parseVersion,
  validateEnvelope,
} from "../src/codec.js";
import { AcpErrorCode } from "../src/errors.js";

describe("component id grammar (spec §7.1)", () => {
  it("accepts valid ids", () => {
    expect(isValidComponentId("sensor.temperature")).toBe(true);
    expect(isValidComponentId("biz.order.refund")).toBe(true);
    expect(isValidComponentId("a.b.c.d")).toBe(true);
    expect(isValidComponentId("edge-sensor.temp-1")).toBe(true);
  });

  it("rejects invalid ids", () => {
    expect(isValidComponentId("Sensor/Temp")).toBe(false);
    expect(isValidComponentId("sensor")).toBe(false); // single segment
    expect(isValidComponentId("a.b.c.d.e")).toBe(false); // > 4 segments
    expect(isValidComponentId("1abc.def")).toBe(false); // starts with digit
    expect(isValidComponentId("sensor..temp")).toBe(false);
    expect(isValidComponentId("传感器.temp")).toBe(false);
    expect(isValidComponentId(42)).toBe(false);
  });
});

describe("version negotiation (spec §12)", () => {
  it("supports equal versions", () => {
    expect(isVersionSupported("0.1", "0.1")).toBe(true);
  });
  it("supports server minor >= client minor", () => {
    expect(isVersionSupported("0.1", "0.2")).toBe(true);
  });
  it("rejects client newer than server", () => {
    expect(isVersionSupported("0.2", "0.1")).toBe(false);
  });
  it("rejects different majors", () => {
    expect(isVersionSupported("1.0", "0.1")).toBe(false);
  });
  it("rejects malformed versions", () => {
    expect(isVersionSupported("0", "0.1")).toBe(false);
    expect(parseVersion("x.y")).toBeNull();
  });
});

describe("envelope validation (spec §3.2 order)", () => {
  const base = { acp: PROTOCOL_VERSION, id: "req-1", op: "discover" };

  it("accepts a valid discover", () => {
    const v = validateEnvelope({ ...base });
    expect(v.ok).toBe(true);
  });

  it("rejects non-objects", () => {
    expect(validateEnvelope("hello")).toMatchObject({ ok: false, code: AcpErrorCode.INVALID_ENVELOPE });
    expect(validateEnvelope(42)).toMatchObject({ ok: false });
    expect(validateEnvelope(null)).toMatchObject({ ok: false });
    expect(validateEnvelope([1])).toMatchObject({ ok: false });
  });

  it("rejects missing fields in order", () => {
    expect(validateEnvelope({})).toMatchObject({ code: AcpErrorCode.INVALID_ENVELOPE, id: null });
    expect(validateEnvelope({ id: "x" })).toMatchObject({ code: AcpErrorCode.INVALID_ENVELOPE });
    expect(validateEnvelope({ acp: "0.1", id: "x" })).toMatchObject({ code: AcpErrorCode.INVALID_ENVELOPE });
  });

  it("rejects unknown ops", () => {
    expect(validateEnvelope({ ...base, op: "exec" })).toMatchObject({ code: AcpErrorCode.UNKNOWN_OP });
  });

  it("rejects bad component ids and call without component", () => {
    expect(validateEnvelope({ ...base, op: "call", component: "BAD" })).toMatchObject({
      code: AcpErrorCode.INVALID_COMPONENT_ID,
    });
    expect(validateEnvelope({ ...base, op: "call" })).toMatchObject({
      code: AcpErrorCode.INVALID_ENVELOPE,
    });
  });

  it("rejects malformed stream/tags", () => {
    expect(validateEnvelope({ ...base, stream: "yes" })).toMatchObject({ ok: false });
    expect(validateEnvelope({ ...base, tags: "iot" })).toMatchObject({ ok: false });
  });
});

describe("error envelope", () => {
  it("omits data when absent and includes it when present", () => {
    const e = errorEnvelope("req-1", AcpErrorCode.COMPONENT_NOT_FOUND, "nope");
    expect(e.error.data).toBeUndefined();
    expect(e.id).toBe("req-1");
    const e2 = errorEnvelope(null, AcpErrorCode.PARSE_ERROR, "bad", { supported: ["0.1"] });
    expect(e2.error.data).toEqual({ supported: ["0.1"] });
    expect(e2.id).toBeNull();
  });
});
