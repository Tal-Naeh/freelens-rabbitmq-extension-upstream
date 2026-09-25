import { describe, expect, it } from "vitest";
import { matchesMessage, matchesReason, parseDeath, summarizeDeaths } from "./dead-letter";

import type { PeekedMessageDto } from "../common/ipc";

function message(properties: Record<string, unknown> | unknown[], overrides: Partial<PeekedMessageDto> = {}) {
  return {
    index: 0,
    payload: '{"order":42}',
    payloadEncoding: "string",
    payloadBytes: 12,
    truncated: false,
    exchange: "",
    routingKey: "orders.dlq",
    redelivered: false,
    messageCount: 0,
    properties,
    contentKind: "json",
    ...overrides,
  } as PeekedMessageDto;
}

// Headers exactly as the Management API returned them on RabbitMQ 4.3.5 (2026-09-24).
const rejected = message({
  message_id: "m-42",
  content_type: "application/json",
  headers: {
    "x-death": [
      {
        count: 1,
        exchange: "shop",
        queue: "orders",
        reason: "rejected",
        "routing-keys": ["order.created"],
        time: 1790274631,
      },
    ],
    "x-first-death-exchange": "shop",
    "x-first-death-queue": "orders",
    "x-first-death-reason": "rejected",
    "x-last-death-exchange": "shop",
    "x-last-death-queue": "orders",
    "x-last-death-reason": "rejected",
  },
});

const expired = message(
  {
    headers: {
      "x-death": [
        {
          count: 1,
          exchange: "",
          queue: "short-ttl",
          reason: "expired",
          "routing-keys": ["short-ttl"],
          time: 1790274631,
        },
      ],
      "x-first-death-queue": "short-ttl",
      "x-first-death-reason": "expired",
      "x-last-death-queue": "short-ttl",
      "x-last-death-reason": "expired",
    },
  },
  { index: 1, payload: "expire me", contentKind: "text" },
);

// Died in `orders` (delivery limit), was retried through `orders.retry`, then expired there.
const multiHop = message(
  {
    headers: {
      "x-death": [
        {
          count: 2,
          exchange: "",
          queue: "orders.retry",
          reason: "expired",
          "routing-keys": ["orders.retry"],
          time: 1790274700,
        },
        {
          count: 1,
          exchange: "shop",
          queue: "orders",
          reason: "delivery_limit",
          "routing-keys": ["order.created"],
          time: 1790274600,
        },
      ],
      "x-first-death-queue": "orders",
      "x-first-death-reason": "delivery_limit",
      "x-last-death-queue": "orders.retry",
      "x-last-death-reason": "expired",
    },
  },
  { index: 2, payload: "poison" },
);

const plain = message([], { index: 3, payload: "never failed", routingKey: "orders" });

describe("parseDeath", () => {
  it("decodes a single rejection, with the original destination for a replay", () => {
    expect(parseDeath(rejected)).toEqual({
      history: [
        {
          queue: "orders",
          reason: "rejected",
          count: 1,
          exchange: "shop",
          routingKeys: ["order.created"],
          time: 1790274631000,
        },
      ],
      total: 1,
      firstQueue: "orders",
      firstReason: "rejected",
      lastQueue: "orders",
      lastReason: "rejected",
      original: { exchange: "shop", routingKeys: ["order.created"], queue: "orders" },
    });
  });

  it("takes the original destination from the first death on a multi-hop history", () => {
    const death = parseDeath(multiHop);
    expect(death?.total).toBe(3);
    expect(death?.lastReason).toBe("expired");
    expect(death?.firstReason).toBe("delivery_limit");
    expect(death?.original).toEqual({ exchange: "shop", routingKeys: ["order.created"], queue: "orders" });
  });

  it("returns undefined for messages that were never dead-lettered, including `properties: []`", () => {
    expect(parseDeath(plain)).toBeUndefined();
    expect(parseDeath(message({ headers: { "x-custom": "1" } }))).toBeUndefined();
  });

  it("tolerates malformed entries and falls back to the x-first-death headers", () => {
    const death = parseDeath(
      message({
        headers: {
          "x-death": [null, { queue: "q" }, "junk"],
          "x-first-death-queue": "q",
          "x-first-death-exchange": "ex",
        },
      }),
    );
    expect(death?.history).toEqual([]);
    expect(death?.original).toEqual({ exchange: "ex", routingKeys: [], queue: "q" });
  });
});

describe("summarizeDeaths", () => {
  it("counts messages by the reason and queue of their most recent death", () => {
    expect(summarizeDeaths([rejected, expired, multiHop, plain])).toEqual({
      total: 4,
      deadLettered: 3,
      byReason: [
        { reason: "expired", messages: 2 },
        { reason: "rejected", messages: 1 },
      ],
      byQueue: [
        { queue: "orders", messages: 1 },
        { queue: "orders.retry", messages: 1 },
        { queue: "short-ttl", messages: 1 },
      ],
    });
  });
});

describe("matchesMessage and matchesReason", () => {
  it("searches payload, routing key, exchange, properties and headers, case-insensitively", () => {
    expect(matchesMessage(rejected, '"ORDER":42')).toBe(true);
    expect(matchesMessage(rejected, "m-42")).toBe(true);
    expect(matchesMessage(rejected, "order.created")).toBe(true);
    expect(matchesMessage(rejected, "nothing-like-this")).toBe(false);
    expect(matchesMessage(rejected, "  ")).toBe(true);
  });

  it("does not search base64 payloads as text", () => {
    const binary = message([], { payload: "aGVsbG8=", payloadEncoding: "base64", contentKind: "binary" });
    expect(matchesMessage(binary, "aGVs")).toBe(false);
  });

  it("summarises and filters a message with only partial headers under the same 'unknown' reason", () => {
    const partial = message({ headers: { "x-first-death-queue": "orders" } }, { index: 9 });
    expect(summarizeDeaths([partial]).byReason).toEqual([{ reason: "unknown", messages: 1 }]);
    expect(matchesReason(partial, "unknown")).toBe(true);
  });

  it("filters by the most recent death reason, or by never dead-lettered", () => {
    const all = [rejected, expired, multiHop, plain];
    expect(all.filter((m) => matchesReason(m, "expired")).map((m) => m.index)).toEqual([1, 2]);
    expect(all.filter((m) => matchesReason(m, "none")).map((m) => m.index)).toEqual([3]);
    expect(all.filter((m) => matchesReason(m, "")).length).toBe(4);
  });
});
