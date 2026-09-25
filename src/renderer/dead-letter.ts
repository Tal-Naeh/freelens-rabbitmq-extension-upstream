/**
 * Decode RabbitMQ dead-letter headers (`x-death`, `x-first-death-*`) on peeked messages, summarise
 * why a batch failed, and search messages. Pure: no IPC, no React.
 *
 * Shape as returned by the Management API (verified on RabbitMQ 4.3.5): `x-death` is a list, most
 * recent death first, one entry per (queue, reason) with `count`, `exchange`, `queue`, `reason`,
 * `routing-keys` (list) and `time` in seconds since the epoch.
 */
import type { PeekedMessageDto } from "../common/ipc";

export interface DeathEntry {
  queue: string;
  reason: string;
  count: number;
  /** Exchange and routing keys the message had when it died in `queue`. */
  exchange: string;
  routingKeys: string[];
  /** Epoch ms. */
  time?: number;
}

export interface DeathInfo {
  /** Most recent first, as the broker orders it. */
  history: DeathEntry[];
  /** Total times the message was dead-lettered, across queues and reasons. */
  total: number;
  firstQueue?: string;
  firstReason?: string;
  lastQueue?: string;
  lastReason?: string;
  /**
   * Where the message was first published to: the exchange and routing keys of its first death.
   * A replay to the original destination would publish here.
   */
  original?: { exchange: string; routingKeys: string[]; queue: string };
}

/** What each dead-letter reason means, for the inspector. */
export const DEATH_REASON_TEXT: Record<string, string> = {
  rejected: "A consumer rejected or nacked it without requeueing.",
  expired: "Its time-to-live ran out (message TTL or the queue's x-message-ttl).",
  maxlen: "The queue was over its length limit (x-max-length / x-max-length-bytes).",
  delivery_limit: "It was redelivered more times than the quorum queue's delivery limit allows.",
};

/** Header keys the decoded view replaces; the raw Headers list hides them. */
export const DEATH_HEADER_KEYS = new Set([
  "x-death",
  "x-first-death-queue",
  "x-first-death-reason",
  "x-first-death-exchange",
  "x-last-death-queue",
  "x-last-death-reason",
  "x-last-death-exchange",
]);

function str(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function headersOf(message: PeekedMessageDto): Record<string, unknown> {
  // The Management API sends `properties: []` when a message has none.
  const headers = (message.properties as { headers?: unknown }).headers;
  return headers && typeof headers === "object" && !Array.isArray(headers) ? (headers as Record<string, unknown>) : {};
}

function toEntry(raw: unknown): DeathEntry | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const r = raw as Record<string, unknown>;
  const queue = str(r.queue);
  const reason = str(r.reason);
  if (!queue || !reason) return undefined;
  const count = typeof r.count === "number" && r.count > 0 ? r.count : 1;
  const keys = Array.isArray(r["routing-keys"])
    ? r["routing-keys"].filter((k): k is string => typeof k === "string")
    : [];
  return {
    queue,
    reason,
    count,
    exchange: str(r.exchange) ?? "",
    routingKeys: keys,
    time: typeof r.time === "number" ? r.time * 1000 : undefined,
  };
}

/** Dead-letter details of a message, or undefined when it was never dead-lettered. */
export function parseDeath(message: PeekedMessageDto): DeathInfo | undefined {
  const headers = headersOf(message);
  const raw = headers["x-death"];
  const history = (Array.isArray(raw) ? raw : []).map(toEntry).filter((e): e is DeathEntry => Boolean(e));
  const firstQueue = str(headers["x-first-death-queue"]);
  const firstReason = str(headers["x-first-death-reason"]);
  if (history.length === 0 && !firstQueue) return undefined;

  const firstEntry =
    history.find((e) => e.queue === firstQueue && (!firstReason || e.reason === firstReason)) ?? history.at(-1);
  const original = firstEntry
    ? { exchange: firstEntry.exchange, routingKeys: firstEntry.routingKeys, queue: firstEntry.queue }
    : firstQueue
      ? { exchange: str(headers["x-first-death-exchange"]) ?? "", routingKeys: [], queue: firstQueue }
      : undefined;

  return {
    history,
    total: history.reduce((s, e) => s + e.count, 0) || 1,
    firstQueue: firstQueue ?? firstEntry?.queue,
    firstReason: firstReason ?? firstEntry?.reason,
    lastQueue: str(headers["x-last-death-queue"]) ?? history[0]?.queue,
    lastReason: str(headers["x-last-death-reason"]) ?? history[0]?.reason,
    original,
  };
}

/** The reason of the most recent death; "unknown" when only partial headers are present. */
export function deathReason(death: DeathInfo): string {
  return death.lastReason ?? "unknown";
}

export interface DeathSummary {
  total: number;
  deadLettered: number;
  /** Most frequent first. Counted per message by its most recent death. */
  byReason: { reason: string; messages: number }[];
  byQueue: { queue: string; messages: number }[];
}

function ranked(counts: Map<string, number>): [string, number][] {
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

/** Why a batch of peeked messages failed: counts by reason and by the queue they died in. */
export function summarizeDeaths(messages: PeekedMessageDto[]): DeathSummary {
  const reasons = new Map<string, number>();
  const queues = new Map<string, number>();
  let deadLettered = 0;
  for (const m of messages) {
    const death = parseDeath(m);
    if (!death) continue;
    deadLettered += 1;
    const reason = deathReason(death);
    const queue = death.lastQueue ?? "unknown";
    reasons.set(reason, (reasons.get(reason) ?? 0) + 1);
    queues.set(queue, (queues.get(queue) ?? 0) + 1);
  }
  return {
    total: messages.length,
    deadLettered,
    byReason: ranked(reasons).map(([reason, n]) => ({ reason, messages: n })),
    byQueue: ranked(queues).map(([queue, n]) => ({ queue, messages: n })),
  };
}

/**
 * Case-insensitive search over what an operator looks for in a failed message: payload (text
 * payloads only), routing key, exchange, and every property and header value.
 */
export function matchesMessage(message: PeekedMessageDto, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const haystack = [
    message.payloadEncoding === "string" ? message.payload : "",
    message.routingKey,
    message.exchange,
    JSON.stringify(message.properties ?? {}),
  ];
  return haystack.some((text) => text.toLowerCase().includes(q));
}

/** Filter by the reason of the most recent death; "" keeps everything, "none" keeps messages never dead-lettered. */
export function matchesReason(message: PeekedMessageDto, reason: string): boolean {
  if (!reason) return true;
  const death = parseDeath(message);
  if (reason === "none") return !death;
  return death !== undefined && deathReason(death) === reason;
}
