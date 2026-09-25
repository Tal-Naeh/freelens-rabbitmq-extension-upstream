/**
 * Health checks for one RabbitMQ target, computed from data the other pages already load
 * (overview, queues, channels). Pure: no IPC, no React, so every rule is unit tested.
 *
 * The rules follow the failure classes operators hit most: resource alarms that block publishers,
 * backlogs nobody consumes, consumers stuck at their prefetch, poison messages being redelivered,
 * silently unroutable publishes and quorum queues that lost replicas.
 */
import type { ChannelDto, NodeDto, OverviewDto, QueueSummaryDto } from "../common/ipc";

export type HealthSeverity = "critical" | "warning" | "info";
export type HealthCategory = "node" | "queue" | "consumer" | "routing";

export type HealthSubject =
  | { kind: "node"; name: string }
  | { kind: "queue"; vhost: string; name: string }
  | { kind: "channel"; name: string }
  | { kind: "cluster" };

export interface HealthFinding {
  /** Stable id (rule + subject) so React keys and table rows stay put across refreshes. */
  id: string;
  rule: string;
  severity: HealthSeverity;
  category: HealthCategory;
  title: string;
  detail: string;
  subject: HealthSubject;
}

export interface HealthInput {
  overview: OverviewDto;
  queues: QueueSummaryDto[];
  channels: ChannelDto[];
  /**
   * When each channel was first seen full with no acks, by channel name (see {@link trackStuckChannels}).
   * One sample cannot tell a hung consumer from one busy with a long job, so a full channel is only
   * a warning once it has stayed that way for {@link HEALTH_THRESHOLDS.stuckForMs}.
   */
  stuckSince?: Record<string, number>;
  now?: number;
}

/** Thresholds; exported so tests and the "What is checked" panel use the same numbers. */
export const HEALTH_THRESHOLDS = {
  /** Fraction of the memory high watermark at which a node is flagged before the alarm fires. */
  memoryRatio: 0.8,
  /** A node is flagged when free disk drops below this multiple of `disk_free_limit`. */
  diskFreeLimitMultiple: 2,
  /** File descriptors, sockets and Erlang processes. */
  resourceRatio: 0.8,
  /** Ready messages above which a queue whose publish rate exceeds its deliver rate is "growing". */
  growingBacklog: 1_000,
  /** Consumer capacity below which consumers cannot keep up with a backlog. */
  consumerCapacity: 0.5,
  /** Unacked messages on a channel without any prefetch limit. */
  unboundedUnacked: 1_000,
  /** How long a channel must stay full with no acks before it is reported as stuck. */
  stuckForMs: 5 * 60_000,
} as const;

/**
 * Queues whose name says they collect failures; a backlog there is parked, not stuck. `dlq`, `dlx`,
 * `dead-letter` and `parking-lot` count anywhere as a name segment; `error(s)` and `failed` only as
 * the last one, so a work queue like `error-reporting` is not mistaken for a dead-letter queue.
 */
const DEAD_LETTER_NAME =
  /(^|[._\-:])(dlq|dlx|dead[._-]?letter(s|ed)?|parking[._-]?lot)([._\-:]|$)|(^|[._\-:])(errors?|failed)$/i;

/** Names of queues other queues dead-letter into through the default exchange (`x-dead-letter-routing-key`). */
function deadLetterTargets(queues: QueueSummaryDto[]): Set<string> {
  const targets = new Set<string>();
  for (const q of queues) {
    const exchange = q.arguments["x-dead-letter-exchange"];
    const routingKey = q.arguments["x-dead-letter-routing-key"];
    if (exchange === "" && typeof routingKey === "string" && routingKey) targets.add(`${q.vhost}\u0000${routingKey}`);
  }
  return targets;
}

export function isDeadLetterQueue(queue: QueueSummaryDto, targets: Set<string> = new Set()): boolean {
  return DEAD_LETTER_NAME.test(queue.name) || targets.has(`${queue.vhost}\u0000${queue.name}`);
}

const SEVERITY_ORDER: Record<HealthSeverity, number> = { critical: 0, warning: 1, info: 2 };

function ratio(used: number | undefined, total: number | undefined): number | undefined {
  if (used === undefined || total === undefined || total <= 0) return undefined;
  return used / total;
}

function pct(value: number): string {
  return `${Math.round(value * 100)}%`;
}

/** `rabbit@orders-broker-server-0.orders-broker-nodes.shop` -> `orders-broker-server-0`; IP hosts stay whole. */
function shortNode(name: string): string {
  const at = name.indexOf("@");
  const host = at >= 0 ? name.slice(at + 1) : name;
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(host) ? host : host.split(".")[0];
}

function queueLabel(q: QueueSummaryDto): string {
  return q.vhost === "/" ? q.name : `${q.vhost}/${q.name}`;
}

function plural(n: number, word: string): string {
  return `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`;
}

function nodeFindings(node: NodeDto): HealthFinding[] {
  const out: HealthFinding[] = [];
  const subject: HealthSubject = { kind: "node", name: node.name };
  const add = (rule: string, severity: HealthSeverity, title: string, detail: string) =>
    out.push({ id: `${rule}:${node.name}`, rule, severity, category: "node", title, detail, subject });
  const label = shortNode(node.name);

  if (!node.running) {
    add("node-down", "critical", `Node ${label} is not running`, "Queues led by this node are unavailable.");
    return out;
  }
  if (node.partitions.length > 0) {
    add(
      "node-partitioned",
      "critical",
      `Node ${label} is partitioned`,
      `It cannot see: ${node.partitions.map(shortNode).join(", ")}.`,
    );
  }
  if (node.memAlarm) {
    add(
      "memory-alarm",
      "critical",
      `Memory alarm on ${label}`,
      "Publishers are blocked cluster-wide until memory use drops below the high watermark.",
    );
  } else {
    const r = ratio(node.memUsed, node.memLimit);
    if (r !== undefined && r >= HEALTH_THRESHOLDS.memoryRatio) {
      add(
        "memory-high",
        "warning",
        `Memory at ${pct(r)} of the watermark on ${label}`,
        "The memory alarm blocks all publishers once the watermark is reached. Unacked messages and large backlogs are the usual cause.",
      );
    }
  }
  if (node.diskFreeAlarm) {
    add(
      "disk-alarm",
      "critical",
      `Disk alarm on ${label}`,
      "Publishers are blocked cluster-wide until free disk rises above the limit.",
    );
  } else if (
    node.diskFree !== undefined &&
    node.diskFreeLimit !== undefined &&
    node.diskFreeLimit > 0 &&
    node.diskFree < node.diskFreeLimit * HEALTH_THRESHOLDS.diskFreeLimitMultiple
  ) {
    add(
      "disk-low",
      "warning",
      `Free disk close to the limit on ${label}`,
      `Less than ${HEALTH_THRESHOLDS.diskFreeLimitMultiple}× disk_free_limit is left; the disk alarm blocks all publishers.`,
    );
  }
  const resources: [string, string, number | undefined, number | undefined, string][] = [
    ["fd-high", "File descriptors", node.fdUsed, node.fdTotal, "New connections are refused once they run out."],
    ["sockets-high", "Sockets", node.socketsUsed, node.socketsTotal, "New connections are refused once they run out."],
    [
      "procs-high",
      "Erlang processes",
      node.procUsed,
      node.procTotal,
      "Steady growth usually means leaked connections or channels.",
    ],
  ];
  for (const [rule, what, used, total, why] of resources) {
    const r = ratio(used, total);
    if (r !== undefined && r >= HEALTH_THRESHOLDS.resourceRatio) {
      add(rule, "warning", `${what} at ${pct(r)} on ${label}`, why);
    }
  }
  return out;
}

function queueFindings(
  q: QueueSummaryDto,
  runningNodes: number,
  downNodes: Set<string>,
  dlqTargets: Set<string>,
): HealthFinding[] {
  const out: HealthFinding[] = [];
  const subject: HealthSubject = { kind: "queue", vhost: q.vhost, name: q.name };
  const add = (rule: string, severity: HealthSeverity, category: HealthCategory, title: string, detail: string) =>
    out.push({ id: `${rule}:${q.vhost}/${q.name}`, rule, severity, category, title, detail, subject });
  const label = queueLabel(q);
  const state = q.state.toLowerCase();
  const offline = q.members && q.online ? q.members.filter((m) => !q.online?.includes(m)) : [];
  const offlineText = offline.length > 0 ? ` Offline: ${offline.map(shortNode).join(", ")}.` : "";

  if (state === "crashed" || state === "terminated") {
    add(
      "queue-crashed",
      "critical",
      "queue",
      `Queue ${label} is ${state}`,
      "The queue process is not serving clients.",
    );
  } else if (state === "down" || state === "minority" || state === "stopped") {
    add(
      "queue-unavailable",
      "warning",
      "queue",
      `Queue ${label} is ${state}`,
      (state === "minority"
        ? "Fewer than a majority of its replicas are online, so it cannot accept writes."
        : "The queue is not available on its node.") + offlineText,
    );
  } else if (offline.some((m) => !downNodes.has(m)) && q.members) {
    // Replicas on a node reported as down are counted in that node's finding instead, so one
    // stopped node does not produce a warning per quorum queue.
    add(
      "replicas-offline",
      "warning",
      "queue",
      `Queue ${label} has ${offline.length} of ${q.members.length} replicas offline`,
      offlineText.trim(),
    );
  }
  if (
    q.type === "quorum" &&
    offline.length === 0 &&
    q.members &&
    runningNodes > 1 &&
    q.members.length < Math.min(3, runningNodes)
  ) {
    add(
      "under-replicated",
      "info",
      "queue",
      `Quorum queue ${label} has ${plural(q.members.length, "replica")} on ${runningNodes} nodes`,
      "It will not survive losing a node. `rabbitmq-queues grow` adds replicas on the other nodes.",
    );
  }

  // Streams keep their messages after delivery and are read by offset, often over the stream
  // protocol, so ready counts and consumer numbers say nothing about a backlog there.
  if (q.type === "stream") return out;

  if (q.ready > 0 && q.consumers === 0) {
    if (isDeadLetterQueue(q, dlqTargets)) {
      add(
        "dead-letters-parked",
        "info",
        "queue",
        `${plural(q.ready, "message")} parked in ${label}`,
        "Looks like a dead-letter queue. Inspect the messages to find why they failed.",
      );
    } else {
      add(
        "no-consumers",
        "warning",
        "consumer",
        `Queue ${label} has ${plural(q.ready, "ready message")} and no consumers`,
        "Nothing is draining it; the backlog only grows.",
      );
    }
  } else if (q.consumers > 0) {
    const publish = q.publish.rate ?? 0;
    const deliver = q.deliverGet.rate ?? 0;
    if (q.ready >= HEALTH_THRESHOLDS.growingBacklog && publish > deliver) {
      add(
        "backlog-growing",
        "warning",
        "consumer",
        `Queue ${label} is growing: ${q.ready.toLocaleString()} ready`,
        `Publish ${publish.toFixed(1)}/s exceeds deliver ${deliver.toFixed(1)}/s with ${plural(q.consumers, "consumer")}.`,
      );
    } else if (
      q.ready > 0 &&
      q.consumerUtilisation !== undefined &&
      q.consumerUtilisation < HEALTH_THRESHOLDS.consumerCapacity
    ) {
      add(
        "low-consumer-capacity",
        "info",
        "consumer",
        `Consumers of ${label} are at ${pct(q.consumerUtilisation)} capacity`,
        "Messages wait although consumers are attached: prefetch too low, or consumers busy acknowledging.",
      );
    }
  }

  if ((q.redeliver.rate ?? 0) > 0) {
    add(
      "redeliveries",
      "warning",
      "consumer",
      `Queue ${label} is redelivering ${(q.redeliver.rate ?? 0).toFixed(1)} msg/s`,
      "Consumers reject or crash on messages: a poison message or a broken downstream dependency.",
    );
  }
  return out;
}

/**
 * The most unacked messages the broker will push to this channel: per-consumer prefetch times
 * consumers, capped by the channel-wide (global) prefetch; undefined when neither is set.
 */
function channelCapacity(ch: ChannelDto): number | undefined {
  const perConsumer = ch.prefetchCount > 0 ? ch.prefetchCount * Math.max(1, ch.consumerCount) : undefined;
  const global = ch.globalPrefetchCount > 0 ? ch.globalPrefetchCount : undefined;
  if (perConsumer === undefined) return global;
  return global === undefined ? perConsumer : Math.min(perConsumer, global);
}

/** A consuming channel holding as many unacked messages as its prefetch allows, with no acks. */
function isChannelFull(ch: ChannelDto): boolean {
  const capacity = channelCapacity(ch);
  return (
    ch.consumerCount > 0 && capacity !== undefined && ch.messagesUnacknowledged >= capacity && (ch.ack.rate ?? 0) === 0
  );
}

/**
 * Carry "first seen full" timestamps from one check to the next: keep the earliest time for
 * channels still full, add new ones, drop the rest. Pure; the page keeps the returned map.
 */
export function trackStuckChannels(
  previous: Record<string, number>,
  channels: ChannelDto[],
  now: number,
): Record<string, number> {
  const next: Record<string, number> = {};
  for (const ch of channels) if (isChannelFull(ch)) next[ch.name] = previous[ch.name] ?? now;
  return next;
}

function channelFindings(ch: ChannelDto, stuckSince: Record<string, number>, now: number): HealthFinding[] {
  const subject: HealthSubject = { kind: "channel", name: ch.name };
  const client = ch.peerHost ? ` from ${ch.peerHost}` : "";

  if (isChannelFull(ch)) {
    const since = stuckSince[ch.name];
    const minutes = since === undefined ? 0 : Math.floor((now - since) / 60_000);
    if (since === undefined || now - since < HEALTH_THRESHOLDS.stuckForMs) return [];
    return [
      {
        id: `consumer-stuck:${ch.name}`,
        rule: "consumer-stuck",
        severity: "warning",
        category: "consumer",
        title: `Channel ${ch.name} has made no acks for ${minutes} minutes`,
        detail: `${ch.messagesUnacknowledged.toLocaleString()} unacked, the most its prefetch allows${client}. The consumer is hung, deadlocked or on a very long job; its messages stay in memory.`,
        subject,
      },
    ];
  }
  if (
    ch.consumerCount > 0 &&
    channelCapacity(ch) === undefined &&
    ch.messagesUnacknowledged >= HEALTH_THRESHOLDS.unboundedUnacked
  ) {
    return [
      {
        id: `unbounded-prefetch:${ch.name}`,
        rule: "unbounded-prefetch",
        severity: "warning",
        category: "consumer",
        title: `Channel ${ch.name} holds ${ch.messagesUnacknowledged.toLocaleString()} unacked without a prefetch limit`,
        detail: `Without basic.qos the broker pushes the whole backlog to this consumer${client} and keeps it in memory until acked.`,
        subject,
      },
    ];
  }
  return [];
}

export function analyzeHealth({
  overview,
  queues,
  channels,
  stuckSince = {},
  now = Date.now(),
}: HealthInput): HealthFinding[] {
  const findings: HealthFinding[] = [];
  for (const node of overview.nodes) findings.push(...nodeFindings(node));

  const returned = overview.rates.returnUnroutable.rate ?? 0;
  const dropped = overview.rates.dropUnroutable.rate ?? 0;
  if (returned + dropped > 0) {
    const parts = [
      dropped > 0 ? `${dropped.toFixed(1)} msg/s dropped` : "",
      returned > 0 ? `${returned.toFixed(1)} msg/s returned to mandatory publishers` : "",
    ].filter(Boolean);
    findings.push({
      id: "unroutable",
      rule: "unroutable",
      severity: "warning",
      category: "routing",
      title: `${(returned + dropped).toFixed(1)} msg/s published with no matching binding`,
      detail: `${parts.join(", ")}. Check the exchange bindings, or add an alternate exchange to catch them.`,
      subject: { kind: "cluster" },
    });
  }

  const runningNodes = overview.nodes.filter((n) => n.running).length;
  const downNodes = new Set(overview.nodes.filter((n) => !n.running).map((n) => n.name));
  for (const f of findings) {
    if (f.rule !== "node-down" || f.subject.kind !== "node") continue;
    const node = f.subject.name;
    const affected = queues.filter((q) => q.members?.includes(node) && !q.online?.includes(node)).length;
    if (affected > 0) {
      f.detail = `${plural(affected, "queue")} ${affected === 1 ? "has" : "have"} a replica here and ${affected === 1 ? "runs" : "run"} with one fewer; queues led only by this node are unavailable.`;
    }
  }
  const dlqTargets = deadLetterTargets(queues);
  for (const q of queues) findings.push(...queueFindings(q, runningNodes, downNodes, dlqTargets));
  for (const ch of channels) findings.push(...channelFindings(ch, stuckSince, now));

  return findings.sort(
    (a, b) => SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity] || a.title.localeCompare(b.title),
  );
}

export function countBySeverity(findings: HealthFinding[]): Record<HealthSeverity, number> {
  const counts: Record<HealthSeverity, number> = { critical: 0, warning: 0, info: 0 };
  for (const f of findings) counts[f.severity] += 1;
  return counts;
}

/** Human description of every rule, for the page's "What is checked" panel. */
export const HEALTH_RULES: { category: HealthCategory; text: string }[] = [
  { category: "node", text: "Nodes that are down or partitioned" },
  {
    category: "node",
    text: `Memory and disk alarms, and memory above ${pct(HEALTH_THRESHOLDS.memoryRatio)} of the watermark or free disk below ${HEALTH_THRESHOLDS.diskFreeLimitMultiple}× the limit`,
  },
  {
    category: "node",
    text: `File descriptors, sockets and Erlang processes above ${pct(HEALTH_THRESHOLDS.resourceRatio)}`,
  },
  { category: "queue", text: "Queues crashed, terminated, down or in minority" },
  { category: "queue", text: "Quorum queues with offline replicas, or fewer replicas than the cluster allows" },
  {
    category: "consumer",
    text: "Ready messages with no consumers; dead-letter queues are reported as info and streams are skipped",
  },
  {
    category: "consumer",
    text: `Backlogs above ${HEALTH_THRESHOLDS.growingBacklog.toLocaleString()} ready growing faster than they drain, and consumer capacity below ${pct(HEALTH_THRESHOLDS.consumerCapacity)}`,
  },
  { category: "consumer", text: "Redeliveries, the sign of a poison message or a failing consumer" },
  {
    category: "consumer",
    text: `Channels full to their prefetch with no acks for ${HEALTH_THRESHOLDS.stuckForMs / 60_000} minutes while this page is open, or holding ${HEALTH_THRESHOLDS.unboundedUnacked.toLocaleString()}+ unacked without a prefetch limit`,
  },
  { category: "routing", text: "Publishes that match no binding, dropped or returned" },
];
