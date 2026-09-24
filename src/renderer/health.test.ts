import { describe, expect, it } from "vitest";
import { analyzeHealth, countBySeverity, HEALTH_THRESHOLDS, isDeadLetterQueue, trackStuckChannels } from "./health";

import type { ChannelDto, NodeDto, OverviewDto, QueueSummaryDto } from "../common/ipc";

function node(overrides: Partial<NodeDto> = {}): NodeDto {
  return {
    name: "rabbit@r-server-0",
    type: "disc",
    running: true,
    memUsed: 100,
    memLimit: 1000,
    memAlarm: false,
    diskFree: 10_000,
    diskFreeLimit: 1_000,
    diskFreeAlarm: false,
    fdUsed: 10,
    fdTotal: 1000,
    socketsUsed: 10,
    socketsTotal: 1000,
    procUsed: 10,
    procTotal: 1000,
    partitions: [],
    ...overrides,
  };
}

function overview(nodes: NodeDto[] = [node()], unroutable?: { returned?: number; dropped?: number }): OverviewDto {
  return {
    clusterName: "rabbit@r",
    rabbitmqVersion: "4.3.5",
    erlangVersion: "27",
    node: nodes[0]?.name ?? "",
    vhosts: ["/"],
    totals: { queues: 0, exchanges: 0, connections: 0, channels: 0, consumers: 0 },
    queueTotals: { messages: 0, ready: 0, unacknowledged: 0 },
    rates: {
      publish: {},
      deliverGet: {},
      ack: {},
      redeliver: {},
      confirm: {},
      returnUnroutable: { rate: unroutable?.returned },
      dropUnroutable: { rate: unroutable?.dropped },
    },
    nodes,
    userTags: [],
    session: { localAddress: "", pod: "", username: "", credentialSource: "" },
  };
}

function queue(overrides: Partial<QueueSummaryDto> = {}): QueueSummaryDto {
  return {
    name: "orders",
    vhost: "/",
    type: "classic",
    state: "running",
    durable: true,
    autoDelete: false,
    exclusive: false,
    messages: 0,
    ready: 0,
    unacknowledged: 0,
    consumers: 1,
    publish: {},
    deliverGet: {},
    ack: {},
    redeliver: {},
    arguments: {},
    ...overrides,
  };
}

function channel(overrides: Partial<ChannelDto> = {}): ChannelDto {
  return {
    name: "10.0.0.5:40000 -> 10.0.0.9:5672 (1)",
    number: 1,
    connectionName: "10.0.0.5:40000 -> 10.0.0.9:5672",
    peerHost: "10.0.0.5",
    user: "app",
    vhost: "/",
    state: "running",
    consumerCount: 1,
    prefetchCount: 10,
    globalPrefetchCount: 0,
    messagesUnacknowledged: 0,
    messagesUnconfirmed: 0,
    messagesUncommitted: 0,
    acksUncommitted: 0,
    confirm: false,
    transactional: false,
    publish: {},
    deliverGet: {},
    ack: {},
    redeliver: {},
    ...overrides,
  };
}

const rules = (input: Parameters<typeof analyzeHealth>[0]) => analyzeHealth(input).map((f) => f.rule);

describe("analyzeHealth", () => {
  it("reports nothing for a healthy broker", () => {
    expect(analyzeHealth({ overview: overview(), queues: [queue()], channels: [channel()] })).toEqual([]);
  });

  it("flags a stopped node and skips its resource checks", () => {
    const findings = analyzeHealth({
      overview: overview([node({ running: false, memAlarm: true })]),
      queues: [],
      channels: [],
    });
    expect(findings.map((f) => f.rule)).toEqual(["node-down"]);
    expect(findings[0]).toMatchObject({ severity: "critical", title: "Node r-server-0 is not running" });
  });

  it("flags alarms as critical and near-limits as warnings", () => {
    expect(
      rules({ overview: overview([node({ memAlarm: true, diskFreeAlarm: true })]), queues: [], channels: [] }),
    ).toEqual(["disk-alarm", "memory-alarm"]);
    expect(
      rules({
        overview: overview([node({ memUsed: 850, diskFree: 1_500, fdUsed: 900, socketsUsed: 950, procUsed: 800 })]),
        queues: [],
        channels: [],
      }).sort(),
    ).toEqual(["disk-low", "fd-high", "memory-high", "procs-high", "sockets-high"]);
  });

  it("flags partitions and unroutable publishes", () => {
    const findings = analyzeHealth({
      overview: overview([node({ partitions: ["rabbit@r-server-1"] })], { returned: 2 }),
      queues: [],
      channels: [],
    });
    expect(findings.map((f) => f.rule)).toEqual(["node-partitioned", "unroutable"]);
    expect(findings[0].detail).toContain("r-server-1");
  });

  it("flags a backlog with no consumers, but only informs for dead-letter queues", () => {
    const findings = analyzeHealth({
      overview: overview(),
      queues: [queue({ ready: 5, consumers: 0 }), queue({ name: "orders.dlq", ready: 7, consumers: 0 })],
      channels: [],
    });
    expect(findings.map((f) => [f.rule, f.severity])).toEqual([
      ["no-consumers", "warning"],
      ["dead-letters-parked", "info"],
    ]);
    expect(findings[0].subject).toEqual({ kind: "queue", vhost: "/", name: "orders" });
  });

  it("recognises common dead-letter queue names without false positives", () => {
    for (const name of ["orders.dlq", "orders-dead-letter", "dlx.orders", "orders_errors", "parking-lot", "failed"]) {
      expect(isDeadLetterQueue(queue({ name }))).toBe(true);
    }
    for (const name of [
      "orders",
      "headless",
      "terrors-feed",
      "dlqueue-service",
      "error-reporting",
      "failed-logins.audit",
    ]) {
      expect(isDeadLetterQueue(queue({ name }))).toBe(false);
    }
  });

  it("flags crashed and minority queues with the right severity", () => {
    expect(
      analyzeHealth({
        overview: overview(),
        queues: [queue({ name: "a", state: "crashed" }), queue({ name: "b", state: "minority" })],
        channels: [],
      }).map((f) => [f.rule, f.severity]),
    ).toEqual([
      ["queue-crashed", "critical"],
      ["queue-unavailable", "warning"],
    ]);
  });

  it("flags quorum replicas offline, and under-replication on a multi-node cluster", () => {
    const nodes = [node(), node({ name: "rabbit@r-server-1" }), node({ name: "rabbit@r-server-2" })];
    const findings = analyzeHealth({
      overview: overview(nodes),
      queues: [
        queue({
          name: "a",
          type: "quorum",
          members: ["rabbit@r-server-0", "rabbit@r-server-1", "rabbit@r-server-2"],
          online: ["rabbit@r-server-0", "rabbit@r-server-1"],
        }),
        queue({ name: "b", type: "quorum", members: ["rabbit@r-server-0"], online: ["rabbit@r-server-0"] }),
      ],
      channels: [],
    });
    expect(findings.map((f) => f.rule)).toEqual(["replicas-offline", "under-replicated"]);
    expect(findings[0].detail).toBe("Offline: r-server-2.");
    // A single-node cluster cannot do better than one replica.
    expect(
      rules({
        overview: overview(),
        queues: [queue({ type: "quorum", members: ["rabbit@r-server-0"], online: ["rabbit@r-server-0"] })],
        channels: [],
      }),
    ).toEqual([]);
  });

  it("flags a growing backlog, low consumer capacity and redeliveries", () => {
    expect(
      rules({
        overview: overview(),
        queues: [
          queue({ name: "a", ready: 5_000, publish: { rate: 50 }, deliverGet: { rate: 10 } }),
          queue({ name: "b", ready: 20, consumerUtilisation: 0.2 }),
          queue({ name: "c", redeliver: { rate: 3 } }),
        ],
        channels: [],
      }).sort(),
    ).toEqual(["backlog-growing", "low-consumer-capacity", "redeliveries"]);
  });

  it("reports a full channel with no acks only once it has stayed full for the threshold", () => {
    const stuck = channel({ name: "stuck", messagesUnacknowledged: 10 });
    const now = 1_000_000_000;
    const input = { overview: overview(), queues: [], channels: [stuck] };
    // First sighting, or not long enough: a consumer on a long job looks the same.
    expect(analyzeHealth({ ...input, now })).toEqual([]);
    expect(analyzeHealth({ ...input, now, stuckSince: { stuck: now - 60_000 } })).toEqual([]);
    const findings = analyzeHealth({
      ...input,
      now,
      stuckSince: { stuck: now - HEALTH_THRESHOLDS.stuckForMs - 60_000 },
    });
    expect(findings.map((f) => [f.rule, f.title])).toEqual([
      ["consumer-stuck", "Channel stuck has made no acks for 6 minutes"],
    ]);
  });

  it("measures a channel against prefetch times consumers, capped by the global prefetch", () => {
    const now = 1_000_000_000;
    const old = now - HEALTH_THRESHOLDS.stuckForMs;
    const channels = [
      // Two consumers with prefetch 10 can hold 20; 10 unacked is not full.
      channel({ name: "half", consumerCount: 2, messagesUnacknowledged: 10 }),
      channel({ name: "full", consumerCount: 2, messagesUnacknowledged: 20 }),
      // Channel-wide prefetch only: bounded, so never "unbounded".
      channel({ name: "global", prefetchCount: 0, globalPrefetchCount: 50, messagesUnacknowledged: 50 }),
      channel({ name: "busy", messagesUnacknowledged: 10, ack: { rate: 4 } }),
    ];
    const stuckSince = { half: old, full: old, global: old, busy: old };
    expect(
      analyzeHealth({ overview: overview(), queues: [], channels, stuckSince, now }).map(
        (f) => (f.subject as { name: string }).name,
      ),
    ).toEqual(["full", "global"]);
  });

  it("flags unbounded unacked whether or not the consumer acks", () => {
    const findings = analyzeHealth({
      overview: overview(),
      queues: [],
      channels: [
        channel({ name: "unbounded", prefetchCount: 0, messagesUnacknowledged: 2_000, ack: { rate: 50 } }),
        channel({ name: "publisher", consumerCount: 0, prefetchCount: 0, messagesUnacknowledged: 0 }),
      ],
    });
    expect(findings.map((f) => f.rule)).toEqual(["unbounded-prefetch"]);
  });

  it("tracks when channels became full and forgets the ones that recovered", () => {
    const full = channel({ name: "a", messagesUnacknowledged: 10 });
    const recovered = channel({ name: "b", messagesUnacknowledged: 1 });
    expect(trackStuckChannels({}, [full, recovered], 100)).toEqual({ a: 100 });
    expect(trackStuckChannels({ a: 50, b: 60 }, [full, recovered], 100)).toEqual({ a: 50 });
  });

  it("skips backlog and consumer rules for streams", () => {
    expect(
      rules({
        overview: overview(),
        queues: [
          queue({
            name: "events",
            type: "stream",
            ready: 3,
            consumers: 0,
            members: ["rabbit@r-server-0"],
            online: ["rabbit@r-server-0"],
          }),
        ],
        channels: [],
      }),
    ).toEqual([]);
  });

  it("counts dropped unroutable publishes, not only returned ones", () => {
    const findings = analyzeHealth({ overview: overview(undefined, { dropped: 3 }), queues: [], channels: [] });
    expect(findings.map((f) => f.rule)).toEqual(["unroutable"]);
    expect(findings[0].detail).toContain("3.0 msg/s dropped");
  });

  it("recognises a dead-letter queue from another queue's dead-letter routing key", () => {
    const findings = analyzeHealth({
      overview: overview(),
      queues: [
        queue({ arguments: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": "orders-retry" } }),
        queue({ name: "orders-retry", ready: 4, consumers: 0 }),
      ],
      channels: [],
    });
    expect(findings.map((f) => f.rule)).toEqual(["dead-letters-parked"]);
  });

  it("reports a minority queue once, naming the offline replicas", () => {
    const findings = analyzeHealth({
      overview: overview([node(), node({ name: "rabbit@r-server-1" }), node({ name: "rabbit@r-server-2" })]),
      queues: [
        queue({
          type: "quorum",
          state: "minority",
          members: ["rabbit@r-server-0", "rabbit@r-server-1", "rabbit@r-server-2"],
          online: ["rabbit@r-server-0"],
        }),
      ],
      channels: [],
    });
    expect(findings.map((f) => f.rule)).toEqual(["queue-unavailable"]);
    expect(findings[0].detail).toContain("Offline: r-server-1, r-server-2.");
  });

  it("sorts critical first and counts by severity", () => {
    const findings = analyzeHealth({
      overview: overview([node({ memAlarm: true })]),
      queues: [queue({ name: "x.dlq", ready: 1, consumers: 0 }), queue({ ready: 1, consumers: 0 })],
      channels: [],
    });
    expect(findings.map((f) => f.severity)).toEqual(["critical", "warning", "info"]);
    expect(countBySeverity(findings)).toEqual({ critical: 1, warning: 1, info: 1 });
  });
});
