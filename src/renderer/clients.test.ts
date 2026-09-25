import { describe, expect, it } from "vitest";
import { addressFingerprint, clientAddresses, groupConnectionsByClient, workloadKey } from "./clients";

import type { ClientPodDto, ConnectionDto } from "../common/ipc";

let seq = 0;
function conn(peerHost: string | undefined, overrides: Partial<ConnectionDto> = {}): ConnectionDto {
  seq += 1;
  return {
    name: `${peerHost ?? "direct"}:${40000 + seq} -> 10.0.9.9:5672`,
    state: "running",
    user: "app",
    vhost: "/",
    protocol: "AMQP 0-9-1",
    channels: 1,
    peerHost,
    ssl: false,
    recvBytes: { rate: 10 },
    sendBytes: { rate: 5 },
    connectedAt: 1_000 + seq,
    clientProperties: { product: "pika", version: "1.3.2" },
    ...overrides,
  };
}

const pods: ClientPodDto[] = [
  { ip: "10.0.0.1", pod: "worker-abc-1", namespace: "apps", workloadKind: "Deployment", workload: "worker" },
  { ip: "10.0.0.2", pod: "worker-abc-2", namespace: "apps", workloadKind: "Deployment", workload: "worker" },
  { ip: "10.0.0.3", pod: "api-0", namespace: "apps", workloadKind: "StatefulSet", workload: "api" },
  { ip: "10.0.0.4", pod: "bare-pod", namespace: "tools" },
];

const connections = [
  conn("10.0.0.1"),
  conn("10.0.0.1"),
  conn("10.0.0.1", { state: "blocked", user: "worker" }),
  conn("10.0.0.2", { connectedAt: 1 }),
  conn("10.0.0.3", { channels: 4 }),
  conn("10.0.0.4"),
  conn("192.168.7.7", { clientProperties: { product: "RabbitMQ .NET Client", version: "7.0" } }),
  conn(undefined, { protocol: "Direct 0-9-1" }),
];

describe("groupConnectionsByClient", () => {
  it("groups by pod, busiest first, with shares and totals", () => {
    const groups = groupConnectionsByClient(connections, pods, "pod");
    expect(groups.map((g) => [g.kind, g.label, g.connections])).toEqual([
      ["pod", "worker-abc-1", 3],
      ["external", "192.168.7.7", 1],
      ["pod", "api-0", 1],
      ["pod", "bare-pod", 1],
      ["unknown", "No peer address", 1],
      ["pod", "worker-abc-2", 1],
    ]);
    const top = groups[0];
    expect(top).toMatchObject({
      namespace: "apps",
      workloadKind: "Deployment",
      workload: "worker",
      ips: ["10.0.0.1"],
      pods: 1,
      channels: 3,
      blocked: 1,
      users: ["app", "worker"],
      libraries: ["pika 1.3.2"],
      recvRate: 30,
      sendRate: 15,
    });
    expect(top.share).toBeCloseTo(3 / 8);
  });

  it("groups by workload, keeping pods without a controller and external clients as their own rows", () => {
    const groups = groupConnectionsByClient(connections, pods, "workload");
    expect(groups.map((g) => [g.kind, g.label, g.connections, g.pods])).toEqual([
      ["workload", "worker", 4, 2],
      ["external", "192.168.7.7", 1, 0],
      ["workload", "api", 1, 1],
      ["pod", "bare-pod", 1, 1],
      ["unknown", "No peer address", 1, 0],
    ]);
    const worker = groups[0];
    expect(worker.ips).toEqual(["10.0.0.1", "10.0.0.2"]);
    expect(worker.oldestConnectedAt).toBe(1);
    expect(worker.share).toBeCloseTo(0.5);
  });

  it("falls back to addresses when no pods could be resolved", () => {
    const groups = groupConnectionsByClient([conn("10.0.0.1"), conn("10.0.0.1")], [], "workload");
    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({ kind: "external", label: "10.0.0.1", connections: 2, share: 1 });
  });

  it("returns nothing for no connections", () => {
    expect(groupConnectionsByClient([], pods, "pod")).toEqual([]);
  });
});

describe("client identification edge cases", () => {
  it("normalises IPv4-mapped peer addresses before matching pods", () => {
    const groups = groupConnectionsByClient([conn("::ffff:10.0.0.3")], pods, "pod");
    expect(groups.map((g) => [g.kind, g.label, g.ips])).toEqual([["pod", "api-0", ["10.0.0.3"]]]);
    expect(clientAddresses([conn("::ffff:10.0.0.3")])).toEqual(["10.0.0.3"]);
  });

  it("flags loopback peers, the address a service-mesh sidecar connects from", () => {
    const groups = groupConnectionsByClient([conn("127.0.0.6"), conn("192.168.7.7")], pods, "pod");
    expect(groups.map((g) => [g.label, g.loopback])).toEqual([
      ["127.0.0.6", true],
      ["192.168.7.7", false],
    ]);
  });

  it("keys pods by the workload they belong to, matching the workload row", () => {
    const [worker] = groupConnectionsByClient(connections, pods, "workload");
    const podRows = groupConnectionsByClient(connections, pods, "pod").filter(
      (g) =>
        g.namespace &&
        g.workloadKind &&
        g.workload &&
        workloadKey(g.namespace, g.workloadKind, g.workload) === worker.key,
    );
    expect(podRows.map((g) => g.label).sort()).toEqual(["worker-abc-1", "worker-abc-2"]);
  });
});

describe("clientAddresses and addressFingerprint", () => {
  it("lists distinct peer addresses and fingerprints them stably", () => {
    const ips = clientAddresses(connections);
    expect(ips).toEqual(["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "192.168.7.7"]);
    expect(addressFingerprint(ips)).toBe(addressFingerprint([...ips]));
    expect(addressFingerprint(ips)).not.toBe(addressFingerprint(ips.slice(1)));
  });
});
