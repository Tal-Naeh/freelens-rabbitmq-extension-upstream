import { describe, expect, it } from "vitest";
import { matchClientPods, normalizeAddress, podWorkload, resolveClientPods } from "./client-pods";

import type { KubeObject, KubeReader } from "./kube-reader";

function pod(
  name: string,
  ip: string,
  opts: {
    namespace?: string;
    owner?: { kind: string; name: string };
    hash?: string;
    phase?: string;
    hostNetwork?: boolean;
    terminating?: boolean;
  } = {},
): KubeObject {
  return {
    metadata: {
      ...(opts.terminating ? { deletionTimestamp: "2026-09-24T09:00:00Z" } : {}),
      name,
      namespace: opts.namespace ?? "apps",
      labels: opts.hash ? { "pod-template-hash": opts.hash } : {},
      ownerReferences: opts.owner ? [opts.owner] : [],
    },
    spec: { nodeName: "node-1", hostNetwork: opts.hostNetwork },
    status: { podIP: ip, phase: opts.phase ?? "Running" },
  };
}

describe("podWorkload", () => {
  it("resolves a ReplicaSet owner to its Deployment through the pod-template-hash", () => {
    expect(
      podWorkload(pod("api-7d9f8-x", "10.0.0.1", { owner: { kind: "ReplicaSet", name: "api-7d9f8" }, hash: "7d9f8" })),
    ).toEqual({ kind: "Deployment", name: "api" });
  });

  it("keeps other owners, and a ReplicaSet without a matching hash, as they are", () => {
    expect(podWorkload(pod("db-0", "10.0.0.2", { owner: { kind: "StatefulSet", name: "db" } }))).toEqual({
      kind: "StatefulSet",
      name: "db",
    });
    expect(podWorkload(pod("rs-x", "10.0.0.3", { owner: { kind: "ReplicaSet", name: "standalone" } }))).toEqual({
      kind: "ReplicaSet",
      name: "standalone",
    });
    expect(podWorkload(pod("bare", "10.0.0.4"))).toBeUndefined();
  });
});

describe("matchClientPods", () => {
  it("keeps running pods whose IP is a client address", () => {
    const pods = [
      pod("api-1", "10.0.0.1", { owner: { kind: "ReplicaSet", name: "api-abc" }, hash: "abc" }),
      pod("other", "10.0.0.9"),
    ];
    expect(matchClientPods(pods, ["10.0.0.1", "192.168.1.5"])).toEqual([
      {
        ip: "10.0.0.1",
        pod: "api-1",
        namespace: "apps",
        workloadKind: "Deployment",
        workload: "api",
        node: "node-1",
      },
    ]);
  });

  it("skips host-network pods and finished pods whose IP was reused", () => {
    const pods = [
      pod("old-job", "10.0.0.1", { phase: "Succeeded" }),
      pod("node-agent", "10.0.0.1", { hostNetwork: true }),
      pod("current", "10.0.0.1"),
    ];
    expect(matchClientPods(pods, ["10.0.0.1"]).map((p) => p.pod)).toEqual(["current"]);
  });
});

describe("matchClientPods edge cases", () => {
  it("prefers the new pod when a terminating pod still holds the same address, in either order", () => {
    const old = pod("old", "10.0.0.1", { terminating: true });
    const fresh = pod("fresh", "10.0.0.1");
    expect(matchClientPods([old, fresh], ["10.0.0.1"]).map((p) => p.pod)).toEqual(["fresh"]);
    expect(matchClientPods([fresh, old], ["10.0.0.1"]).map((p) => p.pod)).toEqual(["fresh"]);
    // A terminating pod alone is still the best answer.
    expect(matchClientPods([old], ["10.0.0.1"]).map((p) => p.pod)).toEqual(["old"]);
  });

  it("matches IPv4-mapped IPv6 client addresses to IPv4 pod IPs", () => {
    expect(normalizeAddress("::ffff:10.0.0.1")).toBe("10.0.0.1");
    expect(normalizeAddress("fd00::1")).toBe("fd00::1");
    expect(matchClientPods([pod("api-1", "10.0.0.1")], ["::ffff:10.0.0.1"]).map((p) => p.ip)).toEqual(["10.0.0.1"]);
  });
});

describe("resolveClientPods", () => {
  it("lists pods once, across all namespaces, and skips the call without addresses", async () => {
    const calls: (string | undefined)[] = [];
    const reader = {
      listPods: async (namespace?: string) => {
        calls.push(namespace);
        return [pod("api-1", "10.0.0.1")];
      },
    } as unknown as KubeReader;
    expect(await resolveClientPods(reader, [])).toEqual([]);
    expect(calls).toEqual([]);
    expect((await resolveClientPods(reader, ["10.0.0.1", "10.0.0.1"])).map((p) => p.pod)).toEqual(["api-1"]);
    expect(calls).toEqual([undefined]);
  });
});
