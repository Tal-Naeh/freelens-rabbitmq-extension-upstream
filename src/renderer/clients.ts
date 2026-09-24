/**
 * Group AMQP connections by the client that opened them: the pod behind the peer address, or the
 * workload (Deployment, StatefulSet…) behind those pods. A leaking client shows up as one row
 * holding a large share of the broker's connections instead of thousands of look-alike lines.
 * Pure: no IPC, no React.
 */
import type { ClientPodDto, ConnectionDto } from "../common/ipc";

export type ClientGrouping = "pod" | "workload";
export type ClientKind = "workload" | "pod" | "external" | "unknown";

export interface ClientGroup {
  /** Stable key: the same client keeps its row across refreshes. */
  key: string;
  kind: ClientKind;
  label: string;
  namespace?: string;
  /** Deployment, StatefulSet… for workload rows and for pods that have a controller. */
  workloadKind?: string;
  workload?: string;
  /** Peer addresses in the group; one for a pod or an external client, several for a workload. */
  ips: string[];
  /** An external address on the loopback interface: usually a service-mesh sidecar proxying the real client. */
  loopback: boolean;
  pods: number;
  connections: number;
  /** Fraction of all grouped connections, 0..1. */
  share: number;
  channels: number;
  blocked: number;
  users: string[];
  /** Client libraries as reported in the AMQP client properties ("pika 1.3.2"). */
  libraries: string[];
  recvRate: number;
  sendRate: number;
  /** Epoch ms of the oldest and newest connection in the group. */
  oldestConnectedAt?: number;
  newestConnectedAt?: number;
}

interface Accumulator extends Omit<ClientGroup, "ips" | "loopback" | "users" | "libraries" | "pods" | "share"> {
  ips: Set<string>;
  users: Set<string>;
  libraries: Set<string>;
  podNames: Set<string>;
}

/** Same normalisation as the Main side: `::ffff:10.0.0.1` -> `10.0.0.1`. */
export function normalizeAddress(ip: string): string {
  return ip.replace(/^::ffff:(?=\d+\.\d+\.\d+\.\d+$)/i, "");
}

export function isLoopback(ip: string): boolean {
  return /^127\./.test(ip) || ip === "::1";
}

/** Key of the workload group a pod belongs to, for drilling down from a workload row to its pods. */
export function workloadKey(namespace: string, kind: string, name: string): string {
  return `workload:${namespace}/${kind}/${name}`;
}

function library(c: ConnectionDto): string | undefined {
  const text = [c.clientProperties.product, c.clientProperties.version].filter(Boolean).join(" ");
  return text || undefined;
}

function identify(
  c: ConnectionDto,
  podsByIp: Map<string, ClientPodDto>,
  by: ClientGrouping,
): Pick<ClientGroup, "key" | "kind" | "label" | "namespace" | "workloadKind" | "workload"> & { pod?: string } {
  const ip = c.peerHost ? normalizeAddress(c.peerHost) : undefined;
  if (!ip) return { key: "unknown", kind: "unknown", label: "No peer address" };
  const pod = podsByIp.get(ip);
  if (!pod) return { key: `ip:${ip}`, kind: "external", label: ip };
  const base = { namespace: pod.namespace, workloadKind: pod.workloadKind, workload: pod.workload, pod: pod.pod };
  if (by === "workload" && pod.workload && pod.workloadKind) {
    return {
      ...base,
      key: workloadKey(pod.namespace, pod.workloadKind, pod.workload),
      kind: "workload",
      label: pod.workload,
    };
  }
  return { ...base, key: `pod:${pod.namespace}/${pod.pod}`, kind: "pod", label: pod.pod };
}

export function groupConnectionsByClient(
  connections: ConnectionDto[],
  pods: ClientPodDto[],
  by: ClientGrouping,
): ClientGroup[] {
  const podsByIp = new Map(pods.map((p) => [p.ip, p]));
  const groups = new Map<string, Accumulator>();

  for (const c of connections) {
    const id = identify(c, podsByIp, by);
    let g = groups.get(id.key);
    if (!g) {
      g = {
        key: id.key,
        kind: id.kind,
        label: id.label,
        namespace: id.namespace,
        workloadKind: id.workloadKind,
        workload: id.workload,
        ips: new Set(),
        users: new Set(),
        libraries: new Set(),
        podNames: new Set(),
        connections: 0,
        channels: 0,
        blocked: 0,
        recvRate: 0,
        sendRate: 0,
      };
      groups.set(id.key, g);
    }
    g.connections += 1;
    g.channels += c.channels;
    if (/block/.test(c.state)) g.blocked += 1;
    g.recvRate += c.recvBytes.rate ?? 0;
    g.sendRate += c.sendBytes.rate ?? 0;
    if (c.peerHost) g.ips.add(normalizeAddress(c.peerHost));
    if (c.user) g.users.add(c.user);
    const lib = library(c);
    if (lib) g.libraries.add(lib);
    if (id.pod) g.podNames.add(id.pod);
    if (c.connectedAt !== undefined) {
      g.oldestConnectedAt = Math.min(g.oldestConnectedAt ?? c.connectedAt, c.connectedAt);
      g.newestConnectedAt = Math.max(g.newestConnectedAt ?? c.connectedAt, c.connectedAt);
    }
  }

  const total = connections.length;
  return [...groups.values()]
    .map(({ ips, users, libraries, podNames, ...g }) => ({
      ...g,
      ips: [...ips].sort(),
      loopback: g.kind === "external" && [...ips].every(isLoopback),
      users: [...users].sort(),
      libraries: [...libraries].sort(),
      pods: podNames.size,
      share: total > 0 ? g.connections / total : 0,
    }))
    .sort((a, b) => b.connections - a.connections || a.label.localeCompare(b.label));
}

/** Distinct peer addresses to resolve to pods. */
export function clientAddresses(connections: ConnectionDto[]): string[] {
  return [
    ...new Set(
      connections
        .map((c) => (c.peerHost ? normalizeAddress(c.peerHost) : ""))
        .filter((ip): ip is string => Boolean(ip)),
    ),
  ].sort();
}

/** Short, order-independent fingerprint of an address set, for resource keys. */
export function addressFingerprint(ips: string[]): string {
  let h = 5381;
  for (const ip of ips) for (let i = 0; i < ip.length; i++) h = ((h << 5) + h + ip.charCodeAt(i)) | 0;
  return `${ips.length}:${(h >>> 0).toString(36)}`;
}
