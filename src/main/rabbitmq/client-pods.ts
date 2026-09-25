/**
 * Map AMQP client addresses to the pods behind them, so the Connections page can say which
 * workload holds the connections instead of listing bare IPs.
 */
import { RABBITMQ_CLIENT_PODS_MAX_IPS } from "../../common/constants";

import type { ClientPodDto } from "../../common/ipc";
import type { KubeObject, KubeReader } from "./kube-reader";

/**
 * The controller behind a pod. A ReplicaSet owner whose name is `<deployment>-<pod-template-hash>`
 * is reported as that Deployment; other owners (StatefulSet, DaemonSet, Job, a bare ReplicaSet)
 * as themselves. Pure.
 */
export function podWorkload(pod: KubeObject): { kind: string; name: string } | undefined {
  const owner = pod.metadata?.ownerReferences?.find((o) => o.kind && o.name);
  if (!owner?.kind || !owner.name) return undefined;
  const hash = pod.metadata?.labels?.["pod-template-hash"];
  if (owner.kind === "ReplicaSet" && hash && owner.name.endsWith(`-${hash}`)) {
    return { kind: "Deployment", name: owner.name.slice(0, -(hash.length + 1)) };
  }
  return { kind: owner.kind, name: owner.name };
}

/** `::ffff:10.0.0.1` (an IPv4-mapped IPv6 address) -> `10.0.0.1`; anything else unchanged. Pure. */
export function normalizeAddress(ip: string): string {
  return ip.replace(/^::ffff:(?=\d+\.\d+\.\d+\.\d+$)/i, "");
}

/**
 * Pods whose IP is one of `ips`. Host-network pods are skipped (they share the node's IP, so the
 * address does not identify them), and so are finished pods, whose IPs the cluster reuses. When a
 * pod that is shutting down still holds an address a new pod was given, the new pod wins. Pure.
 */
export function matchClientPods(pods: KubeObject[], ips: Iterable<string>): ClientPodDto[] {
  const wanted = new Set([...ips].map(normalizeAddress));
  const byIp = new Map<string, { dto: ClientPodDto; terminating: boolean }>();
  for (const pod of pods) {
    const rawIp: unknown = pod.status?.podIP;
    const phase: unknown = pod.status?.phase;
    if (typeof rawIp !== "string") continue;
    const ip = normalizeAddress(rawIp);
    if (!wanted.has(ip)) continue;
    if (pod.spec?.hostNetwork === true || (phase !== "Running" && phase !== "Pending")) continue;
    const name = pod.metadata?.name;
    const namespace = pod.metadata?.namespace;
    if (!name || !namespace) continue;
    const terminating = Boolean((pod.metadata as { deletionTimestamp?: string }).deletionTimestamp);
    const current = byIp.get(ip);
    if (current && (terminating || !current.terminating)) continue;
    const workload = podWorkload(pod);
    byIp.set(ip, {
      terminating,
      dto: {
        ip,
        pod: name,
        namespace,
        workloadKind: workload?.kind,
        workload: workload?.name,
        node: typeof pod.spec?.nodeName === "string" ? pod.spec.nodeName : undefined,
      },
    });
  }
  return [...byIp.values()].map((v) => v.dto);
}

/** List pods across the cluster once and keep the ones matching the (bounded) client addresses. */
export async function resolveClientPods(reader: KubeReader, ips: string[]): Promise<ClientPodDto[]> {
  const bounded = [...new Set(ips.filter((ip) => typeof ip === "string" && ip).map(normalizeAddress))].slice(
    0,
    RABBITMQ_CLIENT_PODS_MAX_IPS,
  );
  if (bounded.length === 0) return [];
  return matchClientPods(await reader.listPods(), bounded);
}
