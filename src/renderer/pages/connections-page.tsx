import { Renderer } from "@freelensapp/extensions";
import { useMemo } from "react";
import { RABBITMQ_CLIENT_PODS_REFRESH_MS, RABBITMQ_LIVE_REFRESH_MS } from "../../common/constants";
import {
  addressFingerprint,
  type ClientGroup,
  type ClientGrouping,
  clientAddresses,
  groupConnectionsByClient,
  workloadKey,
} from "../clients";
import { ConnectionErrorPanel } from "../components/connection-error";
import {
  EmptyState,
  LoadingState,
  MetricStrip,
  PageShell,
  SearchBox,
  StatusDot,
  TargetSelector,
  Toolbar,
  WriteModeToggle,
} from "../components/page-shell";
import { type ColumnSpec, useResizableColumns } from "../components/resizable-columns";
import {
  formatBytesRate,
  formatDuration,
  formatNumber,
  formatRate,
  formatTimestamp,
  matchesQuery,
  shortNodeName,
} from "../format";
import { useDebounced, usePageParam, useResource, useSelectionParam } from "../hooks";
import { RABBITMQ_PAGE_IDS } from "../navigation";
import { useWriteMode } from "../write-mode-store";
import { ConsumersTable } from "./consumers-table";
import { useTargetPage } from "./page-deps";

import type { ChannelDto, ConnectionDto } from "../../common/ipc";
import type { Metric } from "../components/page-shell";
import type { PageDeps, Param } from "./page-deps";

export interface ConnectionsPageProps extends PageDeps {
  params?: { target: Param; query: Param; view: Param };
}

type View = "connections" | "clients" | "channels" | "consumers";
const VIEWS: { value: View; label: string }[] = [
  { value: "connections", label: "Connections" },
  { value: "clients", label: "Clients" },
  { value: "channels", label: "Channels" },
  { value: "consumers", label: "Consumers" },
];

const RABBITMQ_CONNECTIONS_COLUMNS: ColumnSpec[] = [
  { id: "name", width: 280, grow: true, minWidth: 140 },
  { id: "client", width: 160 },
  { id: "user", width: 110 },
  { id: "vhost", width: 80 },
  { id: "state", width: 110 },
  { id: "protocol", width: 130 },
  { id: "channels", width: 90, numeric: true },
  { id: "recv", width: 110, numeric: true },
  { id: "send", width: 110, numeric: true },
  { id: "since", width: 170 },
];

function ConnectionsTable({ items }: { items: ConnectionDto[] }) {
  const col = useResizableColumns("rabbitmq-connections", RABBITMQ_CONNECTIONS_COLUMNS);
  return (
    <Renderer.Component.Table<ConnectionDto>
      tableId="rabbitmq-connections"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "name", orderBy: "asc" }}
      sortable={{
        name: (c) => c.clientProperties.connectionName ?? c.name,
        user: (c) => c.user,
        vhost: (c) => c.vhost,
        state: (c) => c.state,
        channels: (c) => c.channels,
        recv: (c) => c.recvBytes.rate ?? 0,
        send: (c) => c.sendBytes.rate ?? 0,
        since: (c) => c.connectedAt ?? 0,
      }}
    >
      <Renderer.Component.TableHead sticky nowrap>
        <Renderer.Component.TableCell {...col.head("name")} sortBy="name">
          Connection
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("client")}>Client</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("user")} sortBy="user">
          User
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("vhost")} sortBy="vhost">
          Vhost
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("state")} sortBy="state">
          State
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("protocol")}>Protocol</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("channels")} sortBy="channels">
          Channels
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("recv")} sortBy="recv">
          From client
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("send")} sortBy="send">
          To client
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("since")} sortBy="since">
          Connected
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {items.map((c) => (
        <Renderer.Component.TableRow key={c.name} sortItem={c} nowrap>
          <Renderer.Component.TableCell {...col.cell("name")}>
            <span className="RmqMono RmqEllipsis">{c.clientProperties.connectionName ?? c.name}</span>
            {c.clientProperties.connectionName ? <span className="RmqMuted RmqEllipsis RmqMono">{c.name}</span> : null}
            {c.node ? <span className="RmqMuted"> {shortNodeName(c.node)}</span> : null}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("client")}>
            {[c.clientProperties.product, c.clientProperties.version].filter(Boolean).join(" ") || "—"}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("user")}>{c.user}</Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("vhost")}>
            <span className="RmqMono">{c.vhost}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("state")}>
            <StatusDot state={c.state} />
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("protocol")}>
            {c.protocol}
            {c.ssl ? <Renderer.Component.Badge small label="TLS" tooltip={c.sslProtocol} /> : null}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("channels")}>
            {formatNumber(c.channels)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("recv")}>
            {formatBytesRate(c.recvBytes.rate)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("send")}>
            {formatBytesRate(c.sendBytes.rate)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("since")}>
            {formatTimestamp(c.connectedAt)}
          </Renderer.Component.TableCell>
        </Renderer.Component.TableRow>
      ))}
    </Renderer.Component.Table>
  );
}

const RABBITMQ_CHANNELS_COLUMNS: ColumnSpec[] = [
  { id: "name", width: 280, grow: true, minWidth: 140 },
  { id: "user", width: 110 },
  { id: "vhost", width: 80 },
  { id: "state", width: 110 },
  { id: "mode", width: 110 },
  { id: "consumers", width: 100, numeric: true },
  { id: "prefetch", width: 110, numeric: true },
  { id: "unacked", width: 90, numeric: true },
  { id: "unconfirmed", width: 110, numeric: true },
  { id: "publish", width: 100, numeric: true },
  { id: "deliver", width: 100, numeric: true },
];

function ChannelsTable({ items }: { items: ChannelDto[] }) {
  const col = useResizableColumns("rabbitmq-channels", RABBITMQ_CHANNELS_COLUMNS);
  return (
    <Renderer.Component.Table<ChannelDto>
      tableId="rabbitmq-channels"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "name", orderBy: "asc" }}
      sortable={{
        name: (c) => c.name,
        user: (c) => c.user,
        state: (c) => c.state,
        consumers: (c) => c.consumerCount,
        prefetch: (c) => c.prefetchCount,
        unacked: (c) => c.messagesUnacknowledged,
        publish: (c) => c.publish.rate ?? 0,
        deliver: (c) => c.deliverGet.rate ?? 0,
      }}
    >
      <Renderer.Component.TableHead sticky nowrap>
        <Renderer.Component.TableCell {...col.head("name")} sortBy="name">
          Channel
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("user")} sortBy="user">
          User
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("vhost")}>Vhost</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("state")} sortBy="state">
          State
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("mode")}>Mode</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("consumers")} sortBy="consumers">
          Consumers
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("prefetch")} sortBy="prefetch">
          Prefetch
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("unacked")} sortBy="unacked">
          Unacked
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("unconfirmed")}>Unconfirmed</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("publish")} sortBy="publish">
          Publish
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("deliver")} sortBy="deliver">
          Deliver
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {items.map((c) => (
        <Renderer.Component.TableRow key={c.name} sortItem={c} nowrap>
          <Renderer.Component.TableCell {...col.cell("name")}>
            <span className="RmqMono RmqEllipsis" title={c.name}>
              {c.name}
            </span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("user")}>{c.user}</Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("vhost")}>
            <span className="RmqMono">{c.vhost}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("state")}>
            <StatusDot state={c.state} />
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("mode")}>
            <span className="RmqBadges">
              {c.confirm ? <Renderer.Component.Badge small label="confirm" /> : null}
              {c.transactional ? <Renderer.Component.Badge small label="tx" /> : null}
            </span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("consumers")}>
            {formatNumber(c.consumerCount)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("prefetch")}>
            {c.prefetchCount === 0 ? "∞" : formatNumber(c.prefetchCount)}
            {c.globalPrefetchCount > 0 ? (
              <span className="RmqMuted"> / {formatNumber(c.globalPrefetchCount)}</span>
            ) : null}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("unacked")}>
            {formatNumber(c.messagesUnacknowledged)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("unconfirmed")}>
            {formatNumber(c.messagesUnconfirmed)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("publish")}>
            {formatRate(c.publish.rate)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("deliver")}>
            {formatRate(c.deliverGet.rate)}
          </Renderer.Component.TableCell>
        </Renderer.Component.TableRow>
      ))}
    </Renderer.Component.Table>
  );
}

// Widths include padding (border-box). About 1,095 px in total, so the table fits a ~1,200 px content
// area without horizontal scrolling and the client name takes whatever is left. Numeric headers keep
// room for the sort arrow, which sits to the left of the label.
const RABBITMQ_CLIENTS_COLUMNS: ColumnSpec[] = [
  { id: "client", width: 230, grow: true, minWidth: 180 },
  { id: "namespace", width: 95 },
  { id: "kind", width: 105 },
  { id: "pods", width: 50, numeric: true },
  { id: "connections", width: 105, numeric: true },
  { id: "share", width: 80, numeric: true },
  { id: "channels", width: 80, numeric: true },
  { id: "library", width: 105 },
  { id: "recv", width: 90, numeric: true },
  { id: "send", width: 85, numeric: true },
  { id: "oldest", width: 70, numeric: true },
];

function clientKindLabel(g: ClientGroup): string {
  if (g.kind === "workload") return g.workloadKind ?? "Workload";
  if (g.kind === "pod") return g.workloadKind ? `${g.workloadKind} pod` : "Pod";
  if (g.kind === "external") return g.loopback ? "Loopback proxy" : "Not a pod";
  return "—";
}

/** Hover text for the client name: the owning workload of a pod, the addresses and the broker users. */
function clientDetails(g: ClientGroup): string {
  const owner = g.kind === "pod" && g.workload ? [`${g.workloadKind}/${g.workload}`] : [];
  const addresses = g.ips.length > 0 ? [`${g.ips.length === 1 ? "Address" : "Addresses"}: ${g.ips.join(", ")}`] : [];
  const users = g.users.length > 0 ? [`${g.users.length === 1 ? "User" : "Users"}: ${g.users.join(", ")}`] : [];
  return [...owner, ...addresses, ...users].join("\n") || g.label;
}

function ClientsTable({ items, onOpen }: { items: ClientGroup[]; onOpen: (group: ClientGroup) => void }) {
  const col = useResizableColumns("rabbitmq-clients", RABBITMQ_CLIENTS_COLUMNS);
  return (
    <Renderer.Component.Table<ClientGroup>
      tableId="rabbitmq-clients"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "connections", orderBy: "desc" }}
      sortable={{
        client: (g) => g.label,
        namespace: (g) => g.namespace ?? "",
        pods: (g) => g.pods,
        connections: (g) => g.connections,
        share: (g) => g.share,
        channels: (g) => g.channels,
        recv: (g) => g.recvRate,
        send: (g) => g.sendRate,
        // By age, matching what the column shows: ascending = youngest first.
        oldest: (g) => (g.oldestConnectedAt === undefined ? -1 : Date.now() - g.oldestConnectedAt),
      }}
    >
      <Renderer.Component.TableHead sticky nowrap>
        <Renderer.Component.TableCell {...col.head("client")} sortBy="client">
          Client
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("namespace")} sortBy="namespace">
          Namespace
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("kind")}>Kind</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("pods")} sortBy="pods">
          Pods
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("connections")} sortBy="connections">
          Connections
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("share")} sortBy="share">
          Share
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("channels")} sortBy="channels">
          Channels
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("library")}>Client library</Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("recv")} sortBy="recv">
          From client
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("send")} sortBy="send">
          To client
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell {...col.head("oldest")} sortBy="oldest">
          Oldest
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {items.map((g) => (
        <Renderer.Component.TableRow
          key={g.key}
          sortItem={g}
          nowrap
          className={g.kind === "unknown" ? undefined : "clickable"}
          onClick={g.kind === "unknown" ? undefined : () => onOpen(g)}
        >
          <Renderer.Component.TableCell {...col.cell("client")}>
            {/* One line per row: cells lay out inline, so details go in the tooltip and the Kind/Pods columns. */}
            <span className="RmqMono RmqEllipsis" title={clientDetails(g)}>
              {g.label}
            </span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("namespace")}>{g.namespace ?? "—"}</Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("kind")}>{clientKindLabel(g)}</Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("pods")}>
            {g.pods > 0 ? formatNumber(g.pods) : "—"}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("connections")}>
            {formatNumber(g.connections)}
            {g.blocked > 0 ? (
              <Renderer.Component.Badge small label={`${formatNumber(g.blocked)} blocked`} className="error" />
            ) : null}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("share")}>
            {Math.round(g.share * 100)}%
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("channels")}>
            {formatNumber(g.channels)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("library")}>
            <span className="RmqEllipsis">{g.libraries.join(", ") || "—"}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("recv")}>
            {formatBytesRate(g.recvRate)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("send")}>
            {formatBytesRate(g.sendRate)}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell {...col.cell("oldest")}>
            <span title={formatTimestamp(g.oldestConnectedAt)}>
              {g.oldestConnectedAt === undefined ? "—" : formatDuration(Date.now() - g.oldestConnectedAt)}
            </span>
          </Renderer.Component.TableCell>
        </Renderer.Component.TableRow>
      ))}
    </Renderer.Component.Table>
  );
}

const GROUPINGS: Renderer.Component.SelectOption<ClientGrouping>[] = [
  { value: "workload", label: "Group by workload" },
  { value: "pod", label: "Group by pod" },
];

export function ConnectionsPage(props: ConnectionsPageProps) {
  const page = useTargetPage(props, props.params?.target);
  const { target, selection } = page;
  const writeMode = useWriteMode(props.writeMode, target?.targetId);
  const [query, setQuery] = usePageParam(props.params?.query);
  const [rawView, setView] = useSelectionParam("connections.view", props.params?.view);
  const view = (VIEWS.some((v) => v.value === rawView) ? rawView : "connections") as View;
  const debouncedQuery = useDebounced(query);
  const scope = target ? `${page.clusterKey}:${target.targetId}` : undefined;

  const connections = useResource(
    scope ? `connections:${scope}` : undefined,
    () => props.client.connections(page.request()),
    {
      refreshMs: RABBITMQ_LIVE_REFRESH_MS,
      enabled: view === "connections" || view === "clients",
    },
  );
  const [rawGrouping, setGrouping] = useSelectionParam("connections.clientsBy", undefined);
  const grouping: ClientGrouping = rawGrouping === "pod" ? "pod" : "workload";
  // Set when a workload row is opened: the pod grouping then shows exactly that workload's pods.
  const [drillWorkload, setDrillWorkload] = useSelectionParam("connections.clientsWorkload", undefined);
  const addresses = useMemo(() => clientAddresses(connections.data?.items ?? []), [connections.data]);
  // Pods change far less often than connection counters: resolve when the address set changes, then once a minute.
  const clientPods = useResource(
    scope && addresses.length > 0 ? `client-pods:${scope}:${addressFingerprint(addresses)}` : undefined,
    () => props.client.clientPods({ clusterId: props.kubernetesClusterId, ips: addresses }),
    { refreshMs: RABBITMQ_CLIENT_PODS_REFRESH_MS, enabled: view === "clients" },
  );
  const channels = useResource(scope ? `channels:${scope}` : undefined, () => props.client.channels(page.request()), {
    refreshMs: RABBITMQ_LIVE_REFRESH_MS,
    enabled: view === "channels",
  });
  const consumers = useResource(
    scope ? `consumers:${scope}` : undefined,
    () => props.client.consumers(page.request()),
    {
      refreshMs: RABBITMQ_LIVE_REFRESH_MS,
      enabled: view === "consumers",
    },
  );
  const active =
    view === "connections" || view === "clients" ? connections : view === "channels" ? channels : consumers;

  const filteredConnections = useMemo(
    () =>
      (connections.data?.items ?? []).filter((c) =>
        matchesQuery(
          debouncedQuery,
          c.name,
          c.clientProperties.connectionName,
          c.clientProperties.product,
          c.user,
          c.vhost,
          c.peerHost,
          c.state,
        ),
      ),
    [connections.data, debouncedQuery],
  );
  const filteredClients = useMemo(
    () =>
      groupConnectionsByClient(connections.data?.items ?? [], clientPods.data ?? [], grouping).filter(
        (g) =>
          (grouping !== "pod" ||
            !drillWorkload ||
            (g.namespace && g.workloadKind && g.workload
              ? workloadKey(g.namespace, g.workloadKind, g.workload) === drillWorkload
              : false)) &&
          // `ip:` too, so the query left behind by opening a client's connections still finds that client.
          matchesQuery(
            debouncedQuery,
            g.label,
            g.namespace,
            g.workload,
            ...g.ips,
            ...g.ips.map((ip) => `${ip}:`),
            ...g.users,
            ...g.libraries,
          ),
      ),
    [connections.data, clientPods.data, grouping, drillWorkload, debouncedQuery],
  );
  const filteredChannels = useMemo(
    () =>
      (channels.data?.items ?? []).filter((c) =>
        matchesQuery(debouncedQuery, c.name, c.connectionName, c.user, c.vhost, c.state),
      ),
    [channels.data, debouncedQuery],
  );
  const filteredConsumers = useMemo(
    () =>
      (consumers.data?.items ?? []).filter((c) =>
        matchesQuery(debouncedQuery, c.queue, c.consumerTag, c.connectionName, c.user, c.peerHost),
      ),
    [consumers.data, debouncedQuery],
  );

  const metrics: Metric[] = useMemo(() => {
    if (view === "connections") {
      const items = filteredConnections;
      return [
        { label: "Connections", value: formatNumber(items.length) },
        {
          label: "Blocked",
          value: formatNumber(items.filter((c) => /block/.test(c.state)).length),
          tone: items.some((c) => /block/.test(c.state)) ? "error" : undefined,
        },
        { label: "Channels", value: formatNumber(items.reduce((s, c) => s + c.channels, 0)) },
        { label: "TLS", value: formatNumber(items.filter((c) => c.ssl).length) },
        { label: "Inbound", value: formatBytesRate(items.reduce((s, c) => s + (c.recvBytes.rate ?? 0), 0)) },
        { label: "Outbound", value: formatBytesRate(items.reduce((s, c) => s + (c.sendBytes.rate ?? 0), 0)) },
      ];
    }
    if (view === "clients") {
      const items = filteredClients;
      const top = items[0];
      const outside = items.filter((g) => g.kind === "external").length;
      return [
        { label: "Clients", value: formatNumber(items.length) },
        { label: "Connections", value: formatNumber(items.reduce((s, g) => s + g.connections, 0)) },
        {
          label: "Busiest client",
          value: top ? `${Math.round(top.share * 100)}%` : "—",
          title: top ? `${top.label}: ${formatNumber(top.connections)} connections` : undefined,
          tone: top && items.length > 1 && top.share >= 0.5 ? "warning" : undefined,
        },
        {
          label: "Not a pod",
          value: formatNumber(outside),
          title: "Peer addresses that match no running pod: clients outside the cluster, NAT, or a service mesh proxy",
        },
      ];
    }
    if (view === "channels") {
      const items = filteredChannels;
      const noPrefetch = items.filter(
        (c) => c.consumerCount > 0 && c.prefetchCount === 0 && c.globalPrefetchCount === 0,
      ).length;
      return [
        { label: "Channels", value: formatNumber(items.length) },
        { label: "Consumers", value: formatNumber(items.reduce((s, c) => s + c.consumerCount, 0)) },
        { label: "Unacked", value: formatNumber(items.reduce((s, c) => s + c.messagesUnacknowledged, 0)) },
        {
          label: "No prefetch limit",
          value: formatNumber(noPrefetch),
          tone: noPrefetch > 0 ? "warning" : undefined,
          title: "Consuming channels without a prefetch (QoS) limit",
        },
        {
          label: "Flow-controlled",
          value: formatNumber(items.filter((c) => c.state === "flow").length),
          tone: items.some((c) => c.state === "flow") ? "warning" : undefined,
        },
      ];
    }
    const items = filteredConsumers;
    return [
      { label: "Consumers", value: formatNumber(items.length) },
      { label: "Queues consumed", value: formatNumber(new Set(items.map((c) => `${c.vhost}/${c.queue}`)).size) },
      {
        label: "Auto-ack",
        value: formatNumber(items.filter((c) => !c.ackRequired).length),
        tone: items.some((c) => !c.ackRequired) ? "warning" : undefined,
      },
      { label: "Inactive", value: formatNumber(items.filter((c) => !c.active).length) },
    ];
  }, [view, filteredConnections, filteredClients, filteredChannels, filteredConsumers]);

  // A workload row drills down to exactly its pods; a pod or an outside address to its connections.
  // Connection names start with `<peer>:<port> ->`, so `<ip>:` matches that peer and not 10.0.0.1x.
  const openClient = (g: ClientGroup) => {
    if (g.kind === "workload" && g.namespace && g.workloadKind && g.workload) {
      setDrillWorkload(workloadKey(g.namespace, g.workloadKind, g.workload));
      setGrouping("pod");
    } else if (g.ips[0]) {
      setView("connections");
      setQuery(`${g.ips[0]}:`);
    }
  };
  const drillLabel = drillWorkload.replace(/^workload:/, "");
  // Until the first pod lookup answers, every client would read as "Not a pod".
  const podsPending = view === "clients" && addresses.length > 0 && !clientPods.data && !clientPods.error;

  const openQueue = (vh: string, queue: string) =>
    props.navigate(RABBITMQ_PAGE_IDS.queues, {
      target: target?.targetId ?? "",
      queue: `${encodeURIComponent(vh)}/${encodeURIComponent(queue)}`,
      view: "consumers",
    });

  const count =
    view === "connections"
      ? filteredConnections.length
      : view === "clients"
        ? filteredClients.length
        : view === "channels"
          ? filteredChannels.length
          : filteredConsumers.length;
  const total = active.data?.totalCount;

  return (
    <PageShell
      title="Connections & channels"
      subtitle={
        target
          ? `${target.namespace}/${target.name} · live, refreshes every ${RABBITMQ_LIVE_REFRESH_MS / 1000}s`
          : "Select a RabbitMQ cluster"
      }
      actions={
        <>
          <TargetSelector
            targets={selection.targets}
            selected={target}
            onChange={selection.select}
            disabled={selection.discovery.loading}
          />
          <WriteModeToggle store={props.writeMode} target={target} enabled={writeMode} />
          <Renderer.Component.Button plain label="Refresh" onClick={active.reload} disabled={active.loading} />
        </>
      }
    >
      <Renderer.Component.Tabs<View> className="RmqTabs" value={view} onChange={(v) => setView(v)} withBorder>
        {VIEWS.map((v) => (
          <Renderer.Component.Tab key={v.value} value={v.value} label={v.label} />
        ))}
      </Renderer.Component.Tabs>
      <Toolbar>
        <SearchBox value={query} onChange={setQuery} placeholder={`Filter ${view}…`} />
        {view === "clients" ? (
          <Renderer.Component.Select
            options={GROUPINGS}
            value={grouping}
            onChange={(o: Renderer.Component.SelectOption<ClientGrouping> | null) => {
              setDrillWorkload("");
              setGrouping(o?.value ?? "workload");
            }}
            themeName="lens"
            menuPosition="fixed"
          />
        ) : null}
        <span className="RmqToolbarRight">
          {count}
          {view === "clients" ? ` clients from ${formatNumber(connections.data?.items.length)} connections` : null}
          {view !== "clients" && total !== undefined ? ` of ${total}` : ""} {view === "clients" ? "" : view}
          {active.data?.truncated ? " · list truncated" : ""}
        </span>
      </Toolbar>
      {active.data && !podsPending ? <MetricStrip ariaLabel={`${view} summary`} metrics={metrics} /> : null}
      {view === "clients" && grouping === "pod" && drillWorkload ? (
        <p className="RmqMuted">
          Pods of <span className="RmqMono">{drillLabel}</span>{" "}
          <Renderer.Component.Button plain label="Show all pods" onClick={() => setDrillWorkload("")} />
        </p>
      ) : null}
      {view === "clients" && clientPods.error ? (
        <p className="RmqMuted">
          Could not list pods to name the clients ({clientPods.error.message}); showing peer addresses instead.
        </p>
      ) : null}
      {active.error ? (
        <ConnectionErrorPanel
          error={active.error}
          target={target}
          client={props.client}
          clusterId={props.kubernetesClusterId}
          onRetry={active.reload}
        />
      ) : null}
      {active.loading && !active.data ? <LoadingState label={`Loading ${view}…`} /> : null}
      {podsPending && active.data ? <LoadingState label="Matching client addresses to pods…" /> : null}
      {active.data && count === 0 ? <EmptyState icon="power_off" title={`No ${view}`} /> : null}
      {count > 0 && !podsPending ? (
        <div className="RmqTableWrap">
          {view === "connections" ? <ConnectionsTable items={filteredConnections} /> : null}
          {view === "clients" ? <ClientsTable items={filteredClients} onOpen={openClient} /> : null}
          {view === "channels" ? <ChannelsTable items={filteredChannels} /> : null}
          {view === "consumers" ? (
            <ConsumersTable
              consumers={filteredConsumers}
              tableId="rabbitmq-consumers"
              showQueue
              onOpenQueue={openQueue}
            />
          ) : null}
        </div>
      ) : null}
    </PageShell>
  );
}
