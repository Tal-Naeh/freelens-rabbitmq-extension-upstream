import { Renderer } from "@freelensapp/extensions";
import { useMemo, useRef } from "react";
import { RABBITMQ_HEALTH_REFRESH_MS } from "../../common/constants";
import { ConnectionErrorPanel } from "../components/connection-error";
import {
  EmptyState,
  LoadingState,
  MetricStrip,
  PageShell,
  TargetSelector,
  WriteModeToggle,
} from "../components/page-shell";
import { formatTimestamp } from "../format";
import { analyzeHealth, countBySeverity, HEALTH_RULES, trackStuckChannels } from "../health";
import { useResource } from "../hooks";
import { RABBITMQ_PAGE_IDS } from "../navigation";
import { useWriteMode } from "../write-mode-store";
import { useTargetPage } from "./page-deps";

import type { Metric } from "../components/page-shell";
import type { HealthFinding, HealthSeverity } from "../health";
import type { PageDeps, Param } from "./page-deps";

export interface HealthPageProps extends PageDeps {
  params?: { target: Param };
}

const SEVERITY_LABEL: Record<HealthSeverity, string> = { critical: "Critical", warning: "Warning", info: "Info" };
const SEVERITY_TONE: Record<HealthSeverity, string> = { critical: "error", warning: "warning", info: "" };
const CATEGORY_LABEL: Record<HealthFinding["category"], string> = {
  node: "Node",
  queue: "Queue",
  consumer: "Consumers",
  routing: "Routing",
};

export function HealthPage(props: HealthPageProps) {
  const page = useTargetPage(props, props.params?.target);
  const { target, selection } = page;
  const writeMode = useWriteMode(props.writeMode, target?.targetId);
  // When each channel was first seen full with no acks, per target, carried across refreshes.
  const stuckSince = useRef<{ targetId?: string; since: Record<string, number> }>({ since: {} });
  const health = useResource(
    target ? `health:${page.clusterKey}:${target.targetId}` : undefined,
    async () => {
      const [overview, queues, channels] = await Promise.all([
        props.client.overview(page.request()),
        props.client.queues(page.request()),
        props.client.channels(page.request()),
      ]);
      const targetId = target?.targetId;
      const now = Date.now();
      const previous = stuckSince.current.targetId === targetId ? stuckSince.current.since : {};
      const since = trackStuckChannels(previous, channels.items, now);
      stuckSince.current = { targetId, since };
      return {
        targetId,
        findings: analyzeHealth({ overview, queues: queues.items, channels: channels.items, stuckSince: since, now }),
        queueCount: queues.items.length,
        channelCount: channels.items.length,
        truncated: queues.truncated || channels.truncated,
      };
    },
    { refreshMs: RABBITMQ_HEALTH_REFRESH_MS },
  );
  // useResource keeps the previous result while a new target loads; never show one target's findings under another.
  const data = health.data?.targetId === target?.targetId ? health.data : undefined;
  const counts = useMemo(() => countBySeverity(data?.findings ?? []), [data]);

  const open = (f: HealthFinding) => {
    const targetId = target?.targetId ?? "";
    switch (f.subject.kind) {
      case "queue":
        props.navigate(RABBITMQ_PAGE_IDS.queues, {
          target: targetId,
          queue: `${encodeURIComponent(f.subject.vhost)}/${encodeURIComponent(f.subject.name)}`,
        });
        break;
      case "channel":
        props.navigate(RABBITMQ_PAGE_IDS.connections, { target: targetId, view: "channels", query: f.subject.name });
        break;
      case "node":
        props.navigate(RABBITMQ_PAGE_IDS.overview, { target: targetId });
        break;
      case "cluster":
        props.navigate(RABBITMQ_PAGE_IDS.exchanges, { target: targetId });
        break;
    }
  };
  const openLabel = (f: HealthFinding) =>
    ({ queue: "Open queue", channel: "Open channel", node: "Open nodes", cluster: "Open exchanges" })[f.subject.kind];

  const metrics: Metric[] = data
    ? [
        { label: "Critical", value: counts.critical, tone: counts.critical > 0 ? "error" : "ok" },
        { label: "Warnings", value: counts.warning, tone: counts.warning > 0 ? "warning" : "ok" },
        { label: "Info", value: counts.info },
        { label: "Queues checked", value: data.queueCount.toLocaleString() },
        { label: "Channels checked", value: data.channelCount.toLocaleString() },
      ]
    : [];

  return (
    <PageShell
      title="Health"
      subtitle={target ? `${target.namespace}/${target.name}` : "Select a RabbitMQ cluster"}
      actions={
        <>
          <TargetSelector
            targets={selection.targets}
            selected={target}
            onChange={selection.select}
            disabled={selection.discovery.loading}
          />
          <WriteModeToggle store={props.writeMode} target={target} enabled={writeMode} />
          <Renderer.Component.Button plain label="Refresh" onClick={health.reload} disabled={health.loading} />
        </>
      }
    >
      {!target && !selection.discovery.loading ? (
        <EmptyState icon="search_off" title="No RabbitMQ cluster selected">
          <Renderer.Component.Button
            plain
            label="Go to Clusters"
            onClick={() => props.navigate(RABBITMQ_PAGE_IDS.clusters)}
          />
        </EmptyState>
      ) : null}
      {health.error ? (
        <ConnectionErrorPanel
          error={health.error}
          target={target}
          client={props.client}
          clusterId={props.kubernetesClusterId}
          onRetry={health.reload}
        />
      ) : null}
      {health.loading && !data ? <LoadingState label="Checking nodes, queues and channels…" /> : null}

      {data ? (
        <>
          <MetricStrip ariaLabel="Findings by severity" metrics={metrics} />
          {data.truncated ? (
            <p className="RmqMuted">
              The queue or channel list was truncated at the list limit; objects past it were not checked.
            </p>
          ) : null}
          {data.findings.length === 0 ? (
            <EmptyState icon="check_circle" title="No problems found">
              <span className="RmqMuted">Last checked {formatTimestamp(health.loadedAt)}</span>
            </EmptyState>
          ) : (
            <section className="RmqPanel RmqFindings" aria-label="Findings">
              {data.findings.map((f) => (
                <div key={f.id} className="RmqFinding">
                  <span className={`RmqTag ${SEVERITY_TONE[f.severity]}`.trim()}>{SEVERITY_LABEL[f.severity]}</span>
                  <span className="RmqTag">{CATEGORY_LABEL[f.category]}</span>
                  <div className="RmqFindingText">
                    <strong>{f.title}</strong>
                    <span className="RmqMuted">{f.detail}</span>
                  </div>
                  <Renderer.Component.Button plain label={openLabel(f)} onClick={() => open(f)} />
                </div>
              ))}
            </section>
          )}
          <details className="RmqPanel RmqHealthRules">
            <summary>What is checked</summary>
            <ul className="RmqRuleList">
              {HEALTH_RULES.map((r) => (
                <li key={r.text}>
                  <span className="RmqTag">{CATEGORY_LABEL[r.category]}</span>
                  <span>{r.text}</span>
                </li>
              ))}
            </ul>
            <p className="RmqMuted">
              Rates are the Management API's recent samples. Peeking messages in the Message Inspector requeues them,
              which counts as a redelivery for a moment.
            </p>
          </details>
        </>
      ) : null}
    </PageShell>
  );
}
