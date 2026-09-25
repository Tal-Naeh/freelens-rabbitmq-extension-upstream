import { Renderer } from "@freelensapp/extensions";
import { useMemo, useState } from "react";
import { RABBITMQ_PEEK_DEFAULT_COUNT, RABBITMQ_PEEK_MAX_COUNT } from "../../common/constants";
import { parseIpcError } from "../../common/errors";
import { ErrorPanel, KeyValueList, SearchBox } from "../components/page-shell";
import {
  DEATH_HEADER_KEYS,
  DEATH_REASON_TEXT,
  type DeathInfo,
  deathReason,
  matchesMessage,
  matchesReason,
  parseDeath,
  summarizeDeaths,
} from "../dead-letter";
import { formatBytes, formatTimestamp, prettyJson } from "../format";

import type { MessagesPeekDto, PeekedMessageDto, RabbitmqIpcErrorShape } from "../../common/ipc";

function Payload({ message }: { message: PeekedMessageDto }) {
  if (message.contentKind === "binary") {
    return (
      <>
        <p className="RmqMuted">Binary payload shown as base64.</p>
        <pre>{message.payload}</pre>
      </>
    );
  }
  return <pre>{message.contentKind === "json" ? prettyJson(message.payload) : message.payload}</pre>;
}

function routingKeysText(keys: string[]): string {
  return keys.length > 0 ? keys.join(", ") : "(empty routing key)";
}

/** Decoded `x-death`: why the message was dead-lettered, where, how often, and where it came from. */
function DeathHistory({ death }: { death: DeathInfo }) {
  const reason = deathReason(death);
  return (
    <>
      <h3>Dead-letter history</h3>
      <p className="RmqMuted">{DEATH_REASON_TEXT[reason] ?? `Dead-lettered with reason "${reason}".`}</p>
      <KeyValueList
        entries={[
          ...(death.original
            ? ([
                [
                  "Originally published to",
                  <code>
                    {death.original.exchange || "(default exchange)"} · {routingKeysText(death.original.routingKeys)}
                  </code>,
                ],
              ] as [string, JSX.Element][])
            : []),
          ...death.history.map(
            (e) =>
              [
                `${e.reason} in ${e.queue}`,
                <span>
                  {e.count > 1 ? `${e.count} times · ` : ""}
                  <code>
                    {e.exchange || "(default exchange)"} · {routingKeysText(e.routingKeys)}
                  </code>
                  {e.time !== undefined ? ` · ${formatTimestamp(e.time)}` : ""}
                </span>,
              ] as [string, JSX.Element],
          ),
        ]}
      />
    </>
  );
}

function MessageCard({ message }: { message: PeekedMessageDto }) {
  const [open, setOpen] = useState(message.index === 0);
  const props = message.properties;
  const death = useMemo(() => parseDeath(message), [message]);
  // `properties` is `[]` when the broker sent none; the decoded history replaces the x-death headers.
  const rawHeaders = (props.headers as Record<string, unknown> | undefined) ?? {};
  const headers = death
    ? Object.fromEntries(Object.entries(rawHeaders).filter(([k]) => !DEATH_HEADER_KEYS.has(k)))
    : rawHeaders;
  const propEntries = Object.entries(props).filter(([k]) => k !== "headers");
  return (
    <article className="RmqMessage">
      <div className="RmqMessageHead" onClick={() => setOpen((o) => !o)} role="button" tabIndex={0}>
        <Renderer.Component.Icon material={open ? "expand_more" : "chevron_right"} small />
        <span className="RmqMessageIdx">#{message.index + 1}</span>
        <span className="RmqMono">{message.routingKey || "(no routing key)"}</span>
        <span className="RmqMessageMeta">via {message.exchange || "(default exchange)"}</span>
        <span className="RmqMessageSpacer" />
        {death ? (
          <>
            <Renderer.Component.Badge small label={deathReason(death)} className="warning" />
            <span className="RmqMessageMeta">
              from {death.lastQueue ?? "?"}
              {death.total > 1 ? ` · ${death.total}×` : ""}
            </span>
          </>
        ) : null}
        <Renderer.Component.Badge small label={message.contentKind.toUpperCase()} />
        {message.redelivered ? <Renderer.Component.Badge small label="redelivered" /> : null}
        {message.truncated ? <Renderer.Component.Badge small label="truncated" className="warning" /> : null}
        <span className="RmqMessageMeta">{formatBytes(message.payloadBytes)}</span>
      </div>
      {open ? (
        <div className="RmqMessageBody">
          <Payload message={message} />
          {death ? <DeathHistory death={death} /> : null}
          {propEntries.length > 0 ? (
            <>
              <h3>Properties</h3>
              <KeyValueList
                entries={propEntries.map(([k, v]) => [k, <code>{typeof v === "string" ? v : JSON.stringify(v)}</code>])}
              />
            </>
          ) : null}
          {Object.keys(headers).length > 0 ? (
            <>
              <h3>Headers</h3>
              <KeyValueList
                entries={Object.entries(headers).map(([k, v]) => [
                  k,
                  <code>{typeof v === "string" ? v : JSON.stringify(v)}</code>,
                ])}
              />
            </>
          ) : null}
        </div>
      ) : null}
    </article>
  );
}

/**
 * Read-only Message Inspector: fetches up to N messages with `ack_requeue_true`, so nothing is
 * consumed. The broker still flags them `redelivered` — this is inherent to the Management API.
 */
export function MessageInspector({
  peek,
  queueMessages,
}: {
  peek: (count: number) => Promise<MessagesPeekDto>;
  queueMessages: number;
}) {
  const [count, setCount] = useState(String(RABBITMQ_PEEK_DEFAULT_COUNT));
  const [result, setResult] = useState<MessagesPeekDto>();
  const [error, setError] = useState<RabbitmqIpcErrorShape>();
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState("");
  const [reason, setReason] = useState("");
  const messages = result?.messages ?? [];
  const summary = useMemo(() => summarizeDeaths(messages), [messages]);
  const reasonOptions: Renderer.Component.SelectOption<string>[] = [
    { value: "", label: "All messages" },
    ...summary.byReason.map((r) => ({ value: r.reason, label: `${r.reason} (${r.messages})` })),
    ...(summary.deadLettered < summary.total
      ? [{ value: "none", label: `Not dead-lettered (${summary.total - summary.deadLettered})` }]
      : []),
  ];
  // A new peek may not contain the reason picked for the previous batch: fall back to all messages.
  const activeReason = reasonOptions.some((o) => o.value === reason) ? reason : "";
  const shown = useMemo(
    () => messages.filter((m) => matchesReason(m, activeReason) && matchesMessage(m, query)),
    [messages, activeReason, query],
  );

  const run = async () => {
    const n = Math.max(1, Math.min(RABBITMQ_PEEK_MAX_COUNT, Number.parseInt(count, 10) || RABBITMQ_PEEK_DEFAULT_COUNT));
    setBusy(true);
    setError(undefined);
    try {
      setResult(await peek(n));
    } catch (err) {
      setError(parseIpcError(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="RmqMessages">
      <div className="RmqNotice">
        <Renderer.Component.Icon material="verified_user" small />
        <p>
          Safe peek: messages are fetched with <code>ackmode=ack_requeue_true</code> and immediately re-queued. Nothing
          is consumed or lost. Peeked messages will carry the <em>redelivered</em> flag afterwards.
        </p>
      </div>
      <div className="RmqDrawerToolbar">
        <span className="RmqMuted">Messages to peek (1–{RABBITMQ_PEEK_MAX_COUNT})</span>
        <Renderer.Component.Input
          theme="round-black"
          value={count}
          onChange={setCount}
          type="number"
          min={1}
          max={RABBITMQ_PEEK_MAX_COUNT}
          style={{ width: 80 }}
        />
        <Renderer.Component.Button
          primary
          label={busy ? "Peeking…" : "Peek"}
          disabled={busy || queueMessages === 0}
          onClick={() => void run()}
        />
        {queueMessages === 0 ? <span className="RmqMuted">Queue is empty</span> : null}
        {result ? <span className="RmqMuted">{result.messages.length} message(s) fetched</span> : null}
      </div>
      {error ? <ErrorPanel error={error} onRetry={() => void run()} /> : null}
      {summary.deadLettered > 0 ? (
        <div className="RmqNotice warning">
          <Renderer.Component.Icon material="report" small />
          <p>
            <strong>
              {summary.deadLettered} of {summary.total} dead-lettered.
            </strong>{" "}
            Why: {summary.byReason.map((r) => `${r.reason} ${r.messages}`).join(", ")}. Died in:{" "}
            {summary.byQueue.map((q) => `${q.queue} (${q.messages})`).join(", ")}.
          </p>
        </div>
      ) : null}
      {messages.length > 0 ? (
        <div className="RmqDrawerToolbar">
          <SearchBox value={query} onChange={setQuery} placeholder="Search payload, routing key, headers…" />
          {summary.deadLettered > 0 ? (
            <Renderer.Component.Select
              options={reasonOptions}
              value={activeReason}
              onChange={(o: Renderer.Component.SelectOption<string> | null) => setReason(o?.value ?? "")}
              themeName="lens"
              menuPosition="fixed"
            />
          ) : null}
          <span className="RmqMuted">
            {shown.length} of {messages.length} shown
          </span>
        </div>
      ) : null}
      {shown.map((m) => (
        <MessageCard key={m.index} message={m} />
      ))}
      {messages.length > 0 && shown.length === 0 ? <p className="RmqMuted">No peeked message matches.</p> : null}
    </div>
  );
}
