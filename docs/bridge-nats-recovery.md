# Bridge NATS recovery

## Scope

The bridge already requests unlimited NATS reconnects. This handles ordinary
transport loss, but not a connection that has entered the terminal CLOSED state.
Previously the close callback only logged the condition, leaving all subsequent
publishes pointed at a closed connection. The original trigger for api02's
closure is not established by the retained incident logs.

The publisher now has one recovery/sender worker. Normal reconnects remain the
responsibility of nats.go. A nil or terminally closed connection is recreated by
the worker, never by a library callback. Repeated creation/delivery errors use
exponential backoff capped at 30 seconds. A flush timeout resets the NATS
transport only. None of these paths disconnect WhatsApp clients. Intentional
shutdown cancels the worker and cannot trigger connection recreation.

## Durable Outbox

Every enabled publisher event is serialized and committed before network delivery.
The default spool is `storages/bridge-events.db`, separate from chat storage.
Configure it with `nats.outbox_path` in the bridge YAML, or
`BRIDGE_NATS_OUTBOX_PATH`. The directory must be writable and persistent across
container replacement/deployments; do not share a spool between running bridge
processes. The SQLite file and its sidecars contain message data and must remain
private. SQLite WAL and FULL synchronous commits are enabled. The database is
bounded to 1 GiB (plus WAL overhead); reaching capacity causes an explicit error,
not automatic deletion of business events. Monitor free disk space as well.

Queued events are replayed FIFO in batches of 256. Rows are removed only after a
successful NATS flush. Oversized or invalid-subject events are retained with a
`failure` reason and skipped so one event cannot block every account. Inspect
quarantined rows in `bridge_event_outbox`, correct the broker limit or event
source, and clear `failure` only after correction. Events resume automatically
after the correction; no process restart is required for broker outages or
closed connections.

Heartbeats older than 90 seconds are discarded, because replaying an old online
snapshot would misrepresent current state. Business messages and status events
are not expired. Ordinary account heartbeats continue to report current state.

Subjects and existing JSON fields are unchanged. An additive `eventId` and the
`Nats-Msg-Id` header identify the same queued event across retries. A crash after
flush and before deleting the row can produce duplicates; consumers should use
stable event/message IDs for idempotency. Core NATS does not deduplicate from
this header by itself.

This is durable retry up to the Core NATS server, **not end-to-end exactly-once
delivery**. Flush does not acknowledge consumer processing or guarantee server
disk persistence. Offline consumers, permission failures, and broker failure
after receipt require consumer idempotency and potentially a separately designed
JetStream contract. No consumer/proto migration is introduced here. Events that
failed before this change cannot be recovered from a spool that did not exist;
message history recovery is a separate mechanism.

If serialization or disk enqueue fails, an error counter increases, logs are
rate-limited, and readiness stays failed until operator investigation/restart.
There is no claim of durability for events that could not be committed. An
unwritable spool fails service construction instead of silently disabling NATS.

## Monitoring

`/ready` returns 503 when the publisher is disconnected or has an enqueue failure.
Do not make NATS readiness failure automatically restart the bridge: that would
unnecessarily disconnect WhatsApp accounts. Use `/health` for process liveness.

Online counts are sampled in the background every 10 seconds. `/metrics` does
not acquire per-device locks or query SQL. Missing/samples older than 90 seconds
return 503 and omit the online count, rather than advertising stale data as
current. A stuck sampler does not spawn additional sampler goroutines.

New metrics:

- `bridge_metrics_snapshot_fresh`, `bridge_metrics_snapshot_age_seconds`
- `bridge_nats_connected`
- `bridge_nats_connections_created_total` (includes initial creation)
- `bridge_nats_failures_total`, `bridge_nats_enqueue_failures_total`
- `bridge_nats_outbox_pending`, `bridge_nats_outbox_quarantined`
- `bridge_nats_outbox_storage_healthy`
- `bridge_nats_outbox_oldest_age_seconds` (when nonempty)

Alert on disconnects lasting more than 30 seconds, any enqueue failure or
quarantine, oldest queued events older than 60 seconds, and stale metrics. These
are metric signals; an external monitoring system must configure the alerts.

## Verification and Rollout

From `src`, run `go test ./ui/bridge`. To run real-broker fault injection, install
`nats-server` and set `NATS_SERVER_BIN` to its absolute path:

```sh
NATS_SERVER_BIN=/path/to/nats-server go test -race ./ui/bridge -run 'TestNATS|TestMetrics' -count=3
```

The tests cover repeated terminal closure, broker restart, offline enqueue,
publisher restart and FIFO replay with stable IDs, oversized-event quarantine,
expired heartbeats, concurrent enqueues, intentional shutdown, unwritable spool,
and nonblocking/stale metric responses.

Deploy to the test bridge first and verify these metrics and consumer behavior.
An initial process restart is required to load the new code; a running old binary
cannot be retrofitted with the recovery worker. Production rollout must account
for that one-time WhatsApp reconnect. Preserve the outbox file during upgrades
and rollback; an older binary will not drain it. Do not delete pending rows to
make an alert disappear.
