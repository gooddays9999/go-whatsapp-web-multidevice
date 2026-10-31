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
exponential backoff capped at 30 seconds. Batch flushes allow 10 seconds, and
three consecutive flush timeouts reset the NATS transport only. A successful
flush or different error resets the timeout streak. A single slow PONG no longer
forces a reconnect and replays the unconfirmed batch. None of these paths disconnect WhatsApp clients. Intentional
shutdown cancels the worker and cannot trigger connection recreation.

## Durable Outbox

Every enabled publisher event is serialized and committed before network delivery.
The default spool is `storages/bridge-events.db`, separate from chat storage.
Configure it with `nats.outbox_path` in the bridge YAML, or
`BRIDGE_NATS_OUTBOX_PATH`. The directory must be writable and persistent across
container replacement/deployments; do not share a spool between running bridge
processes. The SQLite file and its sidecars contain message data and must remain
private. SQLite WAL and FULL synchronous commits are enabled. The database is
bounded to 1 GiB by default (plus WAL overhead). Set
`BRIDGE_NATS_OUTBOX_MAX_BYTES=8589934592` for an 8 GiB deployment. Values are
integer bytes from 1 MiB through 1 TiB; invalid values fail startup. Changes
require a service restart. Existing rows are retained when expanding or lowering
the limit; SQLite cannot shrink below the existing database page count. Reaching capacity causes an explicit error,
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

If serialization or disk enqueue fails, an error counter increases and logs are
rate-limited. Readiness fails immediately, but is no longer permanently latched
to the cumulative failure count. After 30 seconds without a new enqueue failure,
a successful durable write probe can restore readiness. The failure counter is
not reset, and recovering readiness does not recover the uncommitted events.
The probe inserts and deletes an internal row in one committed transaction, so
no probe event is visible to the NATS sender. A concurrent new failure invalidates
the probe's recovery generation. Statistics and probes run in the sampler, never
in HTTP scrape handlers. Enqueue/transaction budgets are 15/10 seconds to tolerate
burst I/O; an earlier caller deadline still applies.
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
Fleet scans now copy the unsorted registry and try each instance/socket lock
without waiting. Busy accounts are excluded from the confirmed online count and
heartbeat list, and counted separately as unknown. Sampling never overwrites
their cached state or declares them logged out. Per-account RPCs using the
original blocking state API are not repaired by this sampling-only change.

New metrics:

- `bridge_metrics_snapshot_fresh`, `bridge_metrics_snapshot_age_seconds`
- `bridge_accounts_state_unknown` (busy accounts, not confirmed offline)
- `bridge_nats_connected`
- `bridge_nats_connections_created_total` (includes initial creation)
- `bridge_nats_failures_total`, `bridge_nats_enqueue_failures_total`
- `bridge_nats_enqueue_healthy` (current recoverable health, not historical total)
- `bridge_nats_outbox_pending`, `bridge_nats_outbox_quarantined`
- `bridge_nats_outbox_used_bytes`, `bridge_nats_outbox_max_bytes` (allocated pages
  excluding reusable free pages; excludes WAL and is an approximate utilization)
- `bridge_nats_outbox_storage_healthy`
- `bridge_nats_outbox_oldest_age_seconds` (when nonempty)

Alert on disconnects lasting more than 30 seconds, any enqueue failure or
quarantine, oldest queued events older than 60 seconds, and stale metrics. These
are metric signals; an external monitoring system must configure the alerts.
For enqueue failures, alert on a counter increase (for example over five minutes)
or sustained `bridge_nats_enqueue_healthy == 0`, not on the lifetime counter
being nonzero after recovery.

## Bounded API02 Recovery

The operational helper is `scripts/api02-fast-reconnect/`. Build/test from `src`:

```sh
go test -race ../scripts/api02-fast-reconnect/*.go
CGO_ENABLED=0 go build -o /tmp/api02-recover ../scripts/api02-fast-reconnect/main.go ../scripts/api02-fast-reconnect/health.go
```

The helper defaults to a dry run. It discovers the current bridge PID, reads the
existing account DB configuration without logging secrets, and requires a unique
server assignment for API02 (192.151.156.2:29092). It selects non-deleted assigned
accounts in web state 1/2, excludes logged-out/banned states, verifies matching
local environment and persisted JID/phone, and validates proxy configuration.
Each actual Connect rechecks account eligibility and asks the bridge to resolve
the latest proxy, without supplying a cached proxy itself.

Default execution bounds are 12 workers and 3 Connect requests/second. It checks
real `/ready` plus fresh metrics, current enqueue health, backlog <=256, no
quarantine, oldest event <=15s and a 45s quiet period after observed failures.
There is no legacy-readiness bypass. Unhealthy periods block workers instead of
marking every queued candidate deferred. After a continuous 10-minute wait or
termination signal, it stops with `paused_with_checkpoint`, not "all completed".
Progress includes remaining count and is replaced atomically; account results
and candidate lists persist in the private operation directory.

For example, on API02 after installing the helper:

```sh
./api02-recover -out /private/recovery/run2 -run -concurrency 12 -rate 3 \
  -resume-results /private/recovery/run1/results.jsonl
```

Resume inputs can be comma-separated result files, oldest first. The last result
for each account wins, so successful/explicitly offline/login-required accounts
are not retried. Candidates with no recorded result are retained; do not silently
drop unprocessed accounts when resuming a partial pass. Supply the original full-pass results together with interrupted
resume results so accounts not yet processed remain in the checkpoint. A status
query timeout or `account not logged in` alone is not proof of expired sessions.

### Adaptive Pacing Without a Bridge Restart

Recovery candidates are ordered by business priority first: accounts whose
comma-separated `usage` includes `broadcast` and whose `broadcast_direction` is
`two_way` (or blank/null legacy default) precede other purposes. Single-way
broadcast, active and rental accounts do not receive this priority. Previous
online evidence is only a secondary ordering within each purpose tier. Source
account purpose/direction values are read, never rewritten.

`-only-two-way-broadcast` selects an urgent two-way-only pass. Normal full passes
still include other purposes after that tier. Eligibility, current DB proxy,
session/phone matching, deleted-account and explicit-disconnect checks remain
unchanged. Changing this helper's ordering does not require restarting Go bridge;
an already-running automatic startup loop is not retroactively reordered.

On API02 at approximately 23:14 CST on 2026-10-29, the priority-aware helper
replaced only the auxiliary waiting/recovery task. Go bridge remained PID
2214072, with no additional restart. Helper SHA256:
`1bd26d50722c136daaed5934bbad23ca50886a81378434c360f15d4cd38a9c88`.
Files/checkpoints are under
`/opt/go-whatsapp-ims-bridge-prod2/recovery-20261029-two-way/` and unit
`api02-two-way-priority-recover-20261029.service`. The eligible two-way pass
selected 1,340 accounts; three more had no matching local environment. At launch
the old outbox had drained to zero. The priority pass uses six workers with a
2/s Connect cap to bound overlap with the already-running startup restore.
The subsequent full pass waits for startup restore, resumes the priority results
and uses 24 workers/adaptive pacing up to 6/s. Full-pass sorting also puts any
unfinished two-way work ahead of other purposes. Existing online accounts are
checked, not deliberately disconnected; all original checkpoints remain.

The helper supports opt-in `-adaptive`. This changes only the independent
recovery process, not the running Go bridge or its socket concurrency limit.
The original strict gate remains the default. With adaptive pacing and a caller
cap of six Connect calls/second:

- Up to 2,000 pending events, oldest age <=10s and utilization <50%: up to 6/s.
- Up to 20,000 pending events, oldest age <=30s and utilization <70%: up to 3/s.
- Up to 100,000 pending events, oldest age <=60s and utilization <90%: up to 1/s.
- Beyond these bounds, a stalled sender or unhealthy readiness/NATS/storage:
  pause; never bypass write failures, quarantine or stale metrics.

Rapid pending growth (>500 events/second between observations) also reduces
pacing to 1/s. Pending work with missing age/capacity/publish metrics is rejected.
New enqueue failures still require the quiet period and healthy durable writes.
Permits are unbuffered, so a pause cannot accumulate a burst of Connect tokens.
Progress reports include `connect_per_second`, the currently allowed rate rather
than a claim of achieved recovery throughput.

Build/test the helper separately and stop only the previous recovery unit so it
can save its checkpoint. Preserve that results file, validate a bounded pilot,
then resume using both previous and pilot results. Unprocessed candidates remain
eligible, while confirmed online and terminal results are not deliberately
reconnected. Never run overlapping recovery jobs or restart Go bridge for a
helper-only pacing adjustment. A prior checkpoint's pending retry results remain
distinct from accounts whose sessions are explicitly invalid.

On 2026-10-29 around 21:51 CST, the user authorized faster recovery without a
bridge restart. The helper passed local race tests (three runs), vet, and a
server-side test/build. Helper SHA256:
`7d77ba865f85f03ecca3d91358906e2bc8074217e0b9b4afe107bab014ec1bb0`.
The previous `api02-recover-all-outbox8-20261029.service` was stopped with its
checkpoint preserved under
`/opt/go-whatsapp-ims-bridge-prod2/recovery-20261029-adaptive/previous-results.jsonl`.
Two bounded passes checked 120 and 600 already-online accounts without
reconnecting them; these pilots alone are not evidence of faster Connect calls.
The subsequent full unit `api02-recover-adaptive-full-20261029.service` uses
`-adaptive -concurrency 24 -rate 6` and all three earlier results files. Its
progress/results are in that directory's `full/` subdirectory. Go bridge PID
remained 2156996 with zero automatic restarts; its internal concurrency limit
remains 24. At 21:54:31 CST, the full pass had checked 1,218 accounts, including
103 confirmed new recoveries, and was allowing 3 Connect calls/second. Queue
protection remained active; this is an in-progress observation, not a claim of
complete restoration or sustained six-per-second recovery.

After restarting the bridge, do not exclude previously successful accounts from
the candidate set: their old sockets no longer exist. Use `-prioritize-results`
instead of `-resume-results` to restore those accounts first while checking each
current state and leaving already-online accounts connected. Selection is still
restricted to currently eligible API02 accounts with persisted local sessions.
Startup restoration's server/environment matching discrepancy is a separate
issue; the operational pass does not fix that selection rule.

On 2026-10-29, the user explicitly authorized restarting API02 to apply the health
fix. Build ID: `e7bfa45-dirty-df33a485-health-recovery-20261029`; executable SHA256:
`d1a45d7e14a3443e3da26305afd5ea737ec5690c87f3f7be1e1ef414b8423116`.
Private binary/outbox/device-table backups and deployment script are under
`/opt/go-whatsapp-ims-bridge-prod2/releases/health-recovery-20261029`.
The new bridge PID is 2049096. The first resume-only job was stopped with a
checkpoint after discovering that startup restoration still matched only ten
environments. The replacement unit `api02-fast-reconnect-v3-20261029.service`
uses the fresh full eligible set, prioritizes previously confirmed online
accounts, and limits recovery to 12 workers/3 Connect calls per second. Logs and
atomic progress snapshots are under
`/opt/go-whatsapp-ims-bridge-prod2/recovery-20261029-v2/full-after-restart`.
API01 and the Node bridge were not restarted. The historical 1,335 failed
enqueues are not claimed to have been recovered by restoring readiness.
At 07:37 UTC, API02 reported readiness healthy, 526 active connections, zero new
enqueue failures and zero quarantined events; the replacement recovery job was
running. These are observations during recovery, not a claim of full recovery.

## Verification and Rollout

### API02 follow-up: queued events retained

At 11:48 UTC on 2026-10-29, the live goroutine profile showed the metrics and
heartbeat samplers waiting in `Client.IsConnected` behind a socket lock. Other
stacks showed websocket close-handshake reads while `Client.Disconnect` held
that lock. The event sender was waiting for a NATS flush. Broker inspection
showed no JetStream storage backlog: the retained queue is API02's local
`bridge-events.db`, not a broker-side stream. Its 1 GiB code limit had been
reached despite ample physical disk space; retained counts were approximately
1.07 million rows. Enqueue failures at capacity may already have lost events
that never committed.

The user explicitly chose to retain the queue and avoid restarting either the
OS or Go bridge. No purge, broker change, process restart or new recovery pass
was performed for this follow-up. The nonblocking sampler and longer,
consecutive-timeout transport recovery changes are local source changes only;
the existing API02 process cannot load them without a service restart. They do
not remove the 1 GiB bound, repair existing wedged sockets, or constitute a
confirmed recovery of the live backlog. Before rollout, plan queue headroom and
verify net drain rate; do not clear business events or restart on readiness
failure as a substitute for diagnosis.

### API02 8 GiB rollout

The user subsequently authorized deployment of the configurable capacity and
the tested sampler/NATS changes. At approximately 20:12 China Standard Time on
2026-10-29, only `go-whatsapp-ims-bridge-prod2.service` was restarted. Neither
the OS, API01, the shared NATS broker nor the test bridge was restarted. The
test host was used for an isolated Linux CGO build only.

Release: `/opt/go-whatsapp-ims-bridge-prod2/releases/outbox-8g-20261029`.
Build ID: `e7bfa45-dirty-outbox-8g-20261029`; executable SHA256:
`fd98bad89c3e6e71ebbbef1677081950dd5f380e598dea428ab252c0572d2fcb`.
The root module remains based on the existing uncommitted upgrade/fixes, not a
clean origin/main build. The release retains the source archive, previous
executable, private service configuration and a stopped-writer SQLite backup.
That backup contained 954,976 events. No outbox purge was performed.

`90-outbox-capacity.conf` in the service drop-in directory sets
`BRIDGE_NATS_OUTBOX_MAX_BYTES=8589934592`. The actual live connection's metric
confirmed this value, and readiness returned 200 with NATS/outbox healthy.
New PID: 2156996. At 20:12:58 CST, the sampled pending count had dropped to
709,512, confirmed online count was 810, busy/unknown count was 19, and both
NATS delivery failures and enqueue failures since restart were zero. Earlier
process failures are not erased from history or claimed recovered. Automatic
startup restoration was in progress, not complete. No duplicate operational
recovery job was launched over that startup restore.

macOS archive metadata initially caused the candidate binary's help check to
panic on an embedded `._*.sql` file. It was caught before stopping API02;
metadata was removed from the isolated build and the source archive recreated
with `COPYFILE_DISABLE=1 --no-xattrs`. The deployed binary passed the help check
and hash validation. Use that packaging precaution on future macOS builds.

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
