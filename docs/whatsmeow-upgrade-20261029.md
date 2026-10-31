# Whatsmeow upgrade: 2026-10-29

## Revisions

- Old: `v0.0.0-20260730092514-662ad1dc6900`.
- New: `v0.0.0-20261015105041-df33a485cc46`.
- Selection: the version pinned by go-whatsapp-web-multidevice upstream main,
  confirmed again at upstream `3af52ad` on 2026-10-29. This is not a claim to use
  whatsmeow's newest development commit.
- Module checksum: `h1:xeAZj3c2eIlpfXvMjmTvvXEGh63iwSTvkdTtcO9RCk4=`.

The actual vendored source was refreshed, not only the root requirement. The
local `replace go.mau.fi/whatsmeow => ./third_party/whatsmeow` remains active.
Comparing with the verified upstream module leaves exactly the documented
`send.go`, `newsletter.go`, `retry.go` patches and three local test files (plus
patch documentation); upstream repository-only metadata is not vendored.

## Compatibility

Go 1.26.0 is now the minimum because the new library uses Go 1.26 language and
standard-library APIs. Root builds were run with Go 1.26.0, and bottom-layer
tests with Go 1.26.10. The nested upstream go.mod retains its recommended Go 1.27.1 toolchain;
it is not the minimum for building the dependency from the root module. Docker
and release workflow builders are aligned to Go 1.26. Do not deploy using a
builder pinned to Go 1.25 with automatic toolchain downloads disabled.

Only dependency versions required by the new module graph were updated; Fiber
2, gRPC/proto definitions, NATS subjects, and the embedded frontend stay unchanged.
The previous NATS self-healing/outbox and account-isolation fixes remain intact.

The new SetStatusMessage API accepts `types.SetStatusInput`, not a string. The
bridge adapts its existing status text to an explicit text pointer, retaining an
empty string as an explicit clear request and leaving the new duration/emoji
options unset/defaulted. Live behavior of that upstream status operation still
needs staging validation; no WhatsApp account was modified in this upgrade.

Newsletter poll creation/vote extensions and own-linked-device retry behavior
were replayed over the new source and their regression tests retained. See
`src/third_party/whatsmeow/CODEX_PATCHES.md` for the patch inventory. No new
proxy policy or UA reassignment is introduced. The October library exposes a
native websocket UserAgent option, but this upgrade does not automatically change
existing accounts' websocket identity.

## Database Migrations

New automatic migrations are:

- v15: `whatsmeow_device.companion_meta_nonce TEXT NOT NULL DEFAULT ''`.
- v16: `whatsmeow_chat_settings.wasa_root_secret_id TEXT NOT NULL DEFAULT ''`.

Both are additive and marked compatible with v8+ by upstream. Migration tests
start from the exact old v14 schema, with synthetic session/key/token/mapping
data, run the new container's Upgrade twice, and verify:

- Version advances to 16, compatibility version stays 8.
- Existing device/linked-device JIDs, LID and registration identity remain.
- Noise, identity, signed-pre-key and ADV secret bytes remain unchanged.
- Signal session blobs, privacy tokens and PN/LID mappings remain usable.
- Chat settings remain and new columns default to empty values.
- A migrated device loads and saves the new companion nonce correctly.

SQLite validation uses a temporary file. PostgreSQL validation ran through a
temporary tunnel to the test server in a uniquely named, disposable schema with
public excluded from search_path. The schema was removed by test cleanup. No
existing session tables or running bridge processes were changed. To repeat on
an authorized test database, set `BRIDGE_TEST_POSTGRES_DSN` and run:

```sh
go test ./infrastructure/whatsapp -run TestWhatsmeowUpgradeFromV14PreservesSessions -count=1 -v
```

Without that variable the PostgreSQL subtest is explicitly skipped; SQLite still
runs. The fixture is synthetic and does not contain production credentials or
real session keys.

## Rollout and Rollback

The initial source upgrade did not deploy or restart services. The subsequently
authorized test-server deployment is recorded below. Before another rollout:

1. Back up the whatsmeow PostgreSQL/SQLite store, chat/environment registry,
   configuration and persistent NATS outbox. Keep the old binary and build hash.
2. Build with Go 1.26+, deploy to the test bridge, restart it once, and validate
   login, proxy-bound reconnect, retained UA/device profile, incoming/outgoing
   messages and media, delivery/read callbacks, groups, statuses and newsletter
   polls. Synthetic/unit tests do not replace these live checks.
3. Roll production instances one at a time after staging observation. Existing
   persisted sessions are intended to reconnect without being cleared, but do
   not promise every account will recover if WhatsApp has invalidated its session.
4. On rollback, inspect schema version/compatibility and stop all writers before
   restoring any database backup. Do not assume an untested binary rollback is
   safe merely because both migrations declare backward compatibility. Restoring
   an old backup also discards newer session changes; coordinate it explicitly.

The local Docker daemon is unavailable, so the actual Docker image build was not
run. PostgreSQL migration was verified separately as described above.

## Test Deployment Follow-Up

On 2026-10-29, deployed to test host `70.39.76.234` only. Production hosts and
the original Node bridge were not restarted. Ports remain gRPC `29091` and
health/metrics `29191`. Base is `e7bfa45` plus the uncommitted library upgrade
and the outbox batching follow-up; this is not an origin/main-only build.

Release/rollback directory:
`/opt/go-whatsapp-ims-bridge/releases/whatsmeow-df33a485-20261029T034754Z`.
The private backup contains the previous executable/source, PostgreSQL dump,
validated full chatstorage snapshot, latest environment/device SQL exports,
service configuration and first-rollout outbox snapshot. Do not publish backups.
Real session schema migrated from 15 to 16 with compatibility 8; device row count
was 358 immediately before and after migration.

The first restart exposed a burst-load regression in the newly introduced NATS
outbox: one FULL-sync transaction per event caused enqueue deadlines during
concurrent reconnect receipt bursts. About 23,933 enqueue failures were observed,
primarily message status callbacks. Those failed enqueues were not persisted;
do not claim that they were replayed or that callback completeness was verified.

The follow-up groups concurrent inserts into transactions of up to 256 events,
with a 2ms collection interval. FULL synchronous durability is retained and a
successful enqueue still waits for commit. A 2,000-concurrent-insert reopen test
was added, and passed with CGO, purego and the race detector. The bridge package
suite passed. A second test-only restart installed the follow-up, build ID
`e7bfa45-dirty-whatsmeow-df33a485-outbox-batch-20261029` and binary SHA256
`22ab1f0c3c08e15d7eec98c08e519b584518641219aa437223d8df5da0822ee1`.

Post-follow-up observation: readiness/NATS/outbox healthy, no enqueue failures or
quarantined events, and backlog drained. Online metric recovered to approximately
315 versus 331 verified before deployment; a separate baseline-account sweep
verified 314 online and 17 status-query timeouts at that time. Recovery remains
an observation in progress, not evidence that all previous sessions recovered.
No live messages, statuses, reactions or profile modifications were sent as part
of validation. Account 4 / 15812751827 successfully returned its own contact
details, including number, push name and avatar. Account 27's contact query timed
out at 12 seconds; that account's operation is not confirmed healthy by this test.

## API02 Deployment

On 2026-10-29, deployed the same tested executable to `192.151.156.2`, service
`go-whatsapp-ims-bridge-prod2.service`. Existing gRPC `29092` and health/metrics
`29192` remain unchanged. Executable SHA256 is the same `22ab1f0c...822ee1`
listed above. API01 and the Node bridge were not updated or restarted.

Release directory:
`/opt/go-whatsapp-ims-bridge-prod2/releases/whatsmeow-df33a485-batch-20261029`.
Source archive SHA256:
`32692cb4cd84e58cefc1f0493bbbcafb26bd2d21f1c73aaf53d6de03c72a0b8f`.

Before deployment, readiness returned 503, metrics timed out and the old bridge
continuously logged `nats: connection closed`; no reliable pre-restart online
count was available. The API02 host has no C compiler, so its attempted local
CGO-dependent tests failed before deployment. No executable from that attempt
was installed: the already tested test-server executable was transferred and
its full checksum verified instead. The isolated Go runtime is inside the
release directory, not installed into the system PATH.

Backups are private and include the old executable/unit, PostgreSQL schema plus
full data for the two migration-affected tables, a 38GB online SQLite snapshot,
and fresh device/environment SQL exports. This is NOT a full backup of the
131GB PostgreSQL database. PostgreSQL dump catalogs were validated. SQLite's
full quick_check was stopped because of slow disk reads; the snapshot opened
successfully and its device/environment tables were read, but full snapshot
integrity validation is not claimed. Session count was 19,622 immediately before
and after migration from v14 to v16, compatibility v8.

Initial post-deployment observation: new PID 2044386, readiness/NATS/outbox
healthy, GetBridgeStats gRPC succeeded, about 12,492 events published to NATS,
zero enqueue failures and zero quarantined events. Active connections reached
77 by 2026-10-29 06:43 UTC (up from 37 during earlier observation), with zero
enqueue failures and zero process restarts. Startup restore matched only 10 local environments out
of 23,289 account-database candidates, with 23,279 missing environment matches.
This deployment is NOT evidence of complete account recovery or correct server
assignment of those global candidates; do not count the 24,558 registered device
slots as online, and do not bulk reconnect all historical environments blindly.
The startup-restore matching discrepancy needs a separate recovery audit.

## Verification Results

- Root `go test ./...` and `go vet ./...`: passed.
- Root `go test -race ./usecase ./infrastructure/whatsapp ./ui/bridge`: passed,
  with the real-broker NATS recovery tests enabled.
- Root `go test -tags purego ./infrastructure/whatsapp ./ui/bridge ./usecase`:
  passed, including SQLite migration under the pure-Go driver.
- Vendored `GOTOOLCHAIN=go1.26.10 go test ./...` and the same command with
  `-race`: passed; root tests alone do not discover this nested module.
- Both SQLite and real PostgreSQL v14 migration tests: passed. A post-test
  query confirmed no temporary PostgreSQL migration-test schema remained.
- Native macOS CGO build and Linux amd64 `CGO_ENABLED=0 -tags purego` build:
  passed. The cross-build is not a substitute for the production CGO image build.
- Dependency tidy check and whitespace checks: clean.
- Live WhatsApp login/reconnect/send tests and Docker build: not run.
