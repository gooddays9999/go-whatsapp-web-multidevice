# Selective upstream synchronization: 2026-10-29

## Reference

- Repository: https://github.com/aldinokemal/go-whatsapp-web-multidevice
- Fetched upstream main: `4c4c4ed66d5130f1ef15ea2f92e331d1be8a8fbd`
  (2026-10-28).
- Latest published release at inspection: `v9.7.0`, `5e17e8c` (2026-10-25).
- This is a selective backport, not a full v9 migration or release-version bump.
- Pre-existing, uncommitted NATS self-healing/outbox changes are preserved.

## Applied

| Upstream change | Local adaptation |
| --- | --- |
| `4456510` / #903: scoped reaction lookup | Resolve the message by device and chat before choosing from-me and participant fields; retain existing send seams. |
| `1a7f9f7` / #905: scoped revoke lookup | Revoke and local cleanup use the calling device's message copy. |
| `56ac266` / #906: remaining device-scoped reads | Scope media keys, chat lists, history names, wrapper reads, delete-for-me and disappearing-message settings; reject a chat-list read with no account identity. |
| `fa91896` / #910: group-name cache isolation | Cache by account plus group, invalidate on subject changes, and prevent an old fetch from restoring an invalidated name. Unresolved device contexts cannot borrow the global account. |
| `3971c4c` / #864: own linked-device read receipts | Preserve the primary-device filter for contacts, but allow read/read-self from the account's own linked devices. The bridge's NATS receipt path already forwards these types. |

The fork's ephemeral-expiration cache is retained for performance, but its key
now includes the current account as well as the recipient. The upstream
device-scoping fix must not leave that local cache shared across accounts.

The bridge-specific reaction target resolver also uses device-scoped lookup
before invoking the shared message usecase. Its legacy phone-number input still
selects the current account's latest incoming message from that contact.

Upstream regression tests accompany the backports, with local history-sync test
spies adapted to the device-scoped repository method. Bridge-specific regression
coverage checks identical message IDs stored under different accounts and rejects
messages owned only by another account.

## Already Covered

`de6f3c1` / #895 stops a replaced WhatsApp connection from exiting the whole
process. The fork already has an equivalent process-preserving handler plus
bridge reconnect handling. Do not replace those with upstream's explicit-manual-
reconnect behavior or reintroduce a process exit.

## Deferred

- Fiber 3, Go 1.26, standalone dashboard and broad dependency migrations:
  unrelated to the bridge compatibility fixes and require separate validation.
- Upstream's whatsmeow pin is `df33a485cc46` (2026-10-15), while this fork still
  uses its vendored `662ad1dc6900` baseline (2026-07-30). Updating only go.mod
  would have no effect because of the local replace directive. Re-vendoring must
  preserve the fork's newsletter and linked-device retry patches and be tested
  independently; it is not included in this backport.
- Chatwoot conversation-lookup failure handling (#908), inbox identifier
  synchronization (#907), signed/per-device webhook verification (#914), and
  reply formatting (#915): separate Chatwoot integration work. Webhook signature
  verification is security-relevant when that public integration is enabled;
  deferral does not mean it is unnecessary for those deployments.
- MCP OAuth/schema changes, quoted stickers, interactive-button webhook fields,
  version command, Neon setup and README changes: not prerequisites for the
  current gRPC/NATS bridge recovery work.

No server deployment or restart is part of this synchronization.

## Verification

- `go test ./...`: passed.
- `go vet ./usecase ./infrastructure/whatsapp ./ui/bridge`: passed.
- `go test -race ./usecase ./infrastructure/whatsapp ./ui/bridge`: passed,
  with a real NATS broker enabled for the publisher recovery tests.
- `go test -tags purego ./usecase ./infrastructure/whatsapp ./ui/bridge`: passed.
- Existing reaction-by-phone tests remain passing.
