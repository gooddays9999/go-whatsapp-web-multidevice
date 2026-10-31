# Local whatsmeow patches

This vendored copy tracks `go.mau.fi/whatsmeow v0.0.0-20261015105041-df33a485cc46`
(re-vendored 2026-10-29 from the 2026-07-30 `662ad1dc6900` base).
The source baseline matches the whatsmeow revision selected by the upstream
go-whatsapp-web-multidevice project. Go 1.26 or newer is required.

Local changes relative to that baseline are intentionally limited to
`send.go`, `newsletter.go`, `retry.go`, `client.go`, this document, and their
regression test files. Preserve all of them during the next re-vendor.

## Nonblocking connection sampling

`Client.TryIsConnected` uses `socketLock.TryRLock` and returns a separate known
flag. A socket held by dialing or a stalled close handshake is unknown, not
offline. Bridge fleet metrics and heartbeats use this API without starting
per-account goroutines; one busy socket cannot block the complete fleet scan.
The original blocking `IsConnected` API remains unchanged for existing callers.
Tests cover busy sockets and nil/disconnected clients. This prevents the fleet
sampling stall; it does not repair an already wedged socket or hot-patch a
running binary.

## Newsletter polls

The newsletter poll creation/vote fix originated in upstream PR:

- https://github.com/tulir/whatsmeow/pull/908

Patch summary:

- Treat `PollCreationMessageV3` as message type `poll`.
- Add newsletter poll meta nodes for creation and vote sends.

The `newsletter.go` extension also exposes `NewsletterSendPollVote` and constructs
the newsletter add-on stanza with SHA-256 option hashes. Regular encrypted poll
metadata is a separate upstream code path and does not replace this patch.

## Own linked-device retry

Fork commit `25419f4` added the original recipient to cached messages, so an own
linked device requesting a retry via the account's LID receives a correctly
addressed DeviceSentMessage. An ambiguous message ID shared by recipients must
not select a random destination. The upstream October baseline still lacks this
behavior, so the patch was retained. The surrounding upstream Go 1.26 `new(expr)`
syntax and WASA retry encryption behavior are preserved.

Validated behavior:

- Newsletter/channel poll creation + vote work in production (server_id 387 vote confirmed sent 2026-08-02).
- Vendored tests pass: `TestGetTypeFromMessageTreatsPollCreationV3AsPoll`,
  `TestBuildNewsletterPollVoteNodeMatchesWebStanza`,
  `TestOwnDeviceRetryFindsMessageCachedUnderRecipient`,
  `TestOwnDeviceRetryDoesNotGuessBetweenRecipients`.
- These unit tests pass on the October baseline, including race detection.
  They do not constitute a new live WhatsApp production validation.
