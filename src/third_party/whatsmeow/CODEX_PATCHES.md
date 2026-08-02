# Local whatsmeow patches

This vendored copy tracks `go.mau.fi/whatsmeow v0.0.0-20260730092514-662ad1dc6900` (re-vendored 2026-08-02 from the 6/30 `b572e5bcb92b` base) with the newsletter poll creation/vote fix from upstream PR:

- https://github.com/tulir/whatsmeow/pull/908

Patch summary:

- Treat `PollCreationMessageV3` as message type `poll`.
- Add newsletter poll meta nodes for creation and vote sends.

Note: upstream 7/30 independently added `polltype` meta on the regular (encrypted) send path in `getMessageContent`; that is a separate code path from our newsletter patch and the two coexist.

Validated behavior:

- Newsletter/channel poll creation + vote work in production (server_id 387 vote confirmed sent 2026-08-02).
- Vendored tests pass: `TestGetTypeFromMessageTreatsPollCreationV3AsPoll`, `TestBuildNewsletterPollVoteNodeMatchesWebStanza`.
