# Local whatsmeow patches

This vendored copy tracks `go.mau.fi/whatsmeow v0.0.0-20260622185415-5f04eac6dbbb` with the newsletter poll creation fix from upstream PR:

- https://github.com/tulir/whatsmeow/pull/908

Patch summary:

- Treat `PollCreationMessageV3` as message type `poll`.
- Add newsletter poll meta nodes for creation and vote sends.

Validated behavior:

- Newsletter/channel poll creation succeeds in the boss environment.
- Newsletter/channel poll vote still returns server error 479 and needs separate investigation.
