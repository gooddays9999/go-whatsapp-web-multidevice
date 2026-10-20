# Targeted History Recovery

The bridge metrics listener exposes `POST /internal/history-sync` to loopback
clients only. It does not trust forwarded IP headers. Use an SSH session on the
bridge host; do not expose this operation through a public reverse proxy.

Request fields:

- `account_id`: an existing account with an initialized session.
- `chat_jid`: the customer's private phone JID (`number@s.whatsapp.net`).
- `since`, `until`: RFC3339 timestamps defining a past, half-open window of at
  most 24 hours. Cached queries are normalized to UTC.
- `count`: 1-100 messages to request from the primary device.
- `cache_only`: replay verified local text messages without contacting the phone.
- `message_ids`: optional list of up to 100 original IDs. Use this for bulk
  recovery when IDs are known: reads use the full primary key and still enforce
  device, chat, time window, and cryptographic ownership checks.
- `anchor_id`, `anchor_at`, `anchor_from_me`: a real existing message used as the
  history anchor. Required unless `cache_only` is true. WhatsApp requests messages
  immediately **before** this anchor, not after it.
- `legacy_device_id`: optional old storage phone JID, for builds that incorrectly
  stored incoming messages under their default device. This does not change the
  destination account or waive ownership verification.

Time-range cache reads explicitly use the existing chat index on SQLite, not
the potentially huge default-device index. These reads omit unrelated reactions
and media keys, run with a cancellable five-second query deadline, and do not
create new indexes or modify the cache. Narrow the window or specify message IDs
if more than 500 candidate rows match.

Every cached reply must match the requested chat, original timestamp, and
destination account's cryptographic message store. Missing ownership evidence is
reported in `unverified_skipped`, never guessed. The operation never changes the
SQLite storage identity or resends the original broadcast. Cached media without
recoverable content is not replayed.

New primary-device history responses are replayed only for the explicitly
requested account/chat/window. Original message IDs and timestamps are retained;
events use `isBackfill=true` so ims-api deduplicates and does not trigger AI replies.
Regular unsolicited history sync behavior is otherwise unchanged.

`requested_not_yet_confirmed` means only that a peer request was sent. It does not
mean WhatsApp returned history. `published` counts events flushed to NATS, not new
database rows or successful upstream callbacks. Verify persistence and callbacks
in ims-api separately; already-existing messages should not create new rows.

Poll `GET /internal/history-sync?account_id=...&chat_jid=...` for
`history_responses`, `published`, and `unverified_skipped`. Recovery windows expire
after 30 minutes and are not persisted across bridge restarts. At most 256 windows
are retained; manual operations are serialized independently of startup bulk sync.
An active window rejects duplicate POSTs with 409. Failed requests may be retried.

The primary device must support and answer WhatsApp on-demand history requests.
An online linked Web session alone does not guarantee historical availability.
