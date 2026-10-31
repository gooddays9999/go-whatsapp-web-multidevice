# Bridge newsletter filtering

Bridge permanently skips new WhatsApp channel (`@newsletter`) message traffic.
This is not controlled by the older media-only skip option. The filter does not
unsubscribe accounts from channels or delete their WhatsApp history.

The live message handler returns before message conversion, JID normalization,
event enqueue, media download/upload, reaction and revoke handling. Channel
receipts and historical status receipts are also skipped. A publish-boundary
guard catches message payloads carrying a channel primary chat, and targeted
history reply publishing rejects channel chats before its direct NATS path.

Only the primary chat JID is inspected. Private chats, groups, LID chats,
`status@broadcast`, message text containing channel links, and private quoted
channel content are preserved. Existing account/heartbeat events are unchanged.
RPC/proto, NATS subjects and existing non-channel payload contracts do not change.
The rest of the app may still receive/cache channel data from WhatsApp; this is
a Bridge forwarding filter, not a change to upstream REST/channel APIs.

`bridge_newsletter_events_skipped_total` counts skips at Bridge event/publish
boundaries without per-message log spam. The stored-history helper also rejects
channel status events; the counter is not a count of every historical row.

Tests cover early returns before message/media processing, publish guards,
private/group/status preservation, quoted content, channel history receipts and
preservation of existing queued events. Full Bridge/WhatsApp package race tests
with a real local NATS broker and Bridge vet passed before deployment.

## API02 deployment, 2026-10-29

Host: `192.151.156.2`; service: `go-whatsapp-ims-bridge-prod2.service`.
Build ID: `e7bfa45-dirty-newsletter-filter-20261029`.
SHA256: `7975ba501e996743aea69ea84cd3360cbc955e9cc9754093f87b74884a6c0b0f`.
This includes the existing uncommitted upgrade/bridge fixes, not clean main only.

Release directory:
`/opt/go-whatsapp-ims-bridge-prod2/releases/newsletter-filter-20261029`.
The previous executable, private systemd configuration and consistent SQLite
outbox snapshot are in `backup/`. That snapshot contained 2,696,584 events;
the maximum retained row ID was 70,322,320. No queue purge was performed.
The source archive SHA256 was verified end to end:
`dab5c1e51ff6219d5486acc1ff9b7a76bb56b3d27c56326f605ee979f9fba8e8`.

Only API02 Go bridge was restarted (new PID 2214072). Deployment readiness
passed at approximately 23:00:47 Asia/Shanghai. Neither the OS, shared broker,
API01 nor test bridge was restarted; the test host only built the artifact.
The 8 GiB capacity drop-in and existing outbox were retained. Existing queued
channel events are still delivered normally; the change prevents new channel
traffic from entering the Bridge queue. Historical queue replay still has the
usual at-least-once/server-flush limitations.

At 23:01:17, the skip counter was 14,582, pending events had fallen to 2,548,029,
and new-process enqueue/delivery failure counters were zero. Confirmed online
accounts were 492 as startup restore began; this is not a claim of full recovery.

`api02-post-newsletter-recover-20261029.service` waits for automatic startup
restoration and old queue drain before launching the adaptive full helper. Its
script is retained in the release directory. The post-restart helper prioritizes
previous successes rather than excluding them, because pre-restart sockets no
longer exist. Fresh results will be in
`/opt/go-whatsapp-ims-bridge-prod2/recovery-20261029-newsletter/full/`.
Old recovery checkpoints remain intact. No server account state is bulk-written
to fake online status, and no test messages are sent during this deployment.

Follow-up at 23:04:55 CST: readiness remained healthy, 5,578 accounts were
confirmed online, skipped channel events reached 337,913, and the retained queue
had fallen to 1,555,261. Both new-process NATS delivery and enqueue failure
counters were zero. A read-only query of post-backup message-event rows
(`id > 70322320`) found four message events and zero channel events. Those rows
were still pending behind the old queue, so this validates new queue admission,
not downstream consumer processing.

The auxiliary waiting script's `journalctl --since` argument was normalized to
`YYYY-MM-DD HH:MM:SS UTC` for this host's older systemd parser. Only that waiting
unit was restarted to apply the correction. Go bridge remained PID 2214072
with zero automatic restarts; automatic startup restoration was still running.
