# Blob ownership and checkpoint publication

## Scope and invariants

A run is one `handleRunRequest` invocation, including model rounds, tool calls,
resume and inline compaction. `BlobRunContext` is constructed at that boundary,
passed explicitly, and disposed in `finally`. It owns retained payloads, decoded
history/turns, successful and failed Get results, request identities and abort
signals. There is no process-wide history blob cache or SQLite blob dependency.

- A required history reference must restore to a supported message before the
  conversation or summary provider is called. Missing, invalid and timed-out
  history is an error, including when the session is null.
- Original reference order and multiplicity are preserved. Only network reads
  are deduplicated. A failed Get is not automatically repeated within the run.
- Required retained data is never evicted. The initial per-run payload budget
  is 128 MiB with a 100,000-identity cap. These are engineering defaults, not
  measured optimal or official limits. The byte budget is not a JavaScript heap
  limit: decoded objects and collection overhead also use memory.
- A run cannot clear another run's blobs or waiters. KV identities advance on
  the transport session and are not reset by compaction or a reused session.
- Only successful matching Set replies confirm a new blob. Yielding a Set frame
  does not. All pending generated/uploaded payloads, including archived repaired
  messages and turn/step dependencies, pass the same barrier before publication.

## Upload handoff is not a history cache

`UploadConversationBlobs` accepts independent chunks into `UploadHandoff`, keyed
by conversation ID and the exact blob key bytes. It has an initial five-minute
TTL, 128 MiB payload cap and 16,384-entry cap. Expiration is checked lazily on
reads and swept on uploads/stats; no per-entry timers are retained. Capacity
overflow rejects the incoming chunk rather than evicting an accepted live one.
Conflicting duplicate keys reject the whole chunk; identical retries are safe.

Runs copy, rather than consume, only referenced uploads. A completed or cancelled
run does not clear the handoff. The input checkpoint's uploaded roots, all turns,
archives, and available children are retained in that run. Acceptance into the
handoff is not marked as a client Set acknowledgement.

Verified static client behavior: fork upload batches contain up to 100 IDs and
read bytes from the client's local blob store. Missing IDs are omitted; an
entirely missing chunk is not sent. The uploader's independent `fork-UUID` is an
HTTP `x-request-id`, not a future run ID. Fork upload scheduling is asynchronous,
so upload completion is not necessarily before the first run. Unchanged fork
references are shared; not every blob is rekeyed or uploaded. The protobuf RPC
has conversation ID, blobs, chunk index and total chunks, but no run identity.

## KV protocol and encodings

`clientBlobFetch` matches the envelope's result kind and numeric request ID
without decoding payloads in a queue predicate. Successful replies are decoded
once. Absent optional bytes mean not found; present zero-length bytes are a
successful empty value. Client error, timeout, total timeout, cancellation,
malformed result and no-session are distinct outcomes.

Initial operation defaults are batches of at most 32, a 10-second individual
deadline starting at send, and a 60-second operation budget across batches.
Waiters are registered together, so sequential collection does not create
32 independent ten-second waiting periods. One timeout does not cancel sibling
successes or declare the transport dead. Heartbeat timers, request waiters and
owned reply frames are retired; late replies for retired IDs are discarded.

Protocol ID zero is valid and omitted by default protobuf JSON serialization.
The run allocator reserves zero and uses unique positive IDs starting at
900,000. A default-zero reply cannot satisfy a positive request. The low-level
wire builder still supports zero; there is no unsafe missing-ID fallback.

Wire formats are unchanged:

- Project-generated IDs are base64 hash *text*, whose UTF-8 bytes are the key.
  They are not decoded to 32-byte hashes.
- Project JSON values are UTF-8 bytes of base64(JSON).
- Turn/step/archive values are raw protobuf bytes.
- JSON transport adds its normal base64 representation of protobuf byte fields.
- Client-created fork IDs can be raw hash bytes. A reversible internal opaque
  ID representation preserves these through parse, Get, Set and checkpoint
  serialization; that internal prefix is never sent as the wire key.
- Received wire bytes are retained as well as normalized data, so retransmission
  does not alter a client's existing JSON or binary storage representation.

The static client `ControlledKvManager` awaits `blobStore.setBlob` before sending
`SetBlobResult`. In the inspected `ComposerBlobStore`, that promise resolves
after `cursorDiskKVSetBinaryBatch` completes, not merely when enqueued. This is
evidence of client storage-operation completion, not a protocol promise of
fsync, indefinite retention, replication or official cloud storage policy.

## Checkpoint commit order

Rolling, final, inline compaction and explicit summarize all:

1. Construct/retain the candidate's new payloads and dependency references.
2. Validate availability of generated references and retained dependencies.
3. Send pending blobs and wait for every matching successful Set result.
4. Check cancellation and await checkpoint persistence.
5. Publish the checkpoint only after successful persistence.

Compaction artifacts carry their root `HistoryEntry` objects and payloads
directly. Inline compaction uses the live active/resumed turn snapshot, not just
the turn prefix received at run start. Blob failures bypass provider retry and
compaction fallback handling. Failed runs do not delete conversation-wide drafts.

Checkpoint metadata writes are serialized independently of usage writes. An
additive `write_token` column identifies a write. If cancellation arrives while
its SQL completion is awaited, a token-conditional restore reinstates the exact
previous row (or removes that new row), without undoing a newer replacement or
explicit deletion. No transaction wraps the shared usage connection. Successful
persistence completion is the commit boundary; later cancellation does not
undo an already committed checkpoint. Storage errors during cancellation
restoration are explicitly reported, not claimed to be safe.

The checkpoint queue serializes this service's writers only. A competing process
can write between the prior-row read and our insert; the rollback token does not
protect that earlier window. Crash recovery and distributed writer ordering are
not guaranteed. An already acknowledged candidate may remain if the process
crashes or storage fails while compensating a cancelled write.

This is not a distributed conversation merge/version protocol. Untouched
inherited checkpoint references are preserved, not re-audited in full on every
run. A client advertising an old reference is not proof all its historical
descendants remain available. The runtime completeness check covers required
prompt history and the turn being restored; preservation of old archives does
not certify the entire legacy graph.

## Logs and tests

`[SESSION] history blobs from cache` retains these occurrence-count fields:
`requestedBlobs`, `cachedBlobs`, `fetchedFromClient`, `stillMissing`,
`resolvedBlobs`. Cached means usable data already in this run. Invalid message
JSON is not a cache hit. `requestedFromClient` counts actual Get sends for that
load, separately from duplicate references and unique missing keys.

Database tests have per-file temporary HOME and database paths. The shared
setup also isolates HOME-derived spill output. All test runs should use
`--no-cache` so the user's root `node_modules` Vitest cache is not changed.

Coverage includes real `handleRunRequest` continuation, resume, summarize,
inline compaction, pending/error/timeout Set replies, cancellation, concurrent
same-conversation clients, uploaded fork graphs, cold restoration from only
confirmed client bytes, and BiDi output completion while client input is open.
Helper coverage uses fake timers for deadlines and verifies request cleanup,
exact encodings, duplicate ordering, metrics and resource failures.

Baseline measured before changes: 44 files, 493 tests, 492 passed. The existing
`protocol.test.ts` failure expects `<user_rules>` in the structured preamble.
Type checking and lint passed at baseline. That test was not changed.

Final verification on 2026-09-08: both the serial full suite and a four-worker
parallel full suite reported 50 files / 601 tests, with 600 passing and only the
same pre-existing `protocol.test.ts` failure. Type checking and lint passed.
The two installer configuration-preservation tests also passed. No spill ENOENT
flake was observed in those runs; this is not a claim of zero future flakiness.

## Recovery and deployment safety

Normal initialization never drops `agent_blobs`, vacuums it away, or clears old
backups. Old blob tables are offline recovery sources only. Explicit cleanup is
a separate user-approved maintenance operation, not part of deployment.

The pre-drop backup's copied main/WAL pair was byte-verified and opened only as
a private copy. SQLite integrity checks passed for that copy and a consistent
online-backup snapshot of the current CCursor database. Exhaustive *direct*
checkpoint-reference checks found no absent root/archive IDs, but found 31
absent turn IDs in each snapshot. All 31 contain Unicode replacement characters,
consistent with prior lossy decoding; original key bytes cannot be reconstructed
from those strings alone. This does not establish that the client is missing
those blobs. Client comparison, transitive graph validation and actual recovery
have not been completed. No live Cursor `state.vscdb` was queried or modified.

If recovery is needed: preserve sources, identify the exact checkpoint and key
bytes from an intact source, compare every reachable required reference using a
consistent read-only client snapshot or authorized protocol reads, and produce
a reviewable missing-blob manifest. Only after user approval should exact known
payloads be resent through SetBlob, confirmed, and reread before publishing a
repaired checkpoint. Never guess messages or write directly to Cursor storage.

Installer updates preserve existing routes/providers byte-for-byte and do not
inspect Cursor's authentication database. New installation defaults to BYOK OFF
until the user completes login/onboarding. Deployment does not restart Cursor;
the user chooses when to quit with Cmd+Q and reopen it.
