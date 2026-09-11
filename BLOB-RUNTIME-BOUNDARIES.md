# Runtime boundaries: client-owned history, cancellation and conditional saves

Scope: `fix/blob-runtime-boundaries`, based on
`1d93764df900093cbf52bf800211946633c59887`. This supersedes the runtime policies
in the historical `BLOB-RUN-SCOPE.md`. No compression algorithm, model routing,
usage formula, tool business logic, dependency version or product version changed.

## Required history versus metadata

`handleRunRequest` admits one `BlobRunContext`. Its working set, cancellation,
client reads and checkpoint scope are disposed at the run boundary.

- Ordinary continuation calls `probeTurnDynamicToolCount` for an optional hint.
  A legal shell turn has no agent dynamic-tool count. Unavailable or unsuitable
  metadata is logged, not substituted for required prompt history.
- Agent-turn resume loads the parent and `restoreRequiredBlobGraph` validates
  the user message and every old step that the new turn re-encoding preserves.
  A parent Get or a successful new Set does not certify those children.
- Accepted uploaded turns and archives create required dependencies. The same
  graph loader obtains them from this run, the short-lived upload handoff, or
  client KV, then decodes their declared protobuf/history types before model I/O.
- Untouched inherited checkpoint references are preserved without claiming a
  full recursive audit. Required dependency edges never use that exemption.
  Ordinary continuation and summarization do not scan all old turn/archive graphs.
- Upload reads are deduplicated before copying bytes and retained incrementally.
  Reference ordering/multiplicity is unchanged. Decoded uploaded history uses
  the existing run-local history map; there is no new global history cache.

Root restoration remains mandatory for both conversation and summary generation.
Missing, malformed or timed-out required content stops generation/publication.
Existing root, archive and turn wire encodings, including opaque fork keys, stay
unchanged. No historical message is fabricated and no SQLite blob fallback is used.

The log entry `[SESSION] history blobs from cache` retains `requestedBlobs`,
`cachedBlobs`, `fetchedFromClient`, `stillMissing`, and `resolvedBlobs` as occurrence
counts. `requestedFromClient` counts actual Get sends by that load. Invalid
decoding is not a cache hit; required `stillMissing > 0` cannot reach the provider.

## Request context

The protocol's complete inline `requestContext` remains authoritative in dual
mode, so its redundant part references are not fetched or appended again.

For ref-only parts without a complete inline copy:

- Rules are required, including user/team/workspace/cloud instructions.
- MCP parts are required because they include server instructions as well as
  routing/discovery configuration; they are not assumed to be just a directory.
- Skills and unselected custom-subagent catalogs can degrade discovery, with a
  specific part/status/capability log. Explicit skill/context attachments retain
  their existing inline or required-blob paths.
- An explicitly selected subagent must have its definition. An unavailable
  subagent part cannot be treated as optional when that definition is needed.

A failure is never interpreted as proof that the user configured no rules.

## Cancellation and consumer ownership

The registered Run/RunSSE handlers attach the actual Connect streaming signal to
their own session. The BidiAppend unary signal is deliberately not used: Connect
also aborts that signal when an individual append RPC completes normally.

Production translation and summary calls use signal-aware factories. Request
lifecycles forward cancellation to installed SDK APIs: OpenAI Chat/Responses and
Anthropic request options `signal`, and Gemini `config.abortSignal`. A small
Gemini adapter handles its verified late-listener setup race. The transparent
usage wrapper also preserves interruptible return without changing accounting.

Service-facing and provider-facing iterator return/throw abort or close their
owned lifecycle before queueing native async-generator teardown. This handles
pending `next()`, not just consumers already suspended at `yield`. Finally blocks
retire owned heartbeat/deadline timers, listeners and source iterators.

Each summary attempt has a child lifecycle. Idle timeout aborts that attempt;
parent/run cancellation bypasses retry, shorter-output retry and deterministic
fallback. Both explicit and inline compaction pass the real run signal. Normal
iterator exhaustion is not reclassified as cancellation.

Limits: SDK-internal retry sleeps may finish before cancellation is reported,
even though another HTTP retry is prevented. A non-cooperative iterator cannot
be universally interrupted by JavaScript. A pending BiDi input read relies on
transport teardown, but output EOF does not wait for client EOF. Aborting HTTP
does not guarantee immediate remote generation or billing termination.

## Checkpoint validity and commit point

Cursor retains ownership of the prompt bytes and frontend scheduling. We retain
logical `runId` and actual attempt `requestId` separately for correlation, not
as sortable epochs. Checkpoint ownership uses exact `conversationId`; a parent
conversation group or subagent type is not a cancellation/version key.

1. Capture both committed/draft tokens in one SELECT at run admission, before
   asynchronous history/context work. No writer refreshes its scope after a loss.
2. Admit matching client history normally. Resolve legacy compatibility or an
   explicit client-state recovery before admitting a different baseline; never
   silently replace the client's prompt with local history.
3. Retain and validate required dependencies, send every pending blob, and await
   matching successful Set replies before any rolling/final/summary commit.
4. A single SQLite statement compares the committed/draft token pair and writes
   payload plus fresh version. Committed writes atomically tombstone draft, so
   an old draft is not subsequently advertised as an admissible newer baseline.
5. Advance only this writer's expected tokens after success, then publish if the
   run is still active. Conflicts pass unchanged through tool/compaction catches
   to explicit, non-retryable conflict details rather than a generic tool retry.

Cancellation is checked immediately before SQL dispatch. After dispatch, the SQL
outcome decides acceptance: cancellation does not undo a successful write, hide
a real SQL error, or restore an earlier row. The old persistence tail and
read-old/write/compensate machinery were removed. A separate autocommit SQLite
connection prevents checkpoint writes from joining unrelated chat-summary or
usage transactions on the shared connection; no large shared transaction exists.

Deletion retains version tombstones. Old scopes conflict, and the run admission
path rejects newly arriving requests for an explicitly deleted checkpoint rather
than reviving it. Explicit maintenance APIs still require deliberate caller use.

New opaque tokens carry `ccursor-cas-v1:` provenance for the atomic-retirement
invariant. Legacy committed/draft rows can both exist with different histories;
timestamps, row preference and arrival order cannot safely select one. Missing
provenance alone no longer forces users to fork their conversations.
Single-active legacy rows and identical reference triples are not rejected on
provenance alone. Corrupt checkpoint reference JSON is not normalized to `[]`.

### Legacy compatibility and in-place recovery

`admitClientCheckpoint` validates the complete selected graph on the exceptional
recovery path. Ordinary matching continuations retain lazy history validation.
Automatic legacy adoption requires the incoming references to match one candidate
and structurally contain the other: ordered root prefix, identical archive list,
and ordered turns. Only the formerly active last turn may have changed identity;
its agent user reference must match and its old steps must be an exact prefix.
The differing turn structures are decoded; matching children are covered by the
selected graph's validation. Discarded-branch children do not become required
dependencies merely to inspect ancestry. No ordering is inferred from timestamps,
counts, run IDs or missing writer tags.

If that proof is unavailable or the branches genuinely differ, the main chat
uses the existing native AskQuestion interaction. The user can keep this chat's
submitted history under the same conversation ID, or cancel. The question warns
that this does not undo executed tools and that older history can repeat work.
Consent is bound to this live request and the original token pair, with a
five-minute cancellable wait; concurrent changes invalidate consent rather than
refreshing the scope. Background/subagent/summary requests never invent consent.
A declined live interaction emits turnEnded before EOF to avoid a transport retry
loop. No model/tool generation occurs on a declined or failed recovery.

Uploaded-only selected data must receive successful Set replies before adoption.
Before adoption, `conversation_checkpoint_recovery` preserves both exact original
SQLite rows, including tokens and any terminal receipt. Snapshot insertion must
succeed before the original-token CAS can adopt the selected baseline. A crash
or competing write between preservation and adoption may leave an extra snapshot,
but cannot cause an unbacked overwrite. Identical snapshots are deduplicated by
content hash; upgrades never delete them. These are recovery **references**, not
copies of every client-owned blob or a guarantee of indefinite blob availability.
No alternate-state restoration UI or automatic history merge is introduced.

### Correlated terminal redelivery and remaining availability boundary

The inspected wire request has neither the client's checkpoint epoch nor a
checkpoint-applied ACK. A mismatch may be an intentional revert, a delayed
request, or a retry after SQL accepted a checkpoint that the client never applied.
CAS alone cannot distinguish all of them; it still does not order client intent.

`CheckpointDelivery` records a bounded terminal receipt in the **same statement**
as final checkpoint acceptance. It stores a logical run ID, canonical request and
model fingerprints, exact input-state fingerprint, same-attempt emitted checkpoint
fingerprints, and the serialized final stepCompleted/turnEnded/checkpoint frames.
It is only produced for an observed terminal turn, not arbitrary committed rows
(inline compaction also writes committed checkpoints). Old rows have no receipt.

Redelivery requires the same logical run and model identity, plus either the exact
original action/request and input, or a plain native ResumeAction from a recorded
same-run state. A new message/action/run, unknown state, malformed receipt, later
draft (even if subsequently cleared), final write, or deletion is not eligible.
Replay verifies the full saved graph through client KV, confirms any upload-only
bytes with Set, and rechecks both version tokens before publication. It sends
terminal markers and the exact checkpoint,
without model generation, tool execution, or new provider usage recording.
No text deltas are appended a second time; visible transcript reconstruction
depends on native checkpoint hydration, not on a replay of the original stream.

Receipts are capped at 2 MiB and 256 intermediate source fingerprints. Unknown
future request-state fields or exceeding these limits disables automatic replay,
not normal execution. Nonterminal interrupted work, uncorrelated retries and old
delivery gaps without a receipt use explicit recovery instead. This is not an
exactly-once tool ledger, and replay does not create a client-applied ACK. A final
version check cannot make network publication atomic with another process's SQL
write. Native scheduling and checkpoint-epoch application remain client-owned.

This is a conservative project policy, not verified cloud behavior or full
causality protection. Cooperative writers on the same SQLite database receive
atomic cross-connection/process CAS protection. Old unconditional writers,
forged/reused tokens, physical deletion, or restoring an earlier database can
bypass it. SQLite 3.39.4 was exercised; the MATERIALIZED CTE requires 3.35+.
Existing WAL/synchronous settings are unchanged. Power-loss durability, forced
crash recovery, exactly-once client application and rollback of executed tools
are not guaranteed.

## Resource policies

Project initial thresholds, not official or empirically optimal parameters:

- At most 4 admitted model/history runs, with immediate rejection rather than
  an unbounded run queue. Counted retained run payloads total at most 256 MiB;
  each run still has its 128 MiB/100,000-distinct-identity limit.
- One KV batch per run owns at most 32 pending wire operations across all
  helpers. Up to 32 batches may wait, within their existing overall deadlines.
  Cancellation owns even an already-granted lease whose consumer is paused at a
  heartbeat, so return/timeout cannot lose the release callback.
- Upload handoff retains at most 128 MiB payload, 16,384 entries, and separately
  8 MiB of hex-key/conversation-identity bytes. Empty values cannot bypass that
  identity budget. Expiration releases both budgets; accepted history is not evicted.
- SSE admission covers 32 live unclaimed/attached IDs. Ordinary and reordered
  transport queues share a 64 MiB process encoded-payload budget, with 1,024
  queued messages and 1,024 reordered entries per session. Cleanup releases
  charges on consumption, failure, cancellation, closure and expiration.

These counters are not a process heap/RSS ceiling. Decoded objects, V8 string and
collection overhead, transient parsing/serialization, SDK/network buffers and
idle BiDi connections are outside the payload counters. Closed transport IDs
expire after 30 seconds; fresh attempt IDs remain required. Tests exercise small
budgets, real admission refusal, multi-helper contention and release races; no
production memory benchmark or global connection-flood guarantee is claimed.

## Startup and append semantics

Both registered transports wait for a RunRequest, not an arbitrary first frame,
under one 30-second absolute startup deadline. Heartbeats/unsolicited ACKs cannot
extend it. A queued full UserMessageAction can adapt a following ResumeAction;
images, prepends, resolutions and context parts are preserved. Multiple or
conflicting queued actions fail explicitly rather than silently losing text.

BidiAppend accepts verified hex or binary encoding, nested request association,
and zero-based int64 sequence numbers. It releases contiguous sequences and
deduplicates retries. Pending conflicting duplicates fail and retire the session.
This is a minimal receiver adapter to the observed sender, not a copied cloud
reordering implementation. ACK eligibility is checked at arrival and requires
the matching kind/ID of an actually yielded Get/Set, not merely a reserved ID.

## Evidence and verification

Rechecked on 2026-09-09:

- Official static app: 3.14.27. Installed app: 3.18.9; patched files were not
  treated as original source. Pre-injection workbench and agent-host backups
  were used for installed-version call-chain evidence.
- Workbench `pqs.run`/`Uwf`/`runInternal` distinguishes original logical run IDs
  from new attempt UUIDs. `streamFromAgentBackend`/`handleConversationCheckpoint`
  has a client-local checkpoint epoch, not a field exposed to this server.
- `ControlledKvManager` awaits `blobStore.setBlob`; `ComposerBlobStore` resolves
  after `cursorDiskKVSetBinaryBatch`. This is storage-operation completion, not
  fsync, indefinite retention or checkpoint application.
- Local runtime `fromConversationStateStructure` requires root prompts by
  default; LazyReference preserves untouched IDs without universally loading
  them. Request-context preparation has inline fallback on the sender only.
- Baseline agent-host BidiTransport increments sequence from zero, permits 32
  concurrent append sends and retries the same payload/sequence. Its source
  SHA-256 was `e24363d1fcd969c7459fe6dcb5fc2cd835979adc2d322f14b7fcdffd06fb7b92`.
- Installed SDKs exercised: OpenAI 6.46.0, Anthropic 0.111.0, Gemini 2.7.0;
  plugin protobuf 2.12.0 and Connect 2.1.1. No dependency was added or upgraded.

Fresh baseline: 50 files / 601 tests, 600 passed. Final serial and four-worker
runs each: 54 files / 779 tests, 778 passed, no skipped/todo cases. The only
failure remains the untouched `protocol.test.ts:232` `<user_rules>` assertion.
Type checking, repository lint and both isolated installer tests passed.

Behavioral evidence lives in `runtimeBoundaries.integration.test.ts`,
`blobRunLifecycle.integration.test.ts`, `providerCancellation.integration.test.ts`,
`transportStartup.integration.test.ts`, `checkpointVersions.test.ts`,
`checkpointCancellation.test.ts`, `runBlobKv.test.ts`, and existing history,
compaction, upload and database-safety regressions. New bytes enter the simulated
client only through Set frames and matching ACKs. Cold tests expire upload
handoff and use only confirmed client storage. Provider/transport tests retain
production registration, codecs and SDKs, with mocks at external I/O boundaries.

Independent reviews found and then verified fixes for legacy baseline selection,
upload copy amplification, heartbeat-paused KV lease ownership, rolling conflict
propagation, and return-only lifecycle gaps through usage/service wrappers.

### Recovery follow-up verification (2026-09-09)

The recovery/delivery follow-up passed all 94 tests in six focused files, including
the registered RPC cold-redelivery path, original-token recovery races, rejected
Set barriers, discarded-branch missing children, and native consent/cancellation.
The repository regression run passed 758 of 759 tests; its sole failure is the
previously documented, untouched `protocol.test.ts:232` `<user_rules>` assertion.
The unrelated untracked `deathLoopSimulation.test.ts` was explicitly excluded
because it is a one-off report generator, not part of this change.
Type checking, repository lint, explicit lint of the two normally ignored new
runtime modules, and `git diff --check` passed. No dependency or version changed.
Tests used temporary databases and mocked external model I/O. The installed
plugin, live databases and application were not changed or restarted; native
checkpoint hydration and recovery-question rendering still need an installed
client smoke test before release. No commit, package deployment or push was made.

## Data and release safety

No live Cursor `state.vscdb` was read or written. No source CCursor database,
legacy blob table, recovery backup or user configuration was deleted or replaced.
A read-only-source online backup produced a private CCursor snapshot. Offline
read-only integrity checking passed; `agent_blobs`, usage and `write_token`
were present, with 559 committed and 563 draft rows at that snapshot. This is
not a transitive history recovery audit. No old-history repair was attempted.

Routes/providers and other JSON configuration were copied to a private safety
directory and byte-hash checked; old database/main-WAL backups and protected WIP
were hash checked. Installer `update` was found to consume application patch
backups via rename and remove redundant backups. The user explicitly chose to
finish local commits, packaging and read-only preflight but **not execute update**.
Installed extension/application files and the running Cursor instance remain
unchanged; no restart is performed or needed for the still-uninstalled package.
