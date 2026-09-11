import type { InteractionQuery } from '../gen/agent_v1_pb'
import type { LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
import type { ClientBlobFixture } from './blobTestClient'
import { randomUUID } from 'node:crypto'
import { create, toBinary, toJson } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { getCheckpointDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import {
  AgentServerMessageSchema,
  ConversationStepSchema,
  ConversationSummaryArchiveSchema,
  ConversationTurnStructureSchema,
  UserMessageSchema,
} from '../gen/agent_v1_pb'
import { handleRunRequest } from '../handlers/agent/agentOrchestrator'
import { blobIdFromBytes, encodeBinaryBlob, encodeBlob } from '../handlers/agent/blob'
import { BlobIntegrityError } from '../handlers/agent/blobErrors'
import { AGENT_HEARTBEAT_INTERVAL_MS } from '../handlers/agent/constants'
import { processRunResources } from '../handlers/agent/runResources'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import { BlobTestClient, captureAgentServiceHandlers, wireKey } from './blobTestClient'

interface CheckpointFixture {
  roots: ClientBlobFixture[]
  turns?: ClientBlobFixture[]
  archives?: ClientBlobFixture[]
}

type RawCheckpointRow = Record<string, string | number>

interface RecoveryRow {
  reason: string
  checkpoint_rows_json: string
}

const clients: BlobTestClient[] = []
const requests: LLMStreamRequest[] = []
let providerScript: (request: LLMStreamRequest) => AsyncIterable<LLMStreamEvent>

function createHistoryFixture(content: string, role: 'user' | 'assistant' = 'user'): ClientBlobFixture {
  const encoded = encodeBlob({ role, content })
  return { blobId: Buffer.from(encoded.blobId), bytes: Buffer.from(encoded.blobData) }
}

function createBinaryFixture(bytes: Uint8Array): ClientBlobFixture {
  const encoded = encodeBinaryBlob(bytes)
  return { blobId: Buffer.from(encoded.blobId), bytes }
}

function createTurnFixture(user: ClientBlobFixture, steps: ClientBlobFixture[]): ClientBlobFixture {
  return createBinaryFixture(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
    turn: { case: 'agentConversationTurn', value: { userMessage: user.blobId, steps: steps.map(step => step.blobId) } },
  })))
}

function createLegacyExtensionFixture() {
  const root = createHistoryFixture('Preserve the original migration requirements.')
  const extensionRoot = createHistoryFixture('The active turn added a rollback-safe migration.', 'assistant')
  const earlierUser = createBinaryFixture(toBinary(UserMessageSchema, create(UserMessageSchema, {
    text: 'Inspect the existing migration.',
    messageId: 'earlier-user',
  })))
  const earlierStep = createBinaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'assistantMessage', value: { text: 'The existing migration has been inspected.' } },
  })))
  const earlierTurn = createTurnFixture(earlierUser, [earlierStep])
  const activeUser = createBinaryFixture(toBinary(UserMessageSchema, create(UserMessageSchema, {
    text: 'Add a rollback-safe migration.',
    messageId: 'active-user',
  })))
  const originalStep = createBinaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'assistantMessage', value: { text: 'Preparing the migration.' } },
  })))
  const extensionStep = createBinaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'assistantMessage', value: { text: 'Retained the rollback source before changing the migration.' } },
  })))
  const originalActiveTurn = createTurnFixture(activeUser, [originalStep])
  const extendedActiveTurn = createTurnFixture(activeUser, [originalStep, extensionStep])
  const archivedOriginal = createHistoryFixture('Earlier requirement: never remove the recovery source.')
  const summaryText = 'The migration must retain its recovery source.'
  const archivedSummary = createHistoryFixture(summaryText, 'assistant')
  const archive = createBinaryFixture(toBinary(ConversationSummaryArchiveSchema, create(ConversationSummaryArchiveSchema, {
    summarizedMessages: [archivedOriginal.blobId],
    summaryMessage: archivedSummary.blobId,
    summary: summaryText,
  })))
  const committed: CheckpointFixture = { roots: [root], turns: [earlierTurn, originalActiveTurn], archives: [archive] }
  const draft: CheckpointFixture = { roots: [root, extensionRoot], turns: [earlierTurn, extendedActiveTurn], archives: [archive] }
  return {
    committed,
    draft,
    extensionRoot,
    extensionStep,
    fixtures: [
      root,
      extensionRoot,
      earlierUser,
      earlierStep,
      earlierTurn,
      activeUser,
      originalStep,
      extensionStep,
      originalActiveTurn,
      extendedActiveTurn,
      archive,
      archivedOriginal,
      archivedSummary,
    ],
  }
}

function getCheckpointReferences(checkpoint: CheckpointFixture) {
  return {
    rootBlobIds: checkpoint.roots.map(fixture => blobIdFromBytes(fixture.blobId)),
    turnBlobIds: (checkpoint.turns ?? []).map(fixture => blobIdFromBytes(fixture.blobId)),
    summaryArchiveIds: (checkpoint.archives ?? []).map(fixture => blobIdFromBytes(fixture.blobId)),
  }
}

function readCheckpointRows(conversationId: string): Promise<RawCheckpointRow[]> {
  return getCheckpointDatabase().all<RawCheckpointRow>(
    'SELECT * FROM conversation_checkpoints WHERE conversation_id = ? ORDER BY kind',
    [conversationId],
  )
}

function readRecoveryRows(conversationId: string): Promise<RecoveryRow[]> {
  return getCheckpointDatabase().all<RecoveryRow>(
    'SELECT reason, checkpoint_rows_json FROM conversation_checkpoint_recovery WHERE conversation_id = ?',
    [conversationId],
  )
}

async function seedLegacyPair(conversationId: string, committed: CheckpointFixture, draft: CheckpointFixture): Promise<RawCheckpointRow[]> {
  // Normal writers retire draft atomically; only raw legacy rows reproduce this pair.
  for (const [kind, checkpoint] of [['committed', committed], ['draft', draft]] as const) {
    const references = getCheckpointReferences(checkpoint)
    await getCheckpointDatabase().run(`
      INSERT INTO conversation_checkpoints (
        conversation_id, kind, root_blob_ids_json, turn_blob_ids_json, summary_archive_ids_json,
        used_tokens, max_tokens, mode, updated_at, write_token, is_deleted, terminal_receipt_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      conversationId,
      kind,
      JSON.stringify(references.rootBlobIds, null, 2),
      JSON.stringify(references.turnBlobIds, null, 2),
      JSON.stringify(references.summaryArchiveIds, null, 2),
      kind === 'committed' ? 71 : 93,
      200_000,
      'AGENT_MODE_AGENT',
      kind === 'committed' ? 9_000 : 8_000,
      '',
      0,
      '',
    ])
  }
  return readCheckpointRows(conversationId)
}

async function seedUnrelatedPair() {
  const conversationId = randomUUID()
  const committedRoot = createHistoryFixture('Original chat: preserve the migration and its recovery source.')
  const draftRoot = createHistoryFixture('Unrelated draft: design a completely different calendar feature.')
  const committed: CheckpointFixture = { roots: [committedRoot] }
  const draft: CheckpointFixture = { roots: [draftRoot] }
  const originalRows = await seedLegacyPair(conversationId, committed, draft)
  return { conversationId, committed, draft, originalRows, fixtures: [committedRoot, draftRoot] }
}

function startRun(conversationId: string, checkpoint: CheckpointFixture, client: BlobTestClient): BlobTestClient {
  clients.push(client)
  client.start(handleRunRequest({
    runRequest: {
      conversationId,
      runId: randomUUID(),
      action: {
        userMessageAction: {
          userMessage: { text: 'Continue this chat safely.', messageId: randomUUID() },
          requestContext: {},
        },
      },
      requestedModel: { modelId: 'claude-sonnet-4', parameters: [{ id: 'context', value: '200000' }] },
      conversationState: {
        rootPromptMessagesJson: checkpoint.roots.map(fixture => wireKey(fixture.blobId)),
        turns: (checkpoint.turns ?? []).map(fixture => wireKey(fixture.blobId)),
        summaryArchives: (checkpoint.archives ?? []).map(fixture => wireKey(fixture.blobId)),
      },
    },
  }, client.session))
  return client
}

function createWaitingClient(fixtures: ClientBlobFixture[]): BlobTestClient {
  const client = new BlobTestClient(fixtures)
  client.onInteractionQuery = () => {
    // The test sends consent separately, after inspecting the waiting state.
  }
  return client
}

async function waitForRecoveryQuery(client: BlobTestClient): Promise<InteractionQuery> {
  await client.waitFor(current => current.frames.some(frame => frame.message.case === 'interactionQuery'), 'native recovery question')
  const interactions = client.frames.flatMap(frame => frame.message.case === 'interactionQuery' ? [frame.message.value] : [])
  expect(interactions).toHaveLength(1)
  const interaction = interactions[0]!
  expect(interaction.query.case).toBe('askQuestionInteractionQuery')
  if (interaction.query.case !== 'askQuestionInteractionQuery')
    throw new Error('Expected the native ask-question recovery interaction')
  const { args, toolCallId } = interaction.query.value
  expect(Number.isInteger(interaction.id)).toBe(true)
  expect(toolCallId).not.toBe('')
  expect(args?.title).toBe('Recover this conversation')
  expect(args?.questions).toHaveLength(1)
  expect(args?.questions[0]).toMatchObject({ id: 'checkpoint-recovery', allowMultiple: false })
  expect(args?.questions[0]?.options.map(option => option.id)).toEqual(['continue-current', 'cancel'])
  expect(client.frames.map(frame => toJson(AgentServerMessageSchema, frame))).toEqual(expect.arrayContaining([
    expect.objectContaining({
      interactionUpdate: {
        toolCallStarted: expect.objectContaining({
          callId: toolCallId,
          toolCall: { askQuestionToolCall: expect.objectContaining({ args: expect.objectContaining({ title: args!.title }) }) },
        }),
      },
    }),
  ]))
  return interaction
}

function respondToRecovery(client: BlobTestClient, interactionId: number, selectedOptionIds = ['continue-current']): void {
  client.send({
    interactionResponse: {
      id: interactionId,
      askQuestionInteractionResponse: {
        result: { success: { answers: [{ questionId: 'checkpoint-recovery', selectedOptionIds }] } },
      },
    },
  })
}

async function expectRecoverySnapshot(conversationId: string, originalRows: RawCheckpointRow[], reason: string): Promise<void> {
  const snapshots = await readRecoveryRows(conversationId)
  expect(snapshots).toHaveLength(1)
  expect(snapshots[0]?.reason).toBe(reason)
  expect(JSON.parse(snapshots[0]!.checkpoint_rows_json)).toEqual(originalRows)
}

async function expectUnchangedRecoveryState(conversationId: string, originalRows: RawCheckpointRow[], client: BlobTestClient): Promise<void> {
  expect(await readCheckpointRows(conversationId)).toEqual(originalRows)
  expect(await readRecoveryRows(conversationId)).toEqual([])
  expect(requests).toHaveLength(0)
  expect(client.setRequests).toHaveLength(0)
  expect(client.checkpoints).toHaveLength(0)
}

async function* answer(): AsyncIterable<LLMStreamEvent> {
  yield { type: 'text_delta', text: 'Continued the selected conversation safely.' }
  yield { type: 'done', stopReason: 'end_turn', usage: { inputTokens: 200, outputTokens: 10 } }
}

beforeEach(async () => {
  await resetAgentDatabaseForTests()
  requests.length = 0
  providerScript = answer
  vi.spyOn(AnthropicProvider.prototype, 'stream').mockImplementation((request) => {
    requests.push(request)
    return providerScript(request)
  })
})

afterEach(async () => {
  const finishedClients = clients.splice(0)
  for (const client of finishedClients)
    client.cancel()
  await Promise.all(finishedClients.map(client => client.completion))
  vi.useRealTimers()
  vi.restoreAllMocks()
  expect(processRunResources.getStats()).toEqual({ activeRuns: 0, retainedBytes: 0 })
  for (const client of finishedClients)
    expect(client.session.listeners.size).toBe(0)
})

describe('checkpoint recovery through the real run and client protocol', () => {
  it.each(['committed', 'draft'] as const)('auto-admits an extended legacy %s and preserves both raw rows before generation', async (selectedKind) => {
    const conversationId = randomUUID()
    const history = createLegacyExtensionFixture()
    const originalRows = await seedLegacyPair(conversationId, selectedKind === 'committed' ? history.draft : history.committed, selectedKind === 'draft' ? history.draft : history.committed)
    // Enforce backup-before-CAS on the real connection, not just before model entry.
    await getCheckpointDatabase().run(`
      CREATE TEMP TRIGGER require_recovery_snapshot_before_adoption
      BEFORE UPDATE ON conversation_checkpoints
      WHEN OLD.kind = 'committed' AND OLD.write_token = ''
      BEGIN
        SELECT RAISE(ABORT, 'Legacy checkpoint changed before its recovery snapshot')
        WHERE NOT EXISTS (
          SELECT 1 FROM conversation_checkpoint_recovery WHERE conversation_id = OLD.conversation_id
        );
      END
    `)
    providerScript = async function* () {
      expect(await getPersistedConversationCheckpoint(conversationId)).toMatchObject({
        conversationId,
        kind: 'committed',
        ...getCheckpointReferences(history.draft),
      })
      expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
      await expectRecoverySnapshot(conversationId, originalRows, 'legacy-compatible')
      yield* answer()
    }
    const client = startRun(conversationId, history.draft, new BlobTestClient(history.fixtures))

    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(JSON.stringify(requests[0]?.messages)).toContain('The active turn added a rollback-safe migration.')
    expect(client.frames.filter(frame => frame.message.case === 'interactionQuery')).toHaveLength(0)
    expect(new Set(client.getRequests.map(request => wireKey(request.blobId))))
      .toEqual(new Set(history.fixtures.map(fixture => wireKey(fixture.blobId))))
    const committed = await getPersistedConversationCheckpoint(conversationId)
    expect(committed?.rootBlobIds.slice(0, history.draft.roots.length)).toEqual(getCheckpointReferences(history.draft).rootBlobIds)
    expect(committed?.turnBlobIds.slice(0, history.draft.turns!.length)).toEqual(getCheckpointReferences(history.draft).turnBlobIds)
    expect(committed?.summaryArchiveIds).toEqual(getCheckpointReferences(history.draft).summaryArchiveIds)
    expect(client.checkpoints.length).toBeGreaterThan(0)
    await expectRecoverySnapshot(conversationId, originalRows, 'legacy-compatible')
  })

  it('asks about unrelated candidates and continues the original chat only after explicit consent', async () => {
    const history = await seedUnrelatedPair()
    const client = startRun(history.conversationId, history.committed, createWaitingClient(history.fixtures))
    const interaction = await waitForRecoveryQuery(client)
    await expectUnchangedRecoveryState(history.conversationId, history.originalRows, client)

    respondToRecovery(client, interaction.id)
    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(JSON.stringify(requests[0]?.messages)).toContain('Original chat: preserve the migration and its recovery source.')
    expect(JSON.stringify(requests[0]?.messages)).not.toContain('Unrelated draft: design a completely different calendar feature.')
    const committed = await getPersistedConversationCheckpoint(history.conversationId)
    expect(committed?.conversationId).toBe(history.conversationId)
    expect(committed?.rootBlobIds[0]).toBe(getCheckpointReferences(history.committed).rootBlobIds[0])
    expect(committed?.rootBlobIds).not.toContain(getCheckpointReferences(history.draft).rootBlobIds[0])
    expect(await getPersistedConversationCheckpoint(history.conversationId, 'draft')).toBeNull()
    expect(client.checkpoints.length).toBeGreaterThan(0)
    const completedTools = client.frames.flatMap(frame =>
      frame.message.case === 'interactionUpdate' && frame.message.value.message.case === 'toolCallCompleted'
        ? [frame.message.value.message.value]
        : [])
    expect(completedTools).toHaveLength(1)
    expect(interaction.query.value).toMatchObject({ toolCallId: completedTools[0]?.callId })
    await expectRecoverySnapshot(history.conversationId, history.originalRows, 'user-selected')
  })

  it('does not adopt uploaded-only selected history until client storage acknowledges it', async () => {
    const conversationId = randomUUID()
    const history = createLegacyExtensionFixture()
    const originalRows = await seedLegacyPair(conversationId, history.committed, history.draft)
    const client = new BlobTestClient(history.fixtures)
    await client.uploadChunk(captureAgentServiceHandlers().uploadConversationBlobs, {
      conversationId,
      blobIds: [history.extensionRoot.blobId],
      chunkIndex: 0,
      totalChunks: 1,
    })
    client.dropLocalBlobs([history.extensionRoot.blobId])
    vi.spyOn(client, 'acknowledgeSet').mockImplementation(requestId => client.failSet(requestId))
    startRun(conversationId, history.draft, client)
    expect((await client.completion).error).toBeInstanceOf(Error)
    expect(client.setRequests.map(request => wireKey(request.blobId))).toContain(wireKey(history.extensionRoot.blobId))
    expect(await readCheckpointRows(conversationId)).toEqual(originalRows)
    expect(await readRecoveryRows(conversationId)).toEqual([])
    expect(requests).toHaveLength(0)
    expect(client.checkpoints).toHaveLength(0)
  })

  it('allows explicit recovery when only the discarded candidate has a missing turn child', async () => {
    const conversationId = randomUUID()
    const root = createHistoryFixture('Keep the selected branch intact.')
    const user = createBinaryFixture(toBinary(UserMessageSchema, create(UserMessageSchema, {
      text: 'Choose a branch.',
      messageId: 'branch-user',
    })))
    const selectedStep = createBinaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
      message: { case: 'assistantMessage', value: { text: 'Selected branch content.' } },
    })))
    const missingStep = createBinaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
      message: { case: 'assistantMessage', value: { text: 'Unavailable discarded branch content.' } },
    })))
    const selectedTurn = createTurnFixture(user, [selectedStep])
    const discardedTurn = createTurnFixture(user, [missingStep])
    const selected: CheckpointFixture = { roots: [root], turns: [selectedTurn] }
    const originalRows = await seedLegacyPair(conversationId, selected, { roots: [root], turns: [discardedTurn] })
    const client = startRun(conversationId, selected, createWaitingClient([root, user, selectedStep, selectedTurn, discardedTurn]))
    const interaction = await waitForRecoveryQuery(client)
    respondToRecovery(client, interaction.id)
    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(client.getRequests.map(request => wireKey(request.blobId))).not.toContain(wireKey(missingStep.blobId))
    expect(client.checkpoints).toHaveLength(1)
    client.assertCheckpointResolvable(client.checkpoints[0]!)
    await expectRecoverySnapshot(conversationId, originalRows, 'user-selected')
  })

  it.each(['cancel-option', 'abort-run'] as const)('leaves both candidates untouched without generation on %s', async (cancellation) => {
    const history = await seedUnrelatedPair()
    const client = startRun(history.conversationId, history.committed, createWaitingClient(history.fixtures))
    const interaction = await waitForRecoveryQuery(client)
    if (cancellation === 'cancel-option')
      respondToRecovery(client, interaction.id, ['cancel'])
    else
      client.cancel()

    expect(await client.completion).toEqual({})
    await expectUnchangedRecoveryState(history.conversationId, history.originalRows, client)
    if (cancellation === 'cancel-option') {
      expect(client.frames.filter(frame => frame.message.case === 'interactionUpdate' && frame.message.value.message.case === 'turnEnded'))
        .toHaveLength(1)
    }
  })

  it('rejects stale consent if another writer changes the checkpoint while the question is open', async () => {
    const history = await seedUnrelatedPair()
    const client = startRun(history.conversationId, history.committed, createWaitingClient(history.fixtures))
    const interaction = await waitForRecoveryQuery(client)
    const newerRoot = createHistoryFixture('A newer run already committed a different accepted conversation.')
    await persistConversationCheckpoint({
      conversationId: history.conversationId,
      kind: 'committed',
      ...getCheckpointReferences({ roots: [newerRoot] }),
      tokenDetails: { usedTokens: 450, maxTokens: 200_000 },
      mode: 'AGENT_MODE_AGENT',
      updatedAt: Date.now(),
    })
    const newerRows = await readCheckpointRows(history.conversationId)

    respondToRecovery(client, interaction.id)
    const { error } = await client.completion
    expect(error).toBeInstanceOf(ConnectError)
    expect((error as ConnectError).code).toBe(Code.FailedPrecondition)
    expect(String(error)).toMatch(/Checkpoint version conflict/)
    await expectUnchangedRecoveryState(history.conversationId, newerRows, client)
  })

  it.each(['root', 'turn-child'] as const)('does not migrate an otherwise compatible legacy pair with a missing selected %s', async (missingKind) => {
    const conversationId = randomUUID()
    const history = createLegacyExtensionFixture()
    const missing = missingKind === 'root' ? history.extensionRoot : history.extensionStep
    const originalRows = await seedLegacyPair(conversationId, history.committed, history.draft)
    const fixtures = history.fixtures.filter(fixture => wireKey(fixture.blobId) !== wireKey(missing.blobId))
    const client = startRun(conversationId, history.draft, new BlobTestClient(fixtures))

    const { error } = await client.completion
    expect(error).toBeInstanceOf(ConnectError)
    const cause = (error as Error).cause
    expect(cause).toBeInstanceOf(BlobIntegrityError)
    expect((cause as BlobIntegrityError).failures).toEqual(expect.arrayContaining([
      expect.objectContaining({ blobId: blobIdFromBytes(missing.blobId), status: 'not-found' }),
    ]))
    expect(client.getRequests.some(request => wireKey(request.blobId) === wireKey(missing.blobId))).toBe(true)
    expect(client.frames.filter(frame => frame.message.case === 'interactionQuery')).toHaveLength(0)
    await expectUnchangedRecoveryState(conversationId, originalRows, client)
  })

  it('ignores a response for another interaction and accepts the correctly correlated answer afterward', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const history = await seedUnrelatedPair()
    const client = startRun(history.conversationId, history.committed, createWaitingClient(history.fixtures))
    const interaction = await waitForRecoveryQuery(client)

    respondToRecovery(client, interaction.id + 1)
    await vi.advanceTimersByTimeAsync(AGENT_HEARTBEAT_INTERVAL_MS + 1)
    expect(client.frames.some(frame => frame.message.case === 'interactionUpdate' && frame.message.value.message.case === 'heartbeat')).toBe(true)
    await expectUnchangedRecoveryState(history.conversationId, history.originalRows, client)

    respondToRecovery(client, interaction.id)
    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(client.checkpoints.length).toBeGreaterThan(0)
    await expectRecoverySnapshot(history.conversationId, history.originalRows, 'user-selected')
    expect(vi.getTimerCount()).toBe(0)
    expect(client.session.messages.some(message => 'interactionResponse' in message)).toBe(false)
  })
})
