import type { MockInstance } from 'vitest'
import type { AgentServerMessage, ConversationStateStructure } from '../gen/agent_v1_pb'
import type { LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
import type { ClientBlobFixture, OpenClientMessageStream } from './blobTestClient'
import { randomUUID } from 'node:crypto'
import { tmpdir } from 'node:os'
import { create, toBinary, toJson } from '@bufbuild/protobuf'
import { ConnectError } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { getCheckpointDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { queryUsageStats, recordModelUsage } from '../database/usageStats'
import {
  AgentMode,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  ConversationSummaryArchiveSchema,
  ConversationTurnStructureSchema,
  UserMessageSchema,
} from '../gen/agent_v1_pb'
import { ErrorDetailsSchema } from '../gen/aiserver_v1_shared_pb'
import { handleRunRequest } from '../handlers/agent/agentOrchestrator'
import { blobIdFromBytes, encodeBinaryBlob, encodeBlob } from '../handlers/agent/blob'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import { BlobTestClient, captureAgentServiceHandlers, wireKey } from './blobTestClient'

const SUMMARY_TEXT = 'The migration preserves client-owned history and waits for confirmed local-storage blob acknowledgements.'
const providerRequests: LLMStreamRequest[] = []
const clients: BlobTestClient[] = []
const openServiceInputs: OpenClientMessageStream[] = []
let providerScript: (request: LLMStreamRequest) => AsyncIterable<LLMStreamEvent>
let providerStream: MockInstance<AnthropicProvider['stream']>

function messageFixture(message: unknown): ClientBlobFixture {
  const encoded = encodeBlob(message)
  return { blobId: Buffer.from(encoded.blobId), bytes: Buffer.from(encoded.blobData) }
}

function binaryFixture(bytes: Uint8Array): ClientBlobFixture {
  const encoded = encodeBinaryBlob(bytes)
  return { blobId: Buffer.from(encoded.blobId), bytes: new Uint8Array(bytes) }
}

function simpleHistory() {
  return [
    messageFixture({ role: 'system', content: 'Old system scaffold.' }),
    messageFixture({ role: 'user', content: '<user_info>Old workspace.</user_info>' }),
    messageFixture({ role: 'user', content: 'Old user asked to preserve the migration plan.' }),
    messageFixture({ role: 'assistant', content: 'Old answer: keep the existing database schema.' }),
  ]
}

function oldTurnFixtures(rawIds = false, identity = '') {
  const identitySuffix = identity ? ` (${identity})` : ''
  const user = binaryFixture(toBinary(UserMessageSchema, create(UserMessageSchema, {
    text: `Original interrupted request${identitySuffix}`,
    messageId: `original-message-id${identitySuffix}`,
    mode: AgentMode.AGENT,
  })))
  const thinking = binaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'thinkingMessage', value: { text: `Old reasoning that must survive resume.${identitySuffix}` } },
  })))
  const assistant = binaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'assistantMessage', value: { text: `Old partial assistant answer.${identitySuffix}` } },
  })))
  if (rawIds) {
    user.blobId = Uint8Array.from([0xFF, 0x10, 0x80, 0x01, ...Buffer.from(identity)])
    thinking.blobId = Uint8Array.from([0xFE, 0x10, 0x80, 0x02, ...Buffer.from(identity)])
    assistant.blobId = Uint8Array.from([0xFD, 0x10, 0x80, 0x03, ...Buffer.from(identity)])
  }
  const turn = binaryFixture(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
    turn: {
      case: 'agentConversationTurn',
      value: {
        userMessage: user.blobId,
        steps: [thinking.blobId, assistant.blobId],
        requestId: `original-request-id${identitySuffix}`,
        dynamicToolCount: 0,
      },
    },
  })))
  if (rawIds)
    turn.blobId = Uint8Array.from([0xFC, 0x10, 0x80, 0x04, ...Buffer.from(identity)])
  return { user, thinking, assistant, turn, fixtures: [user, thinking, assistant, turn] }
}

function requestFor(options: {
  conversationId: string
  roots?: ClientBlobFixture[]
  turns?: ClientBlobFixture[]
  archives?: ClientBlobFixture[]
  checkpoint?: ConversationStateStructure
  action?: 'conversation' | 'summarize' | 'resume'
  text?: string
  contextTokenLimit?: number
}): Record<string, unknown> {
  const action = options.action ?? 'conversation'
  const conversationState = options.checkpoint
    ? toJson(ConversationStateStructureSchema, options.checkpoint)
    : {
        rootPromptMessagesJson: (options.roots ?? []).map(fixture => wireKey(fixture.blobId)),
        turns: (options.turns ?? []).map(fixture => wireKey(fixture.blobId)),
        summaryArchives: (options.archives ?? []).map(fixture => wireKey(fixture.blobId)),
        tokenDetails: { usedTokens: 100, maxTokens: options.contextTokenLimit ?? 200_000 },
      }
  return {
    runRequest: {
      conversationId: options.conversationId,
      action: action === 'summarize'
        ? { summarizeAction: {} }
        : action === 'resume'
          ? { resumeAction: { requestContext: {} } }
          : {
              userMessageAction: {
                userMessage: {
                  text: options.text ?? 'Continue the migration safely.',
                  messageId: `new-message-${options.conversationId}`,
                  mode: 'AGENT_MODE_AGENT',
                },
                requestContext: {},
              },
            },
      requestedModel: {
        modelId: 'claude-sonnet-4',
        parameters: [{ id: 'context', value: String(options.contextTokenLimit ?? 200_000) }],
      },
      conversationState,
    },
  }
}

function startClient(request: Record<string, unknown>, client: BlobTestClient): BlobTestClient {
  clients.push(client)
  client.start(handleRunRequest(request, client.session))
  return client
}

function startServiceClient(request: Record<string, unknown>, client: BlobTestClient): OpenClientMessageStream {
  const handlers = captureAgentServiceHandlers()
  clients.push(client)
  const input = client.startServiceRun(handlers.run, request)
  openServiceInputs.push(input)
  return input
}

function deferredSignal(): { promise: Promise<void>, resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

async function* answer(text = 'New assistant answer.', inputTokens = 500): AsyncIterable<LLMStreamEvent> {
  yield { type: 'text_delta', text }
  yield { type: 'done', usage: { inputTokens, outputTokens: 20 }, stopReason: 'end_turn' }
}

function hasSummaryStarted(client: BlobTestClient): boolean {
  return client.frames.some(frame => frame.message.case === 'interactionUpdate'
    && frame.message.value.message.case === 'summaryStarted')
}

async function settleRuntime(): Promise<void> {
  // Cross an event-loop turn, not a tiny wallclock deadline. The reader keeps
  // advancing while a missing ACK leaves the actual runtime waiter unresolved.
  await new Promise<void>(resolve => setImmediate(resolve))
  await new Promise<void>(resolve => setImmediate(resolve))
}

async function seedPreviousCheckpoint(conversationId: string, roots: ClientBlobFixture[], turns: ClientBlobFixture[] = [], archives: ClientBlobFixture[] = []) {
  const previous = {
    conversationId,
    kind: 'committed' as const,
    rootBlobIds: roots.map(fixture => blobIdFromBytes(fixture.blobId)),
    turnBlobIds: turns.map(fixture => blobIdFromBytes(fixture.blobId)),
    summaryArchiveIds: archives.map(fixture => blobIdFromBytes(fixture.blobId)),
    tokenDetails: { usedTokens: 1234, maxTokens: 200_000 },
    mode: 'AGENT_MODE_AGENT',
    updatedAt: 123456,
  }
  await persistConversationCheckpoint(previous)
  return previous
}

async function expectCheckpointMatchesClient(conversationId: string, client: BlobTestClient) {
  const checkpoint = client.checkpoints.at(-1)
  expect(checkpoint).toBeDefined()
  if (!checkpoint)
    throw new Error('Expected a committed runtime checkpoint')
  const persisted = await getPersistedConversationCheckpoint(conversationId)
  expect(persisted?.rootBlobIds).toEqual(checkpoint.rootPromptMessagesJson.map(blobIdFromBytes))
  expect(persisted?.turnBlobIds).toEqual(checkpoint.turns.map(blobIdFromBytes))
  expect(persisted?.summaryArchiveIds).toEqual(checkpoint.summaryArchives.map(blobIdFromBytes))
  expect(persisted?.kind).toBe('committed')
  client.assertCheckpointResolvable(checkpoint)
  return checkpoint
}

beforeEach(async () => {
  // setup.ts owns a per-file temporary SQLite database and temporary HOME.
  expect(process.env.BYOK_AGENT_DB_PATH?.startsWith(tmpdir())).toBe(true)
  await resetAgentDatabaseForTests()
  providerRequests.length = 0
  clients.length = 0
  openServiceInputs.length = 0
  providerScript = () => answer()
  // Only the network/provider boundary is replaced. Routing, codecs, history,
  // turn tracking, compaction, checkpoint persistence and blob barriers are real.
  providerStream = vi.spyOn(AnthropicProvider.prototype, 'stream').mockImplementation((request) => {
    providerRequests.push(structuredClone(request))
    return providerScript(request)
  })
})

afterEach(async () => {
  for (const client of clients)
    client.cancel()
  for (const input of openServiceInputs)
    input.close()
  await Promise.all(clients.map(client => client.completion))
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('handleRunRequest required client history', () => {
  const invalidRoots = [
    { name: 'missing', bytes: undefined },
    { name: 'invalid base64', bytes: Buffer.from('not a history blob !!!') },
    { name: 'malformed JSON', bytes: Buffer.from('{"role":"user",') },
    { name: 'unsupported role', bytes: messageFixture({ role: 'alien', content: 'Do not silently drop this.' }).bytes },
    { name: 'unsupported content block', bytes: messageFixture({ role: 'user', content: [{ type: 'unknown-block', text: 'Do not truncate history.' }] }).bytes },
  ]

  for (const action of ['conversation', 'summarize'] as const) {
    it.each(invalidRoots)(`${action} rejects $name roots before provider I/O and preserves the prior checkpoint`, async ({ bytes }) => {
      const conversationId = randomUUID()
      const oldRoots = simpleHistory()
      const required = { blobId: Buffer.from('required-history-root'), bytes: bytes ?? new Uint8Array() }
      const previous = await seedPreviousCheckpoint(conversationId, [...oldRoots, required])
      const client = startClient(
        requestFor({ conversationId, roots: [...oldRoots, required], action }),
        new BlobTestClient([...oldRoots, ...(bytes === undefined ? [] : [required])]),
      )

      const outcome = await client.completion
      expect(outcome.error).toBeInstanceOf(Error)
      expect(String(outcome.error)).toMatch(/blob|conversation data/i)
      expect(providerStream).not.toHaveBeenCalled()
      expect(client.getRequests.filter(request => wireKey(request.blobId) === wireKey(required.blobId))).toHaveLength(1)
      expect(client.setRequests).toHaveLength(0)
      expect(client.checkpoints).toHaveLength(0)
      expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
      expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
      expect(client.session.listeners.size).toBe(0)
    })
  }

  it.each(['conversation', 'summarize'] as const)('rejects unavailable history without a session in the %s path', async (action) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const previous = await seedPreviousCheckpoint(conversationId, roots)
    const receivedFrames: AgentServerMessage[] = []
    await expect((async () => {
      for await (const frame of handleRunRequest(requestFor({ conversationId, roots, action })))
        receivedFrames.push(frame)
    })()).rejects.toThrow(/no-session|conversation data/i)

    expect(providerStream).not.toHaveBeenCalled()
    expect(receivedFrames.some(frame => frame.message.case === 'conversationCheckpointUpdate')).toBe(false)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
  })
})

describe('registered agent service bidi output lifetime', () => {
  it('returns diagnostic history failure while the client input stream remains open', async () => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const missingRoot = messageFixture({ role: 'user', content: 'Required but absent on the client.' })
    const previous = await seedPreviousCheckpoint(conversationId, [...roots, missingRoot])
    const client = new BlobTestClient(roots)
    const input = startServiceClient(requestFor({ conversationId, roots: [...roots, missingRoot] }), client)
    try {
      const outcome = await client.completion
      expect(input.closed).toBe(false)
      expect(input.deliveredEof).toBe(false)
      expect(outcome.error).toBeInstanceOf(ConnectError)
      const error = outcome.error as ConnectError
      expect(error.findDetails(ErrorDetailsSchema)[0]?.details?.title).toBe('Conversation data unavailable; checkpoint preserved')
      expect(error.cause).toMatchObject({
        failures: expect.arrayContaining([expect.objectContaining({
          blobId: blobIdFromBytes(missingRoot.blobId),
          status: 'not-found',
        })]),
      })
      expect(client.getRequests.some(request => wireKey(request.blobId) === wireKey(missingRoot.blobId))).toBe(true)
      expect(providerStream).not.toHaveBeenCalled()
      expect(client.setRequests).toHaveLength(0)
      expect(client.checkpoints).toHaveLength(0)
      expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    }
    finally {
      input.close()
      await client.completion
    }
  })

  it('reaches successful output EOF after real Get/Set replies without waiting for client EOF', async () => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const client = new BlobTestClient(roots)
    const input = startServiceClient(requestFor({ conversationId, roots }), client)
    try {
      expect(await client.completion).toEqual({})
      expect(input.closed).toBe(false)
      expect(input.deliveredEof).toBe(false)
      expect(providerRequests).toHaveLength(1)
      expect(client.getRequests.map(request => wireKey(request.blobId))).toEqual(roots.map(fixture => wireKey(fixture.blobId)))
      expect(client.setRequests.length).toBeGreaterThan(0)
      expect(client.pendingSets.size).toBe(0)
      const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
      expect(client.assertCheckpointResolvable(checkpoint).turns[0]?.steps).toHaveLength(1)
    }
    finally {
      input.close()
      await client.completion
    }
  })
})

describe('handleRunRequest client save barrier', () => {
  it.each([false, true])('preserves the actual SQL outcome when cancellation races completion (write fails: %s)', async (writeFails) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const previous = await seedPreviousCheckpoint(conversationId, roots)
    const unrelatedPrevious = await seedPreviousCheckpoint(randomUUID(), roots)
    const database = getCheckpointDatabase()
    const readCheckpointRow = () => database.get<Record<string, unknown>>(
      'SELECT * FROM conversation_checkpoints WHERE conversation_id = ? AND kind = ?',
      [conversationId, 'committed'],
    )
    const previousRow = await readCheckpointRow()
    const originalRun = database.run.bind(database)
    const insertFinished = deferredSignal()
    const releaseInsertCallback = deferredSignal()
    let heldInsert = false
    const interceptInsert = vi.spyOn(database, 'run').mockImplementation(async (sql, parameters) => {
      const values = parameters as Record<string, unknown> | undefined
      const shouldHold = !heldInsert
        && /INSERT\s+(?:OR\s+REPLACE\s+)?INTO\s+conversation_checkpoints\b/i.test(sql)
        && values?.$conversationId === conversationId
        && values?.$kind === 'committed'
      if (!shouldHold)
        return originalRun(sql, parameters)
      heldInsert = true
      if (writeFails) {
        insertFinished.resolve()
        await releaseInsertCallback.promise
        throw new Error('checkpoint write failed')
      }
      // SQLite performs the real INSERT. Delay only notification of completion,
      // reproducing cancellation while the awaited callback is still pending.
      const result = await originalRun(sql, parameters)
      insertFinished.resolve()
      await releaseInsertCallback.promise
      return result
    })
    const client = startClient(requestFor({ conversationId, roots }), new BlobTestClient(roots))
    try {
      await Promise.race([
        insertFinished.promise,
        client.completion.then((outcome) => {
          throw new Error('Run completed before its final checkpoint INSERT', { cause: outcome.error })
        }),
      ])
      expect(client.setRequests.length).toBeGreaterThan(0)
      expect(client.pendingSets.size).toBe(0)
      expect(client.checkpoints).toHaveLength(0)
      if (writeFails)
        expect(await readCheckpointRow()).toEqual(previousRow)
      else
        expect(await readCheckpointRow()).not.toEqual(previousRow)

      const usageModelId = `usage-during-checkpoint-${randomUUID()}`
      await recordModelUsage({
        providerId: 'independent-provider',
        providerName: 'Independent provider',
        modelId: usageModelId,
        apiModel: 'independent-model',
        usage: { inputTokens: 17, outputTokens: 9 },
      })
      client.cancel()
      releaseInsertCallback.resolve()
      const outcome = await client.completion
      if (writeFails) {
        expect(outcome.error).toBeInstanceOf(Error)
        expect((outcome.error as Error).message).toContain('checkpoint write failed')
        expect(await readCheckpointRow()).toEqual(previousRow)
        expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
      }
      else {
        expect(outcome).toEqual({})
        expect(await readCheckpointRow()).not.toEqual(previousRow)
        const accepted = await getPersistedConversationCheckpoint(conversationId)
        expect(accepted?.rootBlobIds.length).toBeGreaterThan(previous.rootBlobIds.length)
        expect(accepted?.turnBlobIds).toHaveLength(1)
        expect(client.pendingSets.size).toBe(0)
      }
      expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
      expect(await getPersistedConversationCheckpoint(unrelatedPrevious.conversationId)).toEqual(unrelatedPrevious)
      expect(await queryUsageStats(null)).toEqual(expect.arrayContaining([expect.objectContaining({
        modelId: usageModelId,
        requestCount: 1,
        inputTokens: 17,
        outputTokens: 9,
      })]))
      expect(client.checkpoints).toHaveLength(0)
      expect(client.session.listeners.size).toBe(0)
    }
    finally {
      releaseInsertCallback.resolve()
      client.cancel()
      await client.completion
      interceptInsert.mockRestore()
    }
  })

  it('waits for the last matching SetBlobResult, then commits a complete graph and restores cold', async () => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const previous = await seedPreviousCheckpoint(conversationId, roots)
    const client = new BlobTestClient(roots)
    client.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots }), client)
    await client.waitFor(current => current.pendingSets.size > 0, 'outgoing Set requests')
    await settleRuntime()
    expect(client.pendingSets.size).toBeGreaterThan(1)
    const delayedRequestId = [...client.pendingSets.keys()].at(-1)!
    for (const requestId of client.pendingSets.keys()) {
      if (requestId !== delayedRequestId)
        client.acknowledgeSet(requestId)
    }
    client.send({ kvClientMessage: { id: delayedRequestId, getBlobResult: {} } })
    client.send({ kvClientMessage: { id: delayedRequestId + 10_000, setBlobResult: {} } })
    await settleRuntime()

    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    expect(providerRequests).toHaveLength(1)

    client.autoAcknowledgeSets = true
    client.acknowledgeSet(delayedRequestId)
    expect(await client.completion).toEqual({})
    expect(client.session.listeners.size).toBe(0)
    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const graph = client.assertCheckpointResolvable(checkpoint)
    expect(graph.turns.at(-1)?.user.text).toBe('Continue the migration safely.')
    expect(graph.turns.at(-1)?.steps.some(step => step.message.case === 'assistantMessage')).toBe(true)

    // Awaiting completion has executed orchestrator finally/dispose. The fresh
    // session receives no server cache or manufactured new fixtures.
    const coldClient = startClient(requestFor({ conversationId, checkpoint, text: 'What did we decide?' }), client.fork())
    expect(await coldClient.completion).toEqual({})
    const coldRequest = providerRequests.at(-1)!
    expect(JSON.stringify(coldRequest.messages)).toContain('Old answer: keep the existing database schema.')
    expect(JSON.stringify(coldRequest.messages)).toContain('New assistant answer.')
    expect(coldClient.getRequests.map(request => wireKey(request.blobId))).toEqual(expect.arrayContaining([
      ...checkpoint.rootPromptMessagesJson.map(wireKey),
      wireKey(checkpoint.turns.at(-1)!),
    ]))
    await expectCheckpointMatchesClient(conversationId, coldClient)
  })

  it.each(['client-error', 'timeout'] as const)('does not publish or persist when a Set ends in %s', async (failure) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const previous = await seedPreviousCheckpoint(conversationId, roots)
    if (failure === 'timeout')
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
    const client = new BlobTestClient(roots)
    client.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots }), client)
    await client.waitFor(current => current.pendingSets.size > 0, 'outgoing Set requests')
    await settleRuntime()
    const failedRequestId = [...client.pendingSets.keys()].at(-1)!
    for (const requestId of client.pendingSets.keys()) {
      if (requestId !== failedRequestId)
        client.acknowledgeSet(requestId)
    }
    if (failure === 'client-error')
      client.failSet(failedRequestId)
    else
      await vi.advanceTimersByTimeAsync(10_001)

    const outcome = await client.completion
    expect(outcome.error).toBeInstanceOf(Error)
    expect(outcome.error).toMatchObject({
      cause: {
        name: 'BlobTransferError',
        failures: expect.arrayContaining([expect.objectContaining({ status: failure })]),
      },
    })
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    expect(client.session.listeners.size).toBe(0)
  })
})

describe('handleRunRequest run-local lifetime', () => {
  it.each(['cancelled', 'failed'] as const)('isolates concurrent runs for one conversation when the older run is %s', async (failure) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    providerScript = request => answer(JSON.stringify(request.messages).includes('second branch') ? 'Second branch committed.' : 'First branch discarded.')
    const olderClient = new BlobTestClient(roots)
    olderClient.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots, text: 'first branch' }), olderClient)
    await olderClient.waitFor(current => current.pendingSets.size > 0, 'older run save barrier')
    await settleRuntime()
    const newerClient = new BlobTestClient(roots)
    newerClient.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots, text: 'second branch' }), newerClient)
    await newerClient.waitFor(current => current.pendingSets.size > 0, 'newer run save barrier')
    await settleRuntime()

    if (failure === 'cancelled') {
      olderClient.cancel()
      expect(await olderClient.completion).toEqual({})
    }
    newerClient.autoAcknowledgeSets = true
    newerClient.acknowledgePendingSets()
    expect(await newerClient.completion).toEqual({})
    const checkpoint = await expectCheckpointMatchesClient(conversationId, newerClient)
    const committed = await getPersistedConversationCheckpoint(conversationId)

    if (failure === 'failed') {
      const failedRequestId = [...olderClient.pendingSets.keys()][0]!
      olderClient.failSet(failedRequestId)
      olderClient.acknowledgePendingSets()
      expect((await olderClient.completion).error).toBeInstanceOf(Error)
    }
    expect(olderClient.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(committed)
    expect(JSON.stringify(newerClient.assertCheckpointResolvable(checkpoint).rootMessages)).toContain('Second branch committed.')
    expect(JSON.stringify(newerClient.assertCheckpointResolvable(checkpoint).rootMessages)).not.toContain('First branch discarded.')
    expect(olderClient.session.listeners.size).toBe(0)
    expect(newerClient.session.listeners.size).toBe(0)
  })

  it.each(['missing', 'corrupt'] as const)('rejects a %s resume turn before provider I/O without replacing the checkpoint', async (failure) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const oldTurn = oldTurnFixtures()
    const previous = await seedPreviousCheckpoint(conversationId, roots, [oldTurn.turn])
    const fixtures = [...roots, oldTurn.user, oldTurn.thinking, oldTurn.assistant]
    if (failure === 'corrupt')
      fixtures.push({ blobId: oldTurn.turn.blobId, bytes: Uint8Array.from([0x0A, 0xFF]) })
    const client = startClient(
      requestFor({ conversationId, roots, turns: [oldTurn.turn], action: 'resume' }),
      new BlobTestClient(fixtures),
    )

    expect((await client.completion).error).toBeInstanceOf(Error)
    expect(providerStream).not.toHaveBeenCalled()
    expect(client.getRequests.filter(request => wireKey(request.blobId) === wireKey(oldTurn.turn.blobId))).toHaveLength(1)
    expect(client.setRequests).toHaveLength(0)
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    expect(client.session.listeners.size).toBe(0)
  })

  it.each([false, true])('resumes real client-only protobuf turns without dropping old steps (raw IDs: %s)', async (rawIds) => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const oldTurn = oldTurnFixtures(rawIds)
    const client = startClient(
      requestFor({ conversationId, roots, turns: [oldTurn.turn], action: 'resume' }),
      new BlobTestClient([...roots, ...oldTurn.fixtures]),
    )
    expect(await client.completion).toEqual({})
    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const graph = client.assertCheckpointResolvable(checkpoint)
    expect(graph.turns).toHaveLength(1)
    const resumed = graph.turns[0]!
    expect(wireKey(resumed.turn.userMessage)).toBe(wireKey(oldTurn.user.blobId))
    expect(resumed.user.text).toBe('Original interrupted request')
    expect(resumed.turn.requestId).toBe('original-request-id')
    expect(resumed.turn.steps.slice(0, 2).map(wireKey)).toEqual([oldTurn.thinking, oldTurn.assistant].map(fixture => wireKey(fixture.blobId)))
    expect(resumed.steps).toHaveLength(3)
    expect(resumed.steps.at(-1)?.message).toMatchObject({ case: 'assistantMessage', value: { text: 'New assistant answer.' } })
    expect(client.getRequests.filter(request => wireKey(request.blobId) === wireKey(oldTurn.turn.blobId))).toHaveLength(1)
    expect(client.setRequests.some(request => [oldTurn.user, oldTurn.thinking, oldTurn.assistant].some(fixture => wireKey(fixture.blobId) === wireKey(request.blobId)))).toBe(false)
  })
})

function expectUploadedFixturesResent(client: BlobTestClient, fixtures: ClientBlobFixture[]): void {
  for (const fixture of fixtures) {
    const requests = client.setRequests.filter(request => wireKey(request.blobId) === wireKey(fixture.blobId))
    expect(requests).toHaveLength(1)
    expect(Buffer.from(requests[0]!.bytes)).toEqual(Buffer.from(fixture.bytes))
    expect(client.getRequests.some(request => wireKey(request.blobId) === wireKey(fixture.blobId))).toBe(false)
  }
}

describe('agent service upload/fork handoff into handleRunRequest', () => {
  it('confirms uploaded roots, two distinct turn graphs and a prior archive before publishing a cold-restorable checkpoint', async () => {
    const handlers = captureAgentServiceHandlers()
    const conversationId = randomUUID()
    const earlier = oldTurnFixtures(true, 'earlier turn')
    const latest = oldTurnFixtures(true, 'latest turn')
    const roots = [
      ...simpleHistory().slice(0, 2),
      messageFixture({ role: 'user', content: 'Original interrupted request (earlier turn)' }),
      messageFixture({ role: 'assistant', content: 'Old partial assistant answer. (earlier turn)' }),
      messageFixture({ role: 'user', content: 'Original interrupted request (latest turn)' }),
      messageFixture({ role: 'assistant', content: 'Old partial assistant answer. (latest turn)' }),
    ]
    const archivedOriginal = messageFixture({ role: 'user', content: 'Older archived requirement: keep the rollback path intact.' })
    const priorSummaryText = 'Earlier work established a rollback-safe migration.'
    const archivedSummary = messageFixture({
      role: 'assistant',
      content: `Previous conversation summary:\n${priorSummaryText}`,
      providerOptions: { cursor: { isSummary: true } },
    })
    archivedOriginal.blobId = Uint8Array.from([0xFF, 0x81, 0x30, 0x01])
    archivedSummary.blobId = Uint8Array.from([0xFE, 0x81, 0x30, 0x02])
    const archive = binaryFixture(toBinary(ConversationSummaryArchiveSchema, create(ConversationSummaryArchiveSchema, {
      summarizedMessages: [archivedOriginal.blobId],
      summary: priorSummaryText,
      summaryMessage: archivedSummary.blobId,
      windowTail: 2,
    })))
    archive.blobId = Uint8Array.from([0xFD, 0x81, 0x30, 0x03])
    const uploadedFixtures = [...roots, ...earlier.fixtures, ...latest.fixtures, archive, archivedOriginal, archivedSummary]
    expect(new Set(uploadedFixtures.map(fixture => wireKey(fixture.blobId))).size).toBe(uploadedFixtures.length)
    for (const [index, fixture] of earlier.fixtures.entries())
      expect(Buffer.from(fixture.bytes)).not.toEqual(Buffer.from(latest.fixtures[index]!.bytes))

    const previous = await seedPreviousCheckpoint(conversationId, roots, [earlier.turn, latest.turn], [archive])
    const client = new BlobTestClient(uploadedFixtures)
    await client.notifyClone(handlers.notifyConversationClone, conversationId, randomUUID())
    await client.uploadChunk(handlers.uploadConversationBlobs, {
      conversationId,
      blobIds: uploadedFixtures.map(fixture => fixture.blobId),
      chunkIndex: 0,
      totalChunks: 1,
    })
    client.dropLocalBlobs(uploadedFixtures.map(fixture => fixture.blobId))
    for (const fixture of uploadedFixtures)
      expect(() => client.read(fixture.blobId)).toThrow(/unsaved client blob/)
    client.autoAcknowledgeSets = false
    startClient(requestFor({
      conversationId,
      roots,
      turns: [earlier.turn, latest.turn],
      archives: [archive],
      action: 'resume',
    }), client)
    await client.waitFor(current => current.pendingSets.size > 0, 'complete incoming graph Set requests')
    await settleRuntime()
    expectUploadedFixturesResent(client, uploadedFixtures)
    expect(client.getRequests).toHaveLength(0)

    const heldDependencies = [earlier.user.blobId, archivedOriginal.blobId]
    const heldRequests = heldDependencies.map(blobId => client.setRequests.find(request => wireKey(request.blobId) === wireKey(blobId))!)
    const heldRequestIds = new Set(heldRequests.map(request => request.requestId))
    client.autoAcknowledgeSets = true
    for (const requestId of client.pendingSets.keys()) {
      if (!heldRequestIds.has(requestId))
        client.acknowledgeSet(requestId)
    }
    await settleRuntime()
    expect(client.pendingSets.size).toBe(2)
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)

    client.acknowledgeSet(heldRequests[0]!.requestId)
    await settleRuntime()
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    client.acknowledgeSet(heldRequests[1]!.requestId)
    expect(await client.completion).toEqual({})

    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const graph = client.assertCheckpointResolvable(checkpoint)
    expect(graph.turns).toHaveLength(2)
    expect(checkpoint.turns.map(wireKey)[0]).toBe(wireKey(earlier.turn.blobId))
    expect(graph.turns[0]!.user.text).toBe('Original interrupted request (earlier turn)')
    expect(graph.turns[0]!.turn.steps.map(wireKey)).toEqual([earlier.thinking, earlier.assistant].map(fixture => wireKey(fixture.blobId)))
    expect(graph.turns[1]!.user.text).toBe('Original interrupted request (latest turn)')
    expect(graph.turns[1]!.turn.steps.slice(0, 2).map(wireKey)).toEqual([latest.thinking, latest.assistant].map(fixture => wireKey(fixture.blobId)))
    expect(graph.turns[1]!.steps).toHaveLength(3)
    expect(checkpoint.summaryArchives.map(wireKey)).toEqual([wireKey(archive.blobId)])
    expect(graph.archives[0]!.summarizedMessages.map(wireKey)).toEqual([wireKey(archivedOriginal.blobId)])
    expect(graph.archives[0]!.summary).toBe(priorSummaryText)
    for (const fixture of uploadedFixtures)
      expect(Buffer.from(client.read(fixture.blobId))).toEqual(Buffer.from(fixture.bytes))

    // Expire the real short-lived handoff before restoring. Earlier turns and
    // archives keep their original IDs, so disposal of the run alone is not cold.
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(Date.now() + 5 * 60_000 + 1)
    providerScript = () => answer('Cold restored complete graph.')
    const coldClient = startClient(requestFor({ conversationId, checkpoint, action: 'resume' }), client.fork())
    expect(await coldClient.completion).toEqual({})
    expect(new Set(coldClient.getRequests.map(request => wireKey(request.blobId)))).toEqual(new Set([
      ...checkpoint.rootPromptMessagesJson.map(wireKey),
      wireKey(checkpoint.turns.at(-1)!),
      wireKey(graph.turns[1]!.turn.userMessage),
      ...graph.turns[1]!.turn.steps.map(wireKey),
    ]))
    const uploadedKeys = new Set(uploadedFixtures.map(fixture => wireKey(fixture.blobId)))
    expect(coldClient.setRequests.some(request => uploadedKeys.has(wireKey(request.blobId)))).toBe(false)
    const coldCheckpoint = await expectCheckpointMatchesClient(conversationId, coldClient)
    const coldGraph = coldClient.assertCheckpointResolvable(coldCheckpoint)
    expect(coldGraph.turns[0]!.turn).toEqual(graph.turns[0]!.turn)
    expect(coldGraph.turns[1]!.turn.steps.slice(0, 3).map(wireKey)).toEqual(graph.turns[1]!.turn.steps.map(wireKey))
    expect(coldGraph.turns[1]!.steps).toHaveLength(4)
    expect(coldGraph.archives).toEqual(graph.archives)
  })

  it.each([
    { arrival: 'reverse-order', chunkIndexes: [2, 1, 0], totalChunks: 3 },
    { arrival: 'partially delivered', chunkIndexes: [4, 2, 0], totalChunks: 5 },
  ])('resumes a raw-ID fork from $arrival chunks, confirms every dependency and restores from client storage', async ({ chunkIndexes, totalChunks }) => {
    const handlers = captureAgentServiceHandlers()
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const oldTurn = oldTurnFixtures(true)
    const previous = await seedPreviousCheckpoint(conversationId, roots, [oldTurn.turn])
    const client = new BlobTestClient([...roots, ...oldTurn.fixtures])
    await client.notifyClone(handlers.notifyConversationClone, conversationId, randomUUID())
    const chunks = [[oldTurn.turn], [oldTurn.thinking, oldTurn.assistant], [oldTurn.user]]
    for (const [position, fixtures] of chunks.entries()) {
      await client.uploadChunk(handlers.uploadConversationBlobs, {
        conversationId,
        blobIds: fixtures.map(fixture => fixture.blobId),
        chunkIndex: chunkIndexes[position]!,
        totalChunks,
      })
    }
    // Chunks are independent: accepted referenced bytes are usable even when
    // other declared chunk indexes never arrive. Upload ACKs are not Set ACKs.
    client.dropLocalBlobs(oldTurn.fixtures.map(fixture => fixture.blobId))
    for (const fixture of oldTurn.fixtures)
      expect(() => client.read(fixture.blobId)).toThrow(/unsaved client blob/)
    expect(client.setRequests).toHaveLength(0)
    expect(providerStream).not.toHaveBeenCalled()

    client.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots, turns: [oldTurn.turn], action: 'resume' }), client)
    await client.waitFor(current => current.pendingSets.size > 0, 'uploaded fork Set requests')
    await settleRuntime()
    expectUploadedFixturesResent(client, oldTurn.fixtures)

    // Confirm the uploaded turn and every other payload except one old step.
    // The transitive dependency must gate publication, not just the turn blob.
    const delayedRequest = client.setRequests.find(request => wireKey(request.blobId) === wireKey(oldTurn.thinking.blobId))!
    client.autoAcknowledgeSets = true
    for (const requestId of client.pendingSets.keys()) {
      if (requestId !== delayedRequest.requestId)
        client.acknowledgeSet(requestId)
    }
    await settleRuntime()
    expect(client.pendingSets.size).toBe(1)
    expect(() => client.read(oldTurn.thinking.blobId)).toThrow(/unsaved client blob/)
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    expect(providerRequests).toHaveLength(1)

    client.acknowledgeSet(delayedRequest.requestId)
    expect(await client.completion).toEqual({})
    expect(client.session.listeners.size).toBe(0)
    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const resumed = client.assertCheckpointResolvable(checkpoint).turns[0]!
    expect(resumed.user.text).toBe('Original interrupted request')
    expect(wireKey(resumed.turn.userMessage)).toBe(wireKey(oldTurn.user.blobId))
    expect(resumed.turn.steps.slice(0, 2).map(wireKey)).toEqual([oldTurn.thinking, oldTurn.assistant].map(fixture => wireKey(fixture.blobId)))
    expect(resumed.steps).toHaveLength(3)
    for (const fixture of oldTurn.fixtures)
      expect(Buffer.from(client.read(fixture.blobId))).toEqual(Buffer.from(fixture.bytes))

    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(Date.now() + 5 * 60_000 + 1)
    providerScript = () => answer('Cold resumed fork answer.')
    const coldClient = startClient(requestFor({ conversationId, checkpoint, action: 'resume' }), client.fork())
    expect(await coldClient.completion).toEqual({})
    // The latest turn was generated by the first run, never uploaded. All
    // incoming roots and that turn must now arrive through client Get replies.
    expect(wireKey(checkpoint.turns[0]!)).not.toBe(wireKey(oldTurn.turn.blobId))
    expect(new Set(coldClient.getRequests.map(request => wireKey(request.blobId)))).toEqual(new Set([
      ...checkpoint.rootPromptMessagesJson.map(wireKey),
      ...checkpoint.turns.map(wireKey),
      wireKey(resumed.turn.userMessage),
      ...resumed.turn.steps.map(wireKey),
    ]))
    const coldCheckpoint = await expectCheckpointMatchesClient(conversationId, coldClient)
    const coldTurn = coldClient.assertCheckpointResolvable(coldCheckpoint).turns[0]!
    expect(coldTurn.turn.steps.slice(0, 3).map(wireKey)).toEqual(resumed.turn.steps.map(wireKey))
    expect(coldTurn.steps).toHaveLength(4)
    expect(coldTurn.steps.at(-1)?.message).toMatchObject({ case: 'assistantMessage', value: { text: 'Cold resumed fork answer.' } })
    expect(coldTurn.user.text).toBe('Original interrupted request')
  })

  it('shares uploaded raw blobs across concurrent runs and keeps them available after cancellation', async () => {
    const handlers = captureAgentServiceHandlers()
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const oldTurn = oldTurnFixtures(true)
    const previous = await seedPreviousCheckpoint(conversationId, roots, [oldTurn.turn])
    const olderClient = new BlobTestClient([...roots, ...oldTurn.fixtures])
    await olderClient.notifyClone(handlers.notifyConversationClone, conversationId, randomUUID())
    await olderClient.uploadChunk(handlers.uploadConversationBlobs, {
      conversationId,
      blobIds: oldTurn.fixtures.map(fixture => fixture.blobId),
      chunkIndex: 1,
      totalChunks: 2,
    })
    olderClient.dropLocalBlobs(oldTurn.fixtures.map(fixture => fixture.blobId))
    const request = requestFor({ conversationId, roots, turns: [oldTurn.turn], action: 'resume' })
    olderClient.autoAcknowledgeSets = false
    startClient(request, olderClient)
    await olderClient.waitFor(current => current.pendingSets.size > 0, 'first handoff consumer')
    await settleRuntime()

    // Unacknowledged pending Sets are deliberately excluded from this copy.
    const survivingClient = olderClient.fork()
    survivingClient.autoAcknowledgeSets = false
    startClient(request, survivingClient)
    await survivingClient.waitFor(current => current.pendingSets.size > 0, 'concurrent handoff consumer')
    await settleRuntime()
    expectUploadedFixturesResent(olderClient, oldTurn.fixtures)
    expectUploadedFixturesResent(survivingClient, oldTurn.fixtures)
    olderClient.cancel()
    expect(await olderClient.completion).toEqual({})
    expect(olderClient.checkpoints).toHaveLength(0)
    expect(olderClient.session.listeners.size).toBe(0)

    // A third consumer starts only after cancellation. It must still obtain
    // uploads through the RPC handoff, not a copy owned by either active run.
    const laterClient = olderClient.fork()
    laterClient.autoAcknowledgeSets = false
    startClient(request, laterClient)
    await laterClient.waitFor(current => current.pendingSets.size > 0, 'post-cancellation handoff consumer')
    await settleRuntime()
    expectUploadedFixturesResent(laterClient, oldTurn.fixtures)
    laterClient.cancel()
    expect(await laterClient.completion).toEqual({})
    expect(laterClient.checkpoints).toHaveLength(0)
    expect(laterClient.session.listeners.size).toBe(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()

    for (const fixture of oldTurn.fixtures)
      expect(() => survivingClient.read(fixture.blobId)).toThrow(/unsaved client blob/)
    survivingClient.autoAcknowledgeSets = true
    survivingClient.acknowledgePendingSets()
    expect(await survivingClient.completion).toEqual({})
    const checkpoint = await expectCheckpointMatchesClient(conversationId, survivingClient)
    const resumed = survivingClient.assertCheckpointResolvable(checkpoint).turns[0]!
    expect(resumed.user.text).toBe('Original interrupted request')
    expect(resumed.steps).toHaveLength(3)
    expect(resumed.turn.steps.slice(0, 2).map(wireKey)).toEqual([oldTurn.thinking, oldTurn.assistant].map(fixture => wireKey(fixture.blobId)))
    expect(providerRequests).toHaveLength(3)
    expect(survivingClient.session.listeners.size).toBe(0)
  })
})

function compactionHistory() {
  const fixtures = simpleHistory()
  // Plain historical turns force a real cut even after the old tool result is
  // replaced with a small placeholder. These are old client fixtures only.
  for (let turnIndex = 0; turnIndex < 24; turnIndex++) {
    fixtures.push(messageFixture({ role: 'user', content: `Historical task ${turnIndex}: preserve file ${turnIndex}.` }))
    fixtures.push(messageFixture({ role: 'assistant', content: `Historical analysis ${turnIndex}. ${'alpha beta gamma delta epsilon zeta eta theta '.repeat(260)}` }))
  }
  fixtures.push(messageFixture({ role: 'user', content: 'Inspect the migration file and report the result.' }))
  fixtures.push(messageFixture({ role: 'assistant', content: [
    { type: 'tool_use', id: 'legacy-read', name: 'Read', input: { path: '/fixture/migration.ts' } },
  ] }))
  const legacyToolResult = messageFixture({ role: 'user', content: [
    { type: 'tool_result', toolUseId: 'legacy-read', toolName: 'Read', content: `Large old file output. ${'one two three four five six seven eight nine ten '.repeat(1200)}` },
    { type: 'text', text: 'Legacy mixed user text must survive repair.' },
  ] })
  fixtures.push(legacyToolResult)
  fixtures.push(messageFixture({ role: 'assistant', content: 'I have consumed the old file output.' }))
  fixtures.push(messageFixture({ role: 'user', content: 'Keep the migration rollback-safe.' }))
  return { fixtures, legacyToolResult }
}

describe('handleRunRequest compaction lifecycle', () => {
  it('aborts inline summary I/O without another attempt, fallback, model round or checkpoint', async () => {
    const conversationId = randomUUID()
    const { fixtures } = compactionHistory()
    const previous = await seedPreviousCheckpoint(conversationId, fixtures)
    const summaryStarted = deferredSignal()
    let summarySignal: AbortSignal | undefined
    let summaryFinalized = false
    providerScript = (request) => {
      if (request.conversationId) {
        return (async function* (): AsyncIterable<LLMStreamEvent> {
          yield { type: 'tool_use_start', id: 'inline-cancel-read', name: 'Read' }
          yield { type: 'tool_use_delta', id: 'inline-cancel-read', input: '{"path":"/fixture/current.ts"}' }
          yield { type: 'tool_use_done', id: 'inline-cancel-read' }
          yield { type: 'done', usage: { inputTokens: 63_000, outputTokens: 200 }, stopReason: 'tool_use' }
        })()
      }
      return (async function* (): AsyncIterable<LLMStreamEvent> {
        summarySignal = request.signal
        const aborted = deferredSignal()
        const onAbort = () => aborted.resolve()
        request.signal!.addEventListener('abort', onAbort, { once: true })
        summaryStarted.resolve()
        try {
          await aborted.promise
          request.signal!.throwIfAborted()
        }
        finally {
          request.signal!.removeEventListener('abort', onAbort)
          summaryFinalized = true
        }
      })()
    }
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const client = startClient(requestFor({ conversationId, roots: fixtures, contextTokenLimit: 64_000 }), new BlobTestClient(fixtures))
    await summaryStarted.promise
    const acceptedDraft = await getPersistedConversationCheckpoint(conversationId, 'draft')
    expect(acceptedDraft).not.toBeNull()
    expect(client.checkpoints).toHaveLength(1)
    client.cancel()
    expect(await client.completion).toEqual({})
    expect(summarySignal?.aborted).toBe(true)
    expect(summaryFinalized).toBe(true)
    expect(providerRequests).toHaveLength(2)
    expect(client.checkpoints).toHaveLength(1)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toEqual(acceptedDraft)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('preserves an opaque client history key in the new summary archive dependencies', async () => {
    const conversationId = randomUUID()
    const roots = simpleHistory()
    const opaqueHistoryKey = Uint8Array.from([0xFF, 0x80, 0x22, 0x01])
    roots[3]!.blobId = opaqueHistoryKey
    providerScript = () => answer(SUMMARY_TEXT)
    const client = startClient(
      requestFor({ conversationId, roots, action: 'summarize', contextTokenLimit: 24_000 }),
      new BlobTestClient(roots),
    )

    expect(await client.completion).toEqual({})
    expect(providerRequests).toHaveLength(1)
    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const graph = client.assertCheckpointResolvable(checkpoint)
    expect(graph.archives).toHaveLength(1)
    expect(graph.archives[0]!.summarizedMessages.map(wireKey)).toContain(wireKey(opaqueHistoryKey))
  })

  it('summarizes client-only history, sends repaired/archive dependencies, waits for ACK and restores cold', async () => {
    const conversationId = randomUUID()
    const { fixtures, legacyToolResult } = compactionHistory()
    const previous = await seedPreviousCheckpoint(conversationId, fixtures)
    providerScript = () => answer(SUMMARY_TEXT)
    const client = new BlobTestClient(fixtures)
    client.autoAcknowledgeSets = false
    startClient(requestFor({ conversationId, roots: fixtures, action: 'summarize', contextTokenLimit: 64_000 }), client)
    await client.waitFor(current => current.pendingSets.size > 0, 'summary artifact Set requests')
    await settleRuntime()

    expect(hasSummaryStarted(client)).toBe(true)
    expect(client.execKinds).toContain('executeHookArgs')
    expect(providerRequests).toHaveLength(1)
    expect(JSON.stringify(providerRequests[0]!.messages)).toContain('Historical analysis 0.')
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    client.autoAcknowledgeSets = true
    client.acknowledgePendingSets()
    expect(await client.completion).toEqual({})

    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    const graph = client.assertCheckpointResolvable(checkpoint)
    expect(graph.archives).toHaveLength(1)
    expect(JSON.stringify(graph.rootMessages)).toContain(SUMMARY_TEXT)
    expect(JSON.stringify(graph.rootMessages)).toContain('[tool output elided during context compaction')
    expect(JSON.stringify(graph.rootMessages)).toContain('Legacy mixed user text must survive repair.')
    const originalKeys = new Set(fixtures.map(fixture => wireKey(fixture.blobId)))
    const newlyArchived = graph.archives[0]!.summarizedMessages.filter(blobId => !originalKeys.has(wireKey(blobId)))
    expect(newlyArchived.length).toBeGreaterThan(0)
    for (const dependency of newlyArchived)
      expect(client.setRequests.some(request => wireKey(request.blobId) === wireKey(dependency))).toBe(true)
    expect(client.getRequests.filter(request => wireKey(request.blobId) === wireKey(legacyToolResult.blobId))).toHaveLength(1)

    providerScript = () => answer('Cold continuation after summary.')
    const coldClient = startClient(requestFor({ conversationId, checkpoint, text: 'Continue after compacting.' }), client.fork())
    expect(await coldClient.completion).toEqual({})
    expect(JSON.stringify(providerRequests.at(-1)!.messages)).toContain(SUMMARY_TEXT)
    expect(JSON.stringify(providerRequests.at(-1)!.messages)).toContain('[tool output elided during context compaction')
    await expectCheckpointMatchesClient(conversationId, coldClient)
  })

  it('runs inline compaction after a real tool round, preserving active steps and gating the next provider round on ACK', async () => {
    const conversationId = randomUUID()
    const { fixtures } = compactionHistory()
    const previous = await seedPreviousCheckpoint(conversationId, fixtures)
    let conversationCalls = 0
    providerScript = (request) => {
      if (!request.conversationId)
        return answer(SUMMARY_TEXT)
      conversationCalls += 1
      if (conversationCalls === 1) {
        return (async function* (): AsyncIterable<LLMStreamEvent> {
          yield { type: 'text_delta', text: 'Active turn before compaction.' }
          yield { type: 'tool_use_start', id: 'current-read', name: 'Read' }
          yield { type: 'tool_use_delta', id: 'current-read', input: '{"path":"/fixture/current.ts"}' }
          yield { type: 'tool_use_done', id: 'current-read' }
          yield { type: 'done', usage: { inputTokens: 63_000, outputTokens: 200 }, stopReason: 'tool_use' }
        })()
      }
      return answer('Active turn after compaction.')
    }
    const client = new BlobTestClient(fixtures)
    client.readResultText = 'Fresh tool result must be consumed after compaction.'
    startClient(requestFor({ conversationId, roots: fixtures, text: 'Current active user request.', contextTokenLimit: 64_000 }), client)
    await client.waitFor(hasSummaryStarted, 'inline summary start')
    // The preceding rolling checkpoint is acknowledged. Hold only new summary
    // artifacts, to test the barrier between compaction and the next LLM round.
    client.autoAcknowledgeSets = false
    await client.waitFor(current => current.pendingSets.size > 0, 'inline artifact Set requests')
    await settleRuntime()
    expect(conversationCalls).toBe(1)
    expect(providerRequests).toHaveLength(2)
    expect(client.execKinds).toContain('readArgs')
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(previous)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).not.toBeNull()
    expect(client.checkpoints).toHaveLength(1)
    const rollingGraph = client.assertCheckpointResolvable(client.checkpoints[0]!)
    expect(rollingGraph.turns.at(-1)?.user.text).toBe('Current active user request.')
    expect(rollingGraph.turns.at(-1)?.steps.map(step => step.message.case)).toEqual(['assistantMessage', 'toolCall'])

    client.autoAcknowledgeSets = true
    client.acknowledgePendingSets()
    expect(await client.completion).toEqual({})
    expect(conversationCalls).toBe(2)
    const inlineCheckpoint = client.checkpoints.find(checkpoint => checkpoint.summaryArchives.length > 0)!
    const inlineGraph = client.assertCheckpointResolvable(inlineCheckpoint)
    expect(inlineGraph.turns.at(-1)?.turn.steps.map(wireKey)).toEqual(rollingGraph.turns.at(-1)?.turn.steps.map(wireKey))
    expect(inlineGraph.turns.at(-1)?.user.text).toBe('Current active user request.')
    const continuedRequest = providerRequests.at(-1)!
    expect(JSON.stringify(continuedRequest.messages)).toContain(SUMMARY_TEXT)
    expect(JSON.stringify(continuedRequest.messages)).toContain('Fresh tool result must be consumed after compaction.')
    expect(JSON.stringify(continuedRequest.messages)).not.toContain('Historical analysis 0.')
    const checkpoint = await expectCheckpointMatchesClient(conversationId, client)
    expect(client.assertCheckpointResolvable(checkpoint).turns.at(-1)?.steps).toHaveLength(3)

    const coldClient = startClient(requestFor({ conversationId, checkpoint, action: 'resume' }), client.fork())
    expect(await coldClient.completion).toEqual({})
    expect(JSON.stringify(providerRequests.at(-1)!.messages)).toContain(SUMMARY_TEXT)
    await expectCheckpointMatchesClient(conversationId, coldClient)
  })
})
