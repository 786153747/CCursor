import type { JsonObject } from '@bufbuild/protobuf'
import type { LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
import type { ClientBlobFixture } from './blobTestClient'
import { randomUUID } from 'node:crypto'
import { create, toBinary, toJson } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearPersistedConversationCheckpoint, getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { getCheckpointDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { ConversationStateStructureSchema, ConversationStepSchema, ConversationTurnStructureSchema, ShellCommandSchema, ShellOutputSchema, UserMessageSchema } from '../gen/agent_v1_pb'
import { handleRunRequest } from '../handlers/agent/agentOrchestrator'
import { blobIdFromBytes, encodeBinaryBlob, encodeBlob } from '../handlers/agent/blob'
import { processRunResources } from '../handlers/agent/runResources'
import { uploadHandoff } from '../handlers/agent/uploadHandoff'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import { BlobTestClient, captureAgentServiceHandlers, wireKey } from './blobTestClient'

const clients: BlobTestClient[] = []
const requests: LLMStreamRequest[] = []
let providerScript: (request: LLMStreamRequest) => AsyncIterable<LLMStreamEvent>

function historyFixture(content = 'Preserve this required history.'): ClientBlobFixture {
  const encoded = encodeBlob({ role: 'user', content })
  return { blobId: Buffer.from(encoded.blobId), bytes: Buffer.from(encoded.blobData) }
}

function binaryFixture(bytes: Uint8Array): ClientBlobFixture {
  const encoded = encodeBinaryBlob(bytes)
  return { blobId: Buffer.from(encoded.blobId), bytes }
}

function interruptedTurn() {
  const user = binaryFixture(toBinary(UserMessageSchema, create(UserMessageSchema, { text: 'Original request', messageId: 'original-user' })))
  const step = binaryFixture(toBinary(ConversationStepSchema, create(ConversationStepSchema, {
    message: { case: 'assistantMessage', value: { text: 'Original partial answer' } },
  })))
  const turn = binaryFixture(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
    turn: { case: 'agentConversationTurn', value: { userMessage: user.blobId, steps: [step.blobId] } },
  })))
  return { user, step, turn }
}

function runMessage(options: {
  conversationId?: string
  roots?: ClientBlobFixture[]
  turns?: ClientBlobFixture[]
  action?: 'conversation' | 'resume' | 'summarize'
  parts?: JsonObject
  inline?: JsonObject
} = {}): Record<string, unknown> {
  const actionName = options.action ?? 'conversation'
  const requestContext = options.parts ? options.inline : options.inline ?? {}
  const action = actionName === 'resume'
    ? { resumeAction: { requestContext } }
    : actionName === 'summarize'
      ? { summarizeAction: {} }
      : { userMessageAction: { userMessage: { text: 'Continue safely.', messageId: randomUUID() }, requestContext } }
  return {
    runRequest: {
      conversationId: options.conversationId ?? randomUUID(),
      runId: randomUUID(),
      action: { ...action, ...(options.parts ? { requestContextParts: options.parts } : {}) },
      requestedModel: { modelId: 'claude-sonnet-4', parameters: [{ id: 'context', value: actionName === 'summarize' ? '8000' : '200000' }] },
      conversationState: {
        rootPromptMessagesJson: (options.roots ?? []).map(fixture => wireKey(fixture.blobId)),
        turns: (options.turns ?? []).map(fixture => wireKey(fixture.blobId)),
      },
    },
  }
}

function start(message: Record<string, unknown>, client = new BlobTestClient()): BlobTestClient {
  clients.push(client)
  client.start(handleRunRequest(message, client.session))
  return client
}

async function seedCheckpoint(conversationId: string, root: ClientBlobFixture, kind: 'committed' | 'draft' = 'committed') {
  await persistConversationCheckpoint({
    conversationId,
    kind,
    rootBlobIds: [blobIdFromBytes(root.blobId)],
    turnBlobIds: [],
    summaryArchiveIds: [],
    tokenDetails: { usedTokens: 100, maxTokens: 200_000 },
    mode: 'AGENT_MODE_AGENT',
    updatedAt: Date.now(),
  })
}

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

async function* answer(): AsyncIterable<LLMStreamEvent> {
  yield { type: 'text_delta', text: 'A complete new answer.' }
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
  for (const client of clients)
    client.cancel()
  await Promise.all(clients.splice(0).map(client => client.completion))
  expect(processRunResources.getStats()).toEqual({ activeRuns: 0, retainedBytes: 0 })
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('actual required versus auxiliary history paths', () => {
  it('deduplicates upload copies before retaining repeated input references', async () => {
    const conversationId = randomUUID()
    const root = historyFixture()
    const client = new BlobTestClient([root])
    await client.uploadChunk(captureAgentServiceHandlers().uploadConversationBlobs, {
      conversationId,
      blobIds: [root.blobId],
      chunkIndex: 0,
      totalChunks: 1,
    })
    client.dropLocalBlobs([root.blobId])
    const readUpload = vi.spyOn(uploadHandoff, 'read')
    const repeatedRoots = Array.from({ length: 1000 }).fill(root) as ClientBlobFixture[]
    start(runMessage({ conversationId, roots: repeatedRoots }), client)
    expect(await client.completion).toEqual({})
    expect(readUpload).toHaveBeenCalledTimes(1)
    expect(client.getRequests).toHaveLength(0)
    expect(client.setRequests.filter(request => wireKey(request.blobId) === wireKey(root.blobId))).toHaveLength(1)
    expect(client.checkpoints.at(-1)?.rootPromptMessagesJson.slice(0, 1000).map(wireKey)).toEqual(repeatedRoots.map(fixture => wireKey(fixture.blobId)))
  })

  it('continues after a legal shell turn without claiming its unused children are available', async () => {
    const root = historyFixture()
    const command = binaryFixture(toBinary(ShellCommandSchema, create(ShellCommandSchema, { command: 'pwd' })))
    const output = binaryFixture(toBinary(ShellOutputSchema, create(ShellOutputSchema, { stdout: '/workspace' })))
    const shell = binaryFixture(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
      turn: { case: 'shellConversationTurn', value: { shellCommand: command.blobId, shellOutput: output.blobId } },
    })))
    const client = start(runMessage({ roots: [root], turns: [shell] }), new BlobTestClient([root, shell, command, output]))
    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(client.checkpoints.at(-1)?.turns.map(wireKey)[0]).toBe(wireKey(shell.blobId))
    expect(new Set(client.getRequests.map(request => wireKey(request.blobId)))).toEqual(new Set([root, shell].map(fixture => wireKey(fixture.blobId))))
  })

  it.each(['missing-user', 'missing-step', 'corrupt-step'] as const)('rejects parent-only resume: %s before any provider call or Set', async (failure) => {
    const root = historyFixture()
    const old = interruptedTurn()
    const fixtures = [root, old.turn]
    if (failure !== 'missing-user')
      fixtures.push(old.user)
    if (failure === 'missing-user')
      fixtures.push(old.step)
    if (failure === 'corrupt-step')
      fixtures.push({ ...old.step, bytes: Uint8Array.of(0xFF) })
    const client = start(runMessage({ roots: [root], turns: [old.turn], action: 'resume' }), new BlobTestClient(fixtures))
    expect((await client.completion).error).toBeInstanceOf(Error)
    expect(requests).toHaveLength(0)
    expect(client.checkpoints).toHaveLength(0)
    expect(client.setRequests).toHaveLength(0)
    expect(client.getRequests.some(request => wireKey(request.blobId) === wireKey(old.step.blobId))).toBe(true)
  })

  it('does not let a required child timeout become inherited success', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const root = historyFixture()
    const old = interruptedTurn()
    const client = new BlobTestClient([root, old.turn, old.user, old.step])
    const childRequested = deferred()
    const send = client.send.bind(client)
    vi.spyOn(client, 'send').mockImplementation((message) => {
      const envelope = message.kvClientMessage as JsonObject | undefined
      const request = client.getRequests.find(request => request.requestId === envelope?.id)
      if (request && wireKey(request.blobId) === wireKey(old.step.blobId)) {
        childRequested.resolve()
        return
      }
      send(message)
    })
    start(runMessage({ roots: [root], turns: [old.turn], action: 'resume' }), client)
    await childRequested.promise
    await vi.advanceTimersByTimeAsync(10_001)
    expect(String(((await client.completion).error as Error).cause)).toMatch(/timeout/)
    expect(requests).toHaveLength(0)
    expect(client.checkpoints).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['conversation', 'resume', 'summarize'] as const)('validates missing upload children before %s generation', async (action) => {
    const conversationId = randomUUID()
    const root = historyFixture()
    const old = interruptedTurn()
    const client = new BlobTestClient([root, old.turn])
    await client.uploadChunk(captureAgentServiceHandlers().uploadConversationBlobs, {
      conversationId,
      blobIds: [old.turn.blobId],
      chunkIndex: 0,
      totalChunks: 2,
    })
    client.dropLocalBlobs([old.turn.blobId])
    start(runMessage({ conversationId, roots: [root], turns: [old.turn], action }), client)
    expect((await client.completion).error).toBeInstanceOf(Error)
    expect(requests).toHaveLength(0)
    expect(client.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toBeNull()
  })
})

describe('required context and explicit catalog degradation', () => {
  it.each(['rules', 'mcps'] as const)('fails closed for unavailable ref-only %s', async (part) => {
    const client = start(runMessage({ parts: { [`${part}BlobId`]: wireKey(Buffer.from(`missing-${part}`)), dynamicContext: {} } }))
    expect(String(((await client.completion).error as Error).cause)).toMatch(/Required .* context/)
    expect(requests).toHaveLength(0)
    expect(client.setRequests).toHaveLength(0)
  })

  it('does not decode corrupt required rules as an empty rule set', async () => {
    const fixture = { blobId: Buffer.from('bad-rules'), bytes: Uint8Array.of(0xFF) }
    const client = start(runMessage({ parts: { rulesBlobId: wireKey(fixture.blobId) } }), new BlobTestClient([fixture]))
    expect(String(((await client.completion).error as Error).cause)).toMatch(/decode-error/)
    expect(requests).toHaveLength(0)
  })

  it('uses a complete dual inline rule copy exactly once without fetching its reference', async () => {
    const ruleText = 'Never delete the recovery database.'
    const client = start(runMessage({
      parts: { rulesBlobId: wireKey(Buffer.from('unavailable-reference')) },
      inline: { rules: [{ content: ruleText, fullPath: '/workspace/safety.mdc', type: { global: {} } }] },
    }))
    expect(await client.completion).toEqual({})
    expect(client.getRequests).toHaveLength(0)
    expect(JSON.stringify(requests[0]?.messages).split(ruleText)).toHaveLength(2)
  })

  it.each(['skills', 'subagents'] as const)('allows unavailable unselected %s discovery catalog', async (part) => {
    const client = start(runMessage({ parts: { [`${part}BlobId`]: wireKey(Buffer.from(`optional-${part}`)) } }))
    expect(await client.completion).toEqual({})
    expect(requests).toHaveLength(1)
    expect(client.checkpoints.length).toBeGreaterThan(0)
  })
})

describe('run-scoped cancellation, versioning and admission', () => {
  it('preserves divergent legacy candidates when recovery is declined and still permits a client-state fork', async () => {
    const conversationId = randomUUID()
    const committed = historyFixture('Committed candidate')
    const draft = historyFixture('Draft candidate')
    await seedCheckpoint(conversationId, committed)
    await seedCheckpoint(conversationId, draft, 'draft')
    const database = getCheckpointDatabase()
    await database.run('UPDATE conversation_checkpoints SET write_token = kind WHERE conversation_id = ?', [conversationId])
    const readRows = () => database.all('SELECT * FROM conversation_checkpoints WHERE conversation_id = ? ORDER BY kind', [conversationId])
    const previousRows = await readRows()
    for (const chosen of [committed, draft]) {
      const client = start(runMessage({ conversationId, roots: [chosen] }), new BlobTestClient([chosen]))
      expect(await client.completion).toEqual({})
      expect(client.frames.filter(frame => frame.message.case === 'interactionQuery')).toHaveLength(1)
      expect(client.checkpoints).toHaveLength(0)
    }
    expect(requests).toHaveLength(0)
    expect(await readRows()).toEqual(previousRows)
    const forked = start(runMessage({ roots: [committed] }), new BlobTestClient([committed]))
    expect(await forked.completion).toEqual({})
    expect(await readRows()).toEqual(previousRows)
    expect(requests).toHaveLength(1)
  })

  it('does not turn malformed checkpoint metadata into an empty admitted baseline', async () => {
    const conversationId = randomUUID()
    await seedCheckpoint(conversationId, historyFixture())
    const database = getCheckpointDatabase()
    await database.run('UPDATE conversation_checkpoints SET root_blob_ids_json = \'[null]\' WHERE conversation_id = ? AND kind = \'committed\'', [conversationId])
    const client = start(runMessage({ conversationId }))
    expect(String((await client.completion).error)).toMatch(/Checkpoint version conflict/)
    expect(requests).toHaveLength(0)
    expect(await database.get('SELECT root_blob_ids_json FROM conversation_checkpoints WHERE conversation_id = ? AND kind = \'committed\'', [conversationId]))
      .toEqual({ root_blob_ids_json: '[null]' })
  })

  it('does not let a newly arriving delayed run revive an explicitly deleted checkpoint', async () => {
    const conversationId = randomUUID()
    const root = historyFixture()
    await seedCheckpoint(conversationId, root)
    await clearPersistedConversationCheckpoint(conversationId)
    const client = start(runMessage({ conversationId, roots: [root] }), new BlobTestClient([root]))
    expect(String((await client.completion).error)).toMatch(/Checkpoint version conflict/)
    expect(requests).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toBeNull()
    expect(client.checkpoints).toHaveLength(0)
  })

  it('keeps rolling checkpoint conflicts non-retryable through the real tool-processing catch', async () => {
    providerScript = request => requests.indexOf(request) === 0
      ? (async function* (): AsyncIterable<LLMStreamEvent> {
          yield { type: 'tool_use_start', id: 'rolling-read', name: 'Read' }
          yield { type: 'tool_use_delta', id: 'rolling-read', input: '{"path":"/fixture/current.ts"}' }
          yield { type: 'tool_use_done', id: 'rolling-read' }
          yield { type: 'done', stopReason: 'tool_use', usage: { inputTokens: 200, outputTokens: 10 } }
        })()
      : answer()
    const conversationId = randomUUID()
    const older = new BlobTestClient()
    older.autoAcknowledgeSets = false
    start(runMessage({ conversationId }), older)
    await older.waitFor(client => client.pendingSets.size > 0, 'rolling checkpoint Set barrier')
    expect(older.execKinds).toContain('readArgs')
    const newer = start(runMessage({ conversationId }))
    expect(await newer.completion).toEqual({})
    const accepted = await getPersistedConversationCheckpoint(conversationId)
    older.autoAcknowledgeSets = true
    older.acknowledgePendingSets()
    const { error } = await older.completion
    expect(error).toBeInstanceOf(ConnectError)
    expect((error as ConnectError).code).toBe(Code.FailedPrecondition)
    expect(String(error)).toMatch(/Checkpoint version conflict/)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(accepted)
    expect(requests).toHaveLength(2)
  })

  it('rejects a late successful overlapping writer without cancelling its sibling or child conversation', async () => {
    const conversationId = randomUUID()
    const older = new BlobTestClient()
    older.autoAcknowledgeSets = false
    start(runMessage({ conversationId }), older)
    await older.waitFor(client => client.pendingSets.size > 0, 'older run Set barrier')
    const newer = start(runMessage({ conversationId }))
    expect(await newer.completion).toEqual({})
    const accepted = await getPersistedConversationCheckpoint(conversationId)
    const childMessage = runMessage()
    Object.assign(childMessage.runRequest as Record<string, unknown>, { conversationGroupId: conversationId, subagentTypeName: 'explore' })
    const child = start(childMessage)
    expect(await child.completion).toEqual({})
    older.autoAcknowledgeSets = true
    older.acknowledgePendingSets()
    expect(String((await older.completion).error)).toMatch(/Checkpoint version conflict/)
    expect(older.session.cancelledReason).toBeUndefined()
    expect(older.checkpoints).toHaveLength(0)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(accepted)
    expect(requests).toHaveLength(3)
    expect(child.checkpoints.length).toBeGreaterThan(0)
  })

  it.each(['conversation', 'summarize'] as const)('aborts pending %s generation without retry, fallback or checkpoint', async (action) => {
    const started = deferred()
    let finalized = false
    let observedSignal: AbortSignal | undefined
    providerScript = async function* (request) {
      observedSignal = request.signal
      const aborted = deferred()
      const onAbort = () => aborted.resolve()
      request.signal!.addEventListener('abort', onAbort, { once: true })
      started.resolve()
      try {
        await aborted.promise
        request.signal!.throwIfAborted()
      }
      finally {
        request.signal!.removeEventListener('abort', onAbort)
        finalized = true
      }
    }
    const roots = Array.from({ length: 12 }, (_value, index) => {
      const encoded = encodeBlob({ role: index % 2 ? 'assistant' : 'user', content: `Required history ${index}. ${'Context detail. '.repeat(100)}` })
      return { blobId: Buffer.from(encoded.blobId), bytes: Buffer.from(encoded.blobData) }
    })
    const client = start(runMessage({ action, roots }), new BlobTestClient(roots))
    await started.promise
    client.cancel()
    expect(await client.completion).toEqual({})
    expect(observedSignal?.aborted).toBe(true)
    expect(finalized).toBe(true)
    expect(requests).toHaveLength(1)
    expect(client.checkpoints).toHaveLength(0)
    expect(client.session.listeners.size).toBe(0)
  })

  it('rejects a fifth active run and admits a replacement after cancellation', async () => {
    const started = Array.from({ length: 5 }, deferred)
    providerScript = async function* (request) {
      const aborted = deferred()
      const onAbort = () => aborted.resolve()
      request.signal!.addEventListener('abort', onAbort, { once: true })
      started[requests.indexOf(request)]!.resolve()
      try {
        await aborted.promise
      }
      finally {
        request.signal!.removeEventListener('abort', onAbort)
      }
    }
    const active = Array.from({ length: 4 }, () => start(runMessage()))
    await Promise.all(started.slice(0, 4).map(gate => gate.promise))
    const rejected = start(runMessage())
    expect(String((await rejected.completion).error)).toMatch(/Active run capacity/)
    expect(requests).toHaveLength(4)
    active[0]!.cancel()
    await active[0]!.completion
    const replacement = start(runMessage())
    await started[4]!.promise
    expect(processRunResources.getStats().activeRuns).toBe(4)
    replacement.cancel()
  })

  it('redelivers a correlated completed retry and accepts a new action from the current checkpoint', async () => {
    const conversationId = randomUUID()
    const message = runMessage({ conversationId })
    const first = start(message)
    expect(await first.completion).toEqual({})
    const accepted = await getPersistedConversationCheckpoint(conversationId)
    const stale = start(message, first.fork())
    expect(await stale.completion).toEqual({})
    expect(stale.checkpoints.map(state => toJson(ConversationStateStructureSchema, state)))
      .toEqual(first.checkpoints.map(state => toJson(ConversationStateStructureSchema, state)))
    expect(stale.execKinds).toHaveLength(0)
    expect(requests).toHaveLength(1)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(accepted)
    const current = runMessage({ conversationId })
    ;(current.runRequest as Record<string, unknown>).conversationState = toJson(ConversationStateStructureSchema, first.checkpoints.at(-1)!)
    const continuation = start(current, first.fork())
    expect(await continuation.completion).toEqual({})
    expect(requests).toHaveLength(2)
  })
})
