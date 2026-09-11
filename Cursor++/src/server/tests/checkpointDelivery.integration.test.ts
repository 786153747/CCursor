import type { JsonObject } from '@bufbuild/protobuf'
import type { AgentServerMessage } from '../gen/agent_v1_pb'
import type { LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
import { randomUUID } from 'node:crypto'
import { fromBinary, toJson } from '@bufbuild/protobuf'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearDraftCheckpoint, clearPersistedConversationCheckpoint, getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { closeAgentDatabase, getCheckpointDatabase, initDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { AgentServerMessageSchema, ConversationStateStructureSchema } from '../gen/agent_v1_pb'
import { handleRunRequest } from '../handlers/agent/agentOrchestrator'
import { processRunResources } from '../handlers/agent/runResources'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import { BlobTestClient, captureAgentServiceHandlers } from './blobTestClient'

const clients: BlobTestClient[] = []
const requests: LLMStreamRequest[] = []
let providerScript: (request: LLMStreamRequest) => AsyncIterable<LLMStreamEvent>

function createRequest() {
  return {
    runRequest: {
      conversationId: randomUUID(),
      runId: randomUUID(),
      requestedModel: { modelId: 'claude-sonnet-4' },
      action: { userMessageAction: { userMessage: { text: 'Do this work once.', messageId: randomUUID() }, requestContext: {} } },
      conversationState: {},
    },
  }
}

function start(message: Record<string, unknown>, client = new BlobTestClient()): BlobTestClient {
  clients.push(client)
  client.start(handleRunRequest(message, client.session))
  return client
}

async function* answer(): AsyncIterable<LLMStreamEvent> {
  yield { type: 'text_delta', text: 'Saved final answer.' }
  yield { type: 'done', stopReason: 'end_turn', usage: { inputTokens: 200, outputTokens: 10 } }
}

function loseTerminalPublication(client: BlobTestClient) {
  const database = getCheckpointDatabase()
  const executeRun = database.run.bind(database)
  return vi.spyOn(database, 'run').mockImplementation(async (sql, parameters) => {
    const result = await executeRun(sql, parameters)
    if (sql.includes('INSERT INTO conversation_checkpoints') && (parameters as Record<string, unknown>)?.$terminalReceiptJson)
      client.cancel()
    return result
  })
}

async function readRows(conversationId: string) {
  return getCheckpointDatabase().all('SELECT * FROM conversation_checkpoints WHERE conversation_id = ? ORDER BY kind', [conversationId])
}

function updateKind(frame: AgentServerMessage): string | undefined {
  return frame.message.case === 'interactionUpdate' ? frame.message.value.message.case : frame.message.case
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
  vi.restoreAllMocks()
})

describe('accepted terminal checkpoint redelivery', () => {
  it('survives SQL acceptance followed by cancellation and a cold restart, through the registered RPC', async () => {
    const message = createRequest()
    const original = new BlobTestClient()
    const writeFault = loseTerminalPublication(original)
    start(message, original)
    expect(await original.completion).toEqual({})
    expect(original.checkpoints).toHaveLength(0)
    const acceptedRows = await readRows(message.runRequest.conversationId)
    writeFault.mockRestore()
    await closeAgentDatabase()
    await initDatabase()

    const retry = original.fork()
    clients.push(retry)
    const input = retry.startServiceRun(captureAgentServiceHandlers().run, message)
    expect(await retry.completion).toEqual({})
    expect(input.deliveredEof).toBe(false)
    input.close()
    expect(requests).toHaveLength(1)
    expect(retry.execKinds).toHaveLength(0)
    expect(retry.setRequests).toHaveLength(0)
    expect(retry.frames.map(updateKind).filter(kind => kind !== 'kvServerMessage'))
      .toEqual(['stepCompleted', 'turnEnded', 'conversationCheckpointUpdate'])
    const restored = retry.assertCheckpointResolvable(retry.checkpoints[0]!)
    expect(JSON.stringify(restored.rootMessages)).toContain('Saved final answer.')
    expect(await readRows(message.runRequest.conversationId)).toEqual(acceptedRows)
  })

  it('recognizes native ResumeAction from a same-run rolling checkpoint without repeating its tool', async () => {
    providerScript = request => requests.indexOf(request) === 0
      ? (async function* (): AsyncIterable<LLMStreamEvent> {
          yield { type: 'tool_use_start', id: 'read-once', name: 'Read' }
          yield { type: 'tool_use_delta', id: 'read-once', input: '{"path":"/fixture/source.ts"}' }
          yield { type: 'tool_use_done', id: 'read-once' }
          yield { type: 'done', stopReason: 'tool_use', usage: { inputTokens: 200, outputTokens: 10 } }
        })()
      : answer()
    const message = createRequest()
    const original = new BlobTestClient()
    const writeFault = loseTerminalPublication(original)
    start(message, original)
    expect(await original.completion).toEqual({})
    expect(original.execKinds).toEqual(['readArgs'])
    expect(original.checkpoints).toHaveLength(1)
    writeFault.mockRestore()
    const retry = start({ runRequest: {
      ...message.runRequest,
      action: { resumeAction: { requestContext: {} } },
      conversationState: toJson(ConversationStateStructureSchema, original.checkpoints[0]!),
    } }, original.fork())
    expect(await retry.completion).toEqual({})
    expect(requests).toHaveLength(2)
    expect(retry.execKinds).toHaveLength(0)
    expect(retry.checkpoints).toHaveLength(1)
    expect(retry.frames.some(frame => frame.message.case === 'interactionQuery')).toBe(false)
  })

  it.each(['changed-action', 'different-run', 'changed-model'] as const)('does not redeliver for %s', async (change) => {
    const message = createRequest()
    const original = start(message)
    expect(await original.completion).toEqual({})
    const acceptedRows = await readRows(message.runRequest.conversationId)
    const changed = structuredClone(message)
    if (change === 'changed-action')
      changed.runRequest.action.userMessageAction.userMessage.text = 'Different work with the same message id.'
    if (change === 'different-run')
      changed.runRequest.runId = randomUUID()
    if (change === 'changed-model')
      changed.runRequest.requestedModel.modelId = 'glm-5'
    const retry = start(changed, original.fork())
    expect(await retry.completion).toEqual({})
    expect(retry.frames.filter(frame => frame.message.case === 'interactionQuery')).toHaveLength(1)
    expect(retry.checkpoints).toHaveLength(0)
    expect(requests).toHaveLength(1)
    expect(await readRows(message.runRequest.conversationId)).toEqual(acceptedRows)
  })

  it('refuses redelivery if a previously saved turn child has disappeared', async () => {
    const message = createRequest()
    const original = start(message)
    expect(await original.completion).toEqual({})
    const acceptedRows = await readRows(message.runRequest.conversationId)
    const restored = original.assertCheckpointResolvable(original.checkpoints[0]!)
    const retry = original.fork()
    retry.dropLocalBlobs([restored.turns[0]!.turn.steps[0]!])
    start(message, retry)
    expect((await retry.completion).error).toBeInstanceOf(Error)
    expect(retry.checkpoints).toHaveLength(0)
    expect(requests).toHaveLength(1)
    expect(await readRows(message.runRequest.conversationId)).toEqual(acceptedRows)
  })

  it('fences redelivery if the accepted checkpoint changes while fetching its graph', async () => {
    const message = createRequest()
    const original = start(message)
    expect(await original.completion).toEqual({})
    const accepted = (await getPersistedConversationCheckpoint(message.runRequest.conversationId))!
    const retry = original.fork()
    const send = retry.send.bind(retry)
    let heldResponse: JsonObject | undefined
    const sendSpy = vi.spyOn(retry, 'send').mockImplementation((response) => {
      if (!heldResponse && response.kvClientMessage) {
        heldResponse = response
        return
      }
      send(response)
    })
    start(message, retry)
    await retry.waitFor(client => client.getRequests.length > 0, 'replay graph fetch')
    await persistConversationCheckpoint({ ...accepted, updatedAt: Date.now() })
    sendSpy.mockRestore()
    send(heldResponse!)
    expect(String((await retry.completion).error)).toMatch(/Checkpoint version conflict/)
    expect(retry.checkpoints).toHaveLength(0)
    expect(requests).toHaveLength(1)
  })

  it.each(['deleted', 'retired-later-draft'] as const)('invalidates terminal receipts after %s', async (change) => {
    const message = createRequest()
    const original = start(message)
    expect(await original.completion).toEqual({})
    const accepted = (await getPersistedConversationCheckpoint(message.runRequest.conversationId))!
    if (change === 'deleted') {
      await clearPersistedConversationCheckpoint(message.runRequest.conversationId)
    }
    else {
      await persistConversationCheckpoint({ ...accepted, kind: 'draft' })
      await clearDraftCheckpoint(message.runRequest.conversationId)
    }
    const beforeRetry = await readRows(message.runRequest.conversationId)
    const retry = start(message, original.fork())
    await retry.completion
    expect(retry.checkpoints).toHaveLength(0)
    expect(requests).toHaveLength(1)
    expect(await readRows(message.runRequest.conversationId)).toEqual(beforeRetry)
  })

  it('does not create a terminal receipt or checkpoint when client storage rejects Set', async () => {
    const message = createRequest()
    const client = new BlobTestClient()
    vi.spyOn(client, 'acknowledgeSet').mockImplementation(requestId => client.failSet(requestId))
    start(message, client)
    expect((await client.completion).error).toBeInstanceOf(Error)
    expect(client.checkpoints).toHaveLength(0)
    expect(await readRows(message.runRequest.conversationId)).toEqual([])
  })

  it('stores the exact terminal checkpoint payload rather than reconstructing it from mirror references', async () => {
    const message = createRequest()
    const client = start(message)
    expect(await client.completion).toEqual({})
    const row = await getCheckpointDatabase().get<{ terminal_receipt_json: string }>(
      'SELECT terminal_receipt_json FROM conversation_checkpoints WHERE conversation_id = ? AND kind = \'committed\'',
      [message.runRequest.conversationId],
    )
    const receipt = JSON.parse(row!.terminal_receipt_json) as { frames: string[] }
    const storedFrame = fromBinary(AgentServerMessageSchema, Buffer.from(receipt.frames[2]!, 'base64'))
    const actualFrame = client.frames.find(frame => frame.message.case === 'conversationCheckpointUpdate')!
    expect(toJson(AgentServerMessageSchema, storedFrame)).toEqual(toJson(AgentServerMessageSchema, actualFrame))
  })
})
