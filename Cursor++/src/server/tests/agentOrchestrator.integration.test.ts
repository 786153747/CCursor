import type { AgentSession } from '../handlers/agent/session'
import type { LLMMessage } from '../handlers/llm/types'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { resetAgentDatabaseForTests } from '../database/sqlite'
import { encodeBlob } from '../handlers/agent/blob'
import { rebuildConversationHistory } from '../handlers/agent/historyManager'
import { BlobRunContext } from '../handlers/agent/runContext'
import { createEphemeralSession } from '../handlers/agent/session'
import { assertValidAnthropicToolUseContract } from '../handlers/llm/anthropicContract'
import { encodeAnthropicRequestMessages } from '../handlers/llm/conversationCodec'
import { transformMessages } from '../handlers/llm/transformMessages'

let capturedParsed: Array<Record<string, unknown>> = []
const capturedRuntimeRuns: BlobRunContext[] = []
const activeRuns: BlobRunContext[] = []

vi.mock('../handlers/agent/conversationRuntime', () => ({
  async* handleConversationRun(parsed: Record<string, unknown>, session: AgentSession | null, run: BlobRunContext) {
    expect(run).toBeInstanceOf(BlobRunContext)
    expect(run.session).toBe(session)
    expect(run.signal.aborted).toBe(false)
    capturedParsed.push(parsed)
    capturedRuntimeRuns.push(run)
  },
}))

vi.mock('../handlers/agent/summarizeRuntime', () => ({
  async* handleSummarizeAction() {},
}))

async function loadHandleRunRequest() {
  return (await import('../handlers/agent/agentOrchestrator')).handleRunRequest
}

async function withTempAgentDatabase(run: () => Promise<void>): Promise<void> {
  const prevDbPath = process.env.BYOK_AGENT_DB_PATH
  const tempDir = mkdtempSync(join(tmpdir(), 'cursor-byok-agent-db-'))
  process.env.BYOK_AGENT_DB_PATH = join(tempDir, 'cursor.db')
  capturedParsed = []
  await resetAgentDatabaseForTests()

  try {
    await run()
  }
  finally {
    capturedParsed = []
    await resetAgentDatabaseForTests()
    if (prevDbPath === undefined)
      delete process.env.BYOK_AGENT_DB_PATH
    else process.env.BYOK_AGENT_DB_PATH = prevDbPath
    rmSync(tempDir, { recursive: true, force: true })
  }
}

async function exhaust<T>(iterable: AsyncIterable<T>): Promise<void> {
  for await (const _ of iterable) {
    // no-op
  }
}

function buildLegacyAnthropicHistoryBlobs() {
  const system = encodeBlob({ role: 'system', content: 'sys prompt' })
  const preamble = encodeBlob({ role: 'user', content: '<user_info>env</user_info>' })
  const assistant = encodeBlob({
    role: 'assistant',
    content: [
      { type: 'text', text: '我先查一下。' },
      { type: 'tool_use', id: 'call_A', name: 'Read', input: { path: 'a.ts' } },
      { type: 'tool_use', id: 'call_B', name: 'Grep', input: { path: '.', pattern: 'x' } },
    ],
  })
  const legacyUserToolResults = encodeBlob({
    role: 'user',
    content: [
      { type: 'tool_result', toolUseId: 'call_A', toolName: 'Read', content: 'read result' },
      { type: 'tool_result', toolUseId: 'call_B', toolName: 'Grep', content: 'grep result' },
      { type: 'text', text: '继续分析这些结果' },
    ],
  })
  return { system, preamble, assistant, legacyUserToolResults }
}

describe('agent orchestrator / history rebuild integration', () => {
  afterEach(() => {
    capturedParsed = []
    capturedRuntimeRuns.length = 0
    for (const run of activeRuns)
      run.dispose()
    activeRuns.length = 0
  })

  it('rejects an ambiguous empty baseline without restoring or replacing checkpoint history', async () => {
    await withTempAgentDatabase(async () => {
      const { system, preamble, assistant, legacyUserToolResults } = buildLegacyAnthropicHistoryBlobs()
      await persistConversationCheckpoint({
        kind: 'committed',
        conversationId: 'conv-switch',
        rootBlobIds: [system.blobId, preamble.blobId, assistant.blobId, legacyUserToolResults.blobId],
        turnBlobIds: [],
        summaryArchiveIds: ['archive-1'],
        tokenDetails: { usedTokens: 1234, maxTokens: 200000 },
        mode: 'AGENT_MODE_AGENT',
        updatedAt: Date.now(),
      })

      const handleRunRequest = await loadHandleRunRequest()
      const session = createEphemeralSession('empty-client-history')
      const previous = await getPersistedConversationCheckpoint('conv-switch')
      await expect(exhaust(handleRunRequest({
        runRequest: {
          conversationId: 'conv-switch',
          action: {
            userMessageAction: {
              userMessage: { text: '切换模型后继续', mode: 'AGENT_MODE_AGENT' },
              requestContext: {},
            },
          },
          modelDetails: { modelId: 'gpt-5.4-medium' },
          conversationState: {},
        },
      }, session))).rejects.toThrow(/Checkpoint version conflict/)

      expect(capturedParsed).toHaveLength(0)
      expect(capturedRuntimeRuns).toHaveLength(0)
      expect(await getPersistedConversationCheckpoint('conv-switch')).toEqual(previous)
      expect(session.listeners.size).toBe(0)
    })
  })

  it('rebuildConversationHistory replaces restored provider-specific scaffold with current provider scaffold', async () => {
    await withTempAgentDatabase(async () => {
      const oldSystem = encodeBlob({ role: 'system', content: 'OpenAI system prompt mentions ApplyPatch and ReadFile' })
      const oldPreamble = encodeBlob({ role: 'user', content: '<user_info>old provider preamble with ReadFile</user_info>' })
      const historyUser = encodeBlob({ role: 'user', content: 'history user' })
      const run = new BlobRunContext(createEphemeralSession('provider-scaffold'))
      activeRuns.push(run)
      for (const blob of [oldSystem, oldPreamble, historyUser])
        run.blobs.cacheBlob(blob.blobId, blob.blobData)

      const iterator = rebuildConversationHistory({
        historyBlobIds: [oldSystem.blobId, oldPreamble.blobId, historyUser.blobId],
        prependUserMessages: [],
        systemMessage: { role: 'system', content: 'Anthropic system prompt uses Read and must not mention ApplyPatch' },
        preambleUserMessage: { role: 'user', content: '<user_info>new provider preamble with Read</user_info>' },
        currentUserMessage: { role: 'user', content: '继续' },
        systemContent: 'Anthropic system prompt uses Read and must not mention ApplyPatch',
        preambleUserContent: '<user_info>new provider preamble with Read</user_info>',
        sendSystemScaffoldBlob: () => {},
        sendOrderedBlob: () => {},
        run,
      })

      let result: { messages: LLMMessage[], insertedPrependUserTexts: string[] } | undefined
      for (;;) {
        const next = await iterator.next()
        if (next.done) {
          result = next.value
          break
        }
      }

      expect(result?.messages[0]).toEqual({ role: 'system', content: 'Anthropic system prompt uses Read and must not mention ApplyPatch' })
      expect(result?.messages[1]).toEqual({ role: 'user', content: '<user_info>new provider preamble with Read</user_info>' })
      expect(result?.messages[2]).toEqual({ role: 'user', content: 'history user' })
      expect(result?.messages[3]).toEqual({ role: 'user', content: '继续' })
    })
  })

  it('rebuilt legacy anthropic history is repaired to canonical form and can continue across anthropic/openai/gemini', async () => {
    await withTempAgentDatabase(async () => {
      const { system, preamble, assistant, legacyUserToolResults } = buildLegacyAnthropicHistoryBlobs()
      const run = new BlobRunContext(createEphemeralSession('legacy-history'))
      activeRuns.push(run)
      for (const blob of [system, preamble, assistant, legacyUserToolResults])
        run.blobs.cacheBlob(blob.blobId, blob.blobData)

      const iterator = rebuildConversationHistory({
        historyBlobIds: [system.blobId, preamble.blobId, assistant.blobId, legacyUserToolResults.blobId],
        prependUserMessages: [],
        systemMessage: { role: 'system', content: 'sys prompt' },
        preambleUserMessage: { role: 'user', content: '<user_info>env</user_info>' },
        currentUserMessage: { role: 'user', content: '继续' },
        systemContent: 'sys prompt',
        preambleUserContent: '<user_info>env</user_info>',
        sendSystemScaffoldBlob: () => {},
        sendOrderedBlob: () => {},
        run,
      })

      let result: { messages: LLMMessage[], insertedPrependUserTexts: string[] } | undefined
      for (;;) {
        const next = await iterator.next()
        if (next.done) {
          result = next.value
          break
        }
      }

      expect(result).toBeDefined()
      expect(result?.insertedPrependUserTexts).toEqual([])
      expect(result?.messages).toEqual([
        { role: 'system', content: 'sys prompt' },
        { role: 'user', content: '<user_info>env</user_info>' },
        {
          role: 'assistant',
          content: [
            { type: 'text', text: '我先查一下。' },
            { type: 'tool_use', id: 'call_A', name: 'Read', input: { path: 'a.ts' } },
            { type: 'tool_use', id: 'call_B', name: 'Grep', input: { path: '.', pattern: 'x' } },
          ],
        },
        { role: 'tool', toolCallId: 'call_A', toolName: 'Read', content: 'read result' },
        { role: 'tool', toolCallId: 'call_B', toolName: 'Grep', content: 'grep result' },
        { role: 'user', content: [{ type: 'text', text: '继续分析这些结果' }] },
        { role: 'user', content: '继续' },
      ])

      const anthropicCompiled = transformMessages(result!.messages, 'anthropic')
      const anthropicEncoded = encodeAnthropicRequestMessages(anthropicCompiled)
      expect(() => assertValidAnthropicToolUseContract(anthropicEncoded.messages)).not.toThrow()

      const openAICompiled = transformMessages(result!.messages, 'openai-chat')
      expect(openAICompiled.filter(message => message.role === 'tool').map(message => message.toolCallId)).toEqual(['call_A', 'call_B'])

      const geminiCompiled = transformMessages(result!.messages, 'gemini')
      expect(geminiCompiled.filter(message => message.role === 'tool').map(message => message.toolCallId)).toEqual(['call_A', 'call_B'])
    })
  })
})
