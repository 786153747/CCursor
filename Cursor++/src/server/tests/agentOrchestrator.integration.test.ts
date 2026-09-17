import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { resetAgentDatabaseForTests } from '../database/sqlite'
import { encodeBlob } from '../handlers/agent/blob'
import { rebuildConversationHistory } from '../handlers/agent/historyManager'
import { BlobRunContext } from '../handlers/agent/runContext'
import { assertValidAnthropicToolUseContract } from '../handlers/llm/anthropicContract'
import { encodeAnthropicRequestMessages } from '../handlers/llm/conversationCodec'
import { transformMessages } from '../handlers/llm/transformMessages'

let capturedParsed: Array<Record<string, unknown>> = []

vi.mock('../handlers/agent/conversationRuntime', () => ({
  async* handleConversationRun(parsed: Record<string, unknown>) {
    capturedParsed.push(parsed)
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
    try {
      rmSync(tempDir, { recursive: true, force: true })
    }
    catch {
      // Windows 上 sqlite 句柄可能延迟释放, 临时目录清理是尽力而为
    }
  }
}

async function exhaust<T>(iterable: AsyncIterable<T>): Promise<void> {
  for await (const _ of iterable) {
    // no-op
  }
}

/** 跑完生成器并取回它的 return 值 (rebuildConversationHistory 的结果在 return 侧) */
async function drain<T>(generator: AsyncGenerator<unknown, T, void>): Promise<T> {
  let next = await generator.next()
  while (!next.done)
    next = await generator.next()
  return next.value
}

/** 本轮工作集: blob 只活在 run 内存里, 不再落 sqlite (PR #23 起服务端不持有副本) */
function createTestRun(conversationId: string, blobs: Array<{ blobId: string, blobData: string }>): BlobRunContext {
  const run = new BlobRunContext(null, { conversationId })
  for (const blob of blobs)
    run.blobs.cacheBlob(blob.blobId, blob.blobData)
  return run
}

function noopFrames() {
  return function* () {
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
  })

  it('rejects a session-less run whose empty client state conflicts with a committed checkpoint', async () => {
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
      // 客户端提交空 history 但会话已有 committed checkpoint: 这会静默丢掉上下文,
      // 所以运行必须被拒 (有 session 时先弹交互确认, 这里没有 session → 直接冲突错误),
      // 而不是带着空历史继续。checkpoint 本身保留, 供恢复使用。
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
      }))).rejects.toThrow(/Checkpoint version conflict/)

      expect(capturedParsed).toHaveLength(0)
      expect(await getPersistedConversationCheckpoint('conv-switch')).toMatchObject({
        kind: 'committed',
        summaryArchiveIds: ['archive-1'],
      })
    })
  })

  it('rebuildConversationHistory replaces restored provider-specific scaffold with current provider scaffold', async () => {
    await withTempAgentDatabase(async () => {
      const oldSystem = encodeBlob({ role: 'system', content: 'OpenAI system prompt mentions ApplyPatch and ReadFile' })
      const oldPreamble = encodeBlob({ role: 'user', content: '<user_info>old provider preamble with ReadFile</user_info>' })
      const historyUser = encodeBlob({ role: 'user', content: 'history user' })
      const run = createTestRun('conv-scaffold', [oldSystem, oldPreamble, historyUser])

      const result = await drain(rebuildConversationHistory({
        run,
        historyBlobIds: [oldSystem.blobId, oldPreamble.blobId, historyUser.blobId],
        prependUserMessages: [],
        systemMessage: { role: 'system', content: 'Anthropic system prompt uses Read and must not mention ApplyPatch' },
        preambleUserMessage: { role: 'user', content: '<user_info>new provider preamble with Read</user_info>' },
        currentUserMessage: { role: 'user', content: '继续' },
        systemContent: 'Anthropic system prompt uses Read and must not mention ApplyPatch',
        preambleUserContent: '<user_info>new provider preamble with Read</user_info>',
        sendSystemScaffoldBlob: noopFrames(),
        sendOrderedBlob: noopFrames(),
      }))

      expect(result.messages[0]).toEqual({ role: 'system', content: 'Anthropic system prompt uses Read and must not mention ApplyPatch' })
      expect(result.messages[1]).toEqual({ role: 'user', content: '<user_info>new provider preamble with Read</user_info>' })
      expect(result.messages[2]).toEqual({ role: 'user', content: 'history user' })
      expect(result.messages[3]).toEqual({ role: 'user', content: '继续' })
    })
  })

  it('rebuilt legacy anthropic history is repaired to canonical form and can continue across anthropic/openai/gemini', async () => {
    await withTempAgentDatabase(async () => {
      const { system, preamble, assistant, legacyUserToolResults } = buildLegacyAnthropicHistoryBlobs()
      const run = createTestRun('conv-legacy', [system, preamble, assistant, legacyUserToolResults])

      const result = await drain(rebuildConversationHistory({
        run,
        historyBlobIds: [system.blobId, preamble.blobId, assistant.blobId, legacyUserToolResults.blobId],
        prependUserMessages: [],
        systemMessage: { role: 'system', content: 'sys prompt' },
        preambleUserMessage: { role: 'user', content: '<user_info>env</user_info>' },
        currentUserMessage: { role: 'user', content: '继续' },
        systemContent: 'sys prompt',
        preambleUserContent: '<user_info>env</user_info>',
        sendSystemScaffoldBlob: noopFrames(),
        sendOrderedBlob: noopFrames(),
      }))

      expect(result.insertedPrependUserTexts).toEqual([])
      expect(result.messages).toEqual([
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

      const anthropicCompiled = transformMessages(result.messages, 'anthropic')
      const anthropicEncoded = encodeAnthropicRequestMessages(anthropicCompiled)
      expect(() => assertValidAnthropicToolUseContract(anthropicEncoded.messages)).not.toThrow()

      const openAICompiled = transformMessages(result.messages, 'openai-chat')
      expect(openAICompiled.filter(message => message.role === 'tool').map(message => message.toolCallId)).toEqual(['call_A', 'call_B'])

      const geminiCompiled = transformMessages(result.messages, 'gemini')
      expect(geminiCompiled.filter(message => message.role === 'tool').map(message => message.toolCallId)).toEqual(['call_A', 'call_B'])
    })
  })
})
