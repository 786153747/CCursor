import type { ModelUsageSample } from '../database/usageStats'
import type { LLMProvider, LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
/**
 * 每模型 Token 用量统计 — 存储层 + 采集层测试
 *
 * 临时 DB 模式沿用 chatSummary.test.ts:
 *   BYOK_AGENT_DB_PATH → 临时文件 → resetAgentDatabaseForTests()
 *
 * 时区桶 (day/hour) 用 vi.useFakeTimers({ toFake: ['Date'] }) 固定时钟 —
 * 只 fake Date 不 fake 计时器, 保证 sqlite 异步回调照常走事件循环。
 */
import { unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { closeAgentDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { queryUsageStats, recordModelUsage } from '../database/usageStats'
import { withUsageRecording } from '../handlers/llm/usageRecorder'

let tmpDbPath = ''

beforeEach(async () => {
  tmpDbPath = join(tmpdir(), `.tmp-usage-stats-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
  process.env.BYOK_AGENT_DB_PATH = tmpDbPath
  await resetAgentDatabaseForTests()
})

afterEach(async () => {
  vi.useRealTimers()
  await resetAgentDatabaseForTests()
  delete process.env.BYOK_AGENT_DB_PATH
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      unlinkSync(`${tmpDbPath}${suffix}`)
    }
    catch {}
  }
})

const baseSample: ModelUsageSample = {
  providerId: 'test-anthropic',
  providerName: 'Test Anthropic',
  modelId: 'glm-5',
  apiModel: 'glm-5',
  usage: { inputTokens: 100, outputTokens: 50, cacheReadTokens: 20, cacheWriteTokens: 10 },
}

/** 等待 fire-and-forget 的 recordModelUsage 落库 (真实计时器, 短等待) */
async function waitForAsyncWrites(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 50))
}

it('recordModelUsage accumulates tokens and request_count within the same bucket', async () => {
  await recordModelUsage(baseSample)
  await recordModelUsage({ ...baseSample, usage: { inputTokens: 30, outputTokens: 20, cacheReadTokens: 5, cacheWriteTokens: 2 } })

  const rows = await queryUsageStats(null)
  expect(rows.length).toBe(1)
  expect(rows[0]).toMatchObject({
    providerId: 'test-anthropic',
    providerName: 'Test Anthropic',
    modelId: 'glm-5',
    apiModel: 'glm-5',
    requestCount: 2,
    inputTokens: 130,
    outputTokens: 70,
    cacheReadTokens: 25,
    cacheWriteTokens: 12,
  })
})

it('recordModelUsage splits rows across different hours and models', async () => {
  vi.useFakeTimers({ toFake: ['Date'] })
  try {
    vi.setSystemTime(new Date(2026, 7, 30, 10, 30, 0))
    await recordModelUsage(baseSample)
    // 同 model 不同 hour → 不同桶
    vi.setSystemTime(new Date(2026, 7, 30, 12, 30, 0))
    await recordModelUsage(baseSample)
    // 同 hour 不同 model → 不同桶
    vi.setSystemTime(new Date(2026, 7, 30, 10, 30, 0))
    await recordModelUsage({ ...baseSample, modelId: 'claude-sonnet-4', apiModel: 'claude-sonnet-4' })

    const rows = await queryUsageStats(null)
    expect(rows.length).toBe(3)
    expect(new Set(rows.map(row => `${row.day} ${row.hour} ${row.modelId}`)).size).toBe(3)
    // 排序: day, hour 升序 → 同小时两桶 (modelId 字典序) 在前
    expect(rows.map(row => row.hour)).toEqual([10, 10, 12])
  }
  finally {
    vi.useRealTimers()
  }
})

it('recordModelUsage stores 0 for undefined cache fields', async () => {
  await recordModelUsage({ ...baseSample, usage: { inputTokens: 10, outputTokens: 5 } })

  const rows = await queryUsageStats(null)
  expect(rows.length).toBe(1)
  expect(rows[0].cacheReadTokens).toBe(0)
  expect(rows[0].cacheWriteTokens).toBe(0)
})

it('queryUsageStats filters by sinceDay and returns everything for null', async () => {
  vi.useFakeTimers({ toFake: ['Date'] })
  try {
    vi.setSystemTime(new Date(2026, 7, 28, 9, 0, 0))
    await recordModelUsage(baseSample)
    vi.setSystemTime(new Date(2026, 7, 30, 15, 0, 0))
    await recordModelUsage({ ...baseSample, modelId: 'claude-sonnet-4' })

    const sinceRows = await queryUsageStats('2026-08-29')
    expect(sinceRows.length).toBe(1)
    expect(sinceRows[0].day).toBe('2026-08-30')
    expect(sinceRows[0].modelId).toBe('claude-sonnet-4')

    const allRows = await queryUsageStats(null)
    expect(allRows.length).toBe(2)
    expect(allRows.map(row => row.day)).toEqual(['2026-08-28', '2026-08-30'])
  }
  finally {
    vi.useRealTimers()
  }
})

it('withUsageRecording passes events through unchanged and records usage after done', async () => {
  const emittedEvents: LLMStreamEvent[] = [
    { type: 'text_delta', text: 'hello' },
    { type: 'done', usage: { inputTokens: 1000, outputTokens: 400, cacheReadTokens: 300 }, stopReason: 'end_turn' },
  ]
  const mockProvider: LLMProvider = {
    name: 'mock',
    async* stream(_request: LLMStreamRequest): AsyncIterable<LLMStreamEvent> {
      yield emittedEvents[0]
      yield emittedEvents[1]
    },
  }

  const wrapped = withUsageRecording(mockProvider, {
    providerId: 'test-openai',
    providerName: 'Test OpenAI',
    modelId: 'gpt-5.4-medium',
    apiModel: 'gpt-5.4-medium',
  })

  const receivedEvents: LLMStreamEvent[] = []
  for await (const event of wrapped.stream({ model: 'gpt-5.4-medium', messages: [] }))
    receivedEvents.push(event)

  expect(receivedEvents).toEqual(emittedEvents)

  await waitForAsyncWrites()
  const rows = await queryUsageStats(null)
  expect(rows.length).toBe(1)
  expect(rows[0]).toMatchObject({
    providerId: 'test-openai',
    modelId: 'gpt-5.4-medium',
    requestCount: 1,
    inputTokens: 1000,
    outputTokens: 400,
    cacheReadTokens: 300,
    cacheWriteTokens: 0,
  })
})

it('withUsageRecording completes the stream without throwing when the database is uninitialized', async () => {
  await closeAgentDatabase()

  const mockProvider: LLMProvider = {
    name: 'mock',
    async* stream(_request: LLMStreamRequest): AsyncIterable<LLMStreamEvent> {
      yield { type: 'text_delta', text: 'partial' }
      yield { type: 'done', usage: { inputTokens: 10, outputTokens: 4 }, stopReason: 'end_turn' }
    },
  }
  const wrapped = withUsageRecording(mockProvider, {
    providerId: 'test-gemini',
    providerName: 'Test Gemini',
    modelId: 'gemini-3.1-pro-preview',
    apiModel: 'gemini-3.1-pro-preview',
  })

  const receivedEvents: LLMStreamEvent[] = []
  // DB 未初始化: getAgentDatabase() 同步 throw 被 recordModelUsage 内部吞掉,
  // 流本身必须完整走完且不抛错
  for await (const event of wrapped.stream({ model: 'gemini-3.1-pro-preview', messages: [] }))
    receivedEvents.push(event)

  expect(receivedEvents.length).toBe(2)
  expect(receivedEvents[1].type).toBe('done')
})

it('withUsageRecording propagates provider errors to the consumer untouched', async () => {
  const mockProvider: LLMProvider = {
    name: 'failing',
    async* stream(_request: LLMStreamRequest): AsyncIterable<LLMStreamEvent> {
      yield { type: 'text_delta', text: 'before failure' }
      throw new Error('provider exploded')
    },
  }
  const wrapped = withUsageRecording(mockProvider, {
    providerId: 'test-anthropic',
    providerName: 'Test Anthropic',
    modelId: 'glm-5',
    apiModel: 'glm-5',
  })

  const consume = async () => {
    for await (const _event of wrapped.stream({ model: 'glm-5', messages: [] })) {
      // 消费全部事件
    }
  }
  await expect(consume()).rejects.toThrow('provider exploded')
})
