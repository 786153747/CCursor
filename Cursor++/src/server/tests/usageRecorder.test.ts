import type { LLMProvider, LLMStreamEvent } from '../handlers/llm/types'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { recordModelUsage } from '../database/usageStats'
import { withUsageRecording } from '../handlers/llm/usageRecorder'

// 统计落库是 fire-and-forget (void ... .catch), 直接断言 DB 会有竞态;
// 这里替换成 spy 来锁"什么事件该记一条"。
vi.mock('../database/usageStats', () => ({
  recordModelUsage: vi.fn(async () => {}),
}))

const META = {
  providerId: 'provider-alpha',
  providerName: 'Provider Alpha',
  modelId: 'flash',
  apiModel: 'vendor/model-x',
}

/** 按脚本依次吐事件; 元素是 Error 时在吐完之前的事件后抛出。 */
function fakeProvider(script: Array<LLMStreamEvent | Error>): LLMProvider {
  return {
    name: 'fake',
    async* stream() {
      for (const step of script) {
        if (step instanceof Error)
          throw step
        yield step
      }
    },
  }
}

async function drain(provider: LLMProvider): Promise<{ events: LLMStreamEvent[], error?: unknown }> {
  const events: LLMStreamEvent[] = []
  try {
    for await (const event of provider.stream({ model: META.apiModel, messages: [] }))
      events.push(event)
    return { events }
  }
  catch (error) {
    return { events, error }
  }
}

describe('withUsageRecording', () => {
  beforeEach(() => {
    vi.mocked(recordModelUsage).mockClear()
  })

  it('passes every event through and records each done event', async () => {
    const provider = withUsageRecording(fakeProvider([
      { type: 'text_delta', text: 'hi' },
      { type: 'done', stopReason: 'tool_use', usage: { inputTokens: 10, outputTokens: 2 } },
      { type: 'done', stopReason: 'end_turn', usage: { inputTokens: 20, outputTokens: 4, cacheReadTokens: 3 } },
    ]), META)

    const { events, error } = await drain(provider)

    expect(error).toBeUndefined()
    expect(events.map(event => event.type)).toEqual(['text_delta', 'done', 'done'])
    expect(vi.mocked(recordModelUsage).mock.calls.map(call => call[0].usage)).toEqual([
      { inputTokens: 10, outputTokens: 2 },
      { inputTokens: 20, outputTokens: 4, cacheReadTokens: 3 },
    ])
    expect(vi.mocked(recordModelUsage).mock.calls[0]![0]).toMatchObject(META)
  })

  it('records a zero-usage sample for a failed stream and rethrows the original error', async () => {
    const failure = new Error('upstream exploded')
    const provider = withUsageRecording(fakeProvider([
      { type: 'text_delta', text: 'partial' },
      failure,
    ]), META)

    const { events, error } = await drain(provider)

    // 原始错误必须原样上抛 (采集层不得改变重试/中断语义)
    expect(error).toBe(failure)
    expect(events.map(event => event.type)).toEqual(['text_delta'])
    expect(vi.mocked(recordModelUsage)).toHaveBeenCalledTimes(1)
    expect(vi.mocked(recordModelUsage).mock.calls[0]![0]).toMatchObject({
      ...META,
      usage: { inputTokens: 0, outputTokens: 0 },
    })
  })

  it('does not record for a stream that ends without a done event', async () => {
    const provider = withUsageRecording(fakeProvider([
      { type: 'text_delta', text: 'truncated' },
    ]), META)

    const { events, error } = await drain(provider)

    expect(error).toBeUndefined()
    expect(events).toHaveLength(1)
    expect(vi.mocked(recordModelUsage)).not.toHaveBeenCalled()
  })
})
