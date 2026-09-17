import type { HistoryEntry } from '../handlers/agent/historyManager'
import type { LLMContentBlock, LLMMessage } from '../handlers/llm/types'
import { expect, it } from 'vitest'
import { planCompaction } from '../handlers/agent/compactionStrategy'
import { repairHistoryEntries } from '../handlers/agent/historyManager'
import { repairConversationHistory } from '../handlers/llm/transformMessages'

function makeEntry(index: number, message: LLMMessage): HistoryEntry {
  return {
    blobId: `blob-${index}`,
    raw: { role: message.role, content: message.content as unknown },
    message,
  }
}

function hasToolUse(message: LLMMessage): boolean {
  return message.role === 'assistant'
    && typeof message.content !== 'string'
    && message.content.some((block: LLMContentBlock) => block.type === 'tool_use')
}

function buildLegacyCompactionEntries(): HistoryEntry[] {
  return [
    makeEntry(0, { role: 'system', content: 'sys prompt' }),
    makeEntry(1, { role: 'user', content: '<user_info>env</user_info>' }),
    makeEntry(2, { role: 'user', content: 'user-1' }),
    makeEntry(3, { role: 'assistant', content: 'assistant-1' }),
    makeEntry(4, { role: 'user', content: 'user-2' }),
    makeEntry(5, {
      role: 'assistant',
      content: [
        { type: 'text', text: '先查一下。' },
        { type: 'tool_use', id: 'call_A', name: 'Read', input: { path: 'a.ts' } },
      ],
    }),
    makeEntry(6, {
      role: 'user',
      content: [
        { type: 'tool_result', toolUseId: 'call_A', toolName: 'Read', content: 'read result' },
      ],
    }),
    makeEntry(7, { role: 'assistant', content: 'assistant-tail' }),
  ]
}

function toEntries(messages: LLMMessage[]): HistoryEntry[] {
  return messages.map((message, index) => makeEntry(index, message))
}

/** 取"携带工具结果"的那条消息指向的 toolCallId (repair 前藏在 user 里, 后为 role=tool) */
function toolResultToolCallId(message: LLMMessage): string | undefined {
  if (message.role === 'tool')
    return message.toolCallId
  if (typeof message.content === 'string')
    return undefined
  const block = message.content.find((item): item is Extract<LLMContentBlock, { type: 'tool_result' }> => item.type === 'tool_result')
  return block?.toolUseId
}

it('diagnostic: planCompaction keeps an assistant tool_use and its result message on the same side of the cut', () => {
  // 两种形态都要成立:
  //   legacy = tool_result 藏在 role='user' 的消息里 (官方旧 anthropic 回放形态)
  //   repaired = repairConversationHistory 规范化后的 role='tool'
  // 切点一旦落在这一对中间, 摘要侧会拿到没有结果的 tool_use, 尾窗会拿到孤儿 tool_result。
  const legacy = buildLegacyCompactionEntries()
  const repaired = toEntries(repairConversationHistory(legacy.map(entry => entry.message)))

  for (const entries of [legacy, repaired]) {
    const plan = planCompaction(entries, { budgetOverride: 30 })

    expect(plan.leading.map(entry => entry.message.role)).toEqual(['system', 'user'])
    const toolUseIndex = plan.keepTail.findIndex(entry => hasToolUse(entry.message))
    expect(toolUseIndex).toBeGreaterThanOrEqual(0)
    expect(toolResultToolCallId(plan.keepTail[toolUseIndex + 1]!.message)).toBe('call_A')
  }
})

it('after repairConversationHistory canonicalizes legacy anthropic tool results, planCompaction no longer splits the assistant/tool boundary', () => {
  const repairedMessages = repairConversationHistory(buildLegacyCompactionEntries().map(entry => entry.message))
  const repairedEntries = toEntries(repairedMessages)
  const plan = planCompaction(repairedEntries, { budgetOverride: 30 })

  expect(plan.leading.map(entry => entry.message.role)).toEqual(['system', 'user'])
  expect(plan.summarizeEntries.some(entry => hasToolUse(entry.message))).toBe(false)
  expect(plan.keepTail[0]?.message.role).toBe('user')
  expect(plan.keepTail[1]?.message.role).toBe('assistant')
  expect(hasToolUse(plan.keepTail[1]!.message)).toBe(true)
  expect(plan.keepTail[2]?.message.role).toBe('tool')
  expect(plan.keepTail[2]?.message.toolCallId).toBe('call_A')
})

it('runtime helper repairHistoryEntries materializes canonicalized entries before compaction planning', () => {
  const plan = planCompaction(repairHistoryEntries(buildLegacyCompactionEntries()), { budgetOverride: 35 })

  expect(plan.keepTail[1]?.message.role).toBe('assistant')
  expect(hasToolUse(plan.keepTail[1]!.message)).toBe(true)
  expect(plan.keepTail[2]?.message.role).toBe('tool')
  expect(plan.keepTail[2]?.message.toolCallId).toBe('call_A')
})
