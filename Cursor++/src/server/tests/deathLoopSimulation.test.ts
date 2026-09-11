/**
 * deathLoopSimulation.test.ts — 死循环复现模拟器（一次性验证脚本, 跑完即删）
 *
 * 复刻生产事故的完整动态循环: 单指令 + 连续读文件工具流,
 * "增长到触发线 → 压缩 → 继续增长 → 再触发" 跑 20+ 个循环,
 * 用真实 planCompaction（新算法）与旧算法复刻（按条数留 6 条 + while 回退）同场对比。
 *
 * 死循环签名（生产实测, 2026-08 事故日志）:
 *   - 压缩后地板 173K~203K（距触发线仅 15~45K）
 *   - 相邻触发间隔极短（最小 239 token, 即几乎每轮触发）
 *   - 地板逐次单调爬升 184K→203K
 */
import type { HistoryEntry } from '../handlers/agent/historyManager'
import type { LLMContentBlock, LLMMessage } from '../handlers/llm/types'
import { appendFileSync, writeFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { measureMessagesTokens, planCompaction } from '../handlers/agent/compactionStrategy'
import { getAutoCompactThreshold } from '../handlers/agent/usage'
import { makeBlobEntry, makeTokenSizedText, makeVariedReportText } from './compactionBudget.test'

const REPORT_PATH = '/tmp/deathloop-sim.txt'
writeFileSync(REPORT_PATH, '')

const WINDOW = 258_400
const SCAFFOLD_TOKENS = 50_000 // 生产实测 provider usage 与消息侧的口径差 40~55K, 取中上值
const MOCK_SUMMARY_BODY = makeVariedReportText(8_000) // ~2K tokens 的模拟摘要正文

interface CycleRecord {
  cycle: number
  triggerTokens: number
  floorTokens: number
  roundsSinceLast: number
  placeholders: number
  escalation: string
}

/** 事故工作流的轮次模板: 大文件读取为主, 混少量 shell/edit */
const ROUND_RESULT_TOKENS = [40_000, 4_000, 20_000, 40_000, 1_000]

function makeRoundMessages(callIndex: number, resultTokens: number): LLMMessage[] {
  const toolUse: LLMContentBlock = {
    type: 'tool_use',
    id: `call_sim_${callIndex}`,
    name: 'Read',
    input: { path: `/repo/src/module-${callIndex}.ts` },
  }
  return [
    { role: 'assistant', content: [{ type: 'text', text: `Reading module ${callIndex} to continue the refactor.` }, toolUse] },
    { role: 'tool', content: makeTokenSizedText(resultTokens), toolCallId: `call_sim_${callIndex}`, toolName: 'Read' },
  ]
}

function materializeEntries(messages: LLMMessage[]): HistoryEntry[] {
  return messages.map((message) => {
    const extra: Record<string, unknown> = {}
    if (message.toolCallId)
      extra.toolCallId = message.toolCallId
    if (message.toolName)
      extra.toolName = message.toolName
    if (message.providerOptions)
      extra.providerOptions = message.providerOptions
    return makeBlobEntry(message.role, message.content as string | LLMContentBlock[], extra)
  })
}

// ── 旧算法复刻 (fix/autocompact-phase1 的 planCompaction 切分逻辑) ──

function oldHasToolUse(message: LLMMessage): boolean {
  if (typeof message.content === 'string')
    return false
  return message.content.some(block => block.type === 'tool_use')
}

function oldPlanSplit(entries: HistoryEntry[]): { summarize: HistoryEntry[], keepTail: HistoryEntry[] } {
  let index = 0
  if (entries[index]?.message.role === 'system')
    index += 1
  if (entries[index] && typeof entries[index]!.message.content === 'string'
    && (entries[index]!.message.content as string).includes('<user_info>')) {
    index += 1
  }
  const body = entries.slice(index)

  let keepTailCount = body.length > 8 ? 6 : body.length > 2 ? 2 : 0
  let summarizeCount = Math.max(0, body.length - keepTailCount)
  if (summarizeCount === 0 && body.length > 2) {
    keepTailCount = 2
    summarizeCount = Math.max(0, body.length - keepTailCount)
  }
  while (summarizeCount > 0) {
    const first = body[summarizeCount]
    if (!first)
      break
    if (first.message.role === 'tool') {
      summarizeCount--
      continue
    }
    if (first.message.role === 'assistant' && oldHasToolUse(first.message)) {
      summarizeCount--
      continue
    }
    break
  }
  return { summarize: body.slice(0, summarizeCount), keepTail: body.slice(summarizeCount) }
}

// ── 模拟循环驱动 ──

function runSimulation(algorithm: 'new' | 'old', maxCycles: number): CycleRecord[] {
  const threshold = getAutoCompactThreshold(WINDOW)
  const leading: LLMMessage[] = [
    { role: 'system', content: makeTokenSizedText(15_000) },
    { role: 'user', content: `<user_info>\n${makeTokenSizedText(1_000)}\n</user_info>` },
  ]
  let history: LLMMessage[] = [...leading, { role: 'user', content: '把仓库里所有大文件逐个读一遍并重构鉴权模块' }]
  let runningTokens = measureMessagesTokens(history)
  const cycles: CycleRecord[] = []
  let roundsSinceLast = 0
  let callIndex = 0

  for (let step = 0; step < 800 && cycles.length < maxCycles; step++) {
    const roundMessages = makeRoundMessages(callIndex, ROUND_RESULT_TOKENS[callIndex % ROUND_RESULT_TOKENS.length]!)
    callIndex += 1
    history.push(...roundMessages)
    runningTokens += measureMessagesTokens(roundMessages)
    roundsSinceLast += 1

    const providerView = runningTokens + SCAFFOLD_TOKENS
    if (providerView < threshold)
      continue

    // 触发压缩
    const entries = materializeEntries(history)
    const summaryMessage: LLMMessage = { role: 'assistant', content: `Previous conversation summary:\n${MOCK_SUMMARY_BODY}` }
    let newHistory: LLMMessage[]
    let placeholders = 0
    let escalation = '-'

    if (algorithm === 'new') {
      const plan = planCompaction(entries, { contextTokenLimit: WINDOW })
      if (plan.summarizeEntries.length === 0) {
        // 无可压缩 (不应发生于本负载) — 记录后继续
        cycles.push({ cycle: cycles.length + 1, triggerTokens: providerView, floorTokens: runningTokens, roundsSinceLast, placeholders: -1, escalation: 'SKIP' })
        roundsSinceLast = 0
        continue
      }
      placeholders = plan.diagnostics.placeholderCount + plan.diagnostics.inputElidedCount
      escalation = plan.diagnostics.escalationLevel
      newHistory = [...plan.leading.map(entry => entry.message), summaryMessage, ...plan.keepTail.map(entry => entry.message)]
    }
    else {
      const { keepTail } = oldPlanSplit(entries)
      newHistory = [...leading, summaryMessage, ...keepTail.map(entry => entry.message)]
    }

    const floorTokens = measureMessagesTokens(newHistory)
    cycles.push({ cycle: cycles.length + 1, triggerTokens: providerView, floorTokens, roundsSinceLast, placeholders, escalation })
    history = newHistory
    runningTokens = floorTokens
    roundsSinceLast = 0
  }
  return cycles
}

function formatCycles(label: string, cycles: CycleRecord[]): string {
  const lines = [`\n══ ${label} ══`, 'cycle | trigger | floor | rounds | placeholders | escalation']
  for (const record of cycles) {
    lines.push(`${String(record.cycle).padStart(5)} | ${String(record.triggerTokens).padStart(7)} | ${String(record.floorTokens).padStart(6)} | ${String(record.roundsSinceLast).padStart(6)} | ${String(record.placeholders).padStart(12)} | ${record.escalation}`)
  }
  return lines.join('\n')
}

describe('死循环复现模拟 (生产事故动态回归)', () => {
  it('新算法 20 循环: 地板 ≤80K 且不爬升, 触发间隔 ≥3 轮', () => {
    const cycles = runSimulation('new', 20)
    appendFileSync(REPORT_PATH, `${formatCycles(`新算法 (预算制) — 窗口 ${WINDOW}, 触发线 ${getAutoCompactThreshold(WINDOW)}, 脚手架 +${SCAFFOLD_TOKENS}`, cycles)}\n`)

    expect(cycles.length).toBeGreaterThanOrEqual(15)
    for (const record of cycles) {
      // 死循环签名 1: 地板 173~203K → 新算法承诺 ≤80K (消息侧)
      expect(record.floorTokens).toBeLessThanOrEqual(80_000)
      // 死循环签名 2: 几乎每轮触发 → 新算法重巨物负载下也应 ≥3 轮
      expect(record.roundsSinceLast).toBeGreaterThanOrEqual(3)
    }
    // 死循环签名 3: 地板逐次爬升 → 后 5 循环均值不高于前 5 循环均值 + 5K
    const firstFive = cycles.slice(0, 5).reduce((sum, r) => sum + r.floorTokens, 0) / 5
    const lastFive = cycles.slice(-5).reduce((sum, r) => sum + r.floorTokens, 0) / 5
    expect(lastFive).toBeLessThanOrEqual(firstFive + 5_000)
  })

  it('旧算法对照: 复现死循环三签名 (地板 >150K, 间隔 ≤2 轮)', () => {
    const cycles = runSimulation('old', 12)
    appendFileSync(REPORT_PATH, `${formatCycles('旧算法 (按条数留 6 条 + while 回退) — 同负载对照', cycles)}\n`)

    expect(cycles.length).toBeGreaterThanOrEqual(8)
    const floorsAfterWarmup = cycles.slice(2)
    // 签名 1: 地板贴触发线
    const highFloors = floorsAfterWarmup.filter(r => r.floorTokens > 150_000)
    expect(highFloors.length).toBeGreaterThanOrEqual(Math.floor(floorsAfterWarmup.length * 0.8))
    // 签名 2: 触发间隔坍缩 (几乎每 1-2 轮一次)
    const tightIntervals = floorsAfterWarmup.filter(r => r.roundsSinceLast <= 2)
    expect(tightIntervals.length).toBeGreaterThanOrEqual(Math.floor(floorsAfterWarmup.length * 0.8))
  })
})
