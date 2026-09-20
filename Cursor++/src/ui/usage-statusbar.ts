/**
 * Usage status-bar suffix — 周期成本后缀, 追加在 BYOK 状态栏项后面。
 *
 * 复用现有 BYOK 状态栏项, 不新开条目:
 *   `✓ BYOK ◉ $14`
 * 统计窗口跟随 `usage-settings.json` 的 `statusBarScope`
 * (缺省 'month' — 每月 1 日重置; 'today' — 每日零点重置)。
 * 金额统一 USD, 精确值在 tooltip 里。每次用量落库及 scope 切换后刷新,
 * 数据来自单条 SQL 聚合。查询失败 (agent DB 未初始化) 会安排重试,
 * 让后缀不需要等到下一次请求才出现。
 */
import type { UsageBarScope } from '../server/usage/types'
import { onUsageRecorded } from '../server/usage/events'
import { loadUsageSettings } from '../server/usage/settings'
import { queryUsageSummary } from '../server/usage/store'

const RETRY_DELAY_MS = 10_000
const RETRY_MAX = 6

let rerenderBar: () => void = () => {}
let usageSuffix = ''
let usageTooltipLine = ''
let retryCount = 0
let retryTimer: ReturnType<typeof setTimeout> | null = null

function scheduleRetry() {
  if (retryTimer || retryCount >= RETRY_MAX)
    return
  retryCount += 1
  retryTimer = setTimeout(() => {
    retryTimer = null
    void recompute()
  }, RETRY_DELAY_MS)
}

async function recompute() {
  try {
    const settings = loadUsageSettings()
    const scope: UsageBarScope = settings.statusBarScope === 'today' ? 'today' : 'month'
    const summary = await queryUsageSummary(scope)
    usageSuffix = ` $${Math.round(Number(summary.totalCostMicros) / 1e6)}`
    const scopeLabel = scope === 'month' ? '本月' : '今日'
    usageTooltipLine = `${scopeLabel}: ${summary.totalCostFormatted} · ${summary.requestCount} 次请求 · ${summary.okCount} 次成功`
    retryCount = 0
  }
  catch {
    // agent DB not ready yet (server still starting) — clear the suffix
    // and retry a few times so it appears without waiting for a request
    usageSuffix = ''
    usageTooltipLine = ''
    scheduleRetry()
  }
  rerenderBar()
}

/** Attach period-cost suffix updates to the BYOK status-bar rerender cycle. */
export function initUsageStatusBar(rerender: () => void): void {
  rerenderBar = rerender
  onUsageRecorded(() => {
    void recompute()
  })
  void recompute()
}

/** Suffix for statusBarItem.text, e.g. ` $14`. Empty while data is unavailable. */
export function getUsageSuffix(): string {
  return usageSuffix
}

/** One-line period summary for the status-bar tooltip. Empty while unavailable. */
export function getUsageTooltipLine(): string {
  return usageTooltipLine
}

/** Recompute suffix (e.g. after a scope switch) and refresh the bar. */
export function refreshUsageStatusBar(): void {
  retryCount = 0
  void recompute()
}
