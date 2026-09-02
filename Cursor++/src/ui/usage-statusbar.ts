/**
 * Usage status-bar suffix — today cost appended to the BYOK item.
 *
 * Lives inside the existing BYOK status-bar item (no separate entry):
 *   `✓ BYOK ◉ ¥4`
 * The precise cost stays available via the item tooltip. Refreshed on
 * every usage record and on currency change; data comes from a single
 * SQL aggregate. A failed first query (agent DB not initialized yet)
 * schedules retries so the suffix appears without waiting for a request.
 */
import { onUsageRecorded } from '../server/usage/events'
import { loadUsageSettings } from '../server/usage/settings'
import { queryTodaySummary } from '../server/usage/store'

const RETRY_DELAY_MS = 10_000
const RETRY_MAX = 6

let rerenderBar: () => void = () => {}
let usageSuffix = ''
let usageTooltipLine = ''
let retryCount = 0
let retryTimer: ReturnType<typeof setTimeout> | null = null

function currencySymbol(currency: 'CNY' | 'USD'): string {
  return currency === 'CNY' ? '\u00A5' : '$'
}

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
    const summary = await queryTodaySummary(settings.currency)
    usageSuffix = ` ${currencySymbol(settings.currency)}${Math.round(Number(summary.totalCostMicros) / 1e6)}`
    usageTooltipLine = `Today: ${summary.totalCostFormatted} · ${summary.requestCount} requests · ${summary.okCount} ok (${settings.currency})`
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

/** Attach today-cost suffix updates to the BYOK status-bar rerender cycle. */
export function initUsageStatusBar(rerender: () => void): void {
  rerenderBar = rerender
  onUsageRecorded(() => {
    void recompute()
  })
  void recompute()
}

/** Suffix for statusBarItem.text, e.g. ` ¥4`. Empty while data is unavailable. */
export function getUsageSuffix(): string {
  return usageSuffix
}

/** One-line today summary for the status-bar tooltip. Empty while unavailable. */
export function getUsageTooltipLine(): string {
  return usageTooltipLine
}

/** Recompute suffix (e.g. after a currency switch or server start) and refresh the bar. */
export function refreshUsageStatusBar(): void {
  retryCount = 0
  void recompute()
}
