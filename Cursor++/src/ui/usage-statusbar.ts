/**
 * Usage status-bar item — today cost / request count at a glance.
 *
 * Separate item from the BYOK toggle so users can hide it independently
 * via the status-bar context menu. Refreshed on every usage record and
 * on currency change; data comes from a single SQL aggregate.
 */
import type { ExtensionContext, StatusBarItem } from 'vscode'
import * as vscode from 'vscode'
import { onUsageRecorded } from '../server/usage/events'
import { loadUsageSettings } from '../server/usage/settings'
import { queryTodaySummary } from '../server/usage/store'

let usageBarItem: StatusBarItem | null = null

function currencySymbol(currency: 'CNY' | 'USD'): string {
  return currency === 'CNY' ? '\u00A5' : '$'
}

function formatBarCost(micros: bigint, currency: 'CNY' | 'USD'): string {
  return `${currencySymbol(currency)}${Math.round(Number(micros) / 1e6)}`
}

function defaultBarText(): string {
  try {
    return `${formatBarCost(0n, loadUsageSettings().currency)} · 0 req`
  }
  catch {
    return '\u00A50 · 0 req'
  }
}

async function renderUsageBar() {
  if (!usageBarItem)
    return
  try {
    const settings = loadUsageSettings()
    const summary = await queryTodaySummary(settings.currency)
    usageBarItem.text = `${formatBarCost(summary.totalCostMicros, settings.currency)} · ${summary.requestCount} req`
    usageBarItem.tooltip = `Cursor++ Usage — today (${settings.currency})\nCost ${summary.totalCostFormatted} · ${summary.requestCount} requests · ${summary.okCount} ok\n\nClick: open usage panel`
    usageBarItem.show()
  }
  catch {
    // agent DB not ready yet — show the placeholder instead of staying hidden
    usageBarItem.text = defaultBarText()
    usageBarItem.show()
  }
}

export function registerUsageStatusBar(context: ExtensionContext): void {
  usageBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 99)
  usageBarItem.name = 'Cursor++: Usage'
  usageBarItem.command = 'cursor2plus.openUsage'
  usageBarItem.text = defaultBarText()
  usageBarItem.show()
  context.subscriptions.push(usageBarItem)
  const disposeUsageListener = onUsageRecorded(() => {
    void renderUsageBar()
  })
  context.subscriptions.push({ dispose: disposeUsageListener })
  void renderUsageBar()
}

export function refreshUsageStatusBar(): void {
  void renderUsageBar()
}
