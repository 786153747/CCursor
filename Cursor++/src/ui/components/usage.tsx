/**
 * 侧边栏用量面板 — token 优先视角, 中文文案, 金额统一 USD。
 *
 * 核心指标: 总 Tokens / 输入 / 输出 / 缓存率 / 缓存读写;
 * 成本与请求数降为辅助信息。数据来自 extension host 推送的 usage payload
 * (server/usage/store 的 serializeUsageDashboard)。
 */
export function Usage() {
  return (
    <div class="usage-panel" x-show="$store.app.usageOpen">
      {/* Hero: 周期总 Tokens */}
      <div class="usage-hero">
        <div class="usage-today-label">
          <span x-text="$store.app.usageRangeLabel"></span>
          <span>{' 总 Tokens'}</span>
        </div>
        <div class="usage-today" x-text="$store.app.usage ? $store.app.formatUsageTokens($store.app.usage?.summary?.realTotalTokens) : '—'"></div>
        <div class="usage-today-label">
          <span x-text="'共 ' + ($store.app.usage?.summary?.requestCount ?? 0) + ' 次请求'"></span>
          <span x-text="' · 成功率 ' + $store.app.usageSuccessLabel"></span>
          <span
            x-show="$store.app.usageRange !== 'today'"
            x-text="' · 今日 ' + $store.app.formatUsageTokens($store.app.usage?.todayRealTokens ?? 0)"
          >
          </span>
        </div>
      </div>

      {/* 工具条: 范围 / 状态栏窗口 / 刷新 */}
      <div class="usage-toolbar">
        <select x-model="$store.app.usageRange" x-on:change="$store.app.saveUsageSettings()">
          <option value="today">今日</option>
          <option value="7d">近 7 天</option>
          <option value="14d">近 14 天</option>
          <option value="30d">近 30 天</option>
          <option value="month">本月</option>
        </select>
        <button
          class="tiny secondary"
          title="状态栏统计窗口（按日或按月重置）"
          x-on:click="$store.app.toggleUsageBarScope()"
          x-text="$store.app.usageBarScopeLabel"
        >
        </button>
        <button class="tiny secondary" x-on:click="$store.app.loadUsage()">刷新</button>
      </div>

      {/* 核心指标网格: 输入 / 输出 / 缓存率 / 缓存读取 / 缓存写入 / 成本 */}
      <div class="usage-metrics" x-show="$store.app.usage">
        <div class="usage-metric">
          <span class="usage-metric-label">输入 Tokens</span>
          <span class="usage-metric-value" x-text="$store.app.formatUsageTokens($store.app.usage?.summary?.inputTokens)"></span>
        </div>
        <div class="usage-metric">
          <span class="usage-metric-label">输出 Tokens</span>
          <span class="usage-metric-value" x-text="$store.app.formatUsageTokens($store.app.usage?.summary?.outputTokens)"></span>
        </div>
        <div class="usage-metric">
          <span class="usage-metric-label" title="缓存读取 Tokens / 全部输入 Tokens">缓存率</span>
          <span class="usage-metric-value" x-text="$store.app.cacheHitLabel"></span>
        </div>
        <div class="usage-metric">
          <span class="usage-metric-label">缓存读取</span>
          <span class="usage-metric-value" x-text="$store.app.formatUsageTokens($store.app.usage?.summary?.cacheReadTokens)"></span>
        </div>
        <div class="usage-metric">
          <span class="usage-metric-label">缓存写入</span>
          <span class="usage-metric-value" x-text="$store.app.formatUsageTokens($store.app.usage?.summary?.cacheWriteTokens)"></span>
        </div>
        <div class="usage-metric">
          <span class="usage-metric-label">成本 (USD)</span>
          <span class="usage-metric-value" x-text="$store.app.usage ? $store.app.formatUsageCost($store.app.usage?.summary?.totalCostFormatted) : '—'"></span>
        </div>
      </div>

      {/* 每日 token 趋势条 */}
      <div class="usage-trend" x-show="($store.app.usage?.daily?.length || 0) > 1">
        <div class="usage-trend-bars">
          <template x-for="bar in $store.app.usageDailyBars" x-bind:key="bar.date">
            <div class="usage-trend-bar" x-bind:title="bar.title">
              <div class="usage-trend-fill" x-bind:style="'height:' + bar.heightPercent + '%'"></div>
            </div>
          </template>
        </div>
        <div class="usage-trend-axis" x-show="($store.app.usage?.daily?.length || 0) > 1">
          <span x-text="$store.app.usageTrendStartLabel"></span>
          <span x-text="$store.app.usageTrendEndLabel"></span>
        </div>
      </div>

      <div class="usage-unpriced" x-show="$store.app.usage?.summary?.unpricedCount > 0">
        <span x-text="$store.app.usage?.summary?.unpricedCount"></span>
        {' 条请求未定价 — 在模型卡片中填写单价后计入成本'}
      </div>

      <div class="usage-hint" x-show="$store.app.usage && ($store.app.usage?.summary?.requestCount === 0)">
        当前范围暂无记录。
      </div>

      {/* 提供商 / 模型筛选 — 右侧显示 token 总量, tooltip 里带请求数与成本 */}
      <div class="usage-section-title">提供商</div>
      <div class="usage-hint">点击名称展开模型；取消勾选的提供商仍会记录，但不计入统计。</div>
      <template x-for="p in $store.app.usageProvidersVisible" x-bind:key="p.id">
        <div class="usage-provider">
          <div class="usage-provider-row">
            <input
              type="checkbox"
              x-bind:checked="p.selected"
              x-on:change="$store.app.toggleUsageProvider(p.id, $event.target.checked)"
            />
            <span
              class="usage-provider-toggle"
              x-text="$store.app.usageProviderExpanded[p.id] ? '▾' : '▸'"
              x-on:click="$store.app.toggleUsageProviderExpanded(p.id)"
            >
            </span>
            <span class="usage-check-name" x-text="p.name" x-on:click="$store.app.toggleUsageProviderExpanded(p.id)"></span>
            <span
              class="usage-check-cost"
              x-text="$store.app.formatUsageTokens(p.realTotalTokens)"
              x-bind:title="p.requestCount + ' 次请求 · ' + $store.app.formatUsageCost(p.totalCostFormatted)"
            >
            </span>
          </div>
          <template x-if="$store.app.usageProviderExpanded[p.id]">
            <div class="usage-models">
              <template x-for="m in $store.app.usageModelsFor(p.id)" x-bind:key="m.key">
                <label class="usage-check usage-check-nested">
                  <input
                    type="checkbox"
                    x-bind:checked="m.selected"
                    x-on:change="$store.app.toggleUsageModel(m.key, $event.target.checked)"
                  />
                  <span class="usage-check-name" x-text="m.displayName"></span>
                  <span
                    class="usage-check-cost"
                    x-text="$store.app.formatUsageTokens(m.realTotalTokens)"
                    x-bind:title="m.requestCount + ' 次请求 · ' + $store.app.formatUsageCost(m.totalCostFormatted)"
                  >
                  </span>
                </label>
              </template>
              <div class="usage-empty" x-show="!$store.app.usageModelsFor(p.id).length">暂无模型。</div>
            </div>
          </template>
        </div>
      </template>
      <button
        class="usage-show-more"
        x-show="$store.app.usageProvidersHiddenCount > 0 || $store.app.usageShowAllProviders"
        x-on:click="$store.app.usageShowAllProviders = !$store.app.usageShowAllProviders"
        x-text="$store.app.usageHiddenProvidersLabel"
      >
      </button>

      {/* 最近请求 — 右侧显示 token 总量, 展开看明细 */}
      <div class="usage-section-title">最近请求</div>
      <template x-if="!$store.app.usageRecentList.length">
        <div class="usage-empty">该范围暂无请求。</div>
      </template>
      <template x-for="item in $store.app.usageRecentList" x-bind:key="item.requestId">
        <div class="usage-row" x-on:click="$store.app.toggleUsageRecentExpanded(item.requestId)">
          <div class="usage-row-main">
            <div class="usage-row-line">
              <span class="usage-row-time" x-text="$store.app.formatUsageTime(item.createdAt)"></span>
              <span x-text="item.displayName"></span>
            </div>
            <div class="usage-row-detail" x-show="$store.app.usageRecentExpanded[item.requestId]">
              <span x-text="'输入 ' + $store.app.formatUsageTokens(item.inputTokens)"></span>
              <span x-text="'输出 ' + $store.app.formatUsageTokens(item.outputTokens)"></span>
              <span x-text="'缓存读 ' + $store.app.formatUsageTokens(item.cacheReadTokens)"></span>
              <span x-show="item.cacheWriteTokens > 0" x-text="'缓存写 ' + $store.app.formatUsageTokens(item.cacheWriteTokens)"></span>
              <span x-text="$store.app.formatUsageDuration(item.durationMs)"></span>
              <span x-text="item.status === 'error' ? '失败' : $store.app.formatUsageCost(item.totalCostFormatted)"></span>
            </div>
          </div>
          <div class="usage-row-cost">
            <span x-text="item.status === 'error' ? '失败' : $store.app.formatUsageTokens(item.inputTokens + item.outputTokens)"></span>
          </div>
        </div>
      </template>
      <button
        class="usage-show-more"
        x-show="($store.app.usage?.recent || []).length > 3 || $store.app.usageRecentLimit > 3"
        x-on:click="$store.app.usageRecentLimit = $store.app.usageRecentLimit >= 30 ? 3 : 30"
        x-text="$store.app.usageRecentToggleLabel"
      >
      </button>
    </div>
  )
}
