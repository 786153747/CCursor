import { describe, expect, it } from 'vitest'
import { formatLocalTimestamp } from '../logger'

describe('logger timestamp', () => {
  it('renders local time with millisecond precision', () => {
    // new Date(y, m, d, ...) 按本地时区构造, 断言与运行机器时区无关
    const date = new Date(2026, 8, 17, 16, 0, 39, 370)
    expect(formatLocalTimestamp(date)).toBe('2026-09-17 16:00:39.370')
  })

  it('pad single-digit components', () => {
    const date = new Date(2026, 0, 5, 9, 7, 3, 8)
    expect(formatLocalTimestamp(date)).toBe('2026-01-05 09:07:03.008')
  })

  it('does not render UTC (offset applied)', () => {
    const date = new Date(2026, 8, 17, 0, 30, 0, 0)
    // 本地 00:30 在任何非 UTC 时区下都不等于其 UTC 表示
    expect(formatLocalTimestamp(date)).toBe('2026-09-17 00:30:00.000')
  })
})
