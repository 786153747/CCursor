/**
 * 浏览器版 Usage Dashboard 页面 — server 伺服路由 + 页面模板测试
 *
 * HOME 隔离: startServer → initRuntimeConfig 会把 host/port 持久化到
 * ~/.ccursor/routes.json, 测试期间把 HOME 指到临时目录, 所有 .ccursor
 * 读写 (routes.json / providers.json / 默认 db 路径) 全部落在沙箱内,
 * 绝不触碰用户真实配置。
 *
 * 端口: 用 net server listen(0) 预留随机端口再关闭, 交给 Fastify 复用。
 */
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import * as http from 'node:http'
import * as net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { renderDashboardPageHtml } from '../../ui/dashboard/pageChrome'
import { resetAgentDatabaseForTests } from '../database/sqlite'
import { startServer, stopServer } from '../index'

let sandboxHomeDir = ''
let originalHomeEnv = ''
let tmpDbPath = ''
let tmpJsPath = ''
const stubDashboardJs = 'console.log("stub dashboard for tests")'

function reservePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = net.createServer()
    probe.once('error', reject)
    probe.listen(0, '127.0.0.1', () => {
      const addressInfo = probe.address() as net.AddressInfo
      const reservedPort = addressInfo.port
      probe.close(() => resolve(reservedPort))
    })
  })
}

beforeEach(async () => {
  sandboxHomeDir = mkdtempSync(join(tmpdir(), 'ccursor-usage-page-home-'))
  originalHomeEnv = process.env.HOME ?? ''
  process.env.HOME = sandboxHomeDir
  tmpDbPath = join(sandboxHomeDir, 'agent.db')
  process.env.BYOK_AGENT_DB_PATH = tmpDbPath
  tmpJsPath = join(sandboxHomeDir, 'dashboard-stub.js')
  writeFileSync(tmpJsPath, stubDashboardJs)
  await resetAgentDatabaseForTests()
})

afterEach(async () => {
  await stopServer()
  await resetAgentDatabaseForTests()
  delete process.env.BYOK_AGENT_DB_PATH
  process.env.HOME = originalHomeEnv
  rmSync(sandboxHomeDir, { recursive: true, force: true })
})

it('serves the browser usage dashboard page and script when configured', async () => {
  const port = await reservePort()
  const html = renderDashboardPageHtml({ scriptSrc: '/byok/usage.js', includeBrowserThemeDefaults: true })
  const { host } = await startServer({
    host: '127.0.0.1',
    port,
    usageDashboardPage: { html, dashboardJsPath: tmpJsPath },
  })
  const baseUrl = `http://${host}:${port}`

  const pageResponse = await fetch(`${baseUrl}/byok/usage`)
  expect(pageResponse.status).toBe(200)
  expect(pageResponse.headers.get('content-type')).toContain('text/html')
  const pageBody = await pageResponse.text()
  expect(pageBody).toContain('--vscode-button-background: #0e639c')
  expect(pageBody).toContain('@media (prefers-color-scheme: light)')
  expect(pageBody).toContain('<script src="/byok/usage.js"></script>')
  expect(pageBody).not.toContain('<script>console.log')

  const scriptResponse = await fetch(`${baseUrl}/byok/usage.js`)
  expect(scriptResponse.status).toBe(200)
  expect(scriptResponse.headers.get('content-type')).toContain('text/javascript')
  expect(await scriptResponse.text()).toBe(stubDashboardJs)
})

it('returns 503 for the page and 404 for the script when not configured', async () => {
  const port = await reservePort()
  const { host } = await startServer({ host: '127.0.0.1', port })
  const baseUrl = `http://${host}:${port}`

  const pageResponse = await fetch(`${baseUrl}/byok/usage`)
  expect(pageResponse.status).toBe(503)
  const scriptResponse = await fetch(`${baseUrl}/byok/usage.js`)
  expect(scriptResponse.status).toBe(404)
})

it('renderDashboardPageHtml variants: inline vs scriptSrc, theme defaults optional', () => {
  const inlineHtml = renderDashboardPageHtml({ inlineJs: 'alert(1)<\\/script>', includeBrowserThemeDefaults: false })
  expect(inlineHtml).toContain('<script>alert(1)<\\/script></script>')
  expect(inlineHtml).toContain('script-src \'unsafe-inline\'')
  expect(inlineHtml).not.toContain('--vscode-button-background: #0e639c')

  const browserHtml = renderDashboardPageHtml({ scriptSrc: '/byok/usage.js', includeBrowserThemeDefaults: true })
  expect(browserHtml).toContain('<script src="/byok/usage.js"></script>')
  expect(browserHtml).toContain('script-src \'self\'; connect-src \'self\'')
  expect(browserHtml).toContain('--vscode-font-family:')
  expect(browserHtml).toContain('--vscode-editor-background: #1e1e1e')
  expect(browserHtml).toContain('--vscode-editor-widget-background: #252526')
  expect(browserHtml).toContain('--vscode-editor-font-family:')
  expect(browserHtml).toContain('@media (prefers-color-scheme: light)')
})

/** 假 ext host: 挂一个原生 SSE 客户端到 /byok/ext-events, 收集原始文本 */
function subscribeExtEvents(baseUrl: string): {
  connected: Promise<void>
  receivedText: () => string
  close: () => void
} {
  let receivedRaw = ''
  const request = http.get(`${baseUrl}/byok/ext-events`, (response) => {
    response.setEncoding('utf-8')
    response.on('data', (chunk: string) => {
      receivedRaw += chunk
    })
  })
  const connected = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('ext-events connect timeout')), 5000)
    const poll = setInterval(() => {
      if (receivedRaw.includes(': connected')) {
        clearInterval(poll)
        clearTimeout(timer)
        resolve()
      }
    }, 20)
    request.on('error', (err) => {
      clearInterval(poll)
      clearTimeout(timer)
      reject(err)
    })
  })
  return {
    connected,
    receivedText: () => receivedRaw,
    close: () => request.destroy(),
  }
}

it('bridges open-usage to ext-events subscribers with dispatched count', async () => {
  const port = await reservePort()
  const { host } = await startServer({ host: '127.0.0.1', port })
  const baseUrl = `http://${host}:${port}`

  // 无订阅者 → dispatched 0
  const noSubscriberResponse = await fetch(`${baseUrl}/byok/open-usage`, { method: 'POST' })
  expect(await noSubscriberResponse.json()).toEqual({ ok: true, dispatched: 0 })

  // 挂一个 ext host 订阅者 → 收到 openUsagePage 事件, dispatched 1
  const subscriber = subscribeExtEvents(baseUrl)
  await subscriber.connected
  const response = await fetch(`${baseUrl}/byok/open-usage`, { method: 'POST' })
  expect(await response.json()).toEqual({ ok: true, dispatched: 1 })

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('openUsagePage event timeout')), 5000)
    const poll = setInterval(() => {
      if (subscriber.receivedText().includes('event: openUsagePage')) {
        clearInterval(poll)
        clearTimeout(timer)
        resolve()
      }
    }, 20)
  })
  const eventBlock = subscriber.receivedText()
  expect(eventBlock).toContain('event: openUsagePage')
  const dataLine = eventBlock.split('\n').find(line => line.startsWith('data: '))
  expect(dataLine).toBeDefined()
  const eventData = JSON.parse(dataLine!.slice(6)) as { url: string }
  expect(eventData.url).toBe(`${baseUrl}/byok/usage`)

  // 断开并等待 server 侧感知 (close 异步), 避免 afterEach stopServer 等待残留连接
  subscriber.close()
  for (let attempt = 0; attempt < 40; attempt++) {
    const probeResponse = await fetch(`${baseUrl}/byok/open-usage`, { method: 'POST' })
    const probePayload = await probeResponse.json() as { dispatched: number }
    if (probePayload.dispatched === 0)
      break
    await new Promise(resolve => setTimeout(resolve, 50))
  }
})
