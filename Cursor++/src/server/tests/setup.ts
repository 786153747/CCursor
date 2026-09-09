/**
 * Vitest 全局 setup — 注入共享的测试 providers。
 *
 * 设计动机:
 *   Cursor++ 改用 providersStore 作为单一真源后, `resolveModel(unknownId)` 会
 *   直接抛 ModelNotFoundError — 对生产侧是正确的行为 (避免静默走 anthropic),
 *   但测试里不可能每个文件都各自注入 providers, 所以这里通过 vitest 的
 *   setupFiles 机制, 在所有测试用例运行前一次性把合成的 TEST_PROVIDERS 写进
 *   providersStore 的 in-memory cache。
 *
 *   这样任何测试文件里出现的 modelId (claude-sonnet-4, qwen3.5-plus, glm-5,
 *   gpt-5.4-medium, gemini-3.1-pro-preview) 都会被正确解析。
 *
 *   composer-* / 其他未登记的 modelId 仍然会触发 ModelNotFoundError —
 *   前者走 promptProfile.ts 的 fallback 分支绕过 resolveModel,
 *   后者本就是测试"未登记 → 拒绝请求"的预期行为。
 *
 *   auth.value 必须非空: openai SDK v6+ 在 client 构造时就校验 apiKey,
 *   空字符串会让 resolveProviderRuntime/routeModel 直接抛 Missing credentials。
 */
import type { ProvidersConfig } from '../data/defaults'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, beforeEach } from 'vitest'
import { setProvidersForTests } from '../config/providersStore'
import { closeAgentDatabase } from '../database/sqlite'

// setupFiles runs separately for each test file. Never inherit a real DB path.
const testDirectory = mkdtempSync(join(tmpdir(), 'ccursor-server-tests-'))
const testDatabasePath = join(testDirectory, 'cursor.db')
process.env.BYOK_AGENT_DB_PATH = testDatabasePath

// Spill paths use os.homedir(), with no dedicated spill-directory override.
process.env.HOME = testDirectory
process.env.USERPROFILE = testDirectory

beforeEach(() => {
  // Some tests delete their overrides during teardown. Restore safe defaults
  // without replacing overrides supplied by a test file or its own hooks.
  process.env.BYOK_AGENT_DB_PATH ||= testDatabasePath
  process.env.HOME ||= testDirectory
  process.env.USERPROFILE ||= testDirectory
})

afterAll(async () => {
  await closeAgentDatabase()
  rmSync(testDirectory, { recursive: true, force: true })
})

const TEST_PROVIDERS: ProvidersConfig = {
  $schemaVersion: 1,
  providers: [
    {
      id: 'test-openai',
      name: 'Test OpenAI',
      type: 'openai-chat',
      baseUrl: '',
      auth: { kind: 'apiKey', value: 'test-key' },
      models: [
        {
          id: 'gpt-5.4-medium',
          apiModel: 'gpt-5.4-medium',
          displayName: 'GPT 5.4 Medium',
          thinking: false,
          contextTokenLimit: 400000,
        },
        {
          id: 'qwen3.6-plus',
          apiModel: 'qwen3.6-plus',
          displayName: 'Qwen3.6 Plus',
          thinking: false,
          contextTokenLimit: 1000000,
        },
      ],
    },
    {
      id: 'test-gemini',
      name: 'Test Gemini',
      type: 'gemini',
      baseUrl: '',
      auth: { kind: 'apiKey', value: 'test-key' },
      models: [
        {
          id: 'gemini-3.1-pro-preview',
          apiModel: 'gemini-3.1-pro-preview',
          displayName: 'Gemini 3.1 Pro',
          thinking: false,
          contextTokenLimit: 1000000,
        },
      ],
    },
    {
      id: 'test-anthropic',
      name: 'Test Anthropic',
      type: 'anthropic',
      baseUrl: '',
      auth: { kind: 'apiKey', value: 'test-key' },
      models: [
        {
          id: 'claude-sonnet-4',
          apiModel: 'claude-sonnet-4',
          displayName: 'Claude Sonnet 4',
          thinking: false,
          contextTokenLimit: 200000,
        },
        {
          id: 'qwen3.5-plus',
          apiModel: 'qwen3.5-plus',
          displayName: 'Qwen3.5 Plus',
          thinking: false,
          contextTokenLimit: 1000000,
        },
        {
          id: 'glm-5',
          apiModel: 'glm-5',
          displayName: 'GLM 5',
          thinking: false,
          contextTokenLimit: 200000,
        },
      ],
    },
  ],
}

setProvidersForTests(TEST_PROVIDERS)
