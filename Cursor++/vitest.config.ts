import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/server/tests/**/*.test.ts'],
    exclude: ['src/test/**', 'dist/**', 'node_modules/**'],
    // 全局 setup: 注入合成 providers,让所有测试里出现的 modelId
    // 都能命中 providersStore 反向索引 (不再走静默 anthropic fallback)。
    setupFiles: ['src/server/tests/setup.ts'],
    // 单文件首次 import 要把 protocol / checkpointRecovery 等重模块整包转换,
    // 并行跑全量时可达 4~6s, 默认 5s 预算会把正常测试判成超时
    // (实测 protocolKnowledgeBase 冷启动被误杀, 单跑仅 1.8s)。
    // 放宽到 20s 只吸收转换抖动, 真实挂死仍会失败。
    testTimeout: 20_000,
  },
})
