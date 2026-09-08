import type { AgentServerMessage } from '../../gen/agent_v1_pb'
import type { LLMContentBlock, LLMMessage } from '../llm/types'
import { logger } from '../../logger'
import { blobIdToBytes, decodeBlob, encodeBlob, jsonBlobDataFromClientBytes } from './blob'
import { cacheBlob, getCachedBlob } from './blobStore'
import { fetchBlobsFromClient } from './clientBlobFetch'
import type { AgentSession } from './session'
import { kvMessage } from './stream'
import { normalizeBlobMessage, restoreBlobMessageToLLMMessage } from './transcript'
import { createRepairDiagnostics, hasRepairMutations, repairConversationHistory, type RepairDiagnostics } from '../llm/transformMessages'

export interface HistoryEntry {
  blobId: string
  raw: Record<string, unknown>
  message: LLMMessage
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object'
}

export function* sendAndCacheBlob(
  buildKvMessage: (id: number, blobId: string, blobData: string) => AgentServerMessage,
  id: number,
  data: { role: string, content: unknown, toolCallId?: string, toolName?: string, isError?: boolean, providerOptions?: Record<string, unknown> },
  blobIds: string[],
): Generator<AgentServerMessage, void, void> {
  const normalized = normalizeBlobMessage(data)
  const blob = encodeBlob(normalized)
  blobIds.push(blob.blobId)
  cacheBlob(blob.blobId, blob.blobData)
  yield buildKvMessage(id, blob.blobId, blob.blobData)
}

export function* flushMessageBlobs(
  buildKvMessage: (id: number, blobId: string, blobData: string) => AgentServerMessage,
  messages: LLMMessage[],
  startIndex: number,
  blobCounter: number,
  blobIds: string[],
): Generator<AgentServerMessage, { nextIndex: number, blobCounter: number }, void> {
  let nextIndex = startIndex
  let nextBlobCounter = blobCounter

  for (let i = startIndex; i < messages.length; i++) {
    const msg = messages[i]
    yield* sendAndCacheBlob(buildKvMessage, ++nextBlobCounter, {
      role: msg.role,
      content: msg.content,
      toolCallId: msg.toolCallId,
      toolName: msg.toolName,
      isError: msg.isError,
      providerOptions: msg.providerOptions,
    }, blobIds)
    nextIndex = i + 1
  }

  return { nextIndex, blobCounter: nextBlobCounter }
}

export function extractPlainTextContent(message: LLMMessage): string {
  if (typeof message.content === 'string')
    return message.content
  return message.content
    .filter((block): block is Extract<LLMContentBlock, { type: 'text' | 'thinking' }> => block.type === 'text' || block.type === 'thinking')
    .map(block => block.text)
    .join('')
}

export function extractComparableUserTexts(message: LLMMessage): string[] {
  const text = extractPlainTextContent(message).trim()
  if (!text)
    return []

  const values = new Set<string>([text])
  // eslint-disable-next-line regexp/no-super-linear-backtracking
  const userQueryMatches = text.matchAll(/<user_query>\s*([\s\S]*?)\s*<\/user_query>/g)
  for (const match of userQueryMatches) {
    const inner = match[1]?.trim()
    if (inner)
      values.add(inner)
  }

  return [...values]
}

export function hasSystemMessage(messages: LLMMessage[]): boolean {
  return messages.some(message => message.role === 'system')
}

export function isPreambleUserMessage(message: LLMMessage): boolean {
  if (message.role !== 'user' || typeof message.content !== 'string')
    return false
  return message.content.includes('<user_info>')
    && !message.content.includes('<user_query>')
}

export function hasPreambleUserMessage(messages: LLMMessage[]): boolean {
  return messages.some(isPreambleUserMessage)
}

function syncConversationScaffold(messages: LLMMessage[], systemMessage: LLMMessage, preambleUserMessage: LLMMessage): { messages: LLMMessage[], systemReplaced: boolean, preambleReplaced: boolean } {
  const next = [...messages]
  let systemReplaced = false
  let preambleReplaced = false

  const systemIndex = next.findIndex(message => message.role === 'system')
  if (systemIndex >= 0 && next[systemIndex]?.content !== systemMessage.content) {
    next[systemIndex] = systemMessage
    systemReplaced = true
  }

  const preambleIndex = next.findIndex(isPreambleUserMessage)
  if (preambleIndex >= 0 && next[preambleIndex]?.content !== preambleUserMessage.content) {
    next[preambleIndex] = preambleUserMessage
    preambleReplaced = true
  }

  return { messages: next, systemReplaced, preambleReplaced }
}

export function mergePrependUserMessages(
  messages: LLMMessage[],
  prependUserMessages: Array<{ text: string, messageId?: string }>,
): { messages: LLMMessage[], insertedTexts: string[] } {
  if (prependUserMessages.length === 0)
    return { messages, insertedTexts: [] }

  const existingUserTexts = new Set(
    messages
      .filter(message => message.role === 'user')
      .flatMap(extractComparableUserTexts)
      .filter(text => text.length > 0),
  )

  const missing = prependUserMessages
    .map(entry => entry.text)
    .filter(text => text.length > 0 && !existingUserTexts.has(text))

  if (missing.length === 0) {
    logger.info({ prependCount: prependUserMessages.length }, '[SESSION] prepend user messages already satisfied by history')
    return { messages, insertedTexts: [] }
  }

  const insertAt = messages.findIndex(message => message.role !== 'system' && !isPreambleUserMessage(message))
  const prefix = missing.map(text => ({ role: 'user' as const, content: text }))

  logger.info({
    prependCount: prependUserMessages.length,
    missingCount: missing.length,
    firstMissing: missing[0],
  }, '[SESSION] merging prepend user messages into history')

  if (insertAt === -1)
    return { messages: [...messages, ...prefix], insertedTexts: missing }
  return {
    messages: [...messages.slice(0, insertAt), ...prefix, ...messages.slice(insertAt)],
    insertedTexts: missing,
  }
}

/**
 * 摘要 blob 判定 — 双保险 (设计文档 §6 Q6):
 *   1. providerOptions.cursor.isSummary 语义标记 (修复后透传, 语义根治);
 *   2. 内容前缀 fallback: assistant 且以 `Previous conversation summary:` 开头
 *      (本插件格式) 或官方 `[Previous conversation summary]: ` 格式 ——
 *      对修复上线前的存量摘要 blob 立即生效 (标记已丢, 只剩前缀)。
 */
const SUMMARY_CONTENT_PREFIXES = [
  'Previous conversation summary:',
  '[Previous conversation summary]:',
] as const

function extractLeadingTextFromContent(content: unknown): string {
  if (typeof content === 'string')
    return content
  if (Array.isArray(content)) {
    const firstTextBlock = content.find(
      (block): block is Record<string, unknown> => isRecord(block) && block.type === 'text',
    )
    return typeof firstTextBlock?.text === 'string' ? firstTextBlock.text : ''
  }
  return ''
}

export function isSummaryBlobMessage(raw: Record<string, unknown>): boolean {
  const providerOptions = raw.providerOptions
  if (isRecord(providerOptions)) {
    const cursor = providerOptions.cursor
    if (isRecord(cursor) && cursor.isSummary === true)
      return true
  }
  if (raw.role === 'assistant') {
    const text = extractLeadingTextFromContent(raw.content).trimStart()
    return SUMMARY_CONTENT_PREFIXES.some(prefix => text.startsWith(prefix))
  }
  return false
}

export function hydrateHistoryEntries(blobIds: string[]): HistoryEntry[] {
  const entries: HistoryEntry[] = []
  for (const blobId of blobIds) {
    const blobData = getCachedBlob(blobId)
    if (!blobData)
      continue
    try {
      const decoded = decodeBlob(blobData)
      if (!isRecord(decoded))
        continue
      const restored = restoreBlobMessageToLLMMessage(decoded)
      if (!restored)
        continue
      entries.push({ blobId, raw: decoded, message: restored })
    }
    catch (error) {
      logger.warn({ blobId, error: (error as Error).message }, '[SESSION] failed to hydrate history entry')
    }
  }
  return entries
}

export function materializeHistoryEntries(messages: LLMMessage[]): HistoryEntry[] {
  return messages.map((message) => {
    const normalized = normalizeBlobMessage({
      role: message.role,
      content: message.content,
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      isError: message.isError,
      providerOptions: message.providerOptions,
    })
    const blob = encodeBlob(normalized)
    cacheBlob(blob.blobId, blob.blobData)
    return {
      blobId: blob.blobId,
      raw: normalized as unknown as Record<string, unknown>,
      message,
    }
  })
}

function logHistoryRepair(stage: string, diagnostics: RepairDiagnostics, extra: Record<string, unknown> = {}): void {
  if (!hasRepairMutations(diagnostics))
    return
  logger.debug({
    stage,
    ...extra,
    ...diagnostics,
  }, '[HISTORY_REPAIR] canonicalized conversation history')
}

export function repairHistoryEntries(entries: HistoryEntry[]): HistoryEntry[] {
  const diagnostics = createRepairDiagnostics(entries.length)
  const repaired = repairConversationHistory(entries.map(entry => entry.message), diagnostics)
  logHistoryRepair('repairHistoryEntries', diagnostics, {
    entryCount: entries.length,
    inputBlobIds: entries.length,
  })
  return materializeHistoryEntries(repaired)
}

export interface HistoryBlobLoadParams {
  historyBlobIds: string[]
  /** 本 run 的客户端会话; null (测试 / 特殊路径) 时只查内存, 缺失静默跳过 */
  session: AgentSession | null
  /** kvServerMessage.getBlobArgs 的 id 分配器 (agentOrchestrator 全 run 共用一个计数器) */
  allocateBlobId: () => number
}

function collectUncachedBlobIds(blobIds: string[]): string[] {
  return [...new Set(blobIds.filter(blobId => getCachedBlob(blobId) === undefined))]
}

/**
 * 装载对话历史: 内存未命中的 blob 经 getBlobArgs 向客户端取回, 再 hydrate。
 * 客户端是 blob 的唯一持久持有方, 这是历史 blob 进入服务端的唯一途径。
 *
 * `history blobs from cache` 日志字段是用户核验的依据, 字段名保持稳定:
 *   requestedBlobs / cachedBlobs / fetchedFromClient / stillMissing / resolvedBlobs
 *   恒有 cachedBlobs + fetchedFromClient + stillMissing === requestedBlobs。
 */
export async function* loadHistoryEntries(params: HistoryBlobLoadParams): AsyncGenerator<AgentServerMessage, HistoryEntry[], void> {
  const missingBlobIds = collectUncachedBlobIds(params.historyBlobIds)
  if (missingBlobIds.length > 0) {
    const fetchedBytes = yield* fetchBlobsFromClient({
      session: params.session,
      blobIds: missingBlobIds.map(blobIdToBytes),
      allocateBlobId: params.allocateBlobId,
    })
    fetchedBytes.forEach((bytes, index) => {
      const blobData = bytes ? jsonBlobDataFromClientBytes(bytes) : null
      if (blobData)
        cacheBlob(missingBlobIds[index]!, blobData)
      else if (bytes)
        logger.warn({ blobId: missingBlobIds[index] }, '[SESSION] history blob from client is not a decodable message blob')
    })
  }

  const stillMissing = params.historyBlobIds.filter(blobId => getCachedBlob(blobId) === undefined).length
  const historyEntries = hydrateHistoryEntries(params.historyBlobIds)
  const cachedBlobs = params.historyBlobIds.length - missingBlobIds.length
  logger.info({
    requestedBlobs: params.historyBlobIds.length,
    cachedBlobs,
    fetchedFromClient: params.historyBlobIds.length - cachedBlobs - stillMissing,
    stillMissing,
    resolvedBlobs: historyEntries.length,
  }, '[SESSION] history blobs from cache')
  return historyEntries
}

/**
 * 压缩产物里客户端尚未持有的 root blob 必须经 setBlobArgs 送达。
 *
 * createCompactionArtifacts 的 nextRootBlobIds 含 repair 重编码的条目、占位 / 锚点副本,
 * 这些 blobId 是服务端本轮新造的, 客户端从未收到过; 若只发 checkpoint 不发 blob, 客户端
 * checkpoint 就引用了它没有的 blob —— 服务端不落盘, 重启后即成历史空洞。
 * 返回实际补发数; 起始 kv id 由调用方给出 (与摘要 / 归档 blob 的 id 连续)。
 */
export function* sendRootBlobsUnknownToClient(
  nextRootBlobIds: string[],
  clientKnownBlobIds: Iterable<string>,
  firstKvMessageId: number,
): Generator<AgentServerMessage, number, void> {
  const known = new Set(clientKnownBlobIds)
  let sent = 0
  for (const blobId of nextRootBlobIds) {
    if (known.has(blobId))
      continue
    known.add(blobId)
    const blobData = getCachedBlob(blobId)
    if (!blobData) {
      logger.warn({ blobId }, '[SESSION] compacted root blob missing from cache; client checkpoint will reference it anyway')
      continue
    }
    yield kvMessage(firstKvMessageId + sent, blobId, blobData)
    sent++
  }
  if (sent > 0)
    logger.info({ sent, rootBlobs: nextRootBlobIds.length }, '[SESSION] sent compacted root blobs unknown to client')
  return sent
}

export async function* rebuildConversationHistory(params: {
  historyBlobIds: string[]
  prependUserMessages: Array<{ text: string, messageId?: string }>
  systemMessage: LLMMessage
  preambleUserMessage: LLMMessage
  currentUserMessage: LLMMessage
  systemContent: string
  preambleUserContent: string
  sendSystemScaffoldBlob: (data: { role: string, content: unknown, toolCallId?: string, toolName?: string, isError?: boolean }) => Generator<AgentServerMessage, void, void>
  sendOrderedBlob: (data: { role: string, content: unknown, toolCallId?: string, toolName?: string, isError?: boolean }) => Generator<AgentServerMessage, void, void>
} & HistoryBlobLoadParams): AsyncGenerator<AgentServerMessage, { messages: LLMMessage[], insertedPrependUserTexts: string[] }, void> {
  let messages: LLMMessage[] = []
  let insertedPrependUserTexts: string[] = []

  if (params.historyBlobIds.length > 0) {
    const historyEntries = yield* loadHistoryEntries(params)
    messages = historyEntries.map(entry => entry.message)

    const scaffoldSynced = syncConversationScaffold(messages, params.systemMessage, params.preambleUserMessage)
    messages = scaffoldSynced.messages
    if (scaffoldSynced.systemReplaced || scaffoldSynced.preambleReplaced) {
      logger.debug({
        systemReplaced: scaffoldSynced.systemReplaced,
        preambleReplaced: scaffoldSynced.preambleReplaced,
      }, '[HISTORY_REPAIR] replaced provider-specific scaffold in restored history')
    }

    if (!hasSystemMessage(messages)) {
      messages.unshift(params.systemMessage)
      yield* params.sendSystemScaffoldBlob({ role: 'system', content: params.systemContent })
    }

    if (!hasPreambleUserMessage(messages)) {
      const insertAt = messages.findIndex(message => message.role !== 'system')
      if (insertAt === -1)
        messages.push(params.preambleUserMessage)
      else messages.splice(insertAt, 0, params.preambleUserMessage)
      yield* params.sendOrderedBlob({ role: 'user', content: params.preambleUserContent })
    }

    ({ messages, insertedTexts: insertedPrependUserTexts } = mergePrependUserMessages(messages, params.prependUserMessages))
    messages.push(params.currentUserMessage)
  }
  else {
    messages.push(params.systemMessage)
    yield* params.sendSystemScaffoldBlob({ role: 'system', content: params.systemContent })

    messages.push(params.preambleUserMessage)
    yield* params.sendOrderedBlob({ role: 'user', content: params.preambleUserContent });

    ({ messages, insertedTexts: insertedPrependUserTexts } = mergePrependUserMessages(messages, params.prependUserMessages))
    messages.push(params.currentUserMessage)
  }

  const diagnostics = createRepairDiagnostics(messages.length)
  const repaired = repairConversationHistory(messages, diagnostics)
  logHistoryRepair('rebuildConversationHistory', diagnostics, {
    historyBlobIds: params.historyBlobIds.length,
    insertedPrependUserTexts: insertedPrependUserTexts.length,
  })

  return {
    messages: repaired,
    insertedPrependUserTexts,
  }
}
