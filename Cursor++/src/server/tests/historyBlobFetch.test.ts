import type { AgentServerMessage } from '../gen/agent_v1_pb'
import type { AgentSession } from '../handlers/agent/session'
import type { LLMMessage } from '../handlers/llm/types'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { loadPersistedBlob } from '../database/blobs'
import { closeAgentDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { encodeBlob } from '../handlers/agent/blob'
import { cacheBlob, getCachedBlob, resetBlobCacheForTests } from '../handlers/agent/blobStore'
import {
  encodeHistoryBlobIdBytes,
  fetchMissingHistoryBlobs,
  HISTORY_BLOB_FETCH_BATCH_SIZE,
  normalizeFetchedHistoryBlobData,
} from '../handlers/agent/historyBlobFetch'
import { hydrateHistoryEntries, rebuildConversationHistory } from '../handlers/agent/historyManager'
import { parseRunRequest } from '../handlers/agent/protocol'
import { createEphemeralSession, markSessionClosed, pushSessionMessage } from '../handlers/agent/session'

/**
 * 对话历史 blob 回源 (getBlobArgs) — 阶段 1 兼容期行为锁。
 *
 * 模拟客户端 ControlledKvManager: 收到 kvServerMessage.getBlobArgs 后按 blobId
 * 查本地库, 回 kvClientMessage { id, getBlobResult: { blobData } }; 本地没有时
 * 回空 blobData (无 error), 读取抛错时带 error。JSON transport 把 bytes 编成 base64。
 */

interface FakeClientOptions {
  /** blobId (string 形态) → 客户端本地保存的原始 bytes */
  store: Map<string, Uint8Array>
  /** 这些 blobId 永不回包 (模拟超时) */
  neverRespond?: Set<string>
  /** 这些 blobId 回 error */
  respondWithError?: Set<string>
  /** 不回显 KvClientMessage.id (旧客户端) */
  omitRequestId?: boolean
  /** blobData 以原始 Uint8Array 回 (bidi 内存路径) 而非 base64 string (JSON transport) */
  rawBytes?: boolean
  /** 延迟回包 (macrotask), 用于观察一批里同时在飞的请求数 */
  deferReply?: boolean
}

interface FakeClient {
  /** 每条 getBlobArgs 请求, 按发出顺序 */
  requests: Array<{ kvRequestId: number, blobId: string }>
  /** 已发出但尚未回包的请求数的历史峰值序列 (每次回包前记录) */
  bursts: number[]
}

function decodeGetBlobArgs(frame: AgentServerMessage): { kvRequestId: number, blobId: string } | null {
  if (frame.message.case !== 'kvServerMessage')
    return null
  const kv = frame.message.value
  if (kv.message.case !== 'getBlobArgs')
    return null
  return { kvRequestId: kv.id, blobId: Buffer.from(kv.message.value.blobId).toString('utf-8') }
}

/**
 * 驱动 generator 并扮演客户端。返回 generator 的 return 值与观测记录。
 */
async function runWithFakeClient<TReturn>(
  session: AgentSession,
  generator: AsyncGenerator<AgentServerMessage, TReturn, void>,
  options: FakeClientOptions,
): Promise<{ result: TReturn, client: FakeClient, frames: AgentServerMessage[] }> {
  const client: FakeClient = { requests: [], bursts: [] }
  const frames: AgentServerMessage[] = []
  let outstanding = 0

  const reply = (request: { kvRequestId: number, blobId: string }): void => {
    client.bursts.push(outstanding)
    outstanding--
    const idPart = options.omitRequestId ? {} : { id: request.kvRequestId }
    if (options.respondWithError?.has(request.blobId)) {
      pushSessionMessage(session, { kvClientMessage: { ...idPart, getBlobResult: { error: { message: `boom:${request.blobId}` } } } })
      return
    }
    const bytes = options.store.get(request.blobId)
    if (!bytes) {
      pushSessionMessage(session, { kvClientMessage: { ...idPart, getBlobResult: {} } })
      return
    }
    const blobData = options.rawBytes ? bytes : Buffer.from(bytes).toString('base64')
    pushSessionMessage(session, { kvClientMessage: { ...idPart, getBlobResult: { blobData } } })
  }

  let step = await generator.next()
  while (!step.done) {
    const frame = step.value
    frames.push(frame)
    const request = decodeGetBlobArgs(frame)
    if (request) {
      client.requests.push(request)
      outstanding++
      if (!options.neverRespond?.has(request.blobId)) {
        if (options.deferReply)
          setTimeout(reply, 0, request)
        else
          reply(request)
      }
    }
    step = await generator.next()
  }
  return { result: step.value, client, frames }
}

function countGetBlobArgsFrames(frames: AgentServerMessage[]): number {
  return frames.filter(frame => decodeGetBlobArgs(frame) !== null).length
}

/** 造 n 条历史 blob (与生产一致: encodeBlob → base64(JSON) 文本, 客户端保存其 UTF-8 字节) */
function buildHistoryBlobs(count: number, prefix = 'msg'): Array<{ blobId: string, blobData: string, clientBytes: Uint8Array }> {
  return Array.from({ length: count }, (_, index) => {
    const role = index % 2 === 0 ? 'user' : 'assistant'
    const blob = encodeBlob({ role, content: `${prefix}-${index}` })
    return { ...blob, clientBytes: new TextEncoder().encode(blob.blobData) }
  })
}

function toClientStore(blobs: Array<{ blobId: string, clientBytes: Uint8Array }>): Map<string, Uint8Array> {
  return new Map(blobs.map(blob => [blob.blobId, blob.clientBytes]))
}

function allocator(start = 900_000): () => number {
  let next = start
  return () => next++
}

beforeEach(() => {
  resetBlobCacheForTests()
})

afterEach(() => {
  resetBlobCacheForTests()
})

describe('blobId / blobData 编码约定', () => {
  it('encodeHistoryBlobIdBytes 是 parseRunRequest.historyBlobIds 解码的逆运算', () => {
    const blob = encodeBlob({ role: 'user', content: 'x' })
    // 客户端回传 rootPromptMessagesJson 时, protobuf-es JSON 会把 bytes 编成 base64
    const wireForm = Buffer.from(encodeHistoryBlobIdBytes(blob.blobId)).toString('base64')
    const parsed = parseRunRequest({
      runRequest: {
        conversationId: 'c-enc',
        action: { userMessageAction: { userMessage: { text: 'q' }, requestContext: {} } },
        modelDetails: { modelId: 'm' },
        conversationState: { rootPromptMessagesJson: [wireForm] },
      },
    })
    expect(parsed.historyBlobIds).toEqual([blob.blobId])
    // 与 kvMessage(setBlobArgs) 用的 TextEncoder.encode(blobId) 完全一致
    expect(encodeHistoryBlobIdBytes(blob.blobId)).toEqual(new TextEncoder().encode(blob.blobId))
  })

  it('normalizeFetchedHistoryBlobData: 客户端原样回传的 base64 文本字节 → 直接作为 cacheBlob 值 (不再二次 base64)', () => {
    const blob = encodeBlob({ role: 'assistant', content: 'hello' })
    const clientBytes = new TextEncoder().encode(blob.blobData)
    expect(normalizeFetchedHistoryBlobData(clientBytes)).toBe(blob.blobData)
  })

  it('normalizeFetchedHistoryBlobData: 裸 JSON 字节 (非本服务端写入) → 编成 base64', () => {
    const rawJsonBytes = new TextEncoder().encode('{"role":"user","content":"raw"}')
    const normalized = normalizeFetchedHistoryBlobData(rawJsonBytes)
    expect(normalized).toBe(Buffer.from(rawJsonBytes).toString('base64'))
    cacheBlob('raw-json-blob', normalized!)
    expect(hydrateHistoryEntries(['raw-json-blob'])[0]?.message).toEqual({ role: 'user', content: 'raw' })
  })

  it('normalizeFetchedHistoryBlobData: 空 / 不可解码内容 → null', () => {
    expect(normalizeFetchedHistoryBlobData(new Uint8Array(0))).toBeNull()
    expect(normalizeFetchedHistoryBlobData(new TextEncoder().encode('not base64 nor json!!'))).toBeNull()
    expect(normalizeFetchedHistoryBlobData(new TextEncoder().encode(Buffer.from('"just a string"').toString('base64')))).toBeNull()
  })
})

describe('fetchMissingHistoryBlobs', () => {
  it('无 session → 不发帧, 全部 failed', async () => {
    const blobs = buildHistoryBlobs(3)
    const generator = fetchMissingHistoryBlobs({
      session: null,
      missingBlobIds: blobs.map(blob => blob.blobId),
      allocateBlobId: allocator(),
    })
    const frames: AgentServerMessage[] = []
    let step = await generator.next()
    while (!step.done) {
      frames.push(step.value)
      step = await generator.next()
    }
    expect(frames).toEqual([])
    expect(step.value).toEqual({ fetched: 0, failed: 3 })
    expect(getCachedBlob(blobs[0]!.blobId)).toBeUndefined()
  })

  it('缺失 blob 全部从客户端取回并落缓存 (JSON transport base64 string 形态)', async () => {
    const blobs = buildHistoryBlobs(5)
    const session = createEphemeralSession('fetch-all')
    const { result, client } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store: toClientStore(blobs) },
    )
    expect(result).toEqual({ fetched: 5, failed: 0 })
    expect(client.requests.map(request => request.blobId)).toEqual(blobs.map(blob => blob.blobId))
    // 请求 id 来自 allocateBlobId, 单调递增且从 900_000 起
    expect(client.requests.map(request => request.kvRequestId)).toEqual([900_000, 900_001, 900_002, 900_003, 900_004])
    for (const blob of blobs)
      expect(getCachedBlob(blob.blobId)).toBe(blob.blobData)
    expect(hydrateHistoryEntries(blobs.map(blob => blob.blobId)).map(entry => entry.message)).toEqual(
      blobs.map((_, index) => ({ role: index % 2 === 0 ? 'user' : 'assistant', content: `msg-${index}` })),
    )
    // 回包已被消费, 队列不残留
    expect(session.messages).toEqual([])
  })

  it('blobData 以原始 Uint8Array 回包时同样归一 (bidi 内存路径)', async () => {
    const blobs = buildHistoryBlobs(2)
    const session = createEphemeralSession('fetch-raw')
    const { result } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store: toClientStore(blobs), rawBytes: true },
    )
    expect(result).toEqual({ fetched: 2, failed: 0 })
    expect(getCachedBlob(blobs[1]!.blobId)).toBe(blobs[1]!.blobData)
  })

  it('重复的 blobId 只取一次', async () => {
    const blobs = buildHistoryBlobs(2)
    const session = createEphemeralSession('fetch-dedupe')
    const { result, client } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({
        session,
        missingBlobIds: [blobs[0]!.blobId, blobs[1]!.blobId, blobs[0]!.blobId],
        allocateBlobId: allocator(),
      }),
      { store: toClientStore(blobs) },
    )
    expect(result).toEqual({ fetched: 2, failed: 0 })
    expect(client.requests).toHaveLength(2)
  })

  it('客户端回 error / 本地没有 → 对应 blob failed, 其余正常', async () => {
    const blobs = buildHistoryBlobs(4)
    const store = toClientStore(blobs)
    store.delete(blobs[2]!.blobId) // 客户端也没有 → 空回包
    const session = createEphemeralSession('fetch-partial-fail')
    const { result } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store, respondWithError: new Set([blobs[1]!.blobId]) },
    )
    expect(result).toEqual({ fetched: 2, failed: 2 })
    expect(getCachedBlob(blobs[0]!.blobId)).toBe(blobs[0]!.blobData)
    expect(getCachedBlob(blobs[1]!.blobId)).toBeUndefined()
    expect(getCachedBlob(blobs[2]!.blobId)).toBeUndefined()
    expect(getCachedBlob(blobs[3]!.blobId)).toBe(blobs[3]!.blobData)
  })

  it('单条超时 → 该 blob failed, 后续 blob 仍取回, 不阻塞', async () => {
    const blobs = buildHistoryBlobs(4)
    const session = createEphemeralSession('fetch-single-timeout')
    const startedAt = Date.now()
    const { result } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({
        session,
        missingBlobIds: blobs.map(blob => blob.blobId),
        allocateBlobId: allocator(),
        singleTimeoutMs: 60,
      }),
      { store: toClientStore(blobs), neverRespond: new Set([blobs[2]!.blobId]) },
    )
    expect(result).toEqual({ fetched: 3, failed: 1 })
    expect(getCachedBlob(blobs[2]!.blobId)).toBeUndefined()
    expect(getCachedBlob(blobs[3]!.blobId)).toBe(blobs[3]!.blobData)
    expect(Date.now() - startedAt).toBeLessThan(1_000)
  })

  it('探针超时 (通道无响应) → 只发 1 帧即整体放弃, 全部 failed', async () => {
    const blobs = buildHistoryBlobs(10)
    const session = createEphemeralSession('fetch-probe-timeout')
    const { result, client, frames } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({
        session,
        missingBlobIds: blobs.map(blob => blob.blobId),
        allocateBlobId: allocator(),
        singleTimeoutMs: 50,
      }),
      { store: toClientStore(blobs), neverRespond: new Set(blobs.map(blob => blob.blobId)) },
    )
    expect(result).toEqual({ fetched: 0, failed: 10 })
    expect(client.requests).toHaveLength(1)
    expect(countGetBlobArgsFrames(frames)).toBe(1)
  })

  it('整体超时 → 剩余 blob 不再请求、标记 failed, 已取回的保留', async () => {
    const blobs = buildHistoryBlobs(6)
    const session = createEphemeralSession('fetch-total-timeout')
    const startedAt = Date.now()
    const { result, client } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({
        session,
        missingBlobIds: blobs.map(blob => blob.blobId),
        allocateBlobId: allocator(),
        batchSize: 2,
        singleTimeoutMs: 40,
        totalTimeoutMs: 60,
      }),
      // 探针 (blob 0) 立即成功; blob 1 等满 40ms 超时; blob 2 只剩 ~20ms 预算再超时 →
      // 总预算 60ms 耗尽, blob 3 (同批已发出) 与 4/5 (未发出) 全部按 skipped 计
      { store: toClientStore(blobs), neverRespond: new Set([blobs[1]!.blobId, blobs[2]!.blobId]) },
    )
    expect(result).toEqual({ fetched: 1, failed: 5 })
    expect(getCachedBlob(blobs[0]!.blobId)).toBe(blobs[0]!.blobData)
    const requestedBlobIds = client.requests.map(request => request.blobId)
    expect(requestedBlobIds.slice(0, 3)).toEqual([blobs[0]!.blobId, blobs[1]!.blobId, blobs[2]!.blobId])
    expect(requestedBlobIds).not.toContain(blobs[4]!.blobId)
    expect(requestedBlobIds).not.toContain(blobs[5]!.blobId)
    // 整体耗时受 totalTimeoutMs 约束, 不是 6 × singleTimeoutMs
    expect(Date.now() - startedAt).toBeLessThan(200)
  })

  it('并发批次: 40 个缺失 → 探针 1 + 首批余下 31 + 次批 8 (≤ 32 在飞)', async () => {
    expect(HISTORY_BLOB_FETCH_BATCH_SIZE).toBe(32)
    const blobs = buildHistoryBlobs(40)
    const session = createEphemeralSession('fetch-batches')
    const { result, client } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store: toClientStore(blobs), deferReply: true },
    )
    expect(result).toEqual({ fetched: 40, failed: 0 })
    expect(client.requests).toHaveLength(40)
    // bursts[i] = 第 i 次回包时在飞的请求数: 探针阶段为 1; 首批余下 31 个一次性发出,
    // 第一次回包时在飞 31 → 之后递减; 次批 8 个同理。任何时刻在飞数不超过 32。
    expect(Math.max(...client.bursts)).toBe(31)
    expect(client.bursts[0]).toBe(1)
    expect(client.bursts[1]).toBe(31)
    expect(client.bursts[32]).toBe(8)
    // 批次组成: 前 32 个 id 属于首批 (探针 + 31), 后 8 个属于次批
    expect(client.requests.slice(0, 32).map(request => request.blobId)).toEqual(blobs.slice(0, 32).map(blob => blob.blobId))
    expect(client.requests.slice(32).map(request => request.blobId)).toEqual(blobs.slice(32).map(blob => blob.blobId))
    for (const blob of blobs)
      expect(getCachedBlob(blob.blobId)).toBe(blob.blobData)
  })

  it('旧客户端不回显 id → 退化为严格串行 (任何时刻只有 1 个在飞)', async () => {
    const blobs = buildHistoryBlobs(7)
    const session = createEphemeralSession('fetch-legacy-serial')
    const { result, client } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator(), batchSize: 4 }),
      { store: toClientStore(blobs), omitRequestId: true, deferReply: true },
    )
    expect(result).toEqual({ fetched: 7, failed: 0 })
    expect(client.bursts).toEqual([1, 1, 1, 1, 1, 1, 1])
    for (const blob of blobs)
      expect(getCachedBlob(blob.blobId)).toBe(blob.blobData)
  })

  it('并发模式下无 id 的陌生回包不会被串到别的请求上', async () => {
    const blobs = buildHistoryBlobs(3)
    const session = createEphemeralSession('fetch-strict-id')
    // 队列里预先塞一条无 id 的陈旧 getBlobResult (内容是另一个 blob)
    const stale = encodeBlob({ role: 'user', content: 'stale' })
    pushSessionMessage(session, { kvClientMessage: { getBlobResult: { blobData: Buffer.from(new TextEncoder().encode(stale.blobData)).toString('base64') } } })

    const { result } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store: toClientStore(blobs) },
    )
    // 探针允许无 id 回包 —— 它会吃掉这条陈旧包 (与 fetchPartBytes 的兼容语义一致),
    // 但内容经 sha256 之外的 JSON 校验后仍被缓存到探针 blobId 下; 这是旧客户端兼容的既有代价。
    // 关键断言: 并发阶段 (第 2、3 个) 只认各自 id, 结果正确。
    expect(result.fetched + result.failed).toBe(3)
    expect(getCachedBlob(blobs[1]!.blobId)).toBe(blobs[1]!.blobData)
    expect(getCachedBlob(blobs[2]!.blobId)).toBe(blobs[2]!.blobData)
  })
})

describe('rebuildConversationHistory 接线', () => {
  const scaffold = {
    prependUserMessages: [],
    systemMessage: { role: 'system', content: 'sys' } as LLMMessage,
    preambleUserMessage: { role: 'user', content: '<user_info>env</user_info>' } as LLMMessage,
    currentUserMessage: { role: 'user', content: '继续' } as LLMMessage,
    systemContent: 'sys',
    preambleUserContent: '<user_info>env</user_info>',
    * sendSystemScaffoldBlob() {},
    * sendOrderedBlob() {},
  }

  function buildScaffoldedHistory(count: number) {
    const system = encodeBlob({ role: 'system', content: 'sys' })
    const preamble = encodeBlob({ role: 'user', content: '<user_info>env</user_info>' })
    const body = buildHistoryBlobs(count, 'turn')
    const all = [
      { ...system, clientBytes: new TextEncoder().encode(system.blobData) },
      { ...preamble, clientBytes: new TextEncoder().encode(preamble.blobData) },
      ...body,
    ]
    return { all, body }
  }

  it('全部命中缓存 → 不发任何 getBlobArgs 帧', async () => {
    const { all } = buildScaffoldedHistory(4)
    for (const blob of all)
      cacheBlob(blob.blobId, blob.blobData)
    const session = createEphemeralSession('rebuild-all-cached')
    const { result, frames } = await runWithFakeClient(
      session,
      rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session, allocateBlobId: allocator() }),
      { store: toClientStore(all) },
    )
    expect(countGetBlobArgsFrames(frames)).toBe(0)
    expect(result.messages).toHaveLength(all.length + 1) // + currentUserMessage
    expect(result.messages.at(-1)).toEqual({ role: 'user', content: '继续' })
  })

  it('部分缺失 → 只对缺失 id 发帧, 回包后历史完整', async () => {
    const { all, body } = buildScaffoldedHistory(6)
    const missing = [body[1]!, body[4]!]
    for (const blob of all) {
      if (!missing.includes(blob))
        cacheBlob(blob.blobId, blob.blobData)
    }
    const session = createEphemeralSession('rebuild-partial')
    const { result, client } = await runWithFakeClient(
      session,
      rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session, allocateBlobId: allocator() }),
      { store: toClientStore(all) },
    )
    expect(client.requests.map(request => request.blobId)).toEqual(missing.map(blob => blob.blobId))
    // 历史完整: system + preamble + 6 条 + 当前用户消息
    expect(result.messages).toHaveLength(all.length + 1)
    expect(result.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-0',
      'turn-1',
      'turn-2',
      'turn-3',
      'turn-4',
      'turn-5',
      '继续',
    ])
  })

  it('回包 error / 客户端也没有 → 对应 blob 跳过, 其余正常, 对话不中断', async () => {
    const { all, body } = buildScaffoldedHistory(5)
    const errored = body[0]!
    const missingEverywhere = body[3]!
    const store = toClientStore(all)
    store.delete(missingEverywhere.blobId)
    for (const blob of all) {
      if (blob !== errored && blob !== missingEverywhere && blob !== body[2])
        cacheBlob(blob.blobId, blob.blobData)
    }
    const session = createEphemeralSession('rebuild-errors')
    const { result, client } = await runWithFakeClient(
      session,
      rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session, allocateBlobId: allocator() }),
      { store, respondWithError: new Set([errored.blobId]) },
    )
    // 缺失 3 个: body[0] (error) / body[2] (正常取回) / body[3] (客户端也没有 → 空回包)
    expect(client.requests.map(request => request.blobId)).toEqual([errored.blobId, body[2]!.blobId, missingEverywhere.blobId])
    expect(result.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-1',
      'turn-2',
      'turn-4',
      '继续',
    ])
  })

  it('回源途中 session 关闭 (客户端断开) → 立即放弃剩余, rebuild 仍正常返回', async () => {
    const { all, body } = buildScaffoldedHistory(4)
    for (const blob of all) {
      if (blob !== body[1] && blob !== body[2])
        cacheBlob(blob.blobId, blob.blobData)
    }
    const session = createEphemeralSession('rebuild-session-closed')
    const startedAt = Date.now()
    const generator = rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session, allocateBlobId: allocator() })
    // 第一条 getBlobArgs 发出后直接关闭 session, 不回包
    let requests = 0
    let step = await generator.next()
    while (!step.done) {
      if (decodeGetBlobArgs(step.value)) {
        requests++
        markSessionClosed(session)
      }
      step = await generator.next()
    }
    expect(requests).toBe(1)
    expect(step.value.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-0',
      'turn-3',
      '继续',
    ])
    // 没有等 10s 超时
    expect(Date.now() - startedAt).toBeLessThan(2_000)
  })

  it('无 session → 直接跳过回源, 行为与现状一致 (缺失静默跳过, 不发帧)', async () => {
    const { all, body } = buildScaffoldedHistory(3)
    for (const blob of all) {
      if (blob !== body[1])
        cacheBlob(blob.blobId, blob.blobData)
    }
    const generator = rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session: null, allocateBlobId: allocator() })
    const frames: AgentServerMessage[] = []
    let step = await generator.next()
    while (!step.done) {
      frames.push(step.value)
      step = await generator.next()
    }
    expect(countGetBlobArgsFrames(frames)).toBe(0)
    expect(step.value.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-0',
      'turn-2',
      '继续',
    ])
  })
})

describe('阶段 1 双写: 取回的 blob 同时持久化到 SQLite', () => {
  let tempDir: string
  let prevDbPath: string | undefined

  beforeEach(async () => {
    prevDbPath = process.env.BYOK_AGENT_DB_PATH
    tempDir = mkdtempSync(join(tmpdir(), 'cursor-byok-history-fetch-'))
    process.env.BYOK_AGENT_DB_PATH = join(tempDir, 'cursor.db')
    await resetAgentDatabaseForTests()
  })

  afterEach(async () => {
    await closeAgentDatabase()
    if (prevDbPath === undefined)
      delete process.env.BYOK_AGENT_DB_PATH
    else process.env.BYOK_AGENT_DB_PATH = prevDbPath
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('fetchMissingHistoryBlobs 取回后 loadPersistedBlob 可读到同样的 base64 文本', async () => {
    const blobs = buildHistoryBlobs(2)
    const session = createEphemeralSession('fetch-persist')
    const { result } = await runWithFakeClient(
      session,
      fetchMissingHistoryBlobs({ session, missingBlobIds: blobs.map(blob => blob.blobId), allocateBlobId: allocator() }),
      { store: toClientStore(blobs) },
    )
    expect(result).toEqual({ fetched: 2, failed: 0 })
    await vi.waitFor(async () => {
      expect(await loadPersistedBlob(blobs[0]!.blobId)).toBe(blobs[0]!.blobData)
      expect(await loadPersistedBlob(blobs[1]!.blobId)).toBe(blobs[1]!.blobData)
    })
  })
})
