import type { AgentServerMessage } from '../gen/agent_v1_pb'
import type { AgentSession } from '../handlers/agent/session'
import type { LLMMessage } from '../handlers/llm/types'
import { mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { create, toBinary } from '@bufbuild/protobuf'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { closeAgentDatabase, getAgentDatabase, resetAgentDatabaseForTests } from '../database/sqlite'
import { ConversationTurnStructureSchema } from '../gen/agent_v1_pb'
import { binaryBlobDataFromClientBytes, blobIdToBytes, encodeBinaryBlob, encodeBlob, jsonBlobDataFromClientBytes } from '../handlers/agent/blob'
import { cacheBlob, getBlobCacheStats, getCachedBlob, resetBlobCacheForTests } from '../handlers/agent/blobStore'
import { CLIENT_BLOB_FETCH_BATCH_SIZE, fetchBlobsFromClient } from '../handlers/agent/clientBlobFetch'
import { hydrateHistoryEntries, loadHistoryEntries, rebuildConversationHistory, sendRootBlobsUnknownToClient } from '../handlers/agent/historyManager'
import { parseRunRequest } from '../handlers/agent/protocol'
import { createEphemeralSession, markSessionClosed, pushSessionMessage } from '../handlers/agent/session'
import { ensureTurnBlobCached, readTurnBaseline } from '../handlers/agent/turnTracker'

/**
 * 客户端是 blob 的唯一持久持有方 — 服务端经 getBlobArgs 取回的行为锁。
 *
 * 模拟客户端 ControlledKvManager: 收到 kvServerMessage.getBlobArgs 后按 blobId
 * 查本地库, 回 kvClientMessage { id, getBlobResult: { blobData } }; 本地没有时
 * 回空 blobData (无 error), 读取抛错时带 error。JSON transport 把 bytes 编成 base64。
 */

interface FakeClientOptions {
  /** blobId (string 形态) → 客户端本地保存的原始 bytes */
  store: Map<string, Uint8Array>
  /** 这些 blobId 永不回包 (模拟通道死亡) */
  neverRespond?: Set<string>
  /** 这些 blobId 回 error */
  respondWithError?: Set<string>
  /** blobData 以原始 Uint8Array 回 (bidi 内存路径) 而非 base64 string (JSON transport) */
  rawBytes?: boolean
  /** 延迟回包 (macrotask), 用于观察同时在飞的请求数 */
  deferReply?: boolean
}

interface FakeClient {
  requests: Array<{ kvRequestId: number, blobId: string }>
  /** 每次回包前记录的在飞请求数 */
  inFlightAtReply: number[]
}

function decodeGetBlobArgs(frame: AgentServerMessage): { kvRequestId: number, blobId: string } | null {
  if (frame.message.case !== 'kvServerMessage' || frame.message.value.message.case !== 'getBlobArgs')
    return null
  return { kvRequestId: frame.message.value.id, blobId: Buffer.from(frame.message.value.message.value.blobId).toString('utf-8') }
}

function decodeSetBlobArgs(frame: AgentServerMessage): { kvId: number, blobId: string, blobData: string } | null {
  if (frame.message.case !== 'kvServerMessage' || frame.message.value.message.case !== 'setBlobArgs')
    return null
  const value = frame.message.value.message.value
  return {
    kvId: frame.message.value.id,
    blobId: Buffer.from(value.blobId).toString('utf-8'),
    blobData: Buffer.from(value.blobData).toString('utf-8'),
  }
}

async function runWithFakeClient<TReturn>(
  session: AgentSession,
  generator: AsyncGenerator<AgentServerMessage, TReturn, void>,
  options: FakeClientOptions,
): Promise<{ result: TReturn, client: FakeClient, frames: AgentServerMessage[] }> {
  const client: FakeClient = { requests: [], inFlightAtReply: [] }
  const frames: AgentServerMessage[] = []
  let inFlight = 0

  const reply = (request: { kvRequestId: number, blobId: string }): void => {
    client.inFlightAtReply.push(inFlight)
    inFlight--
    if (options.respondWithError?.has(request.blobId)) {
      pushSessionMessage(session, { kvClientMessage: { id: request.kvRequestId, getBlobResult: { error: { message: `boom:${request.blobId}` } } } })
      return
    }
    const bytes = options.store.get(request.blobId)
    if (!bytes) {
      pushSessionMessage(session, { kvClientMessage: { id: request.kvRequestId, getBlobResult: {} } })
      return
    }
    const blobData = options.rawBytes ? bytes : Buffer.from(bytes).toString('base64')
    pushSessionMessage(session, { kvClientMessage: { id: request.kvRequestId, getBlobResult: { blobData } } })
  }

  let step = await generator.next()
  while (!step.done) {
    frames.push(step.value)
    const request = decodeGetBlobArgs(step.value)
    if (request) {
      client.requests.push(request)
      inFlight++
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

async function drain<TReturn>(generator: AsyncGenerator<AgentServerMessage, TReturn, void>): Promise<{ result: TReturn, frames: AgentServerMessage[] }> {
  const frames: AgentServerMessage[] = []
  let step = await generator.next()
  while (!step.done) {
    frames.push(step.value)
    step = await generator.next()
  }
  return { result: step.value, frames }
}

/** 造 n 条历史 blob (与生产一致: encodeBlob → base64(JSON) 文本, 客户端保存其 UTF-8 字节) */
function buildHistoryBlobs(count: number, prefix = 'msg'): Array<{ blobId: string, blobData: string, clientBytes: Uint8Array }> {
  return Array.from({ length: count }, (_, index) => {
    const blob = encodeBlob({ role: index % 2 === 0 ? 'user' : 'assistant', content: `${prefix}-${index}` })
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

describe('blobId / blobData 与客户端字节的互转', () => {
  it('blobIdToBytes 是 parseRunRequest.historyBlobIds 解码的逆运算', () => {
    const blob = encodeBlob({ role: 'user', content: 'x' })
    // 客户端回传 rootPromptMessagesJson 时, protobuf-es JSON 会把 bytes 编成 base64
    const wireForm = Buffer.from(blobIdToBytes(blob.blobId)).toString('base64')
    const parsed = parseRunRequest({
      runRequest: {
        conversationId: 'c-enc',
        action: { userMessageAction: { userMessage: { text: 'q' }, requestContext: {} } },
        modelDetails: { modelId: 'm' },
        conversationState: { rootPromptMessagesJson: [wireForm] },
      },
    })
    expect(parsed.historyBlobIds).toEqual([blob.blobId])
    expect(blobIdToBytes(blob.blobId)).toEqual(new TextEncoder().encode(blob.blobId))
  })

  it('jsonBlobDataFromClientBytes: 客户端原样回传的 base64 文本字节 → 直接作为缓存值 (不二次 base64)', () => {
    const blob = encodeBlob({ role: 'assistant', content: 'hello' })
    expect(jsonBlobDataFromClientBytes(new TextEncoder().encode(blob.blobData))).toBe(blob.blobData)
  })

  it('jsonBlobDataFromClientBytes: 裸 JSON 字节 → base64; 空 / 不可解码 → null', () => {
    const rawJsonBytes = new TextEncoder().encode('{"role":"user","content":"raw"}')
    const normalized = jsonBlobDataFromClientBytes(rawJsonBytes)
    expect(normalized).toBe(Buffer.from(rawJsonBytes).toString('base64'))
    cacheBlob('raw-json-blob', normalized!)
    expect(hydrateHistoryEntries(['raw-json-blob'])[0]?.message).toEqual({ role: 'user', content: 'raw' })

    expect(jsonBlobDataFromClientBytes(new Uint8Array(0))).toBeNull()
    expect(jsonBlobDataFromClientBytes(new TextEncoder().encode('not base64 nor json!!'))).toBeNull()
    expect(jsonBlobDataFromClientBytes(new TextEncoder().encode(Buffer.from('"just a string"').toString('base64')))).toBeNull()
  })

  it('binaryBlobDataFromClientBytes: raw protobuf 字节 → base64 缓存值, readTurnBaseline 可解', () => {
    const turn = encodeBinaryBlob(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
      turn: { case: 'agentConversationTurn', value: { userMessage: new TextEncoder().encode('user-blob'), steps: [], dynamicToolCount: 3 } },
    })))
    cacheBlob(turn.blobId, binaryBlobDataFromClientBytes(turn.blobDataRaw))
    expect(getCachedBlob(turn.blobId)).toBe(turn.blobData)
    expect(readTurnBaseline(turn.blobId)).toMatchObject({ userMessageBlobId: 'user-blob', dynamicToolCount: 3 })
  })
})

describe('fetchBlobsFromClient', () => {
  it('无 session → 不发帧, 全部 null', async () => {
    const { result, frames } = await drain(fetchBlobsFromClient({
      session: null,
      blobIds: [new Uint8Array([1]), new Uint8Array([2])],
      allocateBlobId: allocator(),
    }))
    expect(frames).toEqual([])
    expect(result).toEqual([null, null])
  })

  it('按下标返回字节; 请求 id 单调递增; 回包被消费不残留 (JSON transport base64 string)', async () => {
    const blobs = buildHistoryBlobs(5)
    const session = createEphemeralSession('fetch-all')
    const { result, client } = await runWithFakeClient(
      session,
      fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() }),
      { store: toClientStore(blobs) },
    )
    expect(result.map(bytes => bytes && Buffer.from(bytes).toString('utf-8'))).toEqual(blobs.map(blob => blob.blobData))
    expect(client.requests.map(request => request.kvRequestId)).toEqual([900_000, 900_001, 900_002, 900_003, 900_004])
    expect(session.messages).toEqual([])
  })

  it('blobData 以原始 Uint8Array 回包 (bidi 内存路径) 同样归一', async () => {
    const blobs = buildHistoryBlobs(2)
    const session = createEphemeralSession('fetch-raw')
    const { result } = await runWithFakeClient(
      session,
      fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() }),
      { store: toClientStore(blobs), rawBytes: true },
    )
    expect(result.map(bytes => bytes && Buffer.from(bytes).toString('utf-8'))).toEqual(blobs.map(blob => blob.blobData))
  })

  it('客户端回 error / 本地没有 → 对应位置 null, 其余正常', async () => {
    const blobs = buildHistoryBlobs(4)
    const store = toClientStore(blobs)
    store.delete(blobs[2]!.blobId)
    const session = createEphemeralSession('fetch-partial')
    const { result } = await runWithFakeClient(
      session,
      fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() }),
      { store, respondWithError: new Set([blobs[1]!.blobId]) },
    )
    expect(result.map(bytes => bytes !== null)).toEqual([true, false, false, true])
  })

  it('一次超时即视为通道已死: 剩余不再等待也不再发送', async () => {
    const blobs = buildHistoryBlobs(40)
    const session = createEphemeralSession('fetch-timeout')
    const startedAt = Date.now()
    const { result, client } = await runWithFakeClient(
      session,
      fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator(), timeoutMs: 50 }),
      { store: toClientStore(blobs), neverRespond: new Set([blobs[3]!.blobId]) },
    )
    // 首批 32 个已发出; 前 3 个成功, 第 4 个超时 → 放弃; 次批 8 个从未发出
    expect(client.requests).toHaveLength(32)
    expect(result.slice(0, 3).every(bytes => bytes !== null)).toBe(true)
    expect(result.slice(3).every(bytes => bytes === null)).toBe(true)
    expect(Date.now() - startedAt).toBeLessThan(500)
  })

  it('session 关闭 → 立即放弃, 不等超时', async () => {
    const blobs = buildHistoryBlobs(3)
    const session = createEphemeralSession('fetch-closed')
    const generator = fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() })
    const startedAt = Date.now()
    let requests = 0
    let step = await generator.next()
    while (!step.done) {
      if (decodeGetBlobArgs(step.value)) {
        requests++
        if (requests === 3)
          markSessionClosed(session)
      }
      step = await generator.next()
    }
    expect(step.value).toEqual([null, null, null])
    expect(Date.now() - startedAt).toBeLessThan(1_000)
  })

  it('分批: 40 个 → 32 + 8, 每批一次性发出后逐个按 id 收', async () => {
    expect(CLIENT_BLOB_FETCH_BATCH_SIZE).toBe(32)
    const blobs = buildHistoryBlobs(40)
    const session = createEphemeralSession('fetch-batches')
    const { result, client } = await runWithFakeClient(
      session,
      fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() }),
      { store: toClientStore(blobs), deferReply: true },
    )
    expect(result.every(bytes => bytes !== null)).toBe(true)
    expect(client.requests).toHaveLength(40)
    // 首批第一次回包时 32 个在飞, 次批第一次回包时 8 个在飞
    expect(client.inFlightAtReply[0]).toBe(32)
    expect(client.inFlightAtReply[32]).toBe(8)
    expect(Math.max(...client.inFlightAtReply)).toBe(32)
  })

  it('回包乱序到达也按 id 归位', async () => {
    const blobs = buildHistoryBlobs(3)
    const session = createEphemeralSession('fetch-out-of-order')
    const generator = fetchBlobsFromClient({ session, blobIds: blobs.map(blob => blobIdToBytes(blob.blobId)), allocateBlobId: allocator() })
    const pending: Array<{ kvRequestId: number, blobId: string }> = []
    let step = await generator.next()
    while (!step.done) {
      const request = decodeGetBlobArgs(step.value)
      if (request)
        pending.push(request)
      if (pending.length === 3 && session.messages.length === 0) {
        for (const request of [...pending].reverse()) {
          const bytes = toClientStore(blobs).get(request.blobId)!
          pushSessionMessage(session, { kvClientMessage: { id: request.kvRequestId, getBlobResult: { blobData: Buffer.from(bytes).toString('base64') } } })
        }
      }
      step = await generator.next()
    }
    expect(step.value.map(bytes => bytes && Buffer.from(bytes).toString('utf-8'))).toEqual(blobs.map(blob => blob.blobData))
  })
})

describe('loadHistoryEntries / rebuildConversationHistory', () => {
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

  it('全部命中内存 → 不发任何 getBlobArgs 帧', async () => {
    const { all } = buildScaffoldedHistory(4)
    for (const blob of all)
      cacheBlob(blob.blobId, blob.blobData)
    const session = createEphemeralSession('rebuild-all-cached')
    const { result, client } = await runWithFakeClient(
      session,
      rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session, allocateBlobId: allocator() }),
      { store: toClientStore(all) },
    )
    expect(client.requests).toEqual([])
    expect(result.messages).toHaveLength(all.length + 1)
    expect(result.messages.at(-1)).toEqual({ role: 'user', content: '继续' })
  })

  it('部分缺失 → 只对缺失 id 发帧 (去重), 回包后历史完整并落缓存', async () => {
    const { all, body } = buildScaffoldedHistory(6)
    const missing = [body[1]!, body[4]!]
    for (const blob of all) {
      if (!missing.includes(blob))
        cacheBlob(blob.blobId, blob.blobData)
    }
    const session = createEphemeralSession('rebuild-partial')
    const historyBlobIds = [...all.map(blob => blob.blobId), body[4]!.blobId]
    const { result, client } = await runWithFakeClient(
      session,
      rebuildConversationHistory({ ...scaffold, historyBlobIds, session, allocateBlobId: allocator() }),
      { store: toClientStore(all) },
    )
    expect(client.requests.map(request => request.blobId)).toEqual(missing.map(blob => blob.blobId))
    expect(getCachedBlob(body[1]!.blobId)).toBe(body[1]!.blobData)
    expect(result.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-0',
      'turn-1',
      'turn-2',
      'turn-3',
      'turn-4',
      'turn-5',
      'turn-4',
      '继续',
    ])
  })

  it('回包 error / 客户端也没有 → 对应条目跳过, 其余正常, 对话不中断', async () => {
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

  it('无 session → 只查内存, 缺失静默跳过, 不发帧', async () => {
    const { all, body } = buildScaffoldedHistory(3)
    for (const blob of all) {
      if (blob !== body[1])
        cacheBlob(blob.blobId, blob.blobData)
    }
    const { result, frames } = await drain(rebuildConversationHistory({ ...scaffold, historyBlobIds: all.map(blob => blob.blobId), session: null, allocateBlobId: allocator() }))
    expect(frames.filter(decodeGetBlobArgs)).toEqual([])
    expect(result.messages.map(message => message.content)).toEqual([
      'sys',
      '<user_info>env</user_info>',
      'turn-0',
      'turn-2',
      '继续',
    ])
  })

  it('loadHistoryEntries 直接可供 summarize 路径使用: 取回 + hydrate', async () => {
    const blobs = buildHistoryBlobs(3)
    const session = createEphemeralSession('load-entries')
    const { result, client } = await runWithFakeClient(
      session,
      loadHistoryEntries({ historyBlobIds: blobs.map(blob => blob.blobId), session, allocateBlobId: allocator() }),
      { store: toClientStore(blobs) },
    )
    expect(client.requests).toHaveLength(3)
    expect(result.map(entry => entry.message.content)).toEqual(['msg-0', 'msg-1', 'msg-2'])
  })
})

describe('sendRootBlobsUnknownToClient', () => {
  it('只补发客户端未持有的 root blob, kv id 从起始值连续分配', () => {
    const known = buildHistoryBlobs(3, 'known')
    const fresh = buildHistoryBlobs(2, 'fresh')
    for (const blob of [...known, ...fresh])
      cacheBlob(blob.blobId, blob.blobData)
    const uncached = encodeBlob({ role: 'user', content: 'never cached' })

    const nextRootBlobIds = [known[0]!.blobId, fresh[0]!.blobId, known[1]!.blobId, fresh[1]!.blobId, fresh[0]!.blobId, uncached.blobId]
    const frames: AgentServerMessage[] = []
    const generator = sendRootBlobsUnknownToClient(nextRootBlobIds, known.map(blob => blob.blobId), 7)
    let step = generator.next()
    while (!step.done) {
      frames.push(step.value)
      step = generator.next()
    }

    expect(step.value).toBe(2)
    expect(frames.map(decodeSetBlobArgs)).toEqual([
      { kvId: 7, blobId: fresh[0]!.blobId, blobData: fresh[0]!.blobData },
      { kvId: 8, blobId: fresh[1]!.blobId, blobData: fresh[1]!.blobData },
    ])
  })
})

describe('ensureTurnBlobCached', () => {
  it('内存未命中时向客户端取 raw protobuf 并按二进制归一, 命中时不发帧', async () => {
    const turn = encodeBinaryBlob(toBinary(ConversationTurnStructureSchema, create(ConversationTurnStructureSchema, {
      turn: { case: 'agentConversationTurn', value: { userMessage: new TextEncoder().encode('u'), steps: [], dynamicToolCount: 5 } },
    })))
    const session = createEphemeralSession('turn-restore')
    const first = await runWithFakeClient(
      session,
      ensureTurnBlobCached(turn.blobId, session, allocateBlobId()),
      { store: new Map([[turn.blobId, turn.blobDataRaw]]) },
    )
    expect(first.client.requests.map(request => request.blobId)).toEqual([turn.blobId])
    expect(readTurnBaseline(turn.blobId)?.dynamicToolCount).toBe(5)

    const second = await runWithFakeClient(session, ensureTurnBlobCached(turn.blobId, session, allocateBlobId()), { store: new Map() })
    expect(second.client.requests).toEqual([])
  })
})

function allocateBlobId(): () => number {
  return allocator()
}

describe('blobStore LRU', () => {
  it('超过字节上限时淘汰最久未用的, 读也刷新新鲜度', () => {
    resetBlobCacheForTests({ maxBytes: 30 })
    cacheBlob('a', 'x'.repeat(10))
    cacheBlob('b', 'y'.repeat(10))
    cacheBlob('c', 'z'.repeat(10))
    expect(getBlobCacheStats()).toEqual({ entries: 3, bytes: 30 })

    // 读 a → a 变最新; 再写 d → 淘汰 b
    expect(getCachedBlob('a')).toBe('x'.repeat(10))
    cacheBlob('d', 'w'.repeat(10))
    expect(getCachedBlob('b')).toBeUndefined()
    expect(getCachedBlob('a')).toBeDefined()
    expect(getCachedBlob('c')).toBeDefined()
    expect(getCachedBlob('d')).toBeDefined()
    expect(getBlobCacheStats()).toEqual({ entries: 3, bytes: 30 })

    // 覆盖写同一 key 不重复计字节
    cacheBlob('d', 'v'.repeat(5))
    expect(getBlobCacheStats()).toEqual({ entries: 3, bytes: 25 })
  })

  it('单个超大 blob 也会被保留 (至少留一条)', () => {
    resetBlobCacheForTests({ maxBytes: 8 })
    cacheBlob('big', 'b'.repeat(100))
    expect(getCachedBlob('big')).toBeDefined()
    expect(getBlobCacheStats().entries).toBe(1)
  })
})

describe('sqlite: 旧版 agent_blobs 表一次性删除', () => {
  let tempDir: string
  let prevDbPath: string | undefined

  beforeEach(() => {
    prevDbPath = process.env.BYOK_AGENT_DB_PATH
    tempDir = mkdtempSync(join(tmpdir(), 'cursor-byok-legacy-blobs-'))
    process.env.BYOK_AGENT_DB_PATH = join(tempDir, 'cursor.db')
  })

  afterEach(async () => {
    await closeAgentDatabase()
    if (prevDbPath === undefined)
      delete process.env.BYOK_AGENT_DB_PATH
    else process.env.BYOK_AGENT_DB_PATH = prevDbPath
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('启动时 DROP + VACUUM, 文件收缩, 其它表保留; 再次启动无事发生', async () => {
    await resetAgentDatabaseForTests()
    const database = getAgentDatabase()
    // 模拟旧版留下的表与数据
    await database.exec(`
      CREATE TABLE agent_blobs (blob_id TEXT PRIMARY KEY, blob_data TEXT NOT NULL, created_at INTEGER NOT NULL, last_accessed_at INTEGER NOT NULL);
      CREATE INDEX idx_agent_blobs_last_accessed_at ON agent_blobs(last_accessed_at);
    `)
    const filler = 'x'.repeat(4096)
    for (let index = 0; index < 500; index++)
      await database.run('INSERT INTO agent_blobs VALUES (?, ?, 1, 1)', [`blob-${index}`, filler])
    await database.run(`INSERT INTO conversation_checkpoints (conversation_id, kind, root_blob_ids_json, summary_archive_ids_json, used_tokens, max_tokens, mode, updated_at) VALUES ('c1', 'committed', '["a"]', '[]', 1, 2, 'AGENT_MODE_AGENT', 1)`)
    await database.exec('PRAGMA wal_checkpoint(TRUNCATE)')
    const bytesBefore = statSync(process.env.BYOK_AGENT_DB_PATH!).size
    expect(bytesBefore).toBeGreaterThan(500 * 4096)

    // 重新初始化 = 升级后的第一次启动
    await resetAgentDatabaseForTests()
    const reopened = getAgentDatabase()
    expect(await reopened.get(`SELECT name FROM sqlite_master WHERE type='table' AND name='agent_blobs'`)).toBeUndefined()
    expect(await reopened.get<{ n: number }>('SELECT count(*) AS n FROM conversation_checkpoints')).toEqual({ n: 1 })
    expect(statSync(process.env.BYOK_AGENT_DB_PATH!).size).toBeLessThan(bytesBefore / 10)

    // 第二次启动: 表已不存在, 幂等
    await resetAgentDatabaseForTests()
    expect(await getAgentDatabase().get<{ n: number }>('SELECT count(*) AS n FROM conversation_checkpoints')).toEqual({ n: 1 })
  })
})
