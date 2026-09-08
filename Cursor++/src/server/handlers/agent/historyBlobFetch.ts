/**
 * 对话历史 blob 回源 — 本地缓存未命中时经 getBlobArgs 向客户端取回
 *
 * 官方架构 (workbench ControlledKvManager, 逆向核实):
 *   服务端不存对话 blob; 需要时发 kvServerMessage.getBlobArgs { id, blobId },
 *   客户端 blobStore.getBlob(blobId) 读本地 `agentKv:blob:<hex(blobId)>`,
 *   回 kvClientMessage { id, getBlobResult: { blobData } }。多个请求由客户端
 *   **并发**处理 (每个请求一个 async 任务); 本地没有该 blob 时回包 blobData 为空
 *   (无 error), 读取抛错时才带 error。
 *
 * CCursor 现状: 历史 blob 只查内存 Map + SQLite (blobStore), 未命中静默跳过,
 * 使 `history blobs from cache` 的 resolvedBlobs 小于 requestedBlobs。
 * 本模块补上回源: 阶段 1 (兼容期) 只增加这条兜底路径, 取回的 blob 仍经
 * cacheBlob 双写内存 + SQLite, 本地库照常写; 阶段 2 才关闭持久化。
 *
 * 编码约定 (与 stream.kvMessage / parseRunRequest.historyBlobIds 互为逆运算):
 *   - 服务端发 setBlobArgs 时 blobId = TextEncoder.encode(base64(sha256)) 文本的 UTF-8 字节;
 *   - 客户端在 rootPromptMessagesJson 里原样回传这些字节, parseRunRequest 做
 *     base64 → utf-8 得到 string 形态的 historyBlobIds;
 *   - 因此 getBlobArgs.blobId 只需再 TextEncoder.encode(historyBlobId) 即可。
 *   - blobData: 服务端发的是 TextEncoder.encode(base64(JSON)) (kvMessage 的 JSON blob
 *     分支), 客户端原样保存 (实测与 agent_blobs.blob_data 逐字节一致), 回包 bytes
 *     做 UTF-8 解码即得 cacheBlob 需要的 base64 文本 —— **不是**再做一次 base64 编码。
 *
 * 并发与超时:
 *   - 缺失列表按 HISTORY_BLOB_FETCH_BATCH_SIZE 分批; 首批的第一个 blob 作为探针串行取回:
 *     既探测通道是否活着 (探针超时 → 整体放弃, 不再逐个等 10s), 也探测客户端是否
 *     回显 KvClientMessage.id (不回显的旧客户端 → 退化为严格串行, 与
 *     requestContextParts.fetchPartBytes 的兼容逻辑一致);
 *   - 之后每批一次性 yield 全部 getBlobArgs 帧, 再逐个按各自 id 等回包 ——
 *     先到的回包会先落在 session 队列里, 后续 wait 直接命中;
 *   - 单条等待沿用 10s, 整体上限 60s; 超限的剩余 blob 标记 failed, 不阻塞对话。
 */
import type { AgentServerMessage } from '../../gen/agent_v1_pb'
import { logger } from '../../logger'
import { decodeBlob } from './blob'
import { cacheBlob } from './blobStore'
import { readGetBlobResult } from './kvBlobResult'
import type { AgentSession } from './session'
import { isSessionCancelled } from './session'
import { kvGetBlob } from './stream'
import { waitForMessageMatchingWithHeartbeat } from './wait'

/** 每批最多同时在飞的 getBlobArgs 请求数 —— 防止一次 yield 几百帧 */
export const HISTORY_BLOB_FETCH_BATCH_SIZE = 32
/** 单条 blob 的等待上限 — 客户端本地命中, 正常毫秒级 (与 requestContextParts 一致) */
export const HISTORY_BLOB_FETCH_TIMEOUT_MS = 10_000
/** 一次历史重建里回源的总时长上限 —— 超过即放弃剩余, 不阻塞对话 */
export const HISTORY_BLOB_FETCH_TOTAL_TIMEOUT_MS = 60_000

export interface HistoryBlobFetchResult {
  /** 成功取回并写入缓存的 blob 数 (按去重后的 blobId 计) */
  fetched: number
  /** 未能取回的 blob 数 (超时 / 客户端报错 / 客户端也没有 / 内容不可解码 / 超预算跳过) */
  failed: number
}

export interface FetchMissingHistoryBlobsParams {
  session: AgentSession | null
  missingBlobIds: string[]
  allocateBlobId: () => number
  /** 测试用: 覆盖批大小 / 超时 */
  batchSize?: number
  singleTimeoutMs?: number
  totalTimeoutMs?: number
}

/**
 * historyBlobId (string, base64 形态的 sha256) → getBlobArgs.blobId 字节。
 *
 * 与 stream.kvMessage 发 setBlobArgs 时的 `new TextEncoder().encode(blobId)` 完全一致,
 * 也是 parseRunRequest 里 `Buffer.from(v, 'base64').toString('utf-8')` 的逆运算。
 */
export function encodeHistoryBlobIdBytes(blobId: string): Uint8Array {
  return new TextEncoder().encode(blobId)
}

/**
 * 把客户端回传的 blobData 字节归一成 cacheBlob 需要的 base64 文本。
 *
 * 正常情况 (本服务端写入的历史 blob): 字节本身就是 base64 文本的 UTF-8 编码,
 * 直接解码即可。兜底: 若字节是裸 JSON (非本服务端写入的 blob, 例如官方服务端
 * 产生的对话), 则按 uploadConversationBlobs 的约定编成 base64。
 *
 * 归一后再做一次 decodeBlob 校验, 保证不会把无法解析的内容写进缓存 / SQLite;
 * 校验失败返回 null, 调用方按 failed 计。
 */
export function normalizeFetchedHistoryBlobData(bytes: Uint8Array): string | null {
  if (bytes.length === 0)
    return null
  const asText = Buffer.from(bytes).toString('utf-8')
  const firstVisibleChar = asText.trimStart()[0]
  const candidate = firstVisibleChar === '{' || firstVisibleChar === '['
    ? Buffer.from(bytes).toString('base64')
    : asText
  try {
    const decoded = decodeBlob(candidate)
    if (!decoded || typeof decoded !== 'object')
      return null
    return candidate
  }
  catch {
    return null
  }
}

type SingleFetchOutcome
  = | { status: 'fetched', echoedRequestId: boolean }
    | { status: 'missing_on_client', echoedRequestId: boolean }
    | { status: 'client_error', echoedRequestId: boolean, errorMessage: string }
    | { status: 'undecodable', echoedRequestId: boolean }
    | { status: 'timeout' }

function buildGetBlobResultPredicate(kvRequestId: number, acceptMissingId: boolean): (msg: Record<string, unknown>) => boolean {
  return (msg) => {
    const view = readGetBlobResult(msg)
    if (!view)
      return false
    if (view.requestId === undefined)
      return acceptMissingId
    return view.requestId === kvRequestId
  }
}

/**
 * 等待一条已发出的 getBlobArgs 的回包并落缓存。
 *
 * acceptMissingId: 探针阶段 / 旧客户端串行模式为 true (允许无 id 的回包);
 * 并发模式必须为 false, 否则无 id 的回包会被串到错误的请求上。
 */
async function* awaitBlobResult(
  session: AgentSession,
  blobId: string,
  kvRequestId: number,
  timeoutMs: number,
  acceptMissingId: boolean,
): AsyncGenerator<AgentServerMessage, SingleFetchOutcome, void> {
  const msg = yield* waitForMessageMatchingWithHeartbeat(
    session,
    buildGetBlobResultPredicate(kvRequestId, acceptMissingId),
    timeoutMs,
  )
  const view = msg ? readGetBlobResult(msg) : null
  if (!view)
    return { status: 'timeout' }

  const echoedRequestId = view.requestId !== undefined
  if (view.errorMessage !== undefined)
    return { status: 'client_error', echoedRequestId, errorMessage: view.errorMessage }
  if (!view.blobData)
    return { status: 'missing_on_client', echoedRequestId }

  const normalized = normalizeFetchedHistoryBlobData(view.blobData)
  if (normalized === null)
    return { status: 'undecodable', echoedRequestId }

  cacheBlob(blobId, normalized)
  return { status: 'fetched', echoedRequestId }
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let start = 0; start < items.length; start += size)
    chunks.push(items.slice(start, start + size))
  return chunks
}

/**
 * 向客户端取回本地缓存缺失的历史 blob, 取回的写入 blobStore (内存 + SQLite)。
 *
 * 永不抛错、永不阻塞超过 totalTimeoutMs: 任何失败只体现在返回值的 failed 计数与日志里,
 * 对话照常继续 (与现状"未命中静默跳过"的兜底行为一致, 只是多了一条取回路径)。
 */
export async function* fetchMissingHistoryBlobs(
  params: FetchMissingHistoryBlobsParams,
): AsyncGenerator<AgentServerMessage, HistoryBlobFetchResult, void> {
  const uniqueMissingBlobIds = [...new Set(params.missingBlobIds)]
  if (uniqueMissingBlobIds.length === 0)
    return { fetched: 0, failed: 0 }

  const session = params.session
  if (!session || session.closed || isSessionCancelled(session)) {
    logger.warn({
      requested: uniqueMissingBlobIds.length,
      reason: !session ? 'no_session' : session.closed ? 'session_closed' : 'session_cancelled',
    }, '[SESSION] cannot fetch history blobs from client')
    return { fetched: 0, failed: uniqueMissingBlobIds.length }
  }

  const batchSize = Math.max(1, params.batchSize ?? HISTORY_BLOB_FETCH_BATCH_SIZE)
  const singleTimeoutMs = params.singleTimeoutMs ?? HISTORY_BLOB_FETCH_TIMEOUT_MS
  const totalTimeoutMs = params.totalTimeoutMs ?? HISTORY_BLOB_FETCH_TOTAL_TIMEOUT_MS
  const startedAt = Date.now()
  const deadlineAt = startedAt + totalTimeoutMs
  const remainingBudgetMs = (): number => deadlineAt - Date.now()
  const nextWaitMs = (): number => Math.min(singleTimeoutMs, remainingBudgetMs())
  const sessionGone = (): boolean => session.closed || isSessionCancelled(session)

  let fetched = 0
  let failed = 0
  const outcomeCounts: Record<string, number> = {}
  const recordOutcome = (blobId: string, kvRequestId: number, outcome: SingleFetchOutcome): void => {
    outcomeCounts[outcome.status] = (outcomeCounts[outcome.status] ?? 0) + 1
    if (outcome.status === 'fetched') {
      fetched++
      return
    }
    failed++
    logger.debug({
      requestId: session.requestId,
      kvRequestId,
      blobId,
      status: outcome.status,
      ...(outcome.status === 'client_error' ? { error: outcome.errorMessage } : {}),
    }, '[SESSION] history blob fetch did not yield data')
  }
  const finish = (abortReason?: string): HistoryBlobFetchResult => {
    const unresolved = uniqueMissingBlobIds.length - fetched - failed
    if (unresolved > 0) {
      failed += unresolved
      outcomeCounts.skipped = (outcomeCounts.skipped ?? 0) + unresolved
    }
    const summary = {
      requestId: session.requestId,
      requested: uniqueMissingBlobIds.length,
      fetched,
      failed,
      durationMs: Date.now() - startedAt,
      outcomes: outcomeCounts,
      ...(abortReason ? { abortReason } : {}),
    }
    if (failed > 0)
      logger.warn(summary, '[SESSION] history blobs fetched from client')
    else
      logger.info(summary, '[SESSION] history blobs fetched from client')
    return { fetched, failed }
  }

  // undefined = 尚未探测; 探针回包后才知道客户端是否回显 id (决定并发 / 串行)
  let clientEchoesRequestId: boolean | undefined

  for (const batchBlobIds of chunk(uniqueMissingBlobIds, batchSize)) {
    if (remainingBudgetMs() <= 0)
      return finish('total_timeout')
    if (sessionGone())
      return finish('session_gone')

    let pendingBlobIds = batchBlobIds

    if (clientEchoesRequestId === undefined) {
      // ── 探针: 首批第一个 blob 串行取回 ──
      const probeBlobId = pendingBlobIds[0]!
      const probeKvRequestId = params.allocateBlobId()
      yield kvGetBlob(probeKvRequestId, encodeHistoryBlobIdBytes(probeBlobId))
      const probeOutcome = yield* awaitBlobResult(session, probeBlobId, probeKvRequestId, nextWaitMs(), true)
      recordOutcome(probeBlobId, probeKvRequestId, probeOutcome)
      if (probeOutcome.status === 'timeout') {
        // 通道没有任何回应 (客户端不支持 / 连接已断): 不再逐个等超时, 剩余直接放弃
        logger.warn({ requestId: session.requestId, kvRequestId: probeKvRequestId, blobId: probeBlobId },
          '[SESSION] history blob probe fetch timed out; skipping remaining blobs')
        return finish(sessionGone() ? 'session_gone' : 'probe_timeout')
      }
      clientEchoesRequestId = probeOutcome.echoedRequestId
      if (!clientEchoesRequestId) {
        logger.info({ requestId: session.requestId }, '[SESSION] client does not echo kv request ids; fetching history blobs serially')
      }
      pendingBlobIds = pendingBlobIds.slice(1)
    }

    if (clientEchoesRequestId) {
      // ── 并发: 整批一次发出, 再逐个按 id 等回包 ──
      const inFlight = pendingBlobIds.map(blobId => ({ blobId, kvRequestId: params.allocateBlobId() }))
      for (const request of inFlight)
        yield kvGetBlob(request.kvRequestId, encodeHistoryBlobIdBytes(request.blobId))
      for (const request of inFlight) {
        const waitMs = nextWaitMs()
        if (waitMs <= 0)
          return finish('total_timeout')
        const outcome = yield* awaitBlobResult(session, request.blobId, request.kvRequestId, waitMs, false)
        recordOutcome(request.blobId, request.kvRequestId, outcome)
        if (outcome.status === 'timeout' && sessionGone())
          return finish('session_gone')
      }
    }
    else {
      // ── 串行 (旧客户端不回显 id): 一发一收, 允许无 id 的回包 ──
      for (const blobId of pendingBlobIds) {
        const waitMs = nextWaitMs()
        if (waitMs <= 0)
          return finish('total_timeout')
        const kvRequestId = params.allocateBlobId()
        yield kvGetBlob(kvRequestId, encodeHistoryBlobIdBytes(blobId))
        const outcome = yield* awaitBlobResult(session, blobId, kvRequestId, waitMs, true)
        recordOutcome(blobId, kvRequestId, outcome)
        if (outcome.status === 'timeout' && sessionGone())
          return finish('session_gone')
      }
    }
  }

  return finish()
}
