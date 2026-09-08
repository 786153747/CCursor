/**
 * 向客户端取回 blob — 服务端所有"需要客户端持有的 blob"的唯一取回原语
 *
 * 官方架构 (workbench ControlledKvManager, 逆向核实): 客户端是 blob 的唯一持久持有方。
 * 服务端发 kvServerMessage.getBlobArgs { id, blobId }, 客户端 blobStore.getBlob 读本地
 * `agentKv:blob:<hex(blobId)>` 后回 kvClientMessage { id, getBlobResult: { blobData } };
 * 多个请求由客户端并发处理, 每个请求**必然**有一条回包: 本地没有 → blobData 为空且无 error,
 * 读取抛错 → 带 error。id 原样回显 (uint32, 本服务端分配的 id ≥ 900_000, JSON 里不会因
 * 取默认值 0 而被省略), 因此严格按 id 匹配。
 *
 * 用法: rules/skills/subagents/mcps 四类 requestContext Part、对话历史 blob、resume 时的
 * turn blob、extraContext blob 都经这里取。调用方按 blob 种类把字节转成自己需要的形态。
 *
 * 分批与超时: 每批 ≤ 32 个一次性发出 (客户端并发处理), 再逐个按 id 等回包, 先到的回包
 * 已在 session 队列里、后续 wait 直接命中。单条等待 10s (本地命中正常毫秒级)。客户端对每个
 * 请求都会回包, 所以一旦超时就意味着通道已死 (断连 / 客户端卡死), 剩余不再等、不再发,
 * 整体最多付出一次超时的代价。任何失败只体现在返回值的 null 与日志里, 不抛错、不阻塞对话。
 */
import type { AgentServerMessage } from '../../gen/agent_v1_pb'
import { logger } from '../../logger'
import { toBytes } from './protocol/shared'
import type { AgentSession } from './session'
import { isSessionCancelled } from './session'
import { kvGetBlob } from './stream'
import { waitForMessageMatchingWithHeartbeat } from './wait'

/** 每批最多同时在飞的 getBlobArgs 请求数 —— 防止一次 yield 几百帧 */
export const CLIENT_BLOB_FETCH_BATCH_SIZE = 32
/** 单条 blob 的等待上限 — 客户端本地命中, 正常毫秒级 */
export const CLIENT_BLOB_FETCH_TIMEOUT_MS = 10_000

export interface ClientBlobFetchParams {
  session: AgentSession | null
  /** getBlobArgs.blobId 字节 (历史 blob 用 blobIdToBytes 编码; Part 引用本身就是字节) */
  blobIds: Uint8Array[]
  /** kvServerMessage.id 分配器 —— 全 run 共用一个计数器, 避免回包 id 撞号 */
  allocateBlobId: () => number
  /** 测试用: 覆盖批大小 / 超时 */
  batchSize?: number
  timeoutMs?: number
}

interface GetBlobResultView {
  requestId: number | undefined
  blobData: Uint8Array | null
  errorMessage?: string
}

/** 解包 kvClientMessage.getBlobResult; 不是该类消息返回 null。 */
function readGetBlobResult(msg: Record<string, unknown>): GetBlobResultView | null {
  const kv = msg.kvClientMessage as Record<string, unknown> | undefined
  const result = kv?.getBlobResult as Record<string, unknown> | undefined
  if (!result)
    return null
  const requestId = kv?.id === undefined || kv.id === null ? undefined : Number(kv.id)
  if (result.error) {
    const error = result.error as Record<string, unknown> | string
    const errorMessage = typeof error === 'string'
      ? error
      : typeof error?.message === 'string' ? error.message : JSON.stringify(error)
    return { requestId, blobData: null, errorMessage }
  }
  // JSON transport (SSE 降级) 会把 proto bytes 编成 base64 string; toBytes 统一归一
  return { requestId, blobData: toBytes(result.blobData) ?? null }
}

function isGetBlobResultFor(kvRequestId: number): (msg: Record<string, unknown>) => boolean {
  return msg => readGetBlobResult(msg)?.requestId === kvRequestId
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let start = 0; start < items.length; start += size)
    chunks.push(items.slice(start, start + size))
  return chunks
}

/**
 * 取回 blobIds 对应的字节, 结果与入参下标对齐; 取不到 (无 session / 通道超时 /
 * 客户端没有 / 客户端报错) 的位置为 null。
 */
export async function* fetchBlobsFromClient(
  params: ClientBlobFetchParams,
): AsyncGenerator<AgentServerMessage, Array<Uint8Array | null>, void> {
  const results: Array<Uint8Array | null> = Array.from({ length: params.blobIds.length }, () => null)
  if (params.blobIds.length === 0)
    return results

  const session = params.session
  if (!session) {
    logger.warn({ requested: params.blobIds.length }, '[SESSION] cannot fetch blobs from client without a session')
    return results
  }

  const batchSize = Math.max(1, params.batchSize ?? CLIENT_BLOB_FETCH_BATCH_SIZE)
  const timeoutMs = params.timeoutMs ?? CLIENT_BLOB_FETCH_TIMEOUT_MS
  const startedAt = Date.now()
  let fetched = 0
  let abortReason: string | undefined

  const allIndices = params.blobIds.map((_, index) => index)
  batches: for (const batchIndices of chunk(allIndices, batchSize)) {
    if (session.closed || isSessionCancelled(session)) {
      abortReason = 'session_gone'
      break
    }

    const inFlight = batchIndices.map(index => ({ index, kvRequestId: params.allocateBlobId() }))
    for (const request of inFlight)
      yield kvGetBlob(request.kvRequestId, params.blobIds[request.index]!)

    for (const request of inFlight) {
      const msg = yield* waitForMessageMatchingWithHeartbeat(session, isGetBlobResultFor(request.kvRequestId), timeoutMs)
      const view = msg ? readGetBlobResult(msg) : null
      if (!view) {
        abortReason = session.closed || isSessionCancelled(session) ? 'session_gone' : 'timeout'
        logger.warn({ requestId: session.requestId, kvRequestId: request.kvRequestId, timeoutMs, abortReason },
          '[SESSION] blob fetch from client got no reply; abandoning remaining blobs')
        break batches
      }
      if (view.blobData) {
        results[request.index] = view.blobData
        fetched++
      }
      else {
        logger.debug({ requestId: session.requestId, kvRequestId: request.kvRequestId, error: view.errorMessage },
          '[SESSION] client has no data for requested blob')
      }
    }
  }

  const summary = {
    requestId: session.requestId,
    requested: params.blobIds.length,
    fetched,
    durationMs: Date.now() - startedAt,
    ...(abortReason ? { abortReason } : {}),
  }
  if (fetched === params.blobIds.length)
    logger.info(summary, '[SESSION] blobs fetched from client')
  else
    logger.warn(summary, '[SESSION] blobs fetched from client')
  return results
}
