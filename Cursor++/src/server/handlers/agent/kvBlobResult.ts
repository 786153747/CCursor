/**
 * kvClientMessage.getBlobResult 解包 — requestContextParts 与历史 blob 回源共用。
 *
 * proto: GetBlobResult { optional bytes blob_data = 1; optional Error error = 2 }
 *   - 3.13 起新增 error 字段, 失败时该字段有值、blob_data 为空。
 *   - 客户端本地没有该 blob 时 (ControlledKvManager: blobStore.getBlob 返回空),
 *     回包既无 blob_data 也无 error —— 与 error 同样按"取不到"处理。
 */
import { logger } from '../../logger'
import { toBytes } from './protocol/shared'

export interface GetBlobResultView {
  /** 回包携带的 KvClientMessage.id (uint32); 旧客户端可能不回显, 此时为 undefined */
  requestId: number | undefined
  /** 归一化后的 blob 内容; 取不到 (error / 空) 时为 null */
  blobData: Uint8Array | null
  /** error 字段的可读描述 (仅在客户端明确报错时有值) */
  errorMessage?: string
}

/**
 * 从 kvClientMessage.getBlobResult 中取出 blob 内容。
 *
 * JSON transport 会把 proto bytes 编成 base64 string; 统一经 toBytes 归一后返回。
 * 返回 null 表示回包不是 getBlobResult、客户端报错、或 blob 为空。
 */
export function extractBlobData(msg: Record<string, unknown>): Uint8Array | null {
  const kv = msg.kvClientMessage as Record<string, unknown> | undefined
  const result = kv?.getBlobResult as Record<string, unknown> | undefined
  if (!result)
    return null
  if (result.error) {
    logger.warn({ error: result.error }, '[PROTOCOL] getBlobResult returned error')
    return null
  }
  // JSON transport 会把 proto bytes 编成 base64 string;统一归一后再解码 protobuf。
  return toBytes(result.blobData) ?? null
}

/** 解析回包的 id 与内容, 供需要区分"报错 / 空 / 无 id"的调用方使用。 */
export function readGetBlobResult(msg: Record<string, unknown>): GetBlobResultView | null {
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
  return { requestId, blobData: toBytes(result.blobData) ?? null }
}
