import type { AgentClientMessage } from '../gen/agent_v1_pb'
import { create } from '@bufbuild/protobuf'
import { expect, it } from 'vitest'
import { AgentClientMessageSchema, ClientHeartbeatSchema, GetBlobResultSchema, KvClientMessageSchema } from '../gen/agent_v1_pb'
import { createEphemeralSession, waitForMessageMatching } from '../handlers/agent/session'
import { pumpBidiClientMessages } from '../services/core/AgentService'

/**
 * Bidi (HTTP/2) 上行泵回归锁。
 *
 * 此前 pump 把 kvClientMessage 与 clientHeartbeat 一并丢弃, 导致 bidi 模式下
 * getBlobArgs 的回包 (kvClientMessage.getBlobResult) 永远到不了
 * waitForMessageMatching, requestContextParts / 历史 blob 回源只能靠超时兜底。
 * SSE 降级路径 (BidiAppend → appendMessage) 无此过滤, 所以这里只锁 bidi 泵。
 */

function buildGetBlobResultFrame(requestId: number, blobData: Uint8Array): AgentClientMessage {
  return create(AgentClientMessageSchema, {
    message: {
      case: 'kvClientMessage',
      value: create(KvClientMessageSchema, {
        id: requestId,
        message: {
          case: 'getBlobResult',
          value: create(GetBlobResultSchema, { blobData }),
        },
      }),
    },
  })
}

function buildHeartbeatFrame(): AgentClientMessage {
  return create(AgentClientMessageSchema, {
    message: {
      case: 'clientHeartbeat',
      value: create(ClientHeartbeatSchema, {}),
    },
  })
}

/** 模拟 connect 的 bidi 请求流: 按顺序吐出帧, 然后结束。 */
async function* clientStream(frames: AgentClientMessage[]): AsyncGenerator<AgentClientMessage, void, void> {
  for (const frame of frames)
    yield frame
}

it('bidi pump enqueues kvClientMessage so getBlobArgs waiters can match the getBlobResult', async () => {
  const session = createEphemeralSession('bidi-pump-kv')
  const requestId = 900_001
  const blobBytes = new TextEncoder().encode('eyJyb2xlIjoidXNlciJ9')

  const pump = pumpBidiClientMessages(
    clientStream([buildGetBlobResultFrame(requestId, blobBytes)]),
    session,
  )

  const matched = await waitForMessageMatching(
    session,
    (message) => {
      const kv = message.kvClientMessage as Record<string, unknown> | undefined
      return !!kv?.getBlobResult && Number(kv.id) === requestId
    },
    1_000,
  )
  await pump

  expect(matched).toBeTruthy()
  const kv = matched!.kvClientMessage as Record<string, unknown>
  expect(Number(kv.id)).toBe(requestId)
  // toJson 把 proto bytes 编成 base64 string —— 与 SSE 路径 (appendMessage) 到达的形态一致
  const result = kv.getBlobResult as Record<string, unknown>
  expect(result.blobData).toBe(Buffer.from(blobBytes).toString('base64'))
  // 流结束后 pump 关闭 session
  expect(session.closed).toBe(true)
})

it('bidi pump still drops clientHeartbeat frames', async () => {
  const session = createEphemeralSession('bidi-pump-heartbeat')

  await pumpBidiClientMessages(
    clientStream([buildHeartbeatFrame(), buildHeartbeatFrame()]),
    session,
  )

  expect(session.messages).toEqual([])
  expect(session.closed).toBe(true)
})

it('bidi pump keeps unrelated kv frames queued without blocking a later matching waiter', async () => {
  const session = createEphemeralSession('bidi-pump-kv-ordering')
  const wantedRequestId = 900_007
  const otherRequestId = 900_003

  await pumpBidiClientMessages(
    clientStream([
      buildHeartbeatFrame(),
      buildGetBlobResultFrame(otherRequestId, new Uint8Array([1])),
      buildGetBlobResultFrame(wantedRequestId, new Uint8Array([2])),
    ]),
    session,
  )

  // 队列里只剩两条 kv 帧 (heartbeat 已丢弃), 且可按 id 精确匹配
  expect(session.messages).toHaveLength(2)
  const matched = await waitForMessageMatching(
    session,
    message => Number((message.kvClientMessage as Record<string, unknown> | undefined)?.id) === wantedRequestId,
    100,
  )
  expect(matched).toBeTruthy()
  expect(session.messages).toHaveLength(1)
  expect(Number((session.messages[0]!.kvClientMessage as Record<string, unknown>).id)).toBe(otherRequestId)
})
