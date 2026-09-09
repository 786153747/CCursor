import { toJson } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { AgentServerMessageSchema } from '../gen/agent_v1_pb'
import { blobIdFromBytes, blobIdToBytes, encodeBlob } from '../handlers/agent/blob'
import { RunBlobStore } from '../handlers/agent/blobStore'
import { parseRunRequest } from '../handlers/agent/protocol'
import { checkpoint, kvMessage } from '../handlers/agent/stream'
import { UploadHandoff } from '../handlers/agent/uploadHandoff'

const encodeText = (text: string): Uint8Array => new TextEncoder().encode(text)

function upload(handoff: UploadHandoff, conversationId: string, key: Uint8Array, value: string, chunkIndex = 0, totalChunks = 1): void {
  handoff.putChunk({ conversationId, blobs: [{ id: key, value: encodeText(value) }], chunkIndex, totalChunks })
}

describe('pre-run upload handoff', () => {
  it('isolates exact keys by conversation, including fork and concurrent upload batches', () => {
    const handoff = new UploadHandoff()
    const opaqueKey = Uint8Array.from([255, 128, 0, 65])
    upload(handoff, 'source', opaqueKey, 'source bytes')
    upload(handoff, 'fork', opaqueKey, 'fork bytes', 2, 4)
    upload(handoff, 'fork', encodeText('earlier-chunk'), 'earlier bytes', 0, 4)

    expect(handoff.read('source', opaqueKey)).toEqual(encodeText('source bytes'))
    expect(handoff.read('fork', opaqueKey)).toEqual(encodeText('fork bytes'))
    expect(handoff.read('fork', encodeText('earlier-chunk'))).toEqual(encodeText('earlier bytes'))
    expect(handoff.read('unrelated-run', opaqueKey)).toBeUndefined()
    // The client skips all-missing chunks; totalChunks is not a completeness proof.
    expect(handoff.getStats().entries).toBe(3)
  })

  it('copies into independent run workspaces and expires without clearing active data', () => {
    let clock = 1000
    const handoff = new UploadHandoff({ ttlMs: 100, now: () => clock })
    const key = encodeText('shared')
    upload(handoff, 'same-conversation', key, 'immutable history')
    const firstRun = new RunBlobStore()
    const secondRun = new RunBlobStore()
    for (const store of [firstRun, secondRun]) {
      const bytes = handoff.read('same-conversation', key)!
      store.cacheBlob('shared', Buffer.from(bytes).toString('base64'), bytes)
      bytes.fill(0)
    }
    firstRun.dispose()
    clock += 100
    expect(handoff.read('same-conversation', key)).toBeUndefined()
    expect(handoff.getStats()).toEqual({ entries: 0, bytes: 0 })
    expect(secondRun.getBlob('shared')?.blobDataRaw).toEqual(encodeText('immutable history'))
    secondRun.dispose()
  })

  it('rejects capacity overflow rather than evicting an accepted upload', () => {
    const handoff = new UploadHandoff({ maxBytes: 4, maxEntries: 1 })
    upload(handoff, 'first', encodeText('key'), '1234')
    let failure: unknown
    try {
      upload(handoff, 'second', encodeText('key'), 'x')
    }
    catch (error) {
      failure = error
    }
    expect(failure).toBeInstanceOf(ConnectError)
    expect((failure as ConnectError).code).toBe(Code.ResourceExhausted)
    expect(handoff.read('first', encodeText('key'))).toEqual(encodeText('1234'))
    expect(handoff.read('second', encodeText('key'))).toBeUndefined()
  })

  it('rejects a conflicting chunk atomically and permits idempotent chunk retries', () => {
    const handoff = new UploadHandoff()
    upload(handoff, 'conversation', encodeText('original'), 'kept')
    upload(handoff, 'conversation', encodeText('original'), 'kept')
    expect(() => handoff.putChunk({
      conversationId: 'conversation',
      chunkIndex: 0,
      totalChunks: 1,
      blobs: [
        { id: encodeText('new'), value: encodeText('not accepted') },
        { id: encodeText('original'), value: encodeText('conflict') },
      ],
    })).toThrow(/Conflicting upload/)
    expect(handoff.read('conversation', encodeText('new'))).toBeUndefined()
    expect(handoff.read('conversation', encodeText('original'))).toEqual(encodeText('kept'))
    expect(handoff.getStats()).toEqual({ entries: 1, bytes: 4 })
  })

  it('does not accept unscoped uploads and preserves empty byte values', () => {
    const handoff = new UploadHandoff()
    expect(() => upload(handoff, '', encodeText('key'), 'value')).toThrow(/conversationId/)
    upload(handoff, 'conversation', encodeText('empty'), '')
    expect(handoff.read('conversation', encodeText('empty'))).toEqual(new Uint8Array())
    expect(handoff.read('conversation', encodeText('missing'))).toBeUndefined()
  })
})

describe('exact blob key boundaries', () => {
  it('keeps project history hashes as UTF-8 text, not raw SHA256', () => {
    const encoded = encodeBlob({ role: 'user', content: 'history' })
    expect(blobIdToBytes(encoded.blobId)).toEqual(encodeText(encoded.blobId))
    expect(blobIdToBytes(encoded.blobId).length).toBe(44)
    expect(blobIdFromBytes(blobIdToBytes(encoded.blobId))).toBe(encoded.blobId)
  })

  it('roundtrips fork raw hashes, BOM-prefixed bytes and reserved-prefix text without key aliasing', () => {
    const keys = [
      Uint8Array.from({ length: 32 }, (_, index) => index * 7 + 31),
      Uint8Array.from([239, 187, 191, 65]),
      encodeText('blob-bytes:literal-original-key'),
      Uint8Array.from([255, 254, 128]),
    ]
    const internalIds = keys.map(blobIdFromBytes)
    expect(new Set(internalIds).size).toBe(keys.length)
    for (const [index, blobId] of internalIds.entries()) {
      expect(blobIdToBytes(blobId)).toEqual(keys[index])
      const frame = kvMessage(17, blobId, 'payload')
      if (frame.message.case !== 'kvServerMessage' || frame.message.value.message.case !== 'setBlobArgs')
        throw new Error('Expected a SetBlob frame')
      expect(frame.message.value.message.value.blobId).toEqual(keys[index])
    }
    const frame = toJson(AgentServerMessageSchema, checkpoint(internalIds, 10, 1000, 'AGENT_MODE_AGENT', undefined, {
      turnBlobIds: internalIds,
      summaryArchiveIds: internalIds,
    })) as Record<string, unknown>
    const parsed = parseRunRequest({ runRequest: {
      conversationId: 'fork',
      conversationState: frame.conversationCheckpointUpdate,
      action: { userMessageAction: { userMessage: { text: 'continue' }, requestContext: {} } },
      modelDetails: { modelId: 'gpt-5.4-medium' },
    } })
    expect(parsed.historyBlobIds).toEqual(internalIds)
    expect(parsed.historyTurnBlobIds).toEqual(internalIds)
    expect(parsed.historySummaryArchiveIds).toEqual(internalIds)
  })
})
