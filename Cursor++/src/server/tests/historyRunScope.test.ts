import type { AgentServerMessage } from '../gen/agent_v1_pb'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { blobIdFromBytes, blobIdToBytes, encodeBlob } from '../handlers/agent/blob'
import { fetchBlobsFromClient } from '../handlers/agent/clientBlobFetch'
import { isCompactionLockHeld, releaseCompactionLock, tryAcquireCompactionLock, waitForCompactionLockRelease } from '../handlers/agent/compactionLock'
import { loadHistoryEntries } from '../handlers/agent/historyManager'
import { BlobRunContext } from '../handlers/agent/runContext'
import { createEphemeralSession, pushSessionMessage } from '../handlers/agent/session'
import { logger } from '../logger'

const activeRuns: BlobRunContext[] = []

function createRun(): BlobRunContext {
  const run = new BlobRunContext(createEphemeralSession('history-scope'))
  activeRuns.push(run)
  return run
}

async function readWithClient<ReturnValue>(
  run: BlobRunContext,
  generator: AsyncGenerator<AgentServerMessage, ReturnValue, void>,
  blobs: Map<string, string>,
): Promise<ReturnValue> {
  let step = await generator.next()
  while (!step.done) {
    const envelope = step.value.message
    if (envelope.case === 'kvServerMessage' && envelope.value.message.case === 'getBlobArgs') {
      const bytes = blobs.get(blobIdFromBytes(envelope.value.message.value.blobId))
      pushSessionMessage(run.session!, { kvClientMessage: {
        id: envelope.value.id,
        getBlobResult: bytes === undefined ? {} : { blobData: Buffer.from(bytes).toString('base64') },
      } })
    }
    step = await generator.next()
  }
  return step.value
}

afterEach(() => {
  for (const run of activeRuns.splice(0))
    run.dispose()
  vi.restoreAllMocks()
  vi.useRealTimers()
})

describe('history reference observations', () => {
  it('preserves duplicate ordering while counting usable run hits and actual requests separately', async () => {
    const logs = vi.spyOn(logger, 'info').mockImplementation(() => {})
    const cached = encodeBlob({ role: 'user', content: 'cached' })
    const fetched = encodeBlob({ role: 'assistant', content: 'fetched' })
    const run = createRun()
    run.blobs.cacheBlob(cached.blobId, cached.blobData)
    const historyBlobIds = [cached.blobId, fetched.blobId, cached.blobId, fetched.blobId]
    const params = { run, historyBlobIds }
    const entries = await readWithClient(run, loadHistoryEntries(params), new Map([[fetched.blobId, fetched.blobData]]))
    expect(entries.map(entry => entry.blobId)).toEqual(historyBlobIds)
    expect(entries[0]).toBe(entries[2])
    expect(entries[1]).toBe(entries[3])
    const historyLogs = () => logs.mock.calls.filter(argumentsList => argumentsList[1] === '[SESSION] history blobs from cache')
    expect(historyLogs()[0]?.[0]).toMatchObject({
      requestedBlobs: 4,
      cachedBlobs: 2,
      fetchedFromClient: 2,
      stillMissing: 0,
      resolvedBlobs: 4,
      requestedFromClient: 1,
    })
    await readWithClient(run, loadHistoryEntries(params), new Map())
    expect(historyLogs()[1]?.[0]).toMatchObject({
      requestedBlobs: 4,
      cachedBlobs: 4,
      fetchedFromClient: 0,
      stillMissing: 0,
      resolvedBlobs: 4,
      requestedFromClient: 0,
    })
    expect(run.getSentGetRequestCount()).toBe(1)
  })

  it('counts already-fetched valid bytes as run hits, and never counts invalid JSON messages as hits', async () => {
    const logs = vi.spyOn(logger, 'info').mockImplementation(() => {})
    const fixture = encodeBlob({ role: 'user', content: 'shared raw result' })
    const run = createRun()
    await readWithClient(run, fetchBlobsFromClient({ run, blobIds: [blobIdToBytes(fixture.blobId)] }), new Map([[fixture.blobId, fixture.blobData]]))
    await readWithClient(run, loadHistoryEntries({ run, historyBlobIds: [fixture.blobId] }), new Map())
    expect(logs.mock.calls.at(-1)?.[0]).toMatchObject({ cachedBlobs: 1, fetchedFromClient: 0, requestedFromClient: 0 })

    const invalid = encodeBlob({ role: 'not-a-role', content: 'must not become an empty message' })
    run.blobs.cacheBlob(invalid.blobId, invalid.blobData)
    await expect(readWithClient(run, loadHistoryEntries({ run, historyBlobIds: [invalid.blobId, invalid.blobId] }), new Map())).rejects.toThrow(/decode-error/)
    expect(logs.mock.calls.at(-1)?.[0]).toMatchObject({
      requestedBlobs: 2,
      cachedBlobs: 0,
      fetchedFromClient: 0,
      stillMissing: 2,
      resolvedBlobs: 0,
      requestedFromClient: 0,
    })
  })

  it('remembers failed reads without retrying them for duplicate history references in the same run', async () => {
    const run = createRun()
    const params = { run, historyBlobIds: ['missing', 'missing'] }
    await expect(readWithClient(run, loadHistoryEntries(params), new Map())).rejects.toThrow(/not-found/)
    await expect(readWithClient(run, loadHistoryEntries(params), new Map())).rejects.toThrow(/not-found/)
    expect(run.getSentGetRequestCount()).toBe(1)
    const nextRun = createRun()
    await expect(readWithClient(nextRun, loadHistoryEntries({ ...params, run: nextRun }), new Map())).rejects.toThrow(/not-found/)
    expect(nextRun.getSentGetRequestCount()).toBe(1)
  })
})

describe('run-owned KV queue cleanup', () => {
  it('cancels only its own compaction waiter without releasing another run lock', async () => {
    const conversationId = 'scoped-lock-waiters'
    expect(tryAcquireCompactionLock(conversationId)).toBe(true)
    const controller = new AbortController()
    const removeListener = vi.spyOn(controller.signal, 'removeEventListener')
    const first = waitForCompactionLockRelease(conversationId, controller.signal)
    const siblingFinished = vi.fn()
    const sibling = waitForCompactionLockRelease(conversationId).then(siblingFinished)
    try {
      controller.abort()
      await first
      expect(isCompactionLockHeld(conversationId)).toBe(true)
      expect(siblingFinished).not.toHaveBeenCalled()
      expect(removeListener).toHaveBeenCalledTimes(1)
    }
    finally {
      releaseCompactionLock(conversationId)
    }
    await sibling
    expect(siblingFinished).toHaveBeenCalledTimes(1)
    // A cancelled callback was removed from the lock as well as the abort signal.
    expect(removeListener).toHaveBeenCalledTimes(1)
  })

  it('retires only its own request IDs and discards their late replies without decoding', () => {
    const first = createRun()
    const second = new BlobRunContext(first.session)
    activeRuns.push(second)
    const firstId = first.allocateBlobId()
    const secondId = second.allocateBlobId()
    pushSessionMessage(first.session!, { kvClientMessage: { id: firstId, setBlobResult: {} } })
    const unrelated = { execClientMessage: { id: 7, value: 'other consumer' } }
    pushSessionMessage(first.session!, unrelated)
    const secondReply = { kvClientMessage: { id: secondId, setBlobResult: {} } }
    pushSessionMessage(first.session!, secondReply)
    first.dispose()
    expect(first.session!.messages).toEqual([unrelated, secondReply])
    const readBody = vi.fn(() => {
      throw new Error('Late blob body must not be decoded')
    })
    pushSessionMessage(first.session!, { kvClientMessage: {
      id: firstId,
      get getBlobResult() {
        return readBody()
      },
    } })
    expect(readBody).not.toHaveBeenCalled()
    expect(first.session!.messages).toEqual([unrelated, secondReply])
    expect(first.session!.activeBlobRequestIds).toEqual(new Set([secondId]))
  })
})
