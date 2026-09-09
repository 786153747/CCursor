import type { AgentServerMessage } from '../gen/agent_v1_pb'
import type { BlobRunOptions } from '../handlers/agent/runContext'
import type { AgentSession } from '../handlers/agent/session'
import { create, toJson } from '@bufbuild/protobuf'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { AgentClientMessageSchema } from '../gen/agent_v1_pb'
import { blobIdFromBytes, blobIdToBytes, encodeBinaryBlob, encodeBlob } from '../handlers/agent/blob'
import { BlobIntegrityError } from '../handlers/agent/blobErrors'
import { BlobInactiveError, BlobResourceLimitError, RunBlobStore } from '../handlers/agent/blobStore'
import { BlobTransferError, CLIENT_BLOB_FETCH_BATCH_SIZE, fetchBlobsFromClient, saveCheckpointBlobs } from '../handlers/agent/clientBlobFetch'
import { AGENT_HEARTBEAT_INTERVAL_MS } from '../handlers/agent/constants'
import { BlobRunContext } from '../handlers/agent/runContext'
import { RunResourceBudget } from '../handlers/agent/runResources'
import { createEphemeralSession, markSessionClosed, pushSessionMessage, waitForMessageMatching } from '../handlers/agent/session'
import { AgentRunAbortedError, waitForPromiseWithHeartbeat } from '../handlers/agent/wait'

const activeRuns: BlobRunContext[] = []

function createRun(session: AgentSession | null = createEphemeralSession('blob-kv-test'), options: BlobRunOptions = {}): BlobRunContext {
  const run = new BlobRunContext(session, options)
  activeRuns.push(run)
  return run
}

function readKvFrame(frame: AgentServerMessage): { requestId: number, kind: 'getBlobArgs' | 'setBlobArgs', blobId: Uint8Array, blobData?: Uint8Array } | null {
  if (frame.message.case !== 'kvServerMessage')
    return null
  const envelope = frame.message.value
  if (envelope.message.case === 'getBlobArgs')
    return { requestId: envelope.id, kind: 'getBlobArgs', blobId: envelope.message.value.blobId }
  if (envelope.message.case === 'setBlobArgs')
    return { requestId: envelope.id, kind: 'setBlobArgs', ...envelope.message.value }
  return null
}

async function nextKvFrame<ReturnValue>(generator: AsyncGenerator<AgentServerMessage, ReturnValue, void>): Promise<NonNullable<ReturnType<typeof readKvFrame>>> {
  const step = await generator.next()
  if (step.done)
    throw new Error('Expected a KV send, but the operation finished')
  const request = readKvFrame(step.value)
  if (!request)
    throw new Error('Expected a KV send, not a heartbeat or checkpoint')
  return request
}

async function drain<ReturnValue>(
  generator: AsyncGenerator<AgentServerMessage, ReturnValue, void>,
  onFrame: (frame: AgentServerMessage) => void = () => {},
): Promise<ReturnValue> {
  let step = await generator.next()
  while (!step.done) {
    onFrame(step.value)
    step = await generator.next()
  }
  return step.value
}

function replyGet(session: AgentSession, requestId: number, blobData?: Uint8Array, error?: string): void {
  // Both BiDi and SSE session queues carry protobuf-es toJson messages.
  const message = create(AgentClientMessageSchema, {
    message: {
      case: 'kvClientMessage',
      value: {
        id: requestId,
        message: { case: 'getBlobResult', value: { blobData, error: error === undefined ? undefined : { message: error } } },
      },
    },
  })
  pushSessionMessage(session, toJson(AgentClientMessageSchema, message) as Record<string, unknown>)
}

function replySet(session: AgentSession, requestId: number, error?: string): void {
  const message = create(AgentClientMessageSchema, {
    message: {
      case: 'kvClientMessage',
      value: {
        id: requestId,
        message: { case: 'setBlobResult', value: { error: error === undefined ? undefined : { message: error } } },
      },
    },
  })
  pushSessionMessage(session, toJson(AgentClientMessageSchema, message) as Record<string, unknown>)
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(0)
})

afterEach(() => {
  for (const run of activeRuns.splice(0)) {
    run.dispose()
    if (run.session)
      markSessionClosed(run.session)
  }
  vi.clearAllTimers()
  vi.useRealTimers()
})

describe('process admission and cross-helper KV bounds', () => {
  it.each(['get', 'set'] as const)('releases a %s gate granted while its consumer is suspended at a heartbeat', async (kind) => {
    const run = createRun()
    const releaseBlocker = await run.kvGate.acquire(run.signal)
    run.blobs.cacheBlob('pending-save', 'YQ==')
    const operation: AsyncGenerator<AgentServerMessage, unknown, void> = kind === 'get'
      ? fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('abandoned-read')] })
      : saveCheckpointBlobs(run, ['pending-save'])
    const heartbeatStep = operation.next()
    await vi.advanceTimersByTimeAsync(AGENT_HEARTBEAT_INTERVAL_MS + 1)
    expect((await heartbeatStep).done).toBe(false)
    releaseBlocker?.()
    await Promise.resolve()
    await operation.return(undefined)
    const following = fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('following-read')] })
    const sent = await nextKvFrame(following)
    replyGet(run.session!, sent.requestId, Buffer.from('following'))
    expect((await drain(following))[0]?.status).toBe('ok')
    expect(run.getSentGetRequestCount()).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('releases admission and counted payload after resource failure and constructor failure', () => {
    const budget = new RunResourceBudget({ maxRuns: 2, maxBytes: 10 })
    const first = createRun(null, { resourceBudget: budget })
    const second = createRun(null, { resourceBudget: budget })
    first.blobs.cacheBlob('first', 'AAAAAA')
    expect(() => second.blobs.cacheBlob('second', 'AAAAAA')).toThrow(/Process blob payload/)
    expect(budget.getStats()).toEqual({ activeRuns: 2, retainedBytes: 6 })
    expect(() => createRun(null, { resourceBudget: budget })).toThrow(/Active run capacity/)
    first.dispose()
    second.blobs.cacheBlob('second', 'AAAAAA')
    expect(() => createRun(null, { resourceBudget: budget, maxRecords: 0, inheritedBlobIds: ['bad'] })).toThrow(/record limit/)
    expect(budget.getStats()).toEqual({ activeRuns: 1, retainedBytes: 6 })
    second.dispose()
    expect(budget.getStats()).toEqual({ activeRuns: 0, retainedBytes: 0 })
  })

  it('bounds combined Gets and Sets, counts queue time, and releases cancelled waiters', async () => {
    const run = createRun(undefined, { batchSize: 2, timeoutMs: 1000, overallTimeoutMs: 2000 })
    const first = fetchBlobsFromClient({ run, blobIds: ['first', 'second'].map(blobIdToBytes) })
    const firstRequest = await nextKvFrame(first)
    const secondRequest = await nextKvFrame(first)
    const firstCompletion = drain(first)
    const queued = fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('queued')], overallTimeoutMs: 50 })
    const queuedFrames: AgentServerMessage[] = []
    const queuedCompletion = drain(queued, frame => queuedFrames.push(frame))
    run.blobs.cacheBlob('new', 'bmV3')
    const saving = saveCheckpointBlobs(run, ['new'])
    const nextSet = nextKvFrame(saving)
    await vi.advanceTimersByTimeAsync(51)
    expect((await queuedCompletion)[0]?.status).toBe('overall-timeout')
    expect(queuedFrames).toEqual([])
    expect(run.session!.activeBlobRequestIds?.size).toBe(2)
    replyGet(run.session!, firstRequest.requestId, Buffer.from('first'))
    replyGet(run.session!, secondRequest.requestId, Buffer.from('second'))
    await firstCompletion
    const setRequest = await nextSet
    expect(setRequest.kind).toBe('setBlobArgs')
    expect(run.session!.activeBlobRequestIds?.size).toBe(1)
    replySet(run.session!, setRequest.requestId)
    await drain(saving)
    expect(run.getSentGetRequestCount()).toBe(2)
    expect(run.session!.activeBlobRequestIds?.size).toBe(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('rejects a full KV wait queue and frees queued slots on run cancellation', async () => {
    const run = createRun()
    const release = await run.kvGate.acquire(run.signal)
    const waiting = Array.from({ length: 32 }, () => run.kvGate.acquire(run.signal))
    expect(() => run.kvGate.acquire(run.signal)).toThrow(/queue is full/)
    run.abortController.abort()
    expect(await Promise.all(waiting)).toEqual(Array.from({ length: 32 }).fill(undefined))
    release?.()
    const nextSignal = new AbortController().signal
    const nextRelease = await run.kvGate.acquire(nextSignal)
    expect(nextRelease).toBeTypeOf('function')
    nextRelease?.()
  })
})

describe('run-owned blob retention', () => {
  it('keeps inherited references separate from confirmed client storage', () => {
    const run = createRun(null, { inheritedBlobIds: ['incoming'] })
    run.blobs.assertCheckpointReferences(['incoming'])
    expect(run.blobs.isClientSaved('incoming')).toBe(false)
    run.blobs.cacheBlob('incoming', 'YQ==')
    expect(run.blobs.getPendingBlobs().map(blob => blob.blobId)).toEqual(['incoming'])
    run.blobs.markClientSaved('incoming')
    expect(run.blobs.getPendingBlobs()).toEqual([])
    expect(run.blobs.getCachedBlob('incoming')).toBe('YQ==')
  })

  it('rejects missing transitive dependencies, including pending archive originals', () => {
    const blobs = new RunBlobStore()
    blobs.cacheBlob('archive', 'YQ==', undefined, ['turn'])
    blobs.cacheBlob('turn', 'Yg==', undefined, ['original'])
    expect(() => blobs.assertCheckpointReferences(['archive'])).toThrow(BlobIntegrityError)
    try {
      blobs.assertCheckpointReferences(['archive'])
    }
    catch (error) {
      expect(error).toMatchObject({
        name: 'BlobIntegrityError',
        retryable: false,
        failures: [{ blobId: 'original', status: 'missing-dependency', message: expect.stringContaining('referenced by turn') }],
      })
    }
    expect(() => blobs.assertCheckpointReferences([])).toThrow(/original/)
    blobs.addInheritedReferences(['original'])
    expect(() => blobs.assertCheckpointReferences(['original', 'archive'])).toThrow(/original/)
    blobs.cacheBlob('original', 'Yw==')
    blobs.markClientSaved('original')
    blobs.assertCheckpointReferences(['archive'])
    blobs.markClientSaved('unretained')
    expect(() => blobs.assertCheckpointReferences(['unretained'])).toThrow(/unretained.*missing-reference/)
    expect(blobs.getPendingBlobs().map(blob => blob.blobId)).toEqual(['archive', 'turn'])
  })

  it('throws before exceeding the payload budget instead of evicting required blobs', () => {
    const blobs = new RunBlobStore({ maxBytes: 6 })
    const rawBytes = new Uint8Array([1, 2])
    blobs.cacheBlob('first', 'AQI=', rawBytes)
    rawBytes[0] = 9
    blobs.markClientSaved('first')
    expect(() => blobs.cacheBlob('second', 'Yg==')).toThrow(BlobResourceLimitError)
    expect(() => blobs.cacheBlob('second', 'Yg==')).toThrow(/requires 10 bytes, limit 6.*cannot be evicted/)
    expect(blobs.getBlob('first')).toMatchObject({ blobData: 'AQI=', blobDataRaw: new Uint8Array([1, 2]) })
    expect(blobs.getCachedBlob('second')).toBeUndefined()
    expect(blobs.getStats()).toMatchObject({ entries: 1, bytes: 6 })
  })

  it('preserves exact client JSON wire bytes and rejects conflicting content as an integrity error', () => {
    const blobs = new RunBlobStore()
    const rawJson = new TextEncoder().encode('{"role":"user","content":"client fork"}')
    const normalized = Buffer.from(rawJson).toString('base64')
    blobs.cacheBlob('client-json', normalized, rawJson, ['first-reference'])
    blobs.cacheBlob('client-json', normalized, undefined, ['second-reference'])
    expect(blobs.getBlob('client-json')).toMatchObject({
      blobData: normalized,
      blobDataRaw: rawJson,
      dependencies: ['first-reference', 'second-reference'],
    })
    expect(() => blobs.cacheBlob('client-json', 'YQ==')).toThrow(BlobIntegrityError)
    expect(() => blobs.cacheBlob('client-json', normalized, new Uint8Array([9]))).toThrow(BlobIntegrityError)
    try {
      blobs.cacheBlob('client-json', 'YQ==')
    }
    catch (error) {
      expect(error).toMatchObject({ name: 'BlobIntegrityError', retryable: false, failures: [{ blobId: 'client-json', status: 'content-conflict' }] })
    }
    expect(blobs.getCachedBlob('client-json')).toBe(normalized)
  })

  it('limits distinct records even with zero payload bytes and does not double-count identities', () => {
    const blobs = new RunBlobStore({ maxBytes: 0, maxRecords: 2 })
    blobs.addInheritedReferences(['incoming'])
    blobs.cacheBlob('incoming', '')
    blobs.markClientSaved('incoming')
    blobs.cacheBlob('generated', '', undefined, ['incoming'])
    blobs.reserveBlobRecord('generated')
    expect(blobs.getStats()).toMatchObject({ bytes: 0, entries: 2, records: 2 })
    expect(() => blobs.cacheBlob('overflow', '')).toThrow(BlobResourceLimitError)
    expect(() => blobs.addInheritedReferences(['overflow'])).toThrow(/requires 3 records, limit 2/)
    expect(() => blobs.cacheBlob('generated', '', undefined, ['overflow'])).toThrow(BlobResourceLimitError)
    expect(blobs.getBlob('generated')?.dependencies).toEqual(['incoming'])
    expect(blobs.getCachedBlob('overflow')).toBeUndefined()
    blobs.dispose()
    expect(blobs.getStats().records).toBe(0)
  })

  it('reports disposed access and invalid resource options with terminal named errors', () => {
    const run = createRun()
    run.dispose()
    for (const access of [
      () => run.blobs.cacheBlob('late', 'YQ=='),
      () => run.blobs.assertCheckpointReferences([]),
      () => run.allocateBlobId(),
      () => run.recordGetRequestSent(),
    ]) {
      expect(access).toThrow(BlobInactiveError)
      expect(access).toThrow(BlobIntegrityError)
    }
    const inactive = new BlobInactiveError('inactive')
    expect(inactive.retryable).toBe(false)
    expect(inactive.name).toMatch(/^Blob.*Error$/)
    for (const options of [{ maxBytes: -1 }, { maxRecords: -1 }, { batchSize: 0 }, { timeoutMs: -1 }, { overallTimeoutMs: Number.NaN }])
      expect(() => createRun(null, options)).toThrow(BlobResourceLimitError)
    expect(new BlobResourceLimitError('over budget')).toMatchObject({ name: 'BlobResourceLimitError', retryable: false })
  })

  it('isolates decoded caches and clears all retained state on disposal', () => {
    const first = createRun()
    const second = createRun()
    first.blobs.cacheBlob('blob', 'YQ==')
    first.blobs.historyEntries.set('blob', { blobId: 'blob', raw: {}, message: { role: 'user', content: 'hello' } })
    first.blobs.turnBaselines.set('turn', { userMessageBlobId: 'blob', stepBlobIds: [] })
    expect(second.blobs.getCachedBlob('blob')).toBeUndefined()
    expect(second.blobs.historyEntries.size).toBe(0)
    first.dispose()
    expect(first.blobs.getStats()).toMatchObject({ entries: 0, bytes: 0 })
    expect(first.blobs.historyEntries.size).toBe(0)
    expect(first.blobs.turnBaselines.size).toBe(0)
    expect(first.session!.closed).toBe(false)
    expect(second.signal.aborted).toBe(false)
  })

  it('never reuses request ids on a reused session, including overlapping contexts', () => {
    const session = createEphemeralSession('reused-session')
    const first = createRun(session)
    const overlapping = createRun(session)
    expect(first.allocateBlobId()).toBe(900_000)
    expect(overlapping.allocateBlobId()).toBe(900_001)
    expect(first.allocateBlobId()).toBe(900_002)
    first.dispose()
    expect(createRun(session).allocateBlobId()).toBe(900_003)
    session.nextBlobRequestId = 0xFFFF_FFFF
    expect(overlapping.allocateBlobId()).toBe(0xFFFF_FFFF)
    expect(() => overlapping.allocateBlobId()).toThrow(/id space exhausted/)
  })
})

describe('run-owned client Get', () => {
  it('returns explicit no-session failures without sending frames', async () => {
    const run = createRun(null)
    const unexpectedFrame = vi.fn()
    const result = await drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('one'), blobIdToBytes('one')] }), unexpectedFrame)
    expect(result.map(entry => entry.status)).toEqual(['no-session', 'no-session'])
    expect(await drain(fetchBlobsFromClient({ run, blobIds: [] }))).toEqual([])
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(run.getClientReadCount()).toBe(1)
    expect(run.getSentGetRequestCount()).toBe(0)
  })

  it('deduplicates repeated, concurrent, successful and failed reads in one run', async () => {
    const run = createRun()
    const generator = fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('same'), blobIdToBytes('same'), blobIdToBytes('missing')] })
    const first = await nextKvFrame(generator)
    const missing = await nextKvFrame(generator)
    const ownerResult = drain(generator)
    const duplicateFrames = vi.fn()
    const concurrentResult = drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('same')] }), duplicateFrames)
    replyGet(run.session!, missing.requestId)
    replyGet(run.session!, first.requestId, new Uint8Array([3, 4]))
    const result = await ownerResult
    expect(result.map(entry => entry.status)).toEqual(['ok', 'ok', 'not-found'])
    expect(result[0]).toBe(result[1])
    expect(await concurrentResult).toEqual([result[0]])
    expect(await drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('missing'), blobIdToBytes('same')] }), duplicateFrames)).toEqual([result[2], result[0]])
    expect(duplicateFrames).not.toHaveBeenCalled()
    expect(run.getClientReadCount()).toBe(2)
    expect(run.getSentGetRequestCount()).toBe(2)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('uses exact positive numeric id and kind, preserves order, and decodes only matched replies', async () => {
    const run = createRun()
    const generator = fetchBlobsFromClient({ run, blobIds: ['first', 'second', 'third'].map(blobIdToBytes) })
    const first = await nextKvFrame(generator)
    expect(run.session!.listeners.size).toBe(4)
    const second = await nextKvFrame(generator)
    const third = await nextKvFrame(generator)
    const unreadPayload = vi.fn(() => {
      throw new Error('Unmatched payload must not be decoded')
    })
    const unrelatedResult = {
      get blobData() {
        return unreadPayload()
      },
    }
    // Transports supply encoded byte length independently of payload decoding.
    for (const invalidId of [undefined, null, 0, -1, String(first.requestId), first.requestId + 99])
      pushSessionMessage(run.session!, { kvClientMessage: { id: invalidId, getBlobResult: unrelatedResult } }, 64)
    replySet(run.session!, first.requestId)
    const payload = vi.fn(() => Buffer.from('first').toString('base64'))
    pushSessionMessage(run.session!, {
      kvClientMessage: {
        id: first.requestId,
        getBlobResult: {
          get blobData() {
            return payload()
          },
        },
      },
    }, 64)
    replyGet(run.session!, third.requestId, new TextEncoder().encode('third'))
    pushSessionMessage(run.session!, { kvClientMessage: { id: second.requestId, getBlobResult: { blobData: new TextEncoder().encode('second') } } })
    const result = await drain(generator)
    expect(result.map(entry => entry.status === 'ok' ? Buffer.from(entry.bytes).toString('utf8') : entry.status)).toEqual(['first', 'second', 'third'])
    expect(payload).toHaveBeenCalledTimes(1)
    expect(unreadPayload).not.toHaveBeenCalled()
    // The wrong-kind reply for our completed ID is retired; unrelated IDs remain.
    expect(run.session!.messages.length).toBe(6)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('distinguishes not-found, client error, malformed bytes and valid empty bytes', async () => {
    const run = createRun()
    let index = 0
    const result = await drain(fetchBlobsFromClient({ run, blobIds: ['missing', 'error', 'bad', 'empty', 'good'].map(blobIdToBytes) }), (frame) => {
      const request = readKvFrame(frame)!
      switch (index++) {
        case 0:
          replyGet(run.session!, request.requestId)
          break
        case 1:
          replyGet(run.session!, request.requestId, new Uint8Array([1]), 'read failed')
          break
        case 2:
          pushSessionMessage(run.session!, { kvClientMessage: { id: request.requestId, getBlobResult: { blobData: '%%%invalid%%%' } } })
          break
        case 3:
          replyGet(run.session!, request.requestId, new Uint8Array())
          break
        default:
          replyGet(run.session!, request.requestId, new Uint8Array([5]))
          break
      }
    })
    expect(result.map(entry => entry.status)).toEqual(['not-found', 'client-error', 'decode-error', 'ok', 'ok'])
    expect(result[1]).toMatchObject({ message: 'read failed' })
    expect(result[3]).toEqual({ status: 'ok', bytes: new Uint8Array() })
    expect(run.blobs.isClientSaved('error')).toBe(false)
  })

  it('preserves other successes and later batches when one request times out', async () => {
    const run = createRun(undefined, { timeoutMs: 100, batchSize: 100 })
    const requests: number[] = []
    const pending = drain(fetchBlobsFromClient({ run, blobIds: Array.from({ length: 40 }, (_, index) => blobIdToBytes(`blob-${index}`)) }), (frame) => {
      const request = readKvFrame(frame)
      if (!request)
        return
      requests.push(request.requestId)
      if (Buffer.from(request.blobId).toString() !== 'blob-5')
        replyGet(run.session!, request.requestId, new Uint8Array([7]))
    })
    await vi.advanceTimersByTimeAsync(0)
    expect(CLIENT_BLOB_FETCH_BATCH_SIZE).toBe(32)
    expect(requests).toHaveLength(32)
    await vi.advanceTimersByTimeAsync(100)
    const result = await pending
    expect(requests).toHaveLength(40)
    expect(result[5]).toMatchObject({ status: 'timeout' })
    expect(result.filter(entry => entry.status === 'ok')).toHaveLength(39)
    expect(Date.now()).toBe(100)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('starts each request timeout at its own send, not at batch registration', async () => {
    const run = createRun(undefined, { timeoutMs: 100 })
    const generator = fetchBlobsFromClient({ run, blobIds: ['slow', 'late-send'].map(blobIdToBytes) })
    await nextKvFrame(generator)
    await vi.advanceTimersByTimeAsync(80)
    const second = await nextKvFrame(generator)
    const pending = drain(generator)
    await vi.advanceTimersByTimeAsync(95)
    replyGet(run.session!, second.requestId, new Uint8Array([9]))
    expect((await pending).map(entry => entry.status)).toEqual(['timeout', 'ok'])
    expect(vi.getTimerCount()).toBe(0)
  })

  it('bounds the whole operation and classifies unsent reads without discarding successes', async () => {
    const run = createRun(undefined, { timeoutMs: 100, overallTimeoutMs: 60, batchSize: 1 })
    let sends = 0
    const pending = drain(fetchBlobsFromClient({ run, blobIds: ['first', 'second', 'unsent'].map(blobIdToBytes) }), (frame) => {
      const request = readKvFrame(frame)
      if (request && sends++ === 0)
        setTimeout(() => replyGet(run.session!, request.requestId, new Uint8Array([1])), 40)
    })
    await vi.advanceTimersByTimeAsync(60)
    expect((await pending).map(entry => entry.status)).toEqual(['ok', 'overall-timeout', 'overall-timeout'])
    expect(sends).toBe(2)
    expect(run.getSentGetRequestCount()).toBe(2)
    expect(run.getClientReadCount()).toBe(3)
    expect(vi.getTimerCount()).toBe(0)
    expect(run.session!.listeners.size).toBe(1)
  })

  it('does not let a duplicate reader timeout cancel the original request', async () => {
    const run = createRun(undefined, { timeoutMs: 100 })
    const owner = fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('shared')] })
    const request = await nextKvFrame(owner)
    const ownerResult = drain(owner)
    const duplicateFrames = vi.fn()
    const duplicateResult = drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('shared')], overallTimeoutMs: 10 }), duplicateFrames)
    await vi.advanceTimersByTimeAsync(10)
    expect(await duplicateResult).toMatchObject([{ status: 'overall-timeout' }])
    replyGet(run.session!, request.requestId, new Uint8Array([6]))
    expect(await ownerResult).toEqual([{ status: 'ok', bytes: new Uint8Array([6]) }])
    expect(duplicateFrames).not.toHaveBeenCalled()
    expect(run.signal.aborted).toBe(false)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['ok', 'timeout'] as const)('does not reuse cached %s results or late replies in a later run on the same session', async (oldStatus) => {
    const session = createEphemeralSession('next-run')
    const firstRun = createRun(session, { timeoutMs: 10 })
    const first = fetchBlobsFromClient({ run: firstRun, blobIds: [blobIdToBytes('same')] })
    const oldRequest = await nextKvFrame(first)
    if (oldStatus === 'ok')
      replyGet(session, oldRequest.requestId, new Uint8Array([1]))
    const oldResult = drain(first)
    await vi.advanceTimersByTimeAsync(10)
    expect(await oldResult).toMatchObject([{ status: oldStatus }])
    expect(await drain(fetchBlobsFromClient({ run: firstRun, blobIds: [blobIdToBytes('same')] }))).toMatchObject([{ status: oldStatus }])
    firstRun.dispose()
    replyGet(session, oldRequest.requestId, new Uint8Array([1]))
    const nextRun = createRun(session)
    const next = fetchBlobsFromClient({ run: nextRun, blobIds: [blobIdToBytes('same')] })
    const nextRequest = await nextKvFrame(next)
    expect(nextRequest.requestId).toBeGreaterThan(oldRequest.requestId)
    replyGet(session, nextRequest.requestId, new Uint8Array([2]))
    expect(await drain(next)).toEqual([{ status: 'ok', bytes: new Uint8Array([2]) }])
    expect(session.messages).toHaveLength(0)
  })

  it('expires while a consumer is paused at a send and never emits the remaining frames', async () => {
    const run = createRun(undefined, { timeoutMs: 100, overallTimeoutMs: 50 })
    const generator = fetchBlobsFromClient({ run, blobIds: ['sent', 'unsent'].map(blobIdToBytes) })
    await nextKvFrame(generator)
    await vi.advanceTimersByTimeAsync(50)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
    const result = await generator.next()
    expect(result.done).toBe(true)
    expect(result.value).toMatchObject([{ status: 'overall-timeout' }, { status: 'overall-timeout' }])
    expect(run.getSentGetRequestCount()).toBe(1)
  })

  it('checks current transport state before sending even without a notification', async () => {
    const run = createRun()
    run.session!.closed = true
    const unexpectedFrame = vi.fn()
    expect(await drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('unavailable')] }), unexpectedFrame)).toMatchObject([{ status: 'cancelled' }])
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['cancel', 'close', 'dispose'] as const)('cleans up %s without waiting for timeout or closing other sessions', async (action) => {
    const run = createRun()
    const otherRun = createRun()
    const generator = fetchBlobsFromClient({ run, blobIds: ['first', 'second'].map(blobIdToBytes) })
    await nextKvFrame(generator)
    const independentWait = waitForMessageMatching(run.session!, message => 'execClientMessage' in message, 500)
    if (action === 'cancel')
      pushSessionMessage(run.session!, { conversationAction: { cancelAction: {} } })
    else if (action === 'close')
      markSessionClosed(run.session!)
    else
      run.dispose()
    expect((await drain(generator)).map(entry => entry.status)).toEqual(['cancelled', 'cancelled'])
    expect(otherRun.session!.closed).toBe(false)
    expect(otherRun.signal.aborted).toBe(false)
    if (action === 'dispose') {
      expect(run.session!.closed).toBe(false)
      expect(run.session!.listeners.size).toBe(1)
      expect(vi.getTimerCount()).toBe(1)
      pushSessionMessage(run.session!, { execClientMessage: { id: 123 } })
      expect(await independentWait).toEqual({ execClientMessage: { id: 123 } })
      expect(run.getClientReadCount()).toBe(0)
      expect(run.blobs.getStats().bytes).toBe(0)
    }
    else {
      expect(await independentWait).toBeNull()
    }
    expect(run.session!.listeners.size).toBe(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('cleans up abandoned generators and completes shared pending reads', async () => {
    const run = createRun()
    const generator = fetchBlobsFromClient({ run, blobIds: ['first', 'unsent'].map(blobIdToBytes) })
    await nextKvFrame(generator)
    expect(run.getClientReadCount()).toBe(2)
    expect(run.getSentGetRequestCount()).toBe(1)
    await generator.return([])
    expect(await drain(fetchBlobsFromClient({ run, blobIds: ['first', 'unsent'].map(blobIdToBytes) }))).toMatchObject([{ status: 'cancelled' }, { status: 'cancelled' }])
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
    expect(run.getSentGetRequestCount()).toBe(1)
    run.dispose()
    expect(run.getSentGetRequestCount()).toBe(0)
  })

  it('keeps arbitrary wire keys distinct and bounds raw result retention', async () => {
    const run = createRun(undefined, { maxBytes: 2 })
    const keys = [new Uint8Array([0xFF]), new Uint8Array([0xFE])]
    const result = await drain(fetchBlobsFromClient({ run, blobIds: keys }), (frame) => {
      const request = readKvFrame(frame)!
      replyGet(run.session!, request.requestId, new Uint8Array([request.blobId[0]!]))
    })
    expect(result).toEqual(keys.map(bytes => ({ status: 'ok', bytes })))
    expect(run.getClientReadCount()).toBe(2)
    expect(run.blobs.isClientSaved(String.fromCharCode(0xFFFD))).toBe(false)
    expect(run.blobs.getStats().bytes).toBe(2)
    await expect(drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('overflow')] }), (frame) => {
      replyGet(run.session!, readKvFrame(frame)!.requestId, new Uint8Array([3]))
    })).rejects.toBeInstanceOf(BlobResourceLimitError)
    expect(run.blobs.isClientSaved('overflow')).toBe(false)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['ok', 'not-found', 'client-error'] as const)('bounds cached %s records and cleans partial reservations when the cap is reached', async (status) => {
    const run = createRun(undefined, { maxRecords: 2, maxBytes: 0 })
    await drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('first')] }), (frame) => {
      const request = readKvFrame(frame)!
      replyGet(run.session!, request.requestId, status === 'ok' ? new Uint8Array() : undefined, status === 'client-error' ? 'client failed' : undefined)
    })
    const unexpectedFrame = vi.fn()
    await expect(drain(fetchBlobsFromClient({ run, blobIds: ['second', 'overflow'].map(blobIdToBytes) }), unexpectedFrame)).rejects.toBeInstanceOf(BlobResourceLimitError)
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(run.getSentGetRequestCount()).toBe(1)
    expect(run.getClientReadCount()).toBe(2)
    expect(await drain(fetchBlobsFromClient({ run, blobIds: ['first', 'second'].map(blobIdToBytes) }))).toMatchObject([{ status }, { status: 'cancelled' }])
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('confirms raw fork keys, prefixed text keys and legacy text hashes by exact canonical identity', async () => {
    const run = createRun()
    const rawHash = new Uint8Array(32).fill(0xFF)
    const otherHash = new Uint8Array(32).fill(0xFE)
    const legacyHash = blobIdToBytes(encodeBlob({ role: 'user', content: 'legacy' }).blobId)
    const prefixedText = new TextEncoder().encode('blob-bytes:literal-client-key')
    const result = await drain(fetchBlobsFromClient({ run, blobIds: [rawHash, otherHash, legacyHash, prefixedText] }), (frame) => {
      const request = readKvFrame(frame)!
      replyGet(run.session!, request.requestId, request.blobId[0] === 0xFE ? undefined : new Uint8Array([7]))
    })
    expect(result.map(entry => entry.status)).toEqual(['ok', 'not-found', 'ok', 'ok'])
    for (const wireKey of [rawHash, legacyHash, prefixedText]) {
      const canonicalBlobId = blobIdFromBytes(wireKey)
      expect(run.blobs.isClientSaved(canonicalBlobId)).toBe(true)
      expect(blobIdToBytes(canonicalBlobId)).toEqual(wireKey)
    }
    expect(run.blobs.isClientSaved(blobIdFromBytes(otherHash))).toBe(false)
    expect(run.getSentGetRequestCount()).toBe(4)
  })

  it('reserves default-zero replies for their original request instead of treating them as no-id fallback', async () => {
    const run = createRun(undefined, { timeoutMs: 10 })
    const generator = fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('pending')] })
    const request = await nextKvFrame(generator)
    expect(request.requestId).toBeGreaterThan(0)
    replyGet(run.session!, 0, new Uint8Array([0]))
    expect(run.session!.messages).toEqual([{ kvClientMessage: { getBlobResult: { blobData: 'AA==' } } }])
    const pending = drain(generator)
    await vi.advanceTimersByTimeAsync(10)
    expect(await pending).toMatchObject([{ status: 'timeout' }])
    expect(run.blobs.isClientSaved('pending')).toBe(false)
    expect(run.session!.messages).toHaveLength(1)
    expect(vi.getTimerCount()).toBe(0)
  })
})

describe('checkpoint Set confirmation barrier', () => {
  it('allows inherited-only checkpoints but rejects unavailable refs and pending data without a session', async () => {
    const run = createRun(null, { inheritedBlobIds: ['inherited'] })
    expect(await drain(saveCheckpointBlobs(run, ['inherited']))).toBeUndefined()
    await expect(drain(saveCheckpointBlobs(run, ['unknown']))).rejects.toBeInstanceOf(BlobIntegrityError)
    run.blobs.cacheBlob('inherited', 'YQ==')
    await expect(drain(saveCheckpointBlobs(run, ['inherited']))).rejects.toMatchObject({
      name: 'BlobTransferError',
      retryable: true,
      failures: [{ blobId: 'inherited', status: 'no-session', message: expect.stringMatching(/without a session.*1 pending/) }],
    })
  })

  it('waits for an explicit successful Set reply instead of confirming when yielded', async () => {
    const run = createRun(undefined, { timeoutMs: 100 })
    run.blobs.cacheBlob('pending', 'YQ==')
    const generator = saveCheckpointBlobs(run, ['pending'])
    const request = await nextKvFrame(generator)
    expect(run.blobs.isClientSaved('pending')).toBe(false)
    replyGet(run.session!, request.requestId, new Uint8Array([1]))
    replySet(run.session!, 0)
    const pending = drain(generator)
    await vi.advanceTimersByTimeAsync(90)
    expect(run.blobs.isClientSaved('pending')).toBe(false)
    replySet(run.session!, request.requestId)
    await pending
    expect(run.blobs.isClientSaved('pending')).toBe(true)
    expect(run.blobs.getPendingBlobs()).toEqual([])
    expect(run.session!.messages).toHaveLength(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('sends archive originals and preserves JSON-text versus raw-protobuf bytes', async () => {
    const run = createRun()
    const original = encodeBlob({ role: 'user', content: 'archive original' })
    const archive = encodeBinaryBlob(new Uint8Array([8, 1, 18, 3, 0, 255, 7]))
    run.blobs.cacheBlob(original.blobId, original.blobData)
    run.blobs.cacheBlob(archive.blobId, archive.blobData, archive.blobDataRaw, [original.blobId])
    const frames: AgentServerMessage[] = []
    await drain(saveCheckpointBlobs(run, [archive.blobId]), (frame) => {
      frames.push(frame)
      replySet(run.session!, readKvFrame(frame)!.requestId)
    })
    expect(frames).toHaveLength(2)
    const originalRequest = readKvFrame(frames[0]!)!
    const archiveRequest = readKvFrame(frames[1]!)!
    expect(originalRequest.blobId).toEqual(blobIdToBytes(original.blobId))
    expect(originalRequest.blobData).toEqual(new TextEncoder().encode(original.blobData))
    expect(archiveRequest.blobData).toEqual(archive.blobDataRaw)
    expect(archiveRequest.requestId).toBeGreaterThan(originalRequest.requestId)
    expect(run.blobs.getPendingBlobs()).toEqual([])
    const unexpectedFrame = vi.fn()
    await drain(saveCheckpointBlobs(run, [archive.blobId]), unexpectedFrame)
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(frames.every(frame => frame.message.case === 'kvServerMessage')).toBe(true)
    expect(run.getSentGetRequestCount()).toBe(0)
  })

  it('resends retained client JSON bytes and raw fork ids without re-encoding either', async () => {
    const run = createRun()
    const rawBlobId = new Uint8Array(32).fill(0xFF)
    const canonicalBlobId = blobIdFromBytes(rawBlobId)
    const rawJson = new TextEncoder().encode('{ "role": "user", "content": "fork original" }')
    const normalized = Buffer.from(rawJson).toString('base64')
    run.blobs.cacheBlob(canonicalBlobId, normalized, rawJson)
    run.blobs.cacheBlob(canonicalBlobId, normalized)
    const generator = saveCheckpointBlobs(run, [canonicalBlobId])
    const request = await nextKvFrame(generator)
    expect(request.blobId).toEqual(rawBlobId)
    expect(request.blobData).toEqual(rawJson)
    expect(run.blobs.isClientSaved(canonicalBlobId)).toBe(false)
    replySet(run.session!, request.requestId)
    await drain(generator)
    expect(run.blobs.isClientSaved(canonicalBlobId)).toBe(true)
  })

  it('retains error and timeout blobs while confirming another successful Set', async () => {
    const run = createRun(undefined, { timeoutMs: 100 })
    for (const blobId of ['error', 'timeout', 'success'])
      run.blobs.cacheBlob(blobId, 'YQ==')
    const pending = drain(saveCheckpointBlobs(run, ['success']), (frame) => {
      const request = readKvFrame(frame)!
      const blobId = Buffer.from(request.blobId).toString()
      if (blobId === 'error')
        replySet(run.session!, request.requestId, 'disk rejected write')
      if (blobId === 'success')
        replySet(run.session!, request.requestId)
    })
    const failure = expect(pending).rejects.toMatchObject({
      name: 'BlobTransferError',
      retryable: true,
      failures: [
        { blobId: 'error', requestId: 900_000, status: 'client-error', message: 'disk rejected write' },
        { blobId: 'timeout', requestId: 900_001, status: 'timeout', message: expect.stringContaining('100ms after send') },
      ],
    })
    await vi.advanceTimersByTimeAsync(100)
    await failure
    await expect(pending).rejects.toBeInstanceOf(BlobTransferError)
    expect(run.blobs.getPendingBlobs().map(blob => blob.blobId)).toEqual(['error', 'timeout'])
    expect(run.blobs.isClientSaved('success')).toBe(true)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each([
    { payload: { success: {} }, expectedStatus: 'decode-error', retryable: false },
    { payload: { error: {} }, expectedStatus: 'client-error', retryable: true },
    { payload: null, expectedStatus: 'decode-error', retryable: false },
  ])('rejects malformed or error Set payload $expectedStatus', async ({ payload, expectedStatus, retryable }) => {
    const run = createRun()
    run.blobs.cacheBlob('pending', 'YQ==')
    await expect(drain(saveCheckpointBlobs(run, ['pending']), (frame) => {
      pushSessionMessage(run.session!, { kvClientMessage: { id: readKvFrame(frame)!.requestId, setBlobResult: payload } })
    })).rejects.toMatchObject({
      name: 'BlobTransferError',
      retryable,
      failures: [{ blobId: 'pending', requestId: 900_000, status: expectedStatus, message: expect.any(String) }],
    })
    expect(run.blobs.isClientSaved('pending')).toBe(false)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not allow an old late Set ACK to confirm a retried save', async () => {
    const run = createRun(undefined, { timeoutMs: 10 })
    run.blobs.cacheBlob('pending', 'YQ==')
    const first = saveCheckpointBlobs(run, ['pending'])
    const oldRequest = await nextKvFrame(first)
    const failure = expect(drain(first)).rejects.toThrow(/timeout/)
    await vi.advanceTimersByTimeAsync(10)
    await failure
    replySet(run.session!, oldRequest.requestId)
    const second = saveCheckpointBlobs(run, ['pending'])
    const newRequest = await nextKvFrame(second)
    expect(newRequest.requestId).toBeGreaterThan(oldRequest.requestId)
    expect(run.blobs.isClientSaved('pending')).toBe(false)
    replySet(run.session!, newRequest.requestId)
    await drain(second)
    expect(run.blobs.isClientSaved('pending')).toBe(true)
    expect(run.session!.messages).toHaveLength(0)
  })

  it('enforces a total Set budget across batches and leaves unsent blobs pending', async () => {
    const run = createRun(undefined, { timeoutMs: 100, overallTimeoutMs: 50, batchSize: 1 })
    for (const blobId of ['first', 'second', 'unsent'])
      run.blobs.cacheBlob(blobId, 'YQ==')
    let sends = 0
    const failure = expect(drain(saveCheckpointBlobs(run, ['first']), (frame) => {
      const request = readKvFrame(frame)!
      if (sends++ === 0)
        setTimeout(() => replySet(run.session!, request.requestId), 30)
    })).rejects.toMatchObject({
      name: 'BlobTransferError',
      retryable: true,
      failures: [
        { blobId: 'second', requestId: 900_001, status: 'overall-timeout', message: expect.stringContaining('50ms overall budget') },
        { blobId: 'unsent', status: 'overall-timeout' },
      ],
    })
    await vi.advanceTimersByTimeAsync(50)
    await failure
    expect(sends).toBe(2)
    expect(run.blobs.isClientSaved('first')).toBe(true)
    expect(run.blobs.getPendingBlobs().map(blob => blob.blobId)).toEqual(['second', 'unsent'])
    expect(vi.getTimerCount()).toBe(0)
  })

  it('reports an expired Set budget before sending anything as a typed retryable failure', async () => {
    const run = createRun(undefined, { overallTimeoutMs: 0 })
    run.blobs.cacheBlob('unsent', 'YQ==')
    const unexpectedFrame = vi.fn()
    await expect(drain(saveCheckpointBlobs(run, ['unsent']), unexpectedFrame)).rejects.toMatchObject({
      name: 'BlobTransferError',
      retryable: true,
      failures: [{ blobId: 'unsent', status: 'overall-timeout', message: expect.stringContaining('0ms overall budget') }],
    })
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(run.blobs.isClientSaved('unsent')).toBe(false)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['Get', 'Set'] as const)('cleans all pre-send %s waiters when id exhaustion interrupts batch preparation', async (operation) => {
    const run = createRun()
    run.session!.nextBlobRequestId = 0xFFFF_FFFF
    const unexpectedFrame = vi.fn()
    if (operation === 'Get') {
      await expect(drain(fetchBlobsFromClient({ run, blobIds: ['first', 'second'].map(blobIdToBytes) }), unexpectedFrame)).rejects.toBeInstanceOf(BlobResourceLimitError)
      expect(await drain(fetchBlobsFromClient({ run, blobIds: ['first', 'second'].map(blobIdToBytes) }))).toMatchObject([{ status: 'cancelled' }, { status: 'cancelled' }])
    }
    else {
      run.blobs.cacheBlob('first', 'YQ==')
      run.blobs.cacheBlob('second', 'Yg==')
      await expect(drain(saveCheckpointBlobs(run, ['first', 'second']), unexpectedFrame)).rejects.toBeInstanceOf(BlobResourceLimitError)
      expect(run.blobs.getPendingBlobs()).toHaveLength(2)
    }
    expect(unexpectedFrame).not.toHaveBeenCalled()
    expect(run.getSentGetRequestCount()).toBe(0)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['cancel', 'dispose'] as const)('stops Set batches on %s without confirming pending data', async (action) => {
    const run = createRun()
    run.blobs.cacheBlob('first', 'YQ==')
    run.blobs.cacheBlob('second', 'Yg==')
    const generator = saveCheckpointBlobs(run, ['first', 'second'])
    await nextKvFrame(generator)
    if (action === 'dispose')
      run.dispose()
    else
      pushSessionMessage(run.session!, { conversationAction: { cancelAction: {} } })
    await expect(drain(generator)).rejects.toBeInstanceOf(AgentRunAbortedError)
    expect(run.blobs.isClientSaved('first')).toBe(false)
    expect(run.session!.closed).toBe(false)
    expect(run.session!.listeners.size).toBe(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('cleans up a discarded Set generator and keeps its blob pending', async () => {
    const run = createRun()
    run.blobs.cacheBlob('pending', 'YQ==')
    run.blobs.cacheBlob('unsent', 'Yg==')
    const generator = saveCheckpointBlobs(run, ['pending'])
    await nextKvFrame(generator)
    expect(run.session!.listeners.size).toBe(3)
    await generator.return()
    expect(run.blobs.getPendingBlobs()).toHaveLength(2)
    expect(run.session!.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('recognizes a successful client Get as confirmation, not merely an inherited reference', async () => {
    const run = createRun(undefined, { inheritedBlobIds: ['existing'] })
    const fetched = await drain(fetchBlobsFromClient({ run, blobIds: [blobIdToBytes('existing')] }), (frame) => {
      replyGet(run.session!, readKvFrame(frame)!.requestId, new TextEncoder().encode('YQ=='))
    })
    expect(fetched[0]?.status).toBe('ok')
    run.blobs.cacheBlob('existing', 'YQ==')
    const unexpectedSet = vi.fn()
    await drain(saveCheckpointBlobs(run, ['existing']), unexpectedSet)
    expect(unexpectedSet).not.toHaveBeenCalled()
    expect(run.blobs.isClientSaved('existing')).toBe(true)
  })
})

describe('cancellable session waits and heartbeat timer cleanup', () => {
  it('removes only the aborted waiter and does not consume already queued data for an aborted signal', async () => {
    const session = createEphemeralSession('independent-waits')
    const controller = new AbortController()
    const abandoned = waitForMessageMatching(session, message => 'first' in message, 100, controller.signal)
    const active = waitForMessageMatching(session, message => 'second' in message, 200)
    controller.abort()
    expect(await abandoned).toBeNull()
    expect(session.listeners.size).toBe(1)
    expect(vi.getTimerCount()).toBe(1)
    pushSessionMessage(session, { first: true })
    expect(await waitForMessageMatching(session, message => 'first' in message, 100, controller.signal)).toBeNull()
    expect(session.messages).toEqual([{ first: true }])
    pushSessionMessage(session, { second: true })
    expect(await active).toEqual({ second: true })
    expect(session.listeners.size).toBe(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('clears the losing heartbeat timer on both resolution and rejection', async () => {
    const success = waitForPromiseWithHeartbeat(Promise.resolve('complete'), 1_000)
    expect(await success.next()).toEqual({ done: true, value: 'complete' })
    expect(vi.getTimerCount()).toBe(0)
    const failure = waitForPromiseWithHeartbeat(Promise.reject(new Error('failed')), 1_000)
    await expect(failure.next()).rejects.toThrow('failed')
    expect(vi.getTimerCount()).toBe(0)
  })
})
