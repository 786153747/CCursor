import type { JsonObject, MessageInitShape } from '@bufbuild/protobuf'
import type { ConnectRouter, ServiceImpl } from '@connectrpc/connect'
import type { AgentSession } from '../handlers/agent/session'
import type { LLMStreamEvent, LLMStreamRequest } from '../handlers/llm/types'
import type { ClientBlobFixture } from './blobTestClient'
import { randomUUID } from 'node:crypto'
import { getEventListeners } from 'node:events'
import { tmpdir } from 'node:os'
import { create, fromBinary, fromJson, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, createHandlerContext } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { resetAgentDatabaseForTests } from '../database/sqlite'
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  AgentService,
  RequestContextRulesPartSchema,
} from '../gen/agent_v1_pb'
import { BidiAppendRequestSchema, BidiService } from '../gen/aiserver_v1_pb'
import { BidiRequestIdSchema } from '../gen/aiserver_v1_shared_pb'
import {
  closeSession,
  createEphemeralSession,
  discardSessionMessages,
  getOrCreateSession,
  getTransportResourceUsage,
  markSessionClosed,
  MAX_LIVE_SSE_SESSIONS,
  MAX_PENDING_APPENDS,
  MAX_SESSION_QUEUED_MESSAGES,
  MAX_TRANSPORT_QUEUED_BYTES,
  pushSessionMessage,
  SESSION_RETENTION_MS,
  waitForMessageMatching,
} from '../handlers/agent/session'
import { AGENT_STARTUP_TIMEOUT_MS } from '../handlers/agent/transportStartup'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import registerAgentService from '../services/core/AgentService'
import registerBidiService from '../services/core/BidiService'
import { BlobTestClient, OpenClientMessageStream, wireKey } from './blobTestClient'

const { fetchBoundary } = vi.hoisted(() => ({ fetchBoundary: vi.fn<typeof fetch>() }))

// Early-consumer tests restore the real provider/SDK and stop only HTTP here.
vi.mock('undici', async (importOriginal) => {
  const original = await importOriginal<typeof import('undici')>()
  return { ...original, fetch: fetchBoundary }
})

type TransportKind = 'bidi' | 'sse'
type AgentHandlers = Pick<ServiceImpl<typeof AgentService>, 'run' | 'runSSE'>
type AppendHandler = ServiceImpl<typeof BidiService>['bidiAppend']

/** Capture real production registrations. No orchestrator/session/KV mocks. */
function captureTransportHandlers(): AgentHandlers & { bidiAppend: AppendHandler } {
  let agentHandlers: Partial<ServiceImpl<typeof AgentService>> | undefined
  let bidiHandlers: Partial<ServiceImpl<typeof BidiService>> | undefined
  const router: ConnectRouter = {
    handlers: [],
    service(service, implementation) {
      if (service.typeName === AgentService.typeName)
        agentHandlers = implementation as Partial<ServiceImpl<typeof AgentService>>
      else if (service.typeName === BidiService.typeName)
        bidiHandlers = implementation as Partial<ServiceImpl<typeof BidiService>>
      else
        throw new Error(`Unexpected registered service ${service.typeName}`)
      return this
    },
    rpc() {
      throw new Error('Expected full production service registration')
    },
  }
  registerAgentService(router)
  registerBidiService(router)
  if (!agentHandlers?.run || !agentHandlers.runSSE || !bidiHandlers?.bidiAppend)
    throw new Error('Production transport handlers were not registered')
  return { run: agentHandlers.run, runSSE: agentHandlers.runSSE, bidiAppend: bidiHandlers.bidiAppend }
}

const clients: TransportTestClient[] = []
const directSessions: AgentSession[] = []
const providerRequests: LLMStreamRequest[] = []
const IMAGE_BYTES = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='

class TransportTestClient extends BlobTestClient {
  readonly input = new OpenClientMessageStream()
  readonly disconnectController = new AbortController()
  readonly handlers = captureTransportHandlers()
  readonly context: ReturnType<typeof createHandlerContext>
  readonly appendFailures: unknown[] = []
  readonly withheldSetReplyIds = new Set<number>()
  registeredOutput?: AsyncIterator<MessageInitShape<typeof AgentServerMessageSchema>>
  private outputPause: Promise<void> | undefined
  releaseOutput: () => void = () => {}
  private nextAppendSequence = 0n

  constructor(readonly transport: TransportKind, options: {
    requestId?: string
    timeoutMs?: number
    fixtures?: ClientBlobFixture[]
  } = {}) {
    super(options.fixtures)
    const requestId = options.requestId ?? randomUUID()
    const method = AgentService.method[transport === 'bidi' ? 'run' : 'runSSE']
    this.context = createHandlerContext({
      service: AgentService,
      method,
      protocolName: 'connect',
      requestMethod: 'POST',
      url: `https://transport-test.invalid/${AgentService.typeName}/${method.name}`,
      requestHeader: { 'x-request-id': requestId },
      requestSignal: this.disconnectController.signal,
      timeoutMs: options.timeoutMs,
    })
    clients.push(this)
  }

  get requestId(): string {
    return this.context.requestHeader.get('x-request-id')!
  }

  openRegisteredOutput(): AsyncIterator<MessageInitShape<typeof AgentServerMessageSchema>> {
    const output = this.transport === 'bidi'
      ? this.handlers.run(this.input, this.context)
      : this.handlers.runSSE(create(BidiRequestIdSchema, { requestId: this.requestId }), this.context)
    this.registeredOutput = output[Symbol.asyncIterator]()
    return this.registeredOutput
  }

  startTransport(): void {
    const iterator = this.openRegisteredOutput()
    const output = { [Symbol.asyncIterator]: () => iterator }
    const waitForOutputPause = () => this.outputPause
    let firstSetReceived = false
    this.start((async function* () {
      for await (const frame of output) {
        const message = create(AgentServerMessageSchema, frame)
        yield fromBinary(AgentServerMessageSchema, toBinary(AgentServerMessageSchema, message))
        if (!firstSetReceived && message.message.case === 'kvServerMessage' && message.message.value.message.case === 'setBlobArgs') {
          firstSetReceived = true
          await waitForOutputPause()
        }
      }
    })())
  }

  pauseAfterFirstSet(): void {
    this.outputPause = new Promise((resolve) => {
      this.releaseOutput = resolve
    })
  }

  override send(json: JsonObject): void {
    const envelope = json.kvClientMessage
    if (envelope && typeof envelope === 'object' && !Array.isArray(envelope)
      && 'setBlobResult' in envelope && typeof envelope.id === 'number' && this.withheldSetReplyIds.has(envelope.id)) {
      return
    }
    if (this.transport === 'bidi') {
      const message = fromJson(AgentClientMessageSchema, json)
      this.input.push(fromBinary(AgentClientMessageSchema, toBinary(AgentClientMessageSchema, message)))
    }
    else {
      void this.appendAt(json, this.nextAppendSequence++).catch((error: unknown) => {
        this.appendFailures.push(error)
        this.context.abort(error)
      })
    }
  }

  async appendAt(json: JsonObject, sequence: bigint, binary = false): Promise<void> {
    this.nextAppendSequence = this.nextAppendSequence > sequence ? this.nextAppendSequence : sequence + 1n
    const bytes = toBinary(AgentClientMessageSchema, fromJson(AgentClientMessageSchema, json))
    const request = create(BidiAppendRequestSchema, {
      requestId: { requestId: this.requestId },
      appendSeqno: sequence,
      ...(binary ? { dataBinary: bytes } : { data: Buffer.from(bytes).toString('hex') }),
    })
    const context = createHandlerContext({
      service: BidiService,
      method: BidiService.method.bidiAppend,
      protocolName: 'connect',
      requestMethod: 'POST',
      url: `https://transport-test.invalid/${BidiService.typeName}/BidiAppend`,
    })
    try {
      expect(await this.handlers.bidiAppend(
        fromBinary(BidiAppendRequestSchema, toBinary(BidiAppendRequestSchema, request)),
        context,
      )).toEqual({})
    }
    finally {
      // Connect aborts HandlerContext.signal even for normal RPC completion.
      // This MUST NOT abort the longer-lived SSE stream.
      context.abort()
    }
  }

  disconnect(): void {
    this.disconnectController.abort(new ConnectError('Client disconnected', Code.Canceled))
  }
}

function userAction(text: string): JsonObject {
  return { userMessageAction: { userMessage: { text, messageId: randomUUID(), mode: 'AGENT_MODE_AGENT' } } }
}

function runRequest(action: JsonObject = userAction('Transport user request'), conversationId: string = randomUUID()): JsonObject {
  return {
    runRequest: {
      conversationId,
      action,
      requestedModel: { modelId: 'claude-sonnet-4' },
      conversationState: {},
    },
  }
}

function heartbeat(): JsonObject {
  return { clientHeartbeat: {} }
}

function staleAcknowledgement(requestId = 900_000): JsonObject {
  return { kvClientMessage: { id: requestId, setBlobResult: {} } }
}

async function settleRuntime(): Promise<void> {
  await new Promise<void>(resolve => setImmediate(resolve))
  await new Promise<void>(resolve => setImmediate(resolve))
}

async function* providerAnswer(): AsyncIterable<LLMStreamEvent> {
  yield { type: 'text_delta', text: 'A completed transport response.' }
  yield { type: 'done', usage: { inputTokens: 30, outputTokens: 10 }, stopReason: 'end_turn' }
}

function queuePendingProviderHttpRequest() {
  let resolveStarted!: (signal: AbortSignal) => void
  const started = new Promise<AbortSignal>((resolve) => {
    resolveStarted = resolve
  })
  let abortCount = 0
  fetchBoundary.mockImplementationOnce((_input, options) => {
    const signal = options?.signal
    if (!signal)
      throw new Error('Provider HTTP request did not receive an AbortSignal')
    return new Promise<Response>((_resolve, reject) => {
      const onAbort = (): void => {
        abortCount++
        signal.removeEventListener('abort', onAbort)
        reject(new DOMException('Provider HTTP request aborted', 'AbortError'))
      }
      signal.addEventListener('abort', onAbort, { once: true })
      resolveStarted(signal)
      if (signal.aborted)
        onAbort()
    })
  })
  return {
    started,
    get abortCount() { return abortCount },
  }
}

beforeEach(async () => {
  // setup.ts owns isolated HOME and SQLite per file; never query a real DB.
  expect(process.env.HOME?.startsWith(tmpdir())).toBe(true)
  expect(process.env.BYOK_AGENT_DB_PATH?.startsWith(process.env.HOME!)).toBe(true)
  await resetAgentDatabaseForTests()
  expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  fetchBoundary.mockReset()
  fetchBoundary.mockRejectedValue(new Error('Unexpected provider HTTP request'))
  providerRequests.length = 0
  vi.spyOn(AnthropicProvider.prototype, 'stream').mockImplementation((request) => {
    providerRequests.push(request)
    return providerAnswer()
  })
})

afterEach(async () => {
  for (const client of clients) {
    client.releaseOutput()
    client.context.abort(new ConnectError('Test teardown', Code.Canceled))
    client.input.close()
    if (client.transport === 'sse')
      closeSession(client.requestId)
  }
  await Promise.all(clients.map(client => client.completion))
  await Promise.all(clients.map(client => client.registeredOutput?.return?.()))
  for (const session of directSessions)
    markSessionClosed(session)
  directSessions.length = 0
  clients.length = 0
  vi.clearAllTimers()
  vi.useRealTimers()
  vi.restoreAllMocks()
  expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
})

describe.each<TransportKind>(['bidi', 'sse'])('%s registered startup', (transport) => {
  it('ignores heartbeat and unsolicited ACK before RunRequest and finishes without client EOF', async () => {
    const client = new TransportTestClient(transport)
    client.startTransport()
    client.send(heartbeat())
    client.send(staleAcknowledgement())
    client.send(runRequest())
    expect(await client.completion).toEqual({})
    expect(providerRequests).toHaveLength(1)
    expect(client.checkpoints.length).toBeGreaterThan(0)
    expect(client.input.closed).toBe(false)
    expect(client.input.deliveredEof).toBe(false)
    expect(client.context.signal.aborted).toBe(false)
    expect(client.appendFailures).toEqual([])
  })

  it('preserves the complete queued action across intervening frames, including images and ref-only parts', async () => {
    const rulesId = Buffer.from(`rules-${randomUUID()}`)
    const rulesBytes = toBinary(RequestContextRulesPartSchema, create(RequestContextRulesPartSchema, { cloudRule: 'Keep the queued rules intact.' }))
    const client = new TransportTestClient(transport, { fixtures: [{ blobId: rulesId, bytes: rulesBytes }] })
    client.startTransport()
    client.send({
      conversationAction: {
        userMessageAction: {
          userMessage: {
            text: 'Describe the queued image without losing its context.',
            messageId: 'complete-queued-message',
            mode: 'AGENT_MODE_AGENT',
            selectedContext: { selectedImages: [{ uuid: 'queued-image', mimeType: 'image/png', data: IMAGE_BYTES }] },
          },
          prependUserMessages: [{ text: 'Also preserve this prepended instruction.', messageId: 'prepend-id', mode: 'AGENT_MODE_AGENT' }],
        },
        requestContextParts: {
          rulesBlobId: wireKey(rulesId),
          dynamicContext: { env: { workspacePaths: ['/queued-context-workspace'] } },
        },
      },
    })
    client.send(heartbeat())
    client.send(staleAcknowledgement())
    client.send(heartbeat())
    client.send(runRequest({ resumeAction: { requestContext: { env: { workspacePaths: ['/incorrect-resume-workspace'] } } } }))
    expect(await client.completion).toEqual({})
    expect(providerRequests).toHaveLength(1)
    const prompt = JSON.stringify(providerRequests[0]!.messages)
    expect(prompt).toContain('Describe the queued image')
    expect(prompt).toContain('Also preserve this prepended instruction.')
    expect(prompt).toContain('/queued-context-workspace')
    expect(prompt).not.toContain('/incorrect-resume-workspace')
    expect(prompt).toContain(IMAGE_BYTES)
    expect(client.getRequests.map(request => wireKey(request.blobId))).toContain(wireKey(rulesId))
    const checkpoint = client.checkpoints.at(-1)!
    expect(client.assertCheckpointResolvable(checkpoint).turns.at(-1)!.user.messageId).toBe('complete-queued-message')
  })

  it('rejects multiple queued actions explicitly rather than truncating to the first text', async () => {
    const client = new TransportTestClient(transport)
    client.startTransport()
    client.send({ conversationAction: userAction('First distinct action') })
    client.send(heartbeat())
    client.send({ conversationAction: userAction('Second distinct action') })
    client.send(runRequest({ resumeAction: {} }))
    const outcome = await client.completion
    expect(outcome.error).toMatchObject({ code: Code.FailedPrecondition })
    expect(String(outcome.error)).toContain('multiple actions')
    expect(providerRequests).toEqual([])
    expect(client.frames).toEqual([])
  })

  it('starts an image-only queued action rather than treating absent text as absent user intent', async () => {
    const client = new TransportTestClient(transport)
    client.startTransport()
    client.send({
      conversationAction: {
        userMessageAction: {
          userMessage: {
            messageId: 'image-only-queued-message',
            mode: 'AGENT_MODE_AGENT',
            selectedContext: { selectedImages: [{ uuid: 'only-image', mimeType: 'image/png', data: IMAGE_BYTES }] },
          },
        },
      },
    })
    client.send(runRequest({ resumeAction: {} }))
    expect(await client.completion).toEqual({})
    expect(providerRequests).toHaveLength(1)
    expect(JSON.stringify(providerRequests[0]!.messages)).toContain(IMAGE_BYTES)
  })

  it('rejects an ambiguous queued action plus a separate initial user action', async () => {
    const client = new TransportTestClient(transport)
    client.startTransport()
    client.send({ conversationAction: userAction('Queued action') })
    client.send(runRequest(userAction('Different initial action')))
    expect((await client.completion).error).toMatchObject({ code: Code.FailedPrecondition })
    expect(providerRequests).toEqual([])
  })

  it('does not start prequeued work when the handler request signal is already aborted', async () => {
    const client = new TransportTestClient(transport)
    client.send(runRequest())
    client.disconnectController.abort()
    client.startTransport()
    expect((await client.completion).error).toMatchObject({ code: Code.Canceled })
    expect(providerRequests).toEqual([])
    expect(client.frames).toEqual([])
  })

  it('requires a blob request to be yielded, not just allocated, before accepting its ACK', async () => {
    const client = new TransportTestClient(transport)
    client.pauseAfterFirstSet()
    client.startTransport()
    client.send(runRequest())
    await client.waitFor(current => current.setRequests.length === 1, 'first yielded Set in the allocated batch')
    const futureRequestId = client.setRequests[0]!.requestId + 1
    client.send(staleAcknowledgement(futureRequestId))
    await settleRuntime()
    client.withheldSetReplyIds.add(futureRequestId)
    client.releaseOutput()
    await client.waitFor(current => current.setRequests.some(request => request.requestId === futureRequestId), 'previously unyielded Set')
    let completed = false
    void client.completion.then(() => {
      completed = true
    })
    await settleRuntime()
    expect(completed).toBe(false)
    expect(client.checkpoints).toEqual([])
    client.withheldSetReplyIds.delete(futureRequestId)
    client.send(staleAcknowledgement(futureRequestId))
    expect(await client.completion).toEqual({})
    expect(client.checkpoints.length).toBeGreaterThan(0)
  })

  it('keeps one absolute startup deadline despite repeated heartbeat and ACK traffic', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const client = new TransportTestClient(transport)
    client.startTransport()
    let completed = false
    void client.completion.then(() => {
      completed = true
    })
    for (let interval = 0; interval < 3; interval++) {
      client.send(heartbeat())
      client.send(staleAcknowledgement())
      if (interval === 1)
        client.send({ conversationAction: userAction('A queued action does not reset the budget') })
      await vi.advanceTimersByTimeAsync(AGENT_STARTUP_TIMEOUT_MS / 3 - 1)
      expect(completed).toBe(false)
    }
    await vi.advanceTimersByTimeAsync(3)
    expect((await client.completion).error).toMatchObject({ code: Code.DeadlineExceeded })
    expect(providerRequests).toEqual([])
    expect(client.input.deliveredEof).toBe(false)
  })

  it.each(['cancel', 'disconnect', 'deadline'] as const)('ends startup promptly on %s', async (termination) => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const client = new TransportTestClient(transport, { timeoutMs: termination === 'deadline' ? 100 : undefined })
    client.startTransport()
    client.send(heartbeat())
    client.send({ conversationAction: userAction('Never start this cancelled action') })
    if (termination === 'cancel')
      client.cancel()
    else if (termination === 'disconnect')
      client.disconnect()
    else
      await vi.advanceTimersByTimeAsync(100)
    const outcome = await client.completion
    if (termination !== 'cancel')
      expect(outcome.error).toMatchObject({ code: termination === 'deadline' ? Code.DeadlineExceeded : Code.Canceled })
    else
      expect(outcome).toEqual({})
    expect(providerRequests).toEqual([])
    expect(client.frames).toEqual([])
  })

  it.each(['cancel', 'disconnect', 'deadline'] as const)('forwards runtime %s to a real pending blob save', async (termination) => {
    if (termination === 'deadline')
      vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const client = new TransportTestClient(transport, { timeoutMs: termination === 'deadline' ? 1_000 : undefined })
    client.autoAcknowledgeSets = false
    client.startTransport()
    client.send(runRequest())
    await client.waitFor(current => current.pendingSets.size > 0, 'run-owned blob save')
    if (termination === 'cancel')
      client.cancel()
    else if (termination === 'disconnect')
      client.disconnect()
    else
      await vi.advanceTimersByTimeAsync(1_000)
    const outcome = await client.completion
    if (termination !== 'cancel')
      expect(outcome.error).toMatchObject({ code: termination === 'deadline' ? Code.DeadlineExceeded : Code.Canceled })
    expect(client.checkpoints).toEqual([])
    expect(client.input.deliveredEof).toBe(false)
  })
})

it('runs the actual BiDi entry with already-closed input without entering orchestration', async () => {
  const client = new TransportTestClient('bidi')
  client.input.close()
  client.startTransport()
  expect(await client.completion).toEqual({})
  expect(client.input.deliveredEof).toBe(true)
  expect(providerRequests).toEqual([])
})

it('closes a pending runtime blob operation when actual BiDi input reaches EOF', async () => {
  const client = new TransportTestClient('bidi')
  client.autoAcknowledgeSets = false
  client.startTransport()
  client.send(runRequest())
  await client.waitFor(current => current.pendingSets.size > 0, 'blob save')
  client.input.close()
  await client.completion
  expect(client.input.deliveredEof).toBe(true)
  expect(client.checkpoints).toEqual([])
})

it('orders zero-based concurrent BidiAppend requests and accepts binary data and exact retries', async () => {
  const client = new TransportTestClient('sse')
  client.startTransport()
  const action = { conversationAction: userAction('Sequence zero user action') }
  const request = runRequest({ resumeAction: {} })
  await client.appendAt(request, 3n, true)
  await client.appendAt(request, 3n, true)
  await client.appendAt(staleAcknowledgement(), 2n)
  await client.appendAt(heartbeat(), 1n)
  await settleRuntime()
  expect(providerRequests).toEqual([])
  await client.appendAt(action, 0n, true)
  await client.appendAt(action, 0n, true)
  expect(await client.completion).toEqual({})
  expect(providerRequests).toHaveLength(1)
  expect(JSON.stringify(providerRequests[0]!.messages)).toContain('Sequence zero user action')
})

it('does not let an ACK arriving before allocation become valid after sequence reordering', async () => {
  const client = new TransportTestClient('sse')
  client.autoAcknowledgeSets = false
  client.startTransport()
  await client.appendAt(staleAcknowledgement(900_000), 2n)
  await client.appendAt(runRequest(), 0n)
  await client.waitFor(current => current.pendingSets.size > 0, 'first real blob Set')
  expect(client.setRequests[0]!.requestId).toBe(900_000)
  let completed = false
  void client.completion.then(() => {
    completed = true
  })
  await client.appendAt(heartbeat(), 1n)
  await settleRuntime()
  expect(completed).toBe(false)
  expect(client.checkpoints).toEqual([])
  client.autoAcknowledgeSets = true
  client.acknowledgePendingSets()
  expect(await client.completion).toEqual({})
  expect(client.checkpoints.length).toBeGreaterThan(0)
})

it('associates SSE input only with its nested transport request ID', async () => {
  const firstClient = new TransportTestClient('sse')
  const secondClient = new TransportTestClient('sse')
  firstClient.startTransport()
  secondClient.startTransport()
  await firstClient.appendAt(runRequest(userAction('First transport only')), 0n)
  expect(await firstClient.completion).toEqual({})
  expect(secondClient.frames).toEqual([])
  await secondClient.appendAt(runRequest(userAction('Second transport only')), 0n)
  expect(await secondClient.completion).toEqual({})
  expect(providerRequests).toHaveLength(2)
  expect(JSON.stringify(providerRequests[0]!.messages)).not.toContain('Second transport only')
  expect(JSON.stringify(providerRequests[1]!.messages)).not.toContain('First transport only')
})

it('rejects a second SSE consumer without closing the first consumer session', async () => {
  const firstClient = new TransportTestClient('sse')
  firstClient.startTransport()
  const duplicateClient = new TransportTestClient('sse', { requestId: firstClient.requestId })
  duplicateClient.startTransport()
  expect((await duplicateClient.completion).error).toMatchObject({ code: Code.AlreadyExists })
  firstClient.send(runRequest())
  expect(await firstClient.completion).toEqual({})
  expect(providerRequests).toHaveLength(1)
  await expect(firstClient.appendAt(heartbeat(), 100n)).rejects.toMatchObject({ code: Code.FailedPrecondition })
})

describe.each<TransportKind>(['bidi', 'sse'])('%s registered early consumer teardown', (transport) => {
  it.each(['return', 'throw'] as const)('interrupts pending startup next synchronously on consumer %s without advancing timers', async (termination) => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const startedAt = Date.now()
    const client = new TransportTestClient(transport)
    const initialAbortListeners = getEventListeners(client.context.signal, 'abort')
    const output = client.openRegisteredOutput()
    const pendingNext = Promise.resolve(output.next()).then(result => ({ result }), error => ({ error }))
    client.send({ conversationAction: userAction('This queued action must be released on early consumer exit') })
    await settleRuntime()
    expect(getTransportResourceUsage().queuedBytes).toBeGreaterThan(0)
    const failure = new Error('Consumer abandoned startup')
    const terminalResult = Promise.resolve(termination === 'return' ? output.return!() : output.throw!(failure))
      .then(result => ({ result }), error => ({ error }))

    // These assertions run before awaiting teardown and before any timer tick.
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
    expect(client.context.signal.aborted).toBe(false)
    expect(await pendingNext).toMatchObject({ result: { done: true } })
    if (termination === 'return')
      expect(await terminalResult).toMatchObject({ result: { done: true } })
    else
      expect(await terminalResult).toEqual({ error: failure })
    expect(Date.now()).toBe(startedAt)
    expect(getEventListeners(client.context.signal, 'abort')).toEqual(initialAbortListeners)
    expect(client.input.deliveredEof).toBe(false)
    expect(providerRequests).toEqual([])
  })

  it.each(['return', 'throw'] as const)('aborts only its pending real provider HTTP request on consumer %s before timers', async (termination) => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const startedAt = Date.now()
    // Keep SDK, provider lifecycle, translator, usage recorder and run real.
    // Unlike ordinary fixture tests, this test replaces only the HTTP boundary.
    vi.mocked(AnthropicProvider.prototype.stream).mockRestore()
    const ownedHttpRequest = queuePendingProviderHttpRequest()
    const unrelatedHttpRequest = queuePendingProviderHttpRequest()
    const client = new TransportTestClient(transport)
    const initialAbortListeners = getEventListeners(client.context.signal, 'abort')
    client.startTransport()
    client.send(runRequest())
    const ownedSignal = await Promise.race([
      ownedHttpRequest.started,
      client.completion.then((outcome) => {
        throw new Error('Run ended before provider HTTP started', { cause: outcome.error })
      }),
    ])
    const unrelatedClient = new TransportTestClient(transport)
    unrelatedClient.startTransport()
    unrelatedClient.send(runRequest())
    const unrelatedSignal = await Promise.race([
      unrelatedHttpRequest.started,
      unrelatedClient.completion.then((outcome) => {
        throw new Error('Peer run ended before provider HTTP started', { cause: outcome.error })
      }),
    ])
    expect(ownedSignal.aborted).toBe(false)
    expect(unrelatedSignal.aborted).toBe(false)
    const output = client.registeredOutput!
    const failure = new Error('Consumer abandoned provider request')
    const terminalResult = Promise.resolve(termination === 'return' ? output.return!() : output.throw!(failure))
      .then(result => ({ result }), error => ({ error }))

    expect(ownedSignal.aborted).toBe(true)
    expect(ownedHttpRequest.abortCount).toBe(1)
    expect(unrelatedSignal.aborted).toBe(false)
    expect(unrelatedHttpRequest.abortCount).toBe(0)
    expect(client.context.signal.aborted).toBe(false)
    expect(unrelatedClient.context.signal.aborted).toBe(false)
    expect((await client.completion).error).toBeUndefined()
    if (termination === 'return')
      expect(await terminalResult).toMatchObject({ result: { done: true } })
    else
      expect(await terminalResult).toEqual({ error: failure })
    expect(Date.now()).toBe(startedAt)
    expect(client.input.deliveredEof).toBe(false)
    expect(getEventListeners(client.context.signal, 'abort')).toEqual(initialAbortListeners)
    expect(fetchBoundary).toHaveBeenCalledTimes(2)

    unrelatedClient.disconnect()
    await unrelatedClient.completion
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })
})

it.each(['return', 'throw'] as const)('does not close the original SSE session when a duplicate consumer invokes %s', async (termination) => {
  const originalClient = new TransportTestClient('sse')
  originalClient.startTransport()
  const originalSession = getOrCreateSession(originalClient.requestId)
  const duplicateClient = new TransportTestClient('sse', { requestId: originalClient.requestId })
  const output = duplicateClient.openRegisteredOutput()
  const failedNext = Promise.resolve(output.next()).then(result => ({ result }), error => ({ error }))
  const failure = new Error('Discard the duplicate consumer')
  const terminalResult = Promise.resolve(termination === 'return' ? output.return!() : output.throw!(failure))
    .then(result => ({ result }), error => ({ error }))
  expect(originalSession.closed).toBe(false)
  expect(await failedNext).toMatchObject({ error: { code: Code.AlreadyExists } })
  if (termination === 'return')
    expect(await terminalResult).toMatchObject({ result: { done: true } })
  else
    expect(await terminalResult).toEqual({ error: failure })
  expect(originalSession.closed).toBe(false)
  expect(originalClient.context.signal.aborted).toBe(false)
  originalClient.send(runRequest())
  expect((await originalClient.completion).error).toBeUndefined()
  expect(providerRequests).toHaveLength(1)
})

describe('process transport resource guards (local policies)', () => {
  it.each<TransportKind>(['bidi', 'sse'])('keeps %s queued startup actions charged until RunRequest or cancellation', async (transport) => {
    const client = new TransportTestClient(transport)
    const action = { conversationAction: userAction('Retain this complete queued startup action') }
    const byteLength = toBinary(AgentClientMessageSchema, fromJson(AgentClientMessageSchema, action)).byteLength
    client.startTransport()
    client.send(action)
    await settleRuntime()
    expect(getTransportResourceUsage().queuedBytes).toBe(byteLength)
    expect(providerRequests).toEqual([])
    client.send(heartbeat())
    await settleRuntime()
    expect(getTransportResourceUsage().queuedBytes).toBe(byteLength)
    client.cancel()
    expect(await client.completion).toEqual({})
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })

  it('caps unclaimed and attached SSE IDs together, then admits after cancellation without counting tombstones', async () => {
    const admittedClients: TransportTestClient[] = []
    for (let clientIndex = 0; clientIndex < MAX_LIVE_SSE_SESSIONS; clientIndex++) {
      const client = new TransportTestClient('sse')
      admittedClients.push(client)
      if (clientIndex % 2 === 0)
        client.startTransport()
      else
        await client.appendAt(heartbeat(), 0n)
    }
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: MAX_LIVE_SSE_SESSIONS, queuedBytes: 0 })

    const refusedClient = new TransportTestClient('sse')
    await expect(refusedClient.appendAt(heartbeat(), 0n)).rejects.toMatchObject({ code: Code.ResourceExhausted })
    refusedClient.startTransport()
    expect((await refusedClient.completion).error).toMatchObject({ code: Code.ResourceExhausted })
    expect(getTransportResourceUsage().liveSseSessions).toBe(MAX_LIVE_SSE_SESSIONS)

    admittedClients[0]!.cancel()
    expect(await admittedClients[0]!.completion).toEqual({})
    expect(getTransportResourceUsage().liveSseSessions).toBe(MAX_LIVE_SSE_SESSIONS - 1)
    const replacementClient = new TransportTestClient('sse')
    replacementClient.startTransport()
    replacementClient.send(runRequest())
    expect(await replacementClient.completion).toEqual({})
    expect(providerRequests).toHaveLength(1)
    expect(getTransportResourceUsage().liveSseSessions).toBe(MAX_LIVE_SSE_SESSIONS - 1)
  })

  it('charges reordered payload once across retries and FIFO release, then frees it on real startup consumption', async () => {
    const client = new TransportTestClient('sse')
    const action = { conversationAction: userAction('Preserved action with a missing earlier sequence') }
    const byteLength = toBinary(AgentClientMessageSchema, fromJson(AgentClientMessageSchema, action)).byteLength
    await client.appendAt(action, 1n, true)
    await client.appendAt(action, 1n, true)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 1, queuedBytes: byteLength })

    const session = getOrCreateSession(client.requestId)
    expect(session.messages).toEqual([])
    await client.appendAt(heartbeat(), 0n)
    expect(session.messages).toHaveLength(1)
    expect(session.appendSequence?.pending.size).toBe(0)
    expect(getTransportResourceUsage().queuedBytes).toBe(byteLength)
    client.startTransport()
    client.send(runRequest({ resumeAction: {} }))
    expect(await client.completion).toEqual({})
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })

  it('shares one encoded-byte budget across ordinary, reordered, and actual BiDi input', async () => {
    const ordinaryClient = new TransportTestClient('sse')
    const reorderedClient = new TransportTestClient('sse')
    const payload = { conversationAction: userAction('Q'.repeat(1024 * 1024)) }
    const payloadBytes = toBinary(AgentClientMessageSchema, fromJson(AgentClientMessageSchema, payload)).byteLength
    const fittingMessages = Math.floor(MAX_TRANSPORT_QUEUED_BYTES / payloadBytes)
    let ordinarySequence = 0n
    let reorderedSequence = 1n
    for (let messageIndex = 0; messageIndex < fittingMessages; messageIndex++) {
      if (messageIndex % 2 === 0)
        await ordinaryClient.appendAt(payload, ordinarySequence++, true)
      else
        await reorderedClient.appendAt(payload, reorderedSequence++, true)
    }
    const retainedBytes = fittingMessages * payloadBytes
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 2, queuedBytes: retainedBytes })

    const refusedClient = new TransportTestClient('sse')
    await refusedClient.appendAt({ conversationAction: userAction('Discard this on resource rejection') }, 0n)
    await expect(refusedClient.appendAt(payload, 1n, true)).rejects.toMatchObject({ code: Code.ResourceExhausted })
    expect(getOrCreateSession(refusedClient.requestId).messages).toEqual([])
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 2, queuedBytes: retainedBytes })
    refusedClient.startTransport()
    expect((await refusedClient.completion).error).toMatchObject({ code: Code.ResourceExhausted })

    const bidiClient = new TransportTestClient('bidi')
    bidiClient.startTransport()
    bidiClient.send(payload)
    expect((await bidiClient.completion).error).toMatchObject({ code: Code.ResourceExhausted })
    expect(bidiClient.input.deliveredEof).toBe(false)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 2, queuedBytes: retainedBytes })
    expect(providerRequests).toEqual([])
  })

  it('rejects an ordinary queue flood before RunSSE and releases both its byte charges and admission', async () => {
    const client = new TransportTestClient('sse')
    const frame = { execClientControlMessage: { streamClose: { id: 7 } } }
    for (let messageIndex = 0; messageIndex < MAX_SESSION_QUEUED_MESSAGES; messageIndex++)
      await client.appendAt(frame, BigInt(messageIndex), true)
    const session = getOrCreateSession(client.requestId)
    expect(session.messages).toHaveLength(MAX_SESSION_QUEUED_MESSAGES)
    expect(getTransportResourceUsage().queuedBytes).toBeGreaterThan(0)
    await expect(client.appendAt(frame, BigInt(MAX_SESSION_QUEUED_MESSAGES), true)).rejects.toMatchObject({ code: Code.ResourceExhausted })
    expect(session.messages).toEqual([])
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
    client.startTransport()
    expect((await client.completion).error).toMatchObject({ code: Code.ResourceExhausted })
  })

  it('bounds zero-payload sequence placeholders without accepting an unbounded heartbeat reorder buffer', async () => {
    const client = new TransportTestClient('sse')
    for (let sequence = 1; sequence <= MAX_PENDING_APPENDS; sequence++)
      await client.appendAt(heartbeat(), BigInt(sequence), true)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 1, queuedBytes: 0 })
    await expect(client.appendAt(heartbeat(), BigInt(MAX_PENDING_APPENDS + 1), true)).rejects.toMatchObject({ code: Code.ResourceExhausted })
    expect(getOrCreateSession(client.requestId).appendSequence?.pending.size).toBe(0)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })

    const gapFillingClient = new TransportTestClient('sse')
    for (let sequence = 1; sequence <= MAX_PENDING_APPENDS; sequence++)
      await gapFillingClient.appendAt(heartbeat(), BigInt(sequence), true)
    await gapFillingClient.appendAt(heartbeat(), 0n, true)
    expect(getOrCreateSession(gapFillingClient.requestId).appendSequence?.pending.size).toBe(0)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 1, queuedBytes: 0 })
  })

  it('releases retained payloads and admission when an append retry contradicts its pending sequence', async () => {
    const client = new TransportTestClient('sse')
    await client.appendAt({ conversationAction: userAction('queued action') }, 0n)
    await client.appendAt({ conversationAction: userAction('pending action') }, 2n)
    expect(getTransportResourceUsage().queuedBytes).toBeGreaterThan(0)
    await expect(client.appendAt({ conversationAction: userAction('different action for the same seqno') }, 2n)).rejects.toMatchObject({ code: Code.InvalidArgument })
    expect(getOrCreateSession(client.requestId).messages).toEqual([])
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })

  it.each(['cancel', 'expire'] as const)('releases unclaimed ordinary and reordered payloads immediately on %s', async (termination) => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const client = new TransportTestClient('sse')
    await client.appendAt({ conversationAction: userAction('ordinary queue data') }, 0n, true)
    await client.appendAt({ conversationAction: userAction('reordered queue data') }, 2n, true)
    const session = getOrCreateSession(client.requestId)
    expect(session.messages).toHaveLength(1)
    expect(session.appendSequence?.pending.size).toBe(1)
    expect(getTransportResourceUsage().queuedBytes).toBeGreaterThan(0)
    if (termination === 'cancel')
      await client.appendAt({ conversationAction: { cancelAction: { reason: 'cancel before RunSSE attaches' } } }, 1n)
    else
      await vi.advanceTimersByTimeAsync(SESSION_RETENTION_MS)
    expect(session.messages).toEqual([])
    expect(session.appendSequence?.pending.size).toBe(0)
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })

  it('releases only charged messages on matching/discard and clears orphaned charges after direct queue mutation', async () => {
    const session = createEphemeralSession('direct-queue-compatibility')
    directSessions.push(session)
    const chargedMessage = { execClientMessage: { id: 23, readResult: { success: { content: 'charged contents' } } } }
    const unchargedMessage = { execClientMessage: { id: 24 } }
    const byteLength = Buffer.byteLength(JSON.stringify(chargedMessage), 'utf8')
    pushSessionMessage(session, chargedMessage)
    pushSessionMessage(session, chargedMessage)
    session.messages.push(unchargedMessage)
    expect(getTransportResourceUsage().queuedBytes).toBe(2 * byteLength)
    expect(await waitForMessageMatching(session, message => message === chargedMessage)).toBe(chargedMessage)
    expect(getTransportResourceUsage().queuedBytes).toBe(byteLength)
    discardSessionMessages(session, message => message === chargedMessage || message === unchargedMessage)
    expect(getTransportResourceUsage().queuedBytes).toBe(0)
    expect(session.messages).toEqual([])

    pushSessionMessage(session, chargedMessage)
    session.messages = [unchargedMessage]
    markSessionClosed(session)
    markSessionClosed(session)
    expect(session.messages).toEqual([])
    expect(getTransportResourceUsage()).toEqual({ liveSseSessions: 0, queuedBytes: 0 })
  })
})
