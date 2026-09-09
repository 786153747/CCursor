import type { ProviderEntry, ProviderType } from '../data/defaults'
import type { AgentServerMessage } from '../gen/agent_v1_pb'
import type { LLMProvider, LLMStreamRequest } from '../handlers/llm/types'
import { getEventListeners } from 'node:events'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { setProvidersForTests } from '../config/providersStore'
import { translateStream } from '../handlers/agent/stream'
import { AnthropicProvider } from '../handlers/llm/anthropic'
import { GeminiProvider } from '../handlers/llm/gemini'
import { OpenAIChatProvider } from '../handlers/llm/openai-chat'
import { OpenAIResponsesProvider } from '../handlers/llm/openai-responses'
import { resetProviderInstanceCache, resolveProviderRuntime } from '../handlers/llm/providerRuntime'
import { createProviderRequestLifecycle } from '../handlers/llm/requestLifecycle'
import { withUsageRecording } from '../handlers/llm/usageRecorder'

const { fetchBoundary, recordUsage } = vi.hoisted(() => ({
  fetchBoundary: vi.fn<typeof fetch>(),
  recordUsage: vi.fn().mockResolvedValue(undefined),
}))

// Keep the production providers, SDKs, codecs, usage wrapper and translator real.
// Both custom undici fetch and Gemini's global fetch stop at this HTTP boundary.
vi.mock('undici', async (importOriginal) => {
  const original = await importOriginal<typeof import('undici')>()
  return { ...original, fetch: fetchBoundary }
})
vi.mock('../database/usageStats', () => ({ recordModelUsage: recordUsage }))

function createGate<Value>() {
  let resolve!: (value: Value) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

function queueHttpStream(pendingHeaders = false) {
  const started = createGate<AbortSignal>()
  const readRequested = createGate<void>()
  const headersReady = createGate<Response>()
  let bodyController!: ReadableStreamDefaultController<Uint8Array>
  let requestSignal: AbortSignal | undefined
  let removeAbortListener = () => {}
  const body = new ReadableStream<Uint8Array>({
    start(controller) { bodyController = controller },
    pull() { readRequested.resolve() },
    cancel() { removeAbortListener() },
  }, { highWaterMark: 0 })
  const response = new Response(body, { headers: { 'content-type': 'text/event-stream' } })
  if (!pendingHeaders)
    headersReady.resolve(response)

  fetchBoundary.mockImplementationOnce(async (_input, options) => {
    const signal = options?.signal
    if (!signal)
      throw new Error('SDK request is missing its AbortSignal')
    requestSignal = signal
    const handleAbort = () => {
      const error = new DOMException('HTTP request aborted', 'AbortError')
      bodyController.error(error)
      headersReady.reject(error)
      removeAbortListener()
    }
    removeAbortListener = () => signal.removeEventListener('abort', handleAbort)
    signal.addEventListener('abort', handleAbort, { once: true })
    started.resolve(signal)
    if (signal.aborted)
      handleAbort()
    return headersReady.promise
  })

  return {
    started: started.promise,
    readRequested: readRequested.promise,
    body,
    get signal() { return requestSignal! },
    send(events: unknown[]) {
      const payload = events.map((event) => {
        const eventType = typeof event === 'object' && event !== null && 'type' in event ? `event: ${event.type}\n` : ''
        return `${eventType}data: ${JSON.stringify(event)}\n\n`
      }).join('')
      bodyController.enqueue(new TextEncoder().encode(payload))
    },
    finish() {
      removeAbortListener()
      bodyController.close()
    },
  }
}

interface ProviderCase {
  name: string
  type: ProviderType
  anthropicBetas?: string[]
}

const providerCases: ProviderCase[] = [
  { name: 'OpenAI Chat', type: 'openai-chat' },
  { name: 'OpenAI Responses', type: 'openai-responses' },
  { name: 'Anthropic', type: 'anthropic' },
  { name: 'Anthropic beta', type: 'anthropic', anthropicBetas: ['context-1m-2025-08-07'] },
  { name: 'Gemini', type: 'gemini' },
]

function resolveTestRuntime(providerCase: ProviderCase) {
  return resolveProviderRuntime(`cancellation-${providerCase.type}`)
}

function createProvider(providerCase: ProviderCase): LLMProvider {
  const entry: ProviderEntry = {
    id: 'cancellation-test',
    name: 'Cancellation test',
    type: providerCase.type,
    baseUrl: 'https://provider.invalid',
    auth: { kind: 'apiKey', value: 'synthetic-test-key' },
    models: [],
  }
  switch (providerCase.type) {
    case 'openai-chat': return new OpenAIChatProvider(entry)
    case 'openai-responses': return new OpenAIResponsesProvider(entry)
    case 'anthropic': return new AnthropicProvider(entry)
    case 'gemini': return new GeminiProvider(entry)
  }
}

function createRequest(providerCase: ProviderCase, signal?: AbortSignal): LLMStreamRequest {
  return {
    model: 'test-model',
    messages: [{ role: 'user', content: 'hello' }],
    anthropicBetas: providerCase.anthropicBetas,
    signal,
  }
}

function recordProviderUsage(provider: LLMProvider): LLMProvider {
  return withUsageRecording(provider, {
    providerId: 'cancellation-test',
    providerName: 'Cancellation test',
    modelId: 'test-model',
    apiModel: 'test-model',
  })
}

function createTranslatedStream(providerCase: ProviderCase, signal: AbortSignal) {
  const provider = recordProviderUsage(createProvider(providerCase))
  return translateStream(
    requestSignal => provider.stream(createRequest(providerCase, requestSignal)),
    'test-step',
    undefined,
    1_000,
    undefined,
    signal,
  )
}

function successEvents(providerCase: ProviderCase): unknown[] {
  switch (providerCase.type) {
    case 'openai-chat': return [
      { choices: [{ delta: { content: 'hello' }, finish_reason: null }] },
      { choices: [], usage: { prompt_tokens: 10, completion_tokens: 2 } },
    ]
    case 'openai-responses': return [
      { type: 'response.output_text.delta', delta: 'hello' },
      { type: 'response.completed', response: { status: 'completed', usage: { input_tokens: 10, output_tokens: 2 } } },
    ]
    case 'anthropic': return [
      { type: 'message_start', message: { id: 'test-message', type: 'message', role: 'assistant', model: 'test-model', content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 10, output_tokens: 0 } } },
      { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
      { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hello' } },
      { type: 'content_block_stop', index: 0 },
      { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 2 } },
      { type: 'message_stop' },
    ]
    case 'gemini': return [
      { candidates: [{ content: { role: 'model', parts: [{ text: 'hello' }] } }], usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 2 } },
    ]
  }
}

function frameType(frame: AgentServerMessage): string | undefined {
  return frame.message.case === 'interactionUpdate' ? frame.message.value.message.case : frame.message.case
}

beforeEach(() => {
  vi.useFakeTimers()
  fetchBoundary.mockReset()
  fetchBoundary.mockRejectedValue(new Error('Unexpected external request'))
  recordUsage.mockClear()
  vi.stubGlobal('fetch', fetchBoundary)
  resetProviderInstanceCache()
  setProvidersForTests({
    $schemaVersion: 1,
    providers: [...new Set(providerCases.map(providerCase => providerCase.type))].map(type => ({
      id: `cancellation-${type}`,
      name: `Cancellation ${type}`,
      type,
      baseUrl: 'https://provider.invalid',
      auth: { kind: 'apiKey', value: 'synthetic-test-key' },
      models: [{
        id: `cancellation-${type}`,
        apiModel: 'test-model',
        displayName: 'Cancellation test model',
        thinking: false,
        contextTokenLimit: 200_000,
      }],
    })),
  })
})

afterEach(() => {
  resetProviderInstanceCache()
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

describe.each(providerCases)('$name request cancellation', (providerCase) => {
  it('rejects an already aborted run without calling the SDK HTTP boundary', async () => {
    const run = new AbortController()
    const reason = new Error('run already stopped')
    run.abort(reason)

    await expect(createProvider(providerCase).stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]().next()).rejects.toBe(reason)

    expect(fetchBoundary).not.toHaveBeenCalled()
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not dispatch a live request after cancellation during SDK setup', async () => {
    let dispatchedLiveRequest = false
    fetchBoundary.mockImplementation(async (_input, options) => {
      dispatchedLiveRequest ||= !options?.signal?.aborted
      return new Response('', { headers: { 'content-type': 'text/event-stream' } })
    })
    const run = new AbortController()
    const reason = new Error('cancel during SDK setup')
    const iterator = createProvider(providerCase).stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
    const rejected = expect(iterator.next()).rejects.toBe(reason)

    run.abort(reason)

    await rejected
    expect(dispatchedLiveRequest).toBe(false)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('aborts a pending HTTP request before response headers arrive', async () => {
    const http = queueHttpStream(true)
    const run = new AbortController()
    const reason = new Error('cancel while connecting')
    const iterator = createProvider(providerCase).stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
    const next = iterator.next()
    const rejected = expect(next).rejects.toBe(reason)
    await http.started

    run.abort(reason)

    await rejected
    expect(http.signal.aborted).toBe(true)
    expect(fetchBoundary).toHaveBeenCalledTimes(1)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('aborts pending next without manufacturing completion or recording usage', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const reason = new Error('run stopped during body read')
    const frames = createTranslatedStream(providerCase, run.signal)
    const rejected = expect(frames.next()).rejects.toBe(reason)
    await http.readRequested

    run.abort(reason)

    await rejected
    expect(http.signal.aborted).toBe(true)
    expect(http.body.locked).toBe(false)
    expect(recordUsage).not.toHaveBeenCalled()
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('closes the provider when returning early from a heartbeat with next pending', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const frames = createTranslatedStream(providerCase, run.signal)
    const firstFrame = frames.next()
    await http.readRequested
    await vi.advanceTimersByTimeAsync(1_000)
    expect(frameType((await firstFrame).value!)).toBe('heartbeat')

    await frames.return()

    expect(http.signal.aborted).toBe(true)
    expect(http.body.locked).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(recordUsage).not.toHaveBeenCalled()
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('interrupts a pending translator next before queueing return', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const frames = createTranslatedStream(providerCase, run.signal)
    const rejected = expect(frames.next()).rejects.toMatchObject({ name: 'AbortError' })
    await http.readRequested

    const returned = frames.return()
    expect(http.signal.aborted).toBe(true)
    await rejected
    await returned

    expect(http.body.locked).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(recordUsage).not.toHaveBeenCalled()
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('also interrupts direct provider return while next is pending', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const iterator = createProvider(providerCase).stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
    const rejected = expect(iterator.next()).rejects.toMatchObject({ name: 'AbortError' })
    await http.readRequested

    const returned = iterator.return?.()
    expect(http.signal.aborted).toBe(true)
    await rejected
    await returned

    expect(http.body.locked).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each([
    { phase: 'headers', pendingHeaders: true },
    { phase: 'body', pendingHeaders: false },
  ])('production usage wrapper aborts pending $phase on direct return', async ({ pendingHeaders }) => {
    const http = queueHttpStream(pendingHeaders)
    const run = new AbortController()
    const route = resolveTestRuntime(providerCase)
    const iterator = route.provider.stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
    const rejected = expect(iterator.next()).rejects.toMatchObject({ name: 'AbortError' })
    await (pendingHeaders ? http.started : http.readRequested)

    const returned = iterator.return?.()
    expect(http.signal.aborted).toBe(true)
    await rejected
    await returned

    expect(http.body.locked).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(recordUsage).not.toHaveBeenCalled()
    expect(fetchBoundary).toHaveBeenCalledTimes(1)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('production usage wrapper stays lazy when returned before first next', async () => {
    const run = new AbortController()
    const route = resolveTestRuntime(providerCase)
    const iterator = route.provider.stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()

    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    await expect(iterator.return?.()).resolves.toMatchObject({ done: true })
    await expect(iterator.next()).resolves.toMatchObject({ done: true })

    expect(fetchBoundary).not.toHaveBeenCalled()
    expect(recordUsage).not.toHaveBeenCalled()
    expect(run.signal.aborted).toBe(false)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('production usage wrapper records once at done with unchanged buckets through EOF', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const route = resolveTestRuntime(providerCase)
    const iterator = route.provider.stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
    const firstEvent = iterator.next()
    await http.readRequested
    http.send(successEvents(providerCase))
    http.finish()

    expect((await firstEvent).value).toMatchObject({ type: 'text_delta', text: 'hello' })
    expect(recordUsage).not.toHaveBeenCalled()
    const doneEvent = await iterator.next()
    expect(doneEvent.value).toMatchObject({ type: 'done' })
    expect(recordUsage).toHaveBeenCalledExactlyOnceWith({
      providerId: `cancellation-${providerCase.type}`,
      providerName: `Cancellation ${providerCase.type}`,
      modelId: `cancellation-${providerCase.type}`,
      apiModel: 'test-model',
      usage: doneEvent.value.usage,
    })
    expect((await iterator.next()).done).toBe(true)
    await iterator.return?.()

    expect(recordUsage).toHaveBeenCalledTimes(1)
    expect(http.body.locked).toBe(false)
    expect(http.signal.aborted).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('preserves normal completion and removes timers and run listeners', async () => {
    const http = queueHttpStream()
    const run = new AbortController()
    const frames = createTranslatedStream(providerCase, run.signal)
    const collect = (async () => {
      const types: Array<string | undefined> = []
      for await (const frame of frames)
        types.push(frameType(frame))
      return types
    })()
    await http.readRequested
    http.send(successEvents(providerCase))
    http.finish()

    expect(await collect).toEqual(['textDelta', 'stepCompleted', 'turnEnded'])
    expect(recordUsage).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
      usage: expect.objectContaining({ inputTokens: 10, outputTokens: 2 }),
    }))
    expect(http.body.locked).toBe(false)
    expect(http.signal.aborted).toBe(false)
    expect(run.signal.aborted).toBe(false)
    expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)

    run.abort(new Error('later run shutdown'))
    expect(http.signal.aborted).toBe(false)
  })
})

it('aborts a timed-out attempt without cancelling sibling requests on the same cached provider', async () => {
  const providerCase = providerCases[0]!
  const provider = resolveTestRuntime(providerCase).provider
  const run = new AbortController()
  const timedOutAttempt = createProviderRequestLifecycle(run.signal)
  const timeoutError = new Error('summary attempt idle timeout')
  const firstHttp = queueHttpStream()
  const first = provider.stream(createRequest(providerCase, timedOutAttempt.signal))[Symbol.asyncIterator]()
  const rejected = expect(first.next()).rejects.toBe(timeoutError)
  await firstHttp.readRequested

  const siblingHttp = queueHttpStream()
  const sibling = provider.stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
  const siblingNext = sibling.next()
  await siblingHttp.readRequested

  timedOutAttempt.abort(timeoutError)
  await first.return?.()
  timedOutAttempt.dispose()
  await rejected

  expect(firstHttp.signal.aborted).toBe(true)
  expect(siblingHttp.signal.aborted).toBe(false)
  expect(run.signal.aborted).toBe(false)
  siblingHttp.send(successEvents(providerCase))
  siblingHttp.finish()
  expect((await siblingNext).value).toMatchObject({ type: 'text_delta', text: 'hello' })
  expect((await sibling.next()).value).toMatchObject({ type: 'done' })
  expect((await sibling.next()).done).toBe(true)
  expect(recordUsage).toHaveBeenCalledTimes(1)
  expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
  expect(vi.getTimerCount()).toBe(0)
})

it('production usage wrapper forwards direct throw during a pending read without a parent signal', async () => {
  const providerCase = providerCases[0]!
  const http = queueHttpStream()
  const route = resolveTestRuntime(providerCase)
  const iterator = route.provider.stream(createRequest(providerCase))[Symbol.asyncIterator]()
  const failure = new Error('consumer stopped with error')
  const rejected = expect(iterator.next()).rejects.toBe(failure)
  await http.readRequested

  const thrown = iterator.throw?.(failure)
  expect(http.signal.aborted).toBe(true)
  await rejected
  await expect(thrown).rejects.toBe(failure)

  expect(http.body.locked).toBe(false)
  expect(recordUsage).not.toHaveBeenCalled()
  expect(vi.getTimerCount()).toBe(0)
})

it('preserves callback failures while aborting and closing the real HTTP stream', async () => {
  const providerCase = providerCases[0]!
  const provider = createProvider(providerCase)
  const http = queueHttpStream()
  const failure = new Error('frame callback failed')
  const frames = translateStream(signal => provider.stream(createRequest(providerCase, signal)), 'test', () => {
    throw failure
  })
  const rejected = expect(frames.next()).rejects.toBe(failure)
  await http.readRequested
  http.send(successEvents(providerCase).slice(0, 1))

  await rejected
  expect(http.signal.aborted).toBe(true)
  expect(http.body.locked).toBe(false)
  expect(vi.getTimerCount()).toBe(0)
})

it.each([providerCases[0]!, providerCases[2]!])('$name checks cancellation before an SDK retry', async (providerCase) => {
  const firstRequest = createGate<void>()
  fetchBoundary.mockImplementationOnce(async () => {
    firstRequest.resolve()
    return new Response(JSON.stringify({ error: { message: 'temporarily unavailable' } }), {
      status: 503,
      headers: { 'content-type': 'application/json', 'retry-after': '1' },
    })
  })
  const run = new AbortController()
  const reason = new Error('cancel during SDK backoff')
  const iterator = createProvider(providerCase).stream(createRequest(providerCase, run.signal))[Symbol.asyncIterator]()
  const rejected = expect(iterator.next()).rejects.toBe(reason)
  await firstRequest.promise
  await vi.advanceTimersByTimeAsync(0)
  expect(vi.getTimerCount()).toBeGreaterThan(0)

  run.abort(reason)
  await vi.runAllTimersAsync()
  await rejected

  expect(fetchBoundary).toHaveBeenCalledTimes(1)
  expect(getEventListeners(run.signal, 'abort')).toHaveLength(0)
  expect(vi.getTimerCount()).toBe(0)
})
