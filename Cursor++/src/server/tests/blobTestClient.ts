import type { JsonObject } from '@bufbuild/protobuf'
import type { ConnectRouter, ServiceImpl } from '@connectrpc/connect'
import type { AgentClientMessage, AgentServerMessage, ConversationStateStructure } from '../gen/agent_v1_pb'
import type { AgentSession } from '../handlers/agent/session'
import { randomUUID } from 'node:crypto'
import { create, fromBinary, fromJson, toBinary, toJson } from '@bufbuild/protobuf'
import { createHandlerContext } from '@connectrpc/connect'
import { expect } from 'vitest'
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  AgentService,
  ConversationStepSchema,
  ConversationSummaryArchiveSchema,
  ConversationTurnStructureSchema,
  NotifyConversationCloneRequestSchema,
  UploadConversationBlobsRequestSchema,
  UserMessageSchema,
} from '../gen/agent_v1_pb'
import { createEphemeralSession, pushSessionMessage } from '../handlers/agent/session'
import registerAgentService from '../services/core/AgentService'

type AgentTestHandlers = Pick<ServiceImpl<typeof AgentService>, 'run' | 'uploadConversationBlobs' | 'notifyConversationClone'>

/** Capture the production RPC implementations, without replacing their bodies. */
export function captureAgentServiceHandlers(): AgentTestHandlers {
  let captured: Partial<ServiceImpl<typeof AgentService>> | undefined
  const router: ConnectRouter = {
    handlers: [],
    service(service, implementation) {
      expect(service).toBe(AgentService)
      captured = implementation as Partial<ServiceImpl<typeof AgentService>>
      return this
    },
    rpc() {
      throw new Error('Expected AgentService registration, not an individual RPC')
    },
  }
  registerAgentService(router)
  if (!captured?.run || !captured.uploadConversationBlobs || !captured.notifyConversationClone)
    throw new Error('AgentService did not register run, upload and clone handlers')
  return {
    run: captured.run,
    uploadConversationBlobs: captured.uploadConversationBlobs,
    notifyConversationClone: captured.notifyConversationClone,
  }
}

function createAgentRpcContext(methodName: keyof AgentTestHandlers) {
  const method = AgentService.method[methodName]
  return createHandlerContext({
    service: AgentService,
    method,
    protocolName: 'connect',
    requestMethod: 'POST',
    url: `https://blob-test.invalid/${AgentService.typeName}/${method.name}`,
  })
}

/** Client EOF is controlled only by the test, never by producing the last reply. */
export class OpenClientMessageStream implements AsyncIterableIterator<AgentClientMessage> {
  closed = false
  deliveredEof = false
  private readonly queuedMessages: AgentClientMessage[] = []
  private readonly pendingReads: Array<(result: IteratorResult<AgentClientMessage>) => void> = []

  next(): Promise<IteratorResult<AgentClientMessage>> {
    const message = this.queuedMessages.shift()
    if (message)
      return Promise.resolve({ done: false, value: message })
    if (this.closed) {
      this.deliveredEof = true
      return Promise.resolve({ done: true, value: undefined })
    }
    return new Promise(resolve => this.pendingReads.push(resolve))
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<AgentClientMessage> {
    return this
  }

  push(message: AgentClientMessage): void {
    if (this.closed)
      throw new Error('Cannot send a client message after input EOF')
    const pendingRead = this.pendingReads.shift()
    if (pendingRead)
      pendingRead({ done: false, value: message })
    else
      this.queuedMessages.push(message)
  }

  close(): void {
    this.closed = true
    this.queuedMessages.length = 0
    for (const pendingRead of this.pendingReads.splice(0)) {
      this.deliveredEof = true
      pendingRead({ done: true, value: undefined })
    }
  }
}

export interface ClientBlobFixture {
  blobId: Uint8Array
  bytes: Uint8Array
}

export interface ObservedBlobRequest {
  requestId: number
  blobId: Uint8Array
}

export interface ObservedBlobSet extends ObservedBlobRequest {
  bytes: Uint8Array
}

export interface RuntimeOutcome {
  error?: unknown
}

export function wireKey(blobId: Uint8Array): string {
  return Buffer.from(blobId).toString('base64')
}

function requireRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Expected a protobuf JSON object')
  return value as Record<string, unknown>
}

function decodeWireBytes(value: unknown): Uint8Array {
  if (typeof value !== 'string')
    throw new Error('Expected canonical protobuf JSON bytes')
  return Buffer.from(value, 'base64')
}

/** A client-owned KV, not a server cache. New bytes can enter only through Set. */
export class BlobTestClient {
  readonly session: AgentSession = createEphemeralSession(randomUUID())
  readonly frames: AgentServerMessage[] = []
  readonly getRequests: ObservedBlobRequest[] = []
  readonly setRequests: ObservedBlobSet[] = []
  readonly pendingSets = new Map<number, ObservedBlobSet>()
  readonly checkpoints: ConversationStateStructure[] = []
  readonly execKinds: string[] = []
  autoAcknowledgeSets = true
  readResultText = 'Fixture file contents returned by the client.'
  completion: Promise<RuntimeOutcome> = Promise.resolve({})

  private readonly savedBlobs = new Map<string, Uint8Array>()
  private readonly requestIds = new Set<number>()
  private readonly progressListeners = new Set<() => void>()
  private finished: RuntimeOutcome | undefined
  private sendToService: ((message: AgentClientMessage) => void) | undefined

  constructor(oldFixtures: ClientBlobFixture[] = []) {
    for (const fixture of oldFixtures)
      this.savedBlobs.set(wireKey(fixture.blobId), new Uint8Array(fixture.bytes))
  }

  async uploadChunk(handler: AgentTestHandlers['uploadConversationBlobs'], options: {
    conversationId: string
    blobIds: Uint8Array[]
    chunkIndex: number
    totalChunks: number
  }): Promise<void> {
    const request = create(UploadConversationBlobsRequestSchema, {
      conversationId: options.conversationId,
      blobs: options.blobIds.map(blobId => ({ id: blobId, value: this.read(blobId) })),
      chunkIndex: options.chunkIndex,
      totalChunks: options.totalChunks,
    })
    const decodedRequest = fromBinary(UploadConversationBlobsRequestSchema, toBinary(UploadConversationBlobsRequestSchema, request))
    const response = await handler(decodedRequest, createAgentRpcContext('uploadConversationBlobs'))
    expect(response).toEqual({})
  }

  async notifyClone(handler: AgentTestHandlers['notifyConversationClone'], conversationId: string, sourceConversationId: string): Promise<void> {
    const request = create(NotifyConversationCloneRequestSchema, {
      conversationId,
      sourceConversationId,
      sourceRequestId: 'original-request-id',
    })
    expect(await handler(request, createAgentRpcContext('notifyConversationClone'))).toEqual({})
  }

  /** Model losing the old local copies after successful upload RPCs, before Run. */
  dropLocalBlobs(blobIds: Uint8Array[]): void {
    for (const blobId of blobIds)
      this.savedBlobs.delete(wireKey(blobId))
  }

  start(stream: AsyncIterable<AgentServerMessage>): void {
    this.completion = this.consume(stream)
  }

  startServiceRun(handler: AgentTestHandlers['run'], request: Record<string, unknown>): OpenClientMessageStream {
    const input = new OpenClientMessageStream()
    this.sendToService = message => input.push(message)
    this.send(request as JsonObject)
    const output = handler(input, createAgentRpcContext('run'))
    this.start((async function* () {
      for await (const frame of output)
        yield create(AgentServerMessageSchema, frame)
    })())
    return input
  }

  async waitFor(predicate: (client: BlobTestClient) => boolean, description: string): Promise<void> {
    if (predicate(this))
      return
    return new Promise((resolve, reject) => {
      const checkProgress = (): void => {
        if (predicate(this)) {
          this.progressListeners.delete(checkProgress)
          resolve()
        }
        else if (this.finished) {
          this.progressListeners.delete(checkProgress)
          reject(new Error(`Run finished before ${description}`, { cause: this.finished.error }))
        }
      }
      this.progressListeners.add(checkProgress)
      checkProgress()
    })
  }

  /** Fork only confirmed client storage, after an earlier server context is gone. */
  fork(): BlobTestClient {
    return new BlobTestClient([...this.savedBlobs].map(([blobId, bytes]) => ({
      blobId: Buffer.from(blobId, 'base64'),
      bytes,
    })))
  }

  read(blobId: Uint8Array): Uint8Array {
    const bytes = this.savedBlobs.get(wireKey(blobId))
    if (!bytes)
      throw new Error(`Checkpoint references unsaved client blob ${wireKey(blobId)}`)
    return bytes
  }

  readMessage(blobId: Uint8Array): Record<string, unknown> {
    const text = Buffer.from(this.read(blobId)).toString('utf8')
    const json = text.trimStart().startsWith('{') ? text : Buffer.from(text, 'base64').toString('utf8')
    return requireRecord(JSON.parse(json))
  }

  acknowledgeSet(requestId: number): void {
    const request = this.pendingSets.get(requestId)
    if (!request)
      throw new Error(`Cannot acknowledge unseen Set request ${requestId}`)
    this.savedBlobs.set(wireKey(request.blobId), new Uint8Array(request.bytes))
    this.pendingSets.delete(requestId)
    this.send({ kvClientMessage: { id: requestId, setBlobResult: {} } })
  }

  acknowledgePendingSets(): void {
    for (const requestId of this.pendingSets.keys())
      this.acknowledgeSet(requestId)
  }

  failSet(requestId: number): void {
    if (!this.pendingSets.delete(requestId))
      throw new Error(`Cannot fail unseen Set request ${requestId}`)
    this.send({ kvClientMessage: { id: requestId, setBlobResult: { error: { message: 'Client disk quota exceeded' } } } })
  }

  send(message: JsonObject): void {
    const parsed = fromJson(AgentClientMessageSchema, message)
    if (this.sendToService)
      this.sendToService(parsed)
    else
      pushSessionMessage(this.session, toJson(AgentClientMessageSchema, parsed) as Record<string, unknown>)
  }

  cancel(): void {
    if (this.finished)
      return
    this.send({ conversationAction: { cancelAction: { reason: 'user_stopped_generation' } } })
  }

  /** Validate roots plus protobuf turn/archive edges using only confirmed bytes. */
  assertCheckpointResolvable(checkpoint: ConversationStateStructure) {
    const rootMessages = checkpoint.rootPromptMessagesJson.map(blobId => this.readMessage(blobId))
    const turns = checkpoint.turns.map((blobId) => {
      const decoded = fromBinary(ConversationTurnStructureSchema, this.read(blobId))
      expect(decoded.turn.case).toBe('agentConversationTurn')
      if (decoded.turn.case !== 'agentConversationTurn')
        throw new Error('Expected an agent turn in the checkpoint')
      const turn = decoded.turn.value
      const user = fromBinary(UserMessageSchema, this.read(turn.userMessage))
      const steps = turn.steps.map((stepBlobId) => {
        const step = fromBinary(ConversationStepSchema, this.read(stepBlobId))
        expect(step.message.case).toBeDefined()
        return step
      })
      return { turn, user, steps }
    })
    const archives = checkpoint.summaryArchives.map((blobId) => {
      const archive = fromBinary(ConversationSummaryArchiveSchema, this.read(blobId))
      const summaryMessage = this.readMessage(archive.summaryMessage)
      expect(JSON.stringify(summaryMessage)).toContain(archive.summary)
      expect(archive.summarizedMessages.length).toBeGreaterThan(0)
      for (const dependency of archive.summarizedMessages)
        this.readMessage(dependency)
      return archive
    })
    return { rootMessages, turns, archives }
  }

  private async consume(stream: AsyncIterable<AgentServerMessage>): Promise<RuntimeOutcome> {
    let outcome: RuntimeOutcome = {}
    try {
      for await (const frame of stream) {
        this.frames.push(frame)
        this.receive(frame)
        this.notifyProgress()
      }
    }
    catch (error) {
      outcome = { error }
    }
    this.finished = outcome
    this.notifyProgress()
    return outcome
  }

  private receive(frame: AgentServerMessage): void {
    // Observe real schema JSON: the oneof is a named field, not {case, value}.
    const json = toJson(AgentServerMessageSchema, frame) as Record<string, unknown>
    if (json.kvServerMessage) {
      const envelope = requireRecord(json.kvServerMessage)
      const requestId = envelope.id
      // These are new allocator IDs. A missing proto-default id must not be guessed.
      expect(typeof requestId).toBe('number')
      expect(Number.isInteger(requestId)).toBe(true)
      expect(requestId).toBeGreaterThanOrEqual(900_000)
      const numericRequestId = requestId as number
      expect(this.requestIds.has(numericRequestId)).toBe(false)
      this.requestIds.add(numericRequestId)
      if (envelope.getBlobArgs) {
        const args = requireRecord(envelope.getBlobArgs)
        const blobId = decodeWireBytes(args.blobId)
        this.getRequests.push({ requestId: numericRequestId, blobId })
        const bytes = this.savedBlobs.get(wireKey(blobId))
        this.send({
          kvClientMessage: {
            id: numericRequestId,
            getBlobResult: bytes === undefined ? {} : { blobData: wireKey(bytes) },
          },
        })
      }
      else if (envelope.setBlobArgs) {
        const args = requireRecord(envelope.setBlobArgs)
        const request = {
          requestId: numericRequestId,
          blobId: decodeWireBytes(args.blobId),
          bytes: decodeWireBytes(args.blobData),
        }
        this.setRequests.push(request)
        this.pendingSets.set(numericRequestId, request)
        if (this.autoAcknowledgeSets)
          this.acknowledgeSet(numericRequestId)
      }
      else {
        throw new Error('Unexpected KV request kind')
      }
    }
    else if (json.execServerMessage) {
      const envelope = requireRecord(json.execServerMessage)
      const requestId = envelope.id as number
      if (envelope.executeHookArgs) {
        this.execKinds.push('executeHookArgs')
        this.send({ execClientMessage: { id: requestId, executeHookResult: { response: { preCompact: {} } } } })
      }
      else if (envelope.readArgs) {
        this.execKinds.push('readArgs')
        this.send({ execClientMessage: { id: requestId, readResult: { success: { content: this.readResultText } } } })
        this.send({ execClientControlMessage: { streamClose: { id: requestId } } })
      }
      else {
        throw new Error(`Unexpected external tool request: ${Object.keys(envelope).join(', ')}`)
      }
    }
    else if (frame.message.case === 'conversationCheckpointUpdate') {
      // A checkpoint is invalid even transiently if any dependency lacks an ACK.
      this.assertCheckpointResolvable(frame.message.value)
      this.checkpoints.push(frame.message.value)
    }
  }

  private notifyProgress(): void {
    for (const listener of this.progressListeners)
      listener()
  }
}
