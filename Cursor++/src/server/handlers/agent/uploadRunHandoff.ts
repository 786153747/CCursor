import { fromBinary } from '@bufbuild/protobuf'
import { ConversationSummaryArchiveSchema, ConversationTurnStructureSchema } from '../../gen/agent_v1_pb'
import { binaryBlobDataFromClientBytes, blobIdFromBytes, blobIdToBytes, jsonBlobDataFromClientBytes } from './blob'
import { BlobIntegrityError } from './blobErrors'
import type { ParsedRunRequest } from './protocol'
import type { BlobRunContext } from './runContext'
import { uploadHandoff } from './uploadHandoff'

type UploadReference = { blobId: string, kind: 'history' | 'turn' | 'archive' | 'opaque' }

/** Claim available uploads for the whole retained checkpoint, not only the active turn. */
export function claimUploadedConversationBlobs(parsed: ParsedRunRequest, run: BlobRunContext): void {
  const references: UploadReference[] = [
    ...parsed.historyBlobIds.map(blobId => ({ blobId, kind: 'history' as const })),
    ...parsed.historyTurnBlobIds.map(blobId => ({ blobId, kind: 'turn' as const })),
    ...parsed.historySummaryArchiveIds.map(blobId => ({ blobId, kind: 'archive' as const })),
  ]
  const visited = new Set<string>()
  for (const reference of references) {
    if (visited.has(reference.blobId))
      continue
    visited.add(reference.blobId)
    const bytes = uploadHandoff.read(parsed.conversationId, blobIdToBytes(reference.blobId))
    if (bytes === undefined)
      continue

    let blobData: string
    const dependencies: string[] = []
    try {
      if (reference.kind === 'history') {
        const normalized = jsonBlobDataFromClientBytes(bytes)
        if (normalized === null)
          throw new Error('Invalid uploaded history encoding')
        blobData = normalized
      }
      else {
        blobData = (reference.kind === 'opaque' ? jsonBlobDataFromClientBytes(bytes) : null)
          ?? binaryBlobDataFromClientBytes(bytes)
      }
      if (reference.kind === 'turn') {
        const turn = fromBinary(ConversationTurnStructureSchema, bytes)
        if (turn.turn.case === 'agentConversationTurn') {
          const value = turn.turn.value
          const userMessageBlobId = blobIdFromBytes(value.userMessage)
          const stepBlobIds = value.steps.map(blobIdFromBytes)
          dependencies.push(userMessageBlobId, ...stepBlobIds)
          run.blobs.turnBaselines.set(reference.blobId, {
            userMessageBlobId, stepBlobIds, requestId: value.requestId, dynamicToolCount: value.dynamicToolCount,
          })
        }
        else if (turn.turn.case === 'shellConversationTurn') {
          dependencies.push(blobIdFromBytes(turn.turn.value.shellCommand), blobIdFromBytes(turn.turn.value.shellOutput))
        }
        else {
          throw new Error('Uploaded turn has no supported turn structure')
        }
      }
      if (reference.kind === 'archive') {
        const archive = fromBinary(ConversationSummaryArchiveSchema, bytes)
        dependencies.push(...archive.summarizedMessages.map(blobIdFromBytes))
        if (archive.summaryMessage.byteLength > 0)
          dependencies.push(blobIdFromBytes(archive.summaryMessage))
      }
      if (dependencies.some(blobId => blobId.length === 0))
        throw new Error('Uploaded structure contains an empty blob reference')
    }
    catch (error) {
      throw new BlobIntegrityError([{ blobId: reference.blobId, status: 'decode-error', message: (error as Error).message }])
    }

    // These references came from the input checkpoint graph. Preserve unuploaded
    // inherited references without claiming that they were newly Set-confirmed.
    run.blobs.addInheritedReferences(dependencies)
    run.blobs.cacheBlob(reference.blobId, blobData, bytes, dependencies)
    for (const blobId of dependencies)
      references.push({ blobId, kind: 'opaque' })
  }
}
