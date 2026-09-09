import type { AgentServerMessage } from '../../gen/agent_v1_pb';
import { collectExtraContextBlobIds, parseRunRequest, resolveExtraContextBlobs } from './protocol';
import type { AgentSession } from './session';
import { handleSummarizeAction } from './summarizeRuntime';
import { handleConversationRun } from './conversationRuntime';
import { getPersistedConversationCheckpoint } from '../../database/checkpoints';
import { binaryBlobDataFromClientBytes, blobIdToBytes } from './blob';
import { BlobRunContext } from './runContext';
import { BlobIntegrityError } from './blobErrors';
import { uploadHandoff } from './uploadHandoff';
import { fetchBlobsFromClient } from './clientBlobFetch';
import { logger } from '../../logger';
import { isAgentRunAbortedError } from './wait';
import { applyRequestContextPart, type RequestContextPartName } from './requestContextParts';
import { makeByokConnectError } from '../errors';
import { ErrorDetails_Error } from '../../gen/aiserver_v1_shared_pb';
import { claimUploadedConversationBlobs } from './uploadRunHandoff';

export async function* handleRunRequest(
    msg: Record<string, unknown>,
    session: AgentSession | null = null,
): AsyncIterable<AgentServerMessage> {
    const parsed = parseRunRequest(msg);
    const run = new BlobRunContext(session, {
        inheritedBlobIds: [...parsed.historyBlobIds, ...parsed.historyTurnBlobIds, ...parsed.historySummaryArchiveIds],
    });

    try {
        claimUploadedConversationBlobs(parsed, run);

        const persistedCheckpoint = await getPersistedConversationCheckpoint(parsed.conversationId);
        if (persistedCheckpoint) {
            // 客户端是 source of truth。sqlite checkpoint 仅用于 auto-summarize 持久化,
            // 不用于覆盖客户端的 conversationState。
            const clientSentHistory = parsed.historyBlobIds.length > 0;

            if (!clientSentHistory) {
                // 客户端是 source of truth — 发空就用空, 不从 sqlite 恢复。
                // 空 CS 场景: revert / 新会话。跨模型切换时客户端始终携带 history (日志实证)。
                // sqlite checkpoint 保留不删, 仅用于 auto-summarize 和灾难恢复备份。
                logger.info({
                    conversationId: parsed.conversationId,
                    persistedBlobIds: persistedCheckpoint.rootBlobIds.length,
                }, '[AGENT] empty conversationState with existing checkpoint — trusting client, skipping restore');
            } else {
                // 客户端主动回传了历史 blob → 以客户端为 source of truth。
                // 只在客户端未携带 tokenDetails 时补充一下 sqlite 里缓存的值, 避免上下文用量显示跳变。
                // 注意: summaryArchiveIds 也不做 fallback, 因为客户端已经决定了本轮要带哪些 summary。
                if (!parsed.historyTokenDetails) {
                    parsed.historyTokenDetails = persistedCheckpoint.tokenDetails;
                    logger.debug({
                        conversationId: parsed.conversationId,
                        tokenDetails: parsed.historyTokenDetails,
                    }, '[AGENT] merged tokenDetails from sqlite (client did not provide)');
                }
            }
        }

        // Explicitly attached context is required, unlike optional capability
        // catalogs below. Never replace a missing attachment with a prompt marker.
        if (collectExtraContextBlobIds(parsed).length > 0) {
            for (const blobId of new Set(collectExtraContextBlobIds(parsed))) {
                const bytes = uploadHandoff.read(parsed.conversationId, blobIdToBytes(blobId));
                if (bytes)
                    run.blobs.cacheBlob(blobId, binaryBlobDataFromClientBytes(bytes), bytes);
            }
            resolveExtraContextBlobs(parsed, run.blobs);
            const unresolvedBlobIds = [...new Set(collectExtraContextBlobIds(parsed))];
            if (unresolvedBlobIds.length > 0) {
                const results = yield* fetchBlobsFromClient({
                    run,
                    blobIds: unresolvedBlobIds.map(blobIdToBytes),
                });
                results.forEach((result, index) => {
                    const blobId = unresolvedBlobIds[index]!;
                    if (result.status === 'ok') {
                        run.blobs.cacheBlob(blobId, binaryBlobDataFromClientBytes(result.bytes), result.bytes);
                        run.blobs.markClientSaved(blobId);
                    }
                });
                const failures = results.flatMap((result, index) => result.status === 'ok'
                    ? [] : [{ blobId: unresolvedBlobIds[index]!, status: result.status, message: result.message }]);
                if (failures.length)
                    throw new BlobIntegrityError(failures);
                resolveExtraContextBlobs(parsed, run.blobs);
            }
            const missingContext = collectExtraContextBlobIds(parsed);
            if (missingContext.length)
                throw new BlobIntegrityError(missingContext.map(blobId => ({ blobId, status: 'decode-error' })));
        }

        // Cursor 3.13+ ref_only: rules / skills / subagents / mcps 四类稳定上下文位于客户端
        // transient blob, 一批取回后分别解码合入。dual 模式在 parseRunRequest 中不暴露这些
        // 引用, 因此不会重复 fetch。
        const partReferences: Array<{ partName: RequestContextPartName, blobId: Uint8Array }> = [
            { partName: 'rules', blobId: parsed.rulesBlobId },
            { partName: 'skills', blobId: parsed.skillsBlobId },
            { partName: 'subagents', blobId: parsed.subagentsBlobId },
            { partName: 'mcps', blobId: parsed.mcpsBlobId },
        ].filter((reference): reference is { partName: RequestContextPartName, blobId: Uint8Array } => reference.blobId !== undefined);
        if (partReferences.length > 0) {
            const partBytes = yield* fetchBlobsFromClient({
                run,
                blobIds: partReferences.map(reference => reference.blobId),
            });
            partReferences.forEach((reference, index) => {
                const result = partBytes[index]!;
                if (result.status !== 'ok')
                    logger.warn({ partName: reference.partName, status: result.status }, '[AGENT] optional request context catalog unavailable');
                applyRequestContextPart(parsed, reference.partName, result.status === 'ok' ? result.bytes : null);
            });
        }

        if (parsed.isSummarize) {
            yield* handleSummarizeAction(parsed, session, run);
            return;
        }

        const hasUserContent = parsed.userText || parsed.selectedImages.length > 0
        if (!hasUserContent && !parsed.isResume && !parsed.isExecutePlan && !parsed.isBackgroundTaskCompletion) {
            logger.warn({ keys: Object.keys(msg) }, '[AGENT] runRequest without userText/images, resume, executePlan, summarizeAction, or backgroundTaskCompletionAction');
            return;
        }

        yield* handleConversationRun(parsed, session, run);
    } catch (error) {
        const isBlobCancellation = error instanceof BlobIntegrityError
            && error.failures.length > 0
            && error.failures.every(failure => failure.status === 'cancelled');
        const isAbortSignalError = run.signal.aborted && error instanceof Error && error.name === 'AbortError';
        // Do not suppress a real persistence/restoration error merely because
        // cancellation also happened; that failure needs a diagnostic response.
        if (isAgentRunAbortedError(error) || isBlobCancellation || isAbortSignalError) {
            logger.info({
                conversationId: parsed.conversationId,
                execMessageId: isAgentRunAbortedError(error) ? error.execMessageId : undefined,
                error: error instanceof Error ? error.message : String(error),
            }, '[AGENT] run aborted by client exec control message');
            return;
        }
        if (error instanceof Error && error.name.startsWith('Blob')) {
            const retryable = 'retryable' in error && error.retryable === true;
            throw makeByokConnectError({
                errorCode: ErrorDetails_Error.CUSTOM,
                title: 'Conversation data unavailable; checkpoint preserved',
                detail: error.message,
                isRetryable: retryable,
                additionalInfo: { conversationId: parsed.conversationId, errorClass: error.name },
                cause: error,
            });
        }
        throw error;
    } finally {
        run.dispose();
    }
}
