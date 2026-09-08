import type { AgentServerMessage } from '../../gen/agent_v1_pb';
import { collectExtraContextBlobIds, parseRunRequest, resolveExtraContextBlobs } from './protocol';
import type { AgentSession } from './session';
import { handleSummarizeAction } from './summarizeRuntime';
import { handleConversationRun } from './conversationRuntime';
import { getPersistedConversationCheckpoint } from '../../database/checkpoints';
import { binaryBlobDataFromClientBytes, blobIdToBytes } from './blob';
import { cacheBlob } from './blobStore';
import { fetchBlobsFromClient } from './clientBlobFetch';
import { logger } from '../../logger';
import { isAgentRunAbortedError } from './wait';
import { applyRequestContextPart, type RequestContextPartName } from './requestContextParts';

/**
 * 客户端是所有 blob 的唯一持久持有方; 服务端只有进程内热缓存, 未命中一律经
 * kvServerMessage.getBlobArgs 向客户端取。全 run 的 getBlobArgs 请求 id 共用这一个
 * 计数器: 高位起始值避开 setBlobArgs 的 blobCounter (从 0 递增) 取值区间, 防止回包 id 撞号。
 */
export interface BlobRequestIdAllocator {
    allocateBlobId: () => number
}

export async function* handleRunRequest(
    msg: Record<string, unknown>,
    session: AgentSession | null = null,
): AsyncIterable<AgentServerMessage> {
    const parsed = parseRunRequest(msg);
    let nextBlobRequestId = 900_000;
    const allocateBlobId = (): number => nextBlobRequestId++;

    try {
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

        // extraContextEntries 的 blob 通常由客户端在本 run 之前经 UploadConversationBlobs
        // 上传、已在内存; 未命中的再向客户端取一次 (客户端本地没有 → 保留 blobId,
        // 下游 preamble 用 <extra_context_pending> 占位透出)。
        if (collectExtraContextBlobIds(parsed).length > 0) {
            resolveExtraContextBlobs(parsed);
            const unresolvedBlobIds = collectExtraContextBlobIds(parsed);
            if (unresolvedBlobIds.length > 0) {
                const fetchedBytes = yield* fetchBlobsFromClient({
                    session,
                    blobIds: unresolvedBlobIds.map(blobIdToBytes),
                    allocateBlobId,
                });
                fetchedBytes.forEach((bytes, index) => {
                    if (bytes)
                        cacheBlob(unresolvedBlobIds[index]!, binaryBlobDataFromClientBytes(bytes));
                });
                resolveExtraContextBlobs(parsed);
            }
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
                session,
                blobIds: partReferences.map(reference => reference.blobId),
                allocateBlobId,
            });
            partReferences.forEach((reference, index) => {
                applyRequestContextPart(parsed, reference.partName, partBytes[index] ?? null);
            });
        }

        if (parsed.isSummarize) {
            yield* handleSummarizeAction(parsed, session, { allocateBlobId });
            return;
        }

        const hasUserContent = parsed.userText || parsed.selectedImages.length > 0
        if (!hasUserContent && !parsed.isResume && !parsed.isExecutePlan && !parsed.isBackgroundTaskCompletion) {
            logger.warn({ keys: Object.keys(msg) }, '[AGENT] runRequest without userText/images, resume, executePlan, summarizeAction, or backgroundTaskCompletionAction');
            return;
        }

        yield* handleConversationRun(parsed, session, { allocateBlobId });
    } catch (error) {
        if (isAgentRunAbortedError(error)) {
            logger.info({
                conversationId: parsed.conversationId,
                execMessageId: error.execMessageId,
                error: error.message,
            }, '[AGENT] run aborted by client exec control message');
            return;
        }
        throw error;
    }
}
