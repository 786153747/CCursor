import { randomUUID } from 'crypto';
import type { AgentServerMessage } from '../../gen/agent_v1_pb';
import { workspaceUris, type ParsedRunRequest } from './protocol';
import type { AgentSession } from './session';
import { heartbeat, checkpoint, summary, summaryCompleted, summaryStarted } from './stream';
import { clampTokenDetails, computeContextUsagePercent } from './usage';
import { resolveProviderRuntime } from '../llm';
import type { BlobRunContext } from './runContext';
import { loadHistoryEntries, repairHistoryEntries } from './historyManager';
import { buildSummarySource, createCompactionArtifacts, measureMessagesTokens, planCompaction, retainCompactionArtifacts, streamSummaryWithFallback } from './compactionStrategy';
import { releaseCompactionLock, tryAcquireCompactionLock } from './compactionLock';
import { HEARTBEAT_TICK, pumpWithTimedHeartbeats, waitForRunCompactionLockRelease } from './conversationRuntime';
import { executePreCompactHook } from './hookRuntime';
import { persistConversationCheckpoint } from '../../database/checkpoints';
import { logger } from '../../logger';
import { saveCheckpointBlobs } from './clientBlobFetch';
import { throwIfBlobRunInactive } from './checkpointManager';

export async function* handleSummarizeAction(
    parsed: ParsedRunRequest,
    session: AgentSession | null,
    run: BlobRunContext,
): AsyncIterable<AgentServerMessage> {
    throwIfBlobRunInactive(run);
    const route = resolveProviderRuntime(parsed.modelId);
    // Release wakes every waiter; only a successful acquire grants lock ownership.
    while (!tryAcquireCompactionLock(parsed.conversationId))
        yield* waitForRunCompactionLockRelease(parsed.conversationId, run);
    try {
        throwIfBlobRunInactive(run);
        yield* handleSummarizeActionLocked(parsed, session, route, run);
    }
    finally {
        releaseCompactionLock(parsed.conversationId);
    }
}

async function* handleSummarizeActionLocked(
    parsed: ParsedRunRequest,
    session: AgentSession | null,
    route: ReturnType<typeof resolveProviderRuntime>,
    run: BlobRunContext,
): AsyncIterable<AgentServerMessage> {
    // 历史 blob 与对话路径同源: 内存未命中的经 getBlobArgs 向客户端取
    const hydratedHistoryEntries = yield* loadHistoryEntries({
        historyBlobIds: parsed.historyBlobIds,
        run,
    });
    const historyEntries = repairHistoryEntries(hydratedHistoryEntries, run.blobs);
    const contextTokenLimit = parsed.historyTokenDetails?.maxTokens ?? parsed.contextTokenLimit ?? route.contextTokenLimit;
    const compactionPlan = planCompaction(historyEntries, { contextTokenLimit });
    const currentTokenDetails = clampTokenDetails(
        parsed.historyTokenDetails?.usedTokens ?? measureMessagesTokens(historyEntries.map(entry => entry.message)),
        contextTokenLimit,
    );
    const contextUsagePercent = computeContextUsagePercent(currentTokenDetails.usedTokens, currentTokenDetails.maxTokens);
    const generationId = randomUUID();

    logger.info({
        conversationId: parsed.conversationId,
        model: route.model,
        historyBlobIds: parsed.historyBlobIds.length,
        hydratedEntries: hydratedHistoryEntries.length,
        summarizeEntries: compactionPlan.summarizeEntries.length,
        keepTail: compactionPlan.keepTail.length,
        contextUsagePercent: contextUsagePercent.toFixed(1),
        usedTokens: currentTokenDetails.usedTokens,
        maxTokens: currentTokenDetails.maxTokens,
    }, '[SUMMARIZE] action started');

    yield heartbeat();

    const hookMessage = yield* executePreCompactHook({
        session,
        conversationId: parsed.conversationId,
        generationId,
        modelId: parsed.modelId,
        contextUsagePercent,
        contextTokens: currentTokenDetails.usedTokens,
        contextWindowSize: currentTokenDetails.maxTokens,
        messageCount: historyEntries.length,
        messagesToCompact: compactionPlan.summarizeEntries.length,
        isFirstCompaction: parsed.historySummaryArchiveIds.length === 0,
        execMessageId: 1,
    });

    throwIfBlobRunInactive(run);
    yield summaryStarted();
    throwIfBlobRunInactive(run);

    if (compactionPlan.summarizeEntries.length === 0) {
        // F2: mode==='disabled' 时 plan 同样返回空 summarizeEntries, 但语义是
        // "压缩结构性不可行" (leading 过大/窗口过小), 不是"已经够紧凑" — 文案须区分
        if (compactionPlan.mode === 'disabled') {
            logger.warn({
                conversationId: parsed.conversationId,
                contextTokenLimit,
                leadingTokens: compactionPlan.diagnostics.leadingTokens,
            }, '[AUTOCOMPACT] summarizeAction skipped — compaction structurally infeasible for this window');
        }
        logger.info({
            conversationId: parsed.conversationId,
            origin: 'client_summarize',
            kind: 'committed',
            usedTokens: currentTokenDetails.usedTokens,
            maxTokens: currentTokenDetails.maxTokens,
            rootBlobCount: parsed.historyBlobIds.length,
            summaryArchiveCount: parsed.historySummaryArchiveIds.length,
        }, '[AUTOCOMPACT] checkpoint write');
        yield* saveCheckpointBlobs(run, [
            ...parsed.historyBlobIds,
            ...parsed.historyTurnBlobIds,
            ...parsed.historySummaryArchiveIds,
        ]);
        throwIfBlobRunInactive(run);
        await persistConversationCheckpoint({
            kind: 'committed',
            conversationId: parsed.conversationId,
            rootBlobIds: parsed.historyBlobIds,
            turnBlobIds: parsed.historyTurnBlobIds,
            summaryArchiveIds: parsed.historySummaryArchiveIds,
            tokenDetails: currentTokenDetails,
            mode: parsed.mode,
            updatedAt: Date.now(),
        }, run.signal, run.requireCheckpointWriteScope());

        throwIfBlobRunInactive(run);
        yield summaryCompleted(hookMessage ?? (compactionPlan.mode === 'disabled'
            ? 'Compaction unavailable: system prompt plus reserves exceed this model\'s usable context window. Consider a larger-context model.'
            : 'Conversation already compact enough.'));
        throwIfBlobRunInactive(run);
        yield checkpoint(
            parsed.historyBlobIds,
            currentTokenDetails.usedTokens,
            currentTokenDetails.maxTokens,
            parsed.mode,
            undefined,
            {
                turnBlobIds: parsed.historyTurnBlobIds,
                summaryArchiveIds: parsed.historySummaryArchiveIds,
                workspaceUris: workspaceUris(parsed),
                readPaths: parsed.readPaths,
                modelName: route.model,
                gitRepos: parsed.gitRepos?.map(r => ({ path: r.path, branchName: r.branchName })),
            },
        );
        return;
    }

    // 摘要源构造 (阶段 4): 总预算 min(0.6×窗口×4, 3.2e6) chars, 超限走 max-min 水位分配
    const summarySourceText = buildSummarySource(compactionPlan.summarizeEntries, { contextTokenLimit });

    let summaryText = '';
    const llmStartTime = Date.now();
    logger.info({
        conversationId: parsed.conversationId,
        model: route.model,
        sourceTextLen: summarySourceText.length,
        summarizeEntries: compactionPlan.summarizeEntries.length,
        keepTail: compactionPlan.keepTail.length,
    }, '[SUMMARIZE] LLM summary starting');

    // 三级兜底 (流式, 与 inline 路径同一实现 — 两路行为一致)。
    // 心跳定时驱动 (与 inline 路径同修): 思考模型零事件期若心跳饿死,
    // 客户端 ~93s stall 判死会弃 run 作废在飞行摘要。
    for await (const summaryEvent of pumpWithTimedHeartbeats(signal => streamSummaryWithFallback({
        provider: route.provider,
        signal,
        model: route.model,
        sourceText: summarySourceText,
        contextTokenLimit,
    }), undefined, run.signal)) {
        throwIfBlobRunInactive(run);
        if (summaryEvent === HEARTBEAT_TICK) {
            yield heartbeat();
            continue;
        }
        if (summaryEvent.type === 'delta') {
            summaryText += summaryEvent.text;
            yield summary(summaryEvent.text);
        }
        if (summaryEvent.type === 'done')
            summaryText = summaryEvent.text;
    }
    throwIfBlobRunInactive(run);

    logger.info({
        conversationId: parsed.conversationId,
        summaryLen: summaryText.length,
        durationMs: Date.now() - llmStartTime,
    }, '[SUMMARIZE] LLM summary done');

    const artifacts = createCompactionArtifacts({
        plan: compactionPlan,
        summaryText,
        previousSummaryArchiveIds: parsed.historySummaryArchiveIds,
    });

    retainCompactionArtifacts(run.blobs, artifacts);
    yield* saveCheckpointBlobs(run, [
        ...artifacts.nextRootBlobIds,
        ...parsed.historyTurnBlobIds,
        ...artifacts.nextSummaryArchiveIds,
    ]);

    const compactedUsedTokens = clampTokenDetails(
        // o200k 实测重置 (与 inline 路径同口径, 两路行为一致由单一实现保证)
        measureMessagesTokens([
            ...compactionPlan.leading.map(entry => entry.message),
            { role: 'assistant', content: `Previous conversation summary:\n${artifacts.summaryText}` },
            ...compactionPlan.keepTail.map(entry => entry.message),
        ]),
        currentTokenDetails.maxTokens,
    );

    logger.info({
        conversationId: parsed.conversationId,
        origin: 'client_summarize',
        kind: 'committed',
        usedTokens: compactedUsedTokens.usedTokens,
        maxTokens: compactedUsedTokens.maxTokens,
        rootBlobCount: artifacts.nextRootBlobIds.length,
        summaryArchiveCount: artifacts.nextSummaryArchiveIds.length,
    }, '[AUTOCOMPACT] checkpoint write');
    throwIfBlobRunInactive(run);
    await persistConversationCheckpoint({
        kind: 'committed',
        conversationId: parsed.conversationId,
        rootBlobIds: artifacts.nextRootBlobIds,
        turnBlobIds: parsed.historyTurnBlobIds,
        summaryArchiveIds: artifacts.nextSummaryArchiveIds,
        tokenDetails: compactedUsedTokens,
        mode: parsed.mode,
        updatedAt: Date.now(),
    }, run.signal, run.requireCheckpointWriteScope());

    throwIfBlobRunInactive(run);
    yield checkpoint(
        artifacts.nextRootBlobIds,
        compactedUsedTokens.usedTokens,
        compactedUsedTokens.maxTokens,
        parsed.mode,
        undefined,
        {
            turnBlobIds: parsed.historyTurnBlobIds,
            summaryArchiveIds: artifacts.nextSummaryArchiveIds,
            workspaceUris: workspaceUris(parsed),
            readPaths: parsed.readPaths,
            modelName: route.model,
            gitRepos: parsed.gitRepos?.map(r => ({ path: r.path, branchName: r.branchName })),
        },
    );
    throwIfBlobRunInactive(run);
    yield summaryCompleted(hookMessage ?? 'Chat context summarized.');
}
