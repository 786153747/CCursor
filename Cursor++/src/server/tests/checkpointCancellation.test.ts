import type { CheckpointKind, CheckpointWriteScope, PersistedConversationCheckpoint } from '../database/checkpoints'
import type { AsyncDatabase } from '../database/sqlite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { beginCheckpointWriteScope, CheckpointConflictError, clearDraftCheckpoint, clearPersistedConversationCheckpoint, getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { closeAgentDatabase, getAgentDatabase, getCheckpointDatabase, initDatabase } from '../database/sqlite'
import { recordModelUsage } from '../database/usageStats'

const conversationId = 'checkpoint-cancellation-conversation'
let temporaryDirectory = ''
let previousDatabasePath: string | undefined
let pendingWrites: Promise<void>[] = []
let releasePendingGates: Array<() => void> = []

beforeEach(async () => {
  await closeAgentDatabase()
  previousDatabasePath = process.env.BYOK_AGENT_DB_PATH
  temporaryDirectory = mkdtempSync(join(tmpdir(), 'ccursor-checkpoint-cancellation-'))
  process.env.BYOK_AGENT_DB_PATH = join(temporaryDirectory, 'cursor.db')
  pendingWrites = []
  releasePendingGates = []
  await initDatabase()
})

afterEach(async () => {
  for (const releaseGate of releasePendingGates)
    releaseGate()
  await Promise.allSettled(pendingWrites)
  vi.restoreAllMocks()
  await closeAgentDatabase()
  if (previousDatabasePath === undefined)
    delete process.env.BYOK_AGENT_DB_PATH
  else process.env.BYOK_AGENT_DB_PATH = previousDatabasePath
  rmSync(temporaryDirectory, { recursive: true, force: true })
})

function createDeferred() {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

function makeCheckpoint(label: string, kind: CheckpointKind = 'committed'): PersistedConversationCheckpoint {
  return {
    conversationId,
    kind,
    rootBlobIds: [`root-${label}`],
    turnBlobIds: [`turn-${label}`],
    summaryArchiveIds: [`archive-${label}`],
    tokenDetails: { usedTokens: 456, maxTokens: 200000 },
    mode: 'AGENT_MODE_AGENT',
    updatedAt: 1700000000100,
  }
}

function startCheckpointWrite(checkpoint: PersistedConversationCheckpoint, signal?: AbortSignal, scope?: CheckpointWriteScope): Promise<void> {
  const persistence = persistConversationCheckpoint(checkpoint, signal, scope)
  pendingWrites.push(persistence)
  void persistence.catch(() => {})
  return persistence
}

function readRawCheckpoints(database: AsyncDatabase) {
  return database.all('SELECT * FROM conversation_checkpoints WHERE conversation_id = ? ORDER BY kind', [conversationId])
}

function delayNextCheckpointCompletion() {
  const database = getCheckpointDatabase()
  const statementApplied = createDeferred()
  const releaseCompletion = createDeferred()
  const executeRun = database.run.bind(database)
  let hasDelayedStatement = false
  vi.spyOn(database, 'run').mockImplementation(async (sql, parameters) => {
    if (hasDelayedStatement || !sql.includes('INSERT INTO conversation_checkpoints'))
      return executeRun(sql, parameters)
    hasDelayedStatement = true
    const result = await executeRun(sql, parameters)
    statementApplied.resolve()
    await releaseCompletion.promise
    return result
  })
  releasePendingGates.push(releaseCompletion.resolve)
  return { statementApplied: statementApplied.promise, releaseCompletion: releaseCompletion.resolve }
}

async function recordIndependentUsage() {
  await recordModelUsage({
    providerId: 'independent-provider',
    providerName: 'Independent Provider',
    modelId: 'independent-model',
    apiModel: 'independent-model',
    usage: { inputTokens: 101, outputTokens: 17, cacheReadTokens: 9, cacheWriteTokens: 3 },
  })
}

describe('checkpoint cancellation dispatch boundary', () => {
  it.each([true, false])('keeps an accepted write when aborted during its completion await (prior row: %s)', async (hasPreviousRow) => {
    if (hasPreviousRow)
      await persistConversationCheckpoint(makeCheckpoint('A'))
    const scope = await beginCheckpointWriteScope(conversationId)
    const controller = new AbortController()
    const gate = delayNextCheckpointCompletion()
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal, scope)
    await gate.statementApplied
    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).resolves.toBeUndefined()
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
    // An accepted commit advances the scope even if cancellation arrived meanwhile.
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('rejects an already aborted signal without SQL, timers, or abort listeners', async () => {
    const database = getAgentDatabase()
    const controller = new AbortController()
    controller.abort()
    const databaseSpies = [
      vi.spyOn(getCheckpointDatabase(), 'get'),
      vi.spyOn(getCheckpointDatabase(), 'run'),
      vi.spyOn(getCheckpointDatabase(), 'all'),
      vi.spyOn(database, 'get'),
      vi.spyOn(database, 'run'),
      vi.spyOn(database, 'exec'),
      vi.spyOn(database, 'all'),
      vi.spyOn(database, 'prepare'),
      vi.spyOn(database, 'transaction'),
    ]
    const timeoutSpy = vi.spyOn(globalThis, 'setTimeout')
    const intervalSpy = vi.spyOn(globalThis, 'setInterval')
    const listenerSpy = vi.spyOn(controller.signal, 'addEventListener')

    await expect(startCheckpointWrite(makeCheckpoint('B'), controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
    for (const databaseSpy of databaseSpies)
      expect(databaseSpy).not.toHaveBeenCalled()
    expect(timeoutSpy).not.toHaveBeenCalled()
    expect(intervalSpy).not.toHaveBeenCalled()
    expect(listenerSpy).not.toHaveBeenCalled()
  })

  it('checks cancellation after the version snapshot and before dispatching the CAS', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const previousRows = await readRawCheckpoints(database)
    const controller = new AbortController()
    const snapshotRead = createDeferred()
    const releaseRead = createDeferred()
    releasePendingGates.push(releaseRead.resolve)
    const checkpointDatabase = getCheckpointDatabase()
    const executeAll = checkpointDatabase.all.bind(checkpointDatabase)
    vi.spyOn(checkpointDatabase, 'all').mockImplementationOnce(async <Row = unknown>(sql: string, parameters?: unknown): Promise<Row[]> => {
      const rows = await executeAll<Row>(sql, parameters)
      snapshotRead.resolve()
      await releaseRead.promise
      return rows
    })
    const runSpy = vi.spyOn(checkpointDatabase, 'run')
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await snapshotRead.promise
    controller.abort()
    releaseRead.resolve()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(runSpy).not.toHaveBeenCalled()
    expect(await readRawCheckpoints(database)).toEqual(previousRows)
  })

  it('preserves payload and version on a SQLite failure and permits retry with the same scope', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const previousRows = await readRawCheckpoints(database)
    const scope = await beginCheckpointWriteScope(conversationId)
    await recordIndependentUsage()
    const usageRows = await database.all('SELECT * FROM model_usage_stats')
    await database.exec(`
      CREATE TRIGGER reject_checkpoint_insert BEFORE INSERT ON conversation_checkpoints
      WHEN NEW.root_blob_ids_json = '["root-B"]'
      BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END;
    `)

    await expect(startCheckpointWrite(makeCheckpoint('B'), undefined, scope)).rejects.toThrow('injected checkpoint failure')
    expect(await readRawCheckpoints(database)).toEqual(previousRows)
    expect(await database.all('SELECT * FROM model_usage_stats')).toEqual(usageRows)
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('does not restore A over cross-connection B then C when the delayed writer is cancelled', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const scope = await beginCheckpointWriteScope(conversationId)
    const controller = new AbortController()
    const gate = delayNextCheckpointCompletion()
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal, scope)
    await gate.statementApplied
    vi.resetModules()
    const externalDatabaseModule = await import('../database/sqlite')
    const externalCheckpoints = await import('../database/checkpoints')
    try {
      await externalDatabaseModule.initDatabase()
      expect(externalDatabaseModule.getAgentDatabase()).not.toBe(database)
      await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('C'))
      controller.abort()
      gate.releaseCompletion()

      await expect(persistence).resolves.toBeUndefined()
      expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
      await expect(persistConversationCheckpoint(makeCheckpoint('stale-D'), undefined, scope)).rejects.toBeInstanceOf(CheckpointConflictError)
    }
    finally {
      await externalDatabaseModule.closeAgentDatabase()
    }
  })

  it('keeps a stale write as a conflict, not cancellation or compensating restoration', async () => {
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const staleScope = await beginCheckpointWriteScope(conversationId)
    vi.resetModules()
    const externalDatabaseModule = await import('../database/sqlite')
    const externalCheckpoints = await import('../database/checkpoints')
    try {
      await externalDatabaseModule.initDatabase()
      await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('B'))
      const controller = new AbortController()
      const gate = delayNextCheckpointCompletion()
      const staleWrite = startCheckpointWrite(makeCheckpoint('C'), controller.signal, staleScope)
      await gate.statementApplied
      controller.abort()
      gate.releaseCompletion()

      await expect(staleWrite).rejects.toBeInstanceOf(CheckpointConflictError)
      expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
    }
    finally {
      await externalDatabaseModule.closeAgentDatabase()
    }
  })

  it('does not hold a transaction or roll back usage while checkpoint completion is delayed', async () => {
    const database = getAgentDatabase()
    const transactionSpy = vi.spyOn(database, 'transaction')
    const execSpy = vi.spyOn(database, 'exec')
    const controller = new AbortController()
    const gate = delayNextCheckpointCompletion()
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.statementApplied
    await recordIndependentUsage()
    const usageRows = await database.all('SELECT * FROM model_usage_stats')
    expect(usageRows).toHaveLength(1)
    controller.abort()
    gate.releaseCompletion()

    await persistence
    expect(await database.all('SELECT * FROM model_usage_stats')).toEqual(usageRows)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
    expect(transactionSpy).not.toHaveBeenCalled()
    expect(execSpy).not.toHaveBeenCalled()
  })

  it.each<CheckpointKind>(['committed', 'draft'])('does not resurrect a cleared %s checkpoint after cancellation', async (kind) => {
    const database = getAgentDatabase()
    const controller = new AbortController()
    const gate = delayNextCheckpointCompletion()
    const persistence = startCheckpointWrite(makeCheckpoint('B', kind), controller.signal)
    await gate.statementApplied
    if (kind === 'draft')
      await clearDraftCheckpoint(conversationId)
    else await clearPersistedConversationCheckpoint(conversationId)
    const tombstones = await readRawCheckpoints(database)
    controller.abort()
    gate.releaseCompletion()

    await persistence
    expect(await getPersistedConversationCheckpoint(conversationId, kind)).toBeNull()
    expect(await readRawCheckpoints(database)).toEqual(tombstones)
  })

  it('keeps a completed checkpoint after later cancellation', async () => {
    const controller = new AbortController()
    await persistConversationCheckpoint(makeCheckpoint('A'), controller.signal)
    const acceptedRows = await readRawCheckpoints(getAgentDatabase())
    controller.abort()
    expect(await readRawCheckpoints(getAgentDatabase())).toEqual(acceptedRows)
  })
})
