import type { CheckpointKind, PersistedConversationCheckpoint } from '../database/checkpoints'
import type { AsyncDatabase } from '../database/sqlite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearDraftCheckpoint, clearPersistedConversationCheckpoint, getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { closeAgentDatabase, getAgentDatabase, initDatabase } from '../database/sqlite'
import { recordModelUsage } from '../database/usageStats'
import { logger } from '../logger'

const conversationId = 'checkpoint-cancellation-conversation'
let temporaryDirectory = ''
let previousDatabasePath: string | undefined
let pendingWrites: Promise<void>[] = []
let releasePendingGates: Array<() => void> = []

interface StoredCheckpointRow {
  conversation_id: string
  kind: CheckpointKind
  root_blob_ids_json: string
  turn_blob_ids_json: string
  summary_archive_ids_json: string
  used_tokens: number
  max_tokens: number
  mode: string
  updated_at: number
  write_token: string
}

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
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
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

function startCheckpointWrite(checkpoint: PersistedConversationCheckpoint, signal?: AbortSignal): Promise<void> {
  const persistence = persistConversationCheckpoint(checkpoint, signal)
  pendingWrites.push(persistence)
  // Gates are released and writes are settled even if an assertion fails early.
  void persistence.catch(() => {})
  return persistence
}

async function seedRawCheckpoint(database: AsyncDatabase, overrides: Partial<StoredCheckpointRow> = {}): Promise<StoredCheckpointRow> {
  const row: StoredCheckpointRow = {
    conversation_id: conversationId,
    kind: 'committed',
    root_blob_ids_json: ' [ "root-A" , "root-A" ] ',
    turn_blob_ids_json: '[\n "turn-A"\n]',
    summary_archive_ids_json: ' [ "archive-A" ] ',
    used_tokens: 123,
    max_tokens: 100000,
    mode: 'AGENT_MODE_ASK',
    updated_at: 1700000000000,
    write_token: 'original-write-token',
    ...overrides,
  }
  await database.run(`
    INSERT OR REPLACE INTO conversation_checkpoints (
      conversation_id, kind, root_blob_ids_json, turn_blob_ids_json,
      summary_archive_ids_json, used_tokens, max_tokens, mode, updated_at, write_token
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    row.conversation_id,
    row.kind,
    row.root_blob_ids_json,
    row.turn_blob_ids_json,
    row.summary_archive_ids_json,
    row.used_tokens,
    row.max_tokens,
    row.mode,
    row.updated_at,
    row.write_token,
  ])
  return row
}

function readRawCheckpoint(database: AsyncDatabase, kind: CheckpointKind = 'committed') {
  return database.get<StoredCheckpointRow>(`
    SELECT * FROM conversation_checkpoints WHERE conversation_id = ? AND kind = ?
  `, [conversationId, kind])
}

function delayNextCheckpointInsertCompletion(database: AsyncDatabase) {
  const insertApplied = createDeferred()
  const releaseCompletion = createDeferred()
  const executeRun = database.run.bind(database)
  let hasDelayedInsert = false
  vi.spyOn(database, 'run').mockImplementation(async (sql, parameters) => {
    const shouldDelay = !hasDelayedInsert && sql.includes('INSERT OR REPLACE INTO conversation_checkpoints')
    if (!shouldDelay)
      return executeRun(sql, parameters)

    hasDelayedInsert = true
    try {
      const result = await executeRun(sql, parameters)
      insertApplied.resolve()
      await releaseCompletion.promise
      return result
    }
    catch (error) {
      insertApplied.reject(error)
      throw error
    }
  })
  releasePendingGates.push(releaseCompletion.resolve)
  return { insertApplied: insertApplied.promise, releaseCompletion: releaseCompletion.resolve }
}

describe('checkpoint cancellation safety', () => {
  it('restores the exact prior committed row when cancellation occurs during the insert await', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.insertApplied
    expect((await readRawCheckpoint(database))?.root_blob_ids_json).toBe('["root-B"]')

    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(await readRawCheckpoint(database)).toEqual(previousRow)
  })

  it('removes a cancelled insert when no previous row existed and leaves other kinds intact', async () => {
    const database = getAgentDatabase()
    const draftRow = await seedRawCheckpoint(database, { kind: 'draft' })
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.insertApplied

    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(await readRawCheckpoint(database)).toBeUndefined()
    expect(await readRawCheckpoint(database, 'draft')).toEqual(draftRow)
  })

  it('lets a queued valid writer read the restored row and survive earlier and later cancellation', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    const firstController = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const cancelledWrite = startCheckpointWrite(makeCheckpoint('B'), firstController.signal)
    await gate.insertApplied

    const readSpy = vi.spyOn(database, 'get')
    const validCheckpoint = makeCheckpoint('C')
    const validWrite = startCheckpointWrite(validCheckpoint, new AbortController().signal)
    await Promise.resolve()
    expect(readSpy).not.toHaveBeenCalled()
    firstController.abort()
    gate.releaseCompletion()

    await expect(cancelledWrite).rejects.toMatchObject({ name: 'AbortError' })
    await validWrite
    expect(readSpy).toHaveBeenCalledTimes(1)
    expect(await readSpy.mock.results[0].value).toEqual(previousRow)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(validCheckpoint)

    const laterController = new AbortController()
    laterController.abort()
    await expect(startCheckpointWrite(makeCheckpoint('D'), laterController.signal)).rejects.toMatchObject({ name: 'AbortError' })
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(validCheckpoint)
  })

  it('preserves the previous row on an ordinary SQLite insert failure and keeps the queue usable', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    await database.exec(`
      CREATE TRIGGER reject_checkpoint_insert BEFORE INSERT ON conversation_checkpoints
      WHEN NEW.root_blob_ids_json = '["root-B"]'
      BEGIN SELECT RAISE(ABORT, 'injected checkpoint insert failure'); END;
    `)

    await expect(startCheckpointWrite(makeCheckpoint('B'), new AbortController().signal))
      .rejects
      .toThrow('injected checkpoint insert failure')
    expect(await readRawCheckpoint(database)).toEqual(previousRow)

    const validCheckpoint = makeCheckpoint('C')
    await startCheckpointWrite(validCheckpoint)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(validCheckpoint)
  })

  it.each([true, false])('does not undo an external replacement with a different token (prior row: %s)', async (hasPreviousRow) => {
    const database = getAgentDatabase()
    if (hasPreviousRow)
      await seedRawCheckpoint(database)
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.insertApplied

    // A fresh module opens a separate real SQLite connection, outside our queue.
    vi.resetModules()
    const externalDatabaseModule = await import('../database/sqlite')
    try {
      await externalDatabaseModule.initDatabase()
      const externalRow = await seedRawCheckpoint(externalDatabaseModule.getAgentDatabase(), {
        root_blob_ids_json: '["external-C"]',
        updated_at: 1700000000200,
        write_token: 'external-writer-token',
      })
      controller.abort()
      gate.releaseCompletion()

      await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
      expect(await readRawCheckpoint(database)).toEqual(externalRow)
    }
    finally {
      await externalDatabaseModule.closeAgentDatabase()
    }
  })

  it('does not roll back unrelated usage writes made while checkpoint completion is delayed', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.insertApplied

    await recordModelUsage({
      providerId: 'independent-provider',
      providerName: 'Independent Provider',
      modelId: 'independent-model',
      apiModel: 'independent-model',
      usage: { inputTokens: 101, outputTokens: 17, cacheReadTokens: 9, cacheWriteTokens: 3 },
    })
    const usageBeforeCancellation = await database.all('SELECT * FROM model_usage_stats')
    expect(usageBeforeCancellation).toHaveLength(1)
    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(await readRawCheckpoint(database)).toEqual(previousRow)
    expect(await database.all('SELECT * FROM model_usage_stats')).toEqual(usageBeforeCancellation)
  })

  it('rejects an already aborted signal without SQL, timers, or abort listeners', async () => {
    const database = getAgentDatabase()
    const controller = new AbortController()
    controller.abort()
    const databaseSpies = [
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

  it('checks cancellation again after reading the previous row, before issuing any insert', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    const controller = new AbortController()
    const rowRead = createDeferred()
    const releaseRead = createDeferred()
    releasePendingGates.push(releaseRead.resolve)
    const executeGet = database.get.bind(database)
    vi.spyOn(database, 'get').mockImplementationOnce(async <Row = unknown>(sql: string, parameters?: unknown): Promise<Row | undefined> => {
      const row = await executeGet<Row>(sql, parameters)
      rowRead.resolve()
      await releaseRead.promise
      return row
    })
    const runSpy = vi.spyOn(database, 'run')
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await rowRead.promise
    controller.abort()
    releaseRead.resolve()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(runSpy).not.toHaveBeenCalled()
    expect(await readRawCheckpoint(database)).toEqual(previousRow)
  })

  it.each<CheckpointKind>(['committed', 'draft'])('does not resurrect an explicitly cleared %s checkpoint', async (kind) => {
    const database = getAgentDatabase()
    await seedRawCheckpoint(database, { kind })
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B', kind), controller.signal)
    await gate.insertApplied

    if (kind === 'draft')
      await clearDraftCheckpoint(conversationId)
    else await clearPersistedConversationCheckpoint(conversationId)
    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).rejects.toMatchObject({ name: 'AbortError' })
    expect(await readRawCheckpoint(database, kind)).toBeUndefined()
  })

  it('logs and propagates restoration failure instead of reporting safely restored cancellation', async () => {
    const database = getAgentDatabase()
    await seedRawCheckpoint(database)
    await database.exec(`
      CREATE TRIGGER reject_checkpoint_restore BEFORE UPDATE ON conversation_checkpoints
      WHEN NEW.write_token = 'original-write-token'
      BEGIN SELECT RAISE(ABORT, 'injected checkpoint restoration failure'); END;
    `)
    const errorSpy = vi.spyOn(logger, 'error')
    const controller = new AbortController()
    const gate = delayNextCheckpointInsertCompletion(database)
    const persistence = startCheckpointWrite(makeCheckpoint('B'), controller.signal)
    await gate.insertApplied
    controller.abort()
    gate.releaseCompletion()

    await expect(persistence).rejects.toThrow('injected checkpoint restoration failure')
    expect(errorSpy).toHaveBeenCalledWith(
      expect.objectContaining({ conversationId, kind: 'committed', error: expect.stringContaining('restoration failure') }),
      expect.stringContaining('previous checkpoint may not be preserved'),
    )
    expect((await readRawCheckpoint(database))?.root_blob_ids_json).toBe('["root-B"]')

    await startCheckpointWrite(makeCheckpoint('C'))
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('keeps a successfully completed checkpoint when its signal is aborted later', async () => {
    const database = getAgentDatabase()
    const controller = new AbortController()
    await startCheckpointWrite(makeCheckpoint('C'), controller.signal)
    const committedRow = await readRawCheckpoint(database)
    controller.abort()

    expect(await readRawCheckpoint(database)).toEqual(committedRow)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('gives ordinary one-argument writes unique tokens without reading prior rows', async () => {
    const database = getAgentDatabase()
    const readSpy = vi.spyOn(database, 'get')
    await startCheckpointWrite(makeCheckpoint('A'))
    expect(readSpy).not.toHaveBeenCalled()
    const firstRow = await readRawCheckpoint(database)
    readSpy.mockClear()

    await startCheckpointWrite(makeCheckpoint('B'))
    expect(readSpy).not.toHaveBeenCalled()
    const secondRow = await readRawCheckpoint(database)
    expect(firstRow?.write_token).toEqual(expect.any(String))
    expect(firstRow?.write_token).not.toBe('')
    expect(secondRow?.write_token).toEqual(expect.any(String))
    expect(secondRow?.write_token).not.toBe(firstRow?.write_token)
  })
})

describe('checkpoint write-token migration', () => {
  it('adds a default token to an existing checkpoint table without rewriting its metadata', async () => {
    const database = getAgentDatabase()
    const previousRow = await seedRawCheckpoint(database)
    await database.exec('ALTER TABLE conversation_checkpoints DROP COLUMN write_token')
    await closeAgentDatabase()
    await initDatabase()

    expect(await readRawCheckpoint(getAgentDatabase())).toEqual({ ...previousRow, write_token: '' })
    await startCheckpointWrite(makeCheckpoint('C'), new AbortController().signal)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('adds the token after the older kind migration without changing its insert column count', async () => {
    const database = getAgentDatabase()
    await database.exec(`
      DROP TABLE conversation_checkpoints;
      CREATE TABLE conversation_checkpoints (
        conversation_id TEXT PRIMARY KEY NOT NULL,
        root_blob_ids_json TEXT NOT NULL,
        summary_archive_ids_json TEXT NOT NULL,
        used_tokens INTEGER NOT NULL,
        max_tokens INTEGER NOT NULL,
        mode TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
    `)
    await database.run(`INSERT INTO conversation_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)`, [
      conversationId,
      ' [ "root-A" ] ',
      ' [ "archive-A" ] ',
      123,
      100000,
      'AGENT_MODE_ASK',
      1700000000000,
    ])
    await closeAgentDatabase()
    await initDatabase()

    expect(await readRawCheckpoint(getAgentDatabase())).toEqual({
      conversation_id: conversationId,
      kind: 'committed',
      root_blob_ids_json: ' [ "root-A" ] ',
      turn_blob_ids_json: '[]',
      summary_archive_ids_json: ' [ "archive-A" ] ',
      used_tokens: 123,
      max_tokens: 100000,
      mode: 'AGENT_MODE_ASK',
      updated_at: 1700000000000,
      write_token: '',
    })
  })
})
