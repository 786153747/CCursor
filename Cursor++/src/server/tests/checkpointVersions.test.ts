import type { CheckpointKind, PersistedConversationCheckpoint } from '../database/checkpoints'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { adoptConversationCheckpoint, beginCheckpointWriteScope, CheckpointConflictError, clearDraftCheckpoint, clearPersistedConversationCheckpoint, getPersistedConversationCheckpoint, persistConversationCheckpoint } from '../database/checkpoints'
import { closeAgentDatabase, getAgentDatabase, getCheckpointDatabase, initDatabase } from '../database/sqlite'
import { recordModelUsage } from '../database/usageStats'

const conversationId = 'checkpoint-version-conversation'
let temporaryDirectory = ''
let previousDatabasePath: string | undefined
let externalDatabaseModule: typeof import('../database/sqlite')
let externalCheckpoints: typeof import('../database/checkpoints')

beforeEach(async () => {
  await closeAgentDatabase()
  previousDatabasePath = process.env.BYOK_AGENT_DB_PATH
  temporaryDirectory = mkdtempSync(join(tmpdir(), 'ccursor-checkpoint-versions-'))
  process.env.BYOK_AGENT_DB_PATH = join(temporaryDirectory, 'cursor.db')
  await initDatabase()
  // Separate module instances own separate native SQLite connections, not a mocked queue.
  vi.resetModules()
  externalDatabaseModule = await import('../database/sqlite')
  externalCheckpoints = await import('../database/checkpoints')
  await externalDatabaseModule.initDatabase()
  expect(externalDatabaseModule.getAgentDatabase()).not.toBe(getAgentDatabase())
  expect(externalDatabaseModule.getCheckpointDatabase()).not.toBe(getCheckpointDatabase())
})

afterEach(async () => {
  vi.restoreAllMocks()
  await externalDatabaseModule.closeAgentDatabase()
  await closeAgentDatabase()
  if (previousDatabasePath === undefined)
    delete process.env.BYOK_AGENT_DB_PATH
  else process.env.BYOK_AGENT_DB_PATH = previousDatabasePath
  rmSync(temporaryDirectory, { recursive: true, force: true })
})

function makeCheckpoint(label: string, kind: CheckpointKind = 'committed'): PersistedConversationCheckpoint {
  return {
    conversationId,
    kind,
    rootBlobIds: [`root-${label}`],
    turnBlobIds: [`turn-${label}`],
    summaryArchiveIds: [`archive-${label}`],
    tokenDetails: { usedTokens: 123, maxTokens: 200000 },
    mode: 'AGENT_MODE_AGENT',
    // Deliberately identical: validity must not be based on timestamp order.
    updatedAt: 1700000000000,
  }
}

function readRawRows() {
  return getAgentDatabase().all('SELECT * FROM conversation_checkpoints WHERE conversation_id = ? ORDER BY kind', [conversationId])
}

async function seedLegacyCheckpoint(checkpoint: PersistedConversationCheckpoint, writeToken: string): Promise<void> {
  // Reproduce the old independent per-kind writes, without modern draft retirement.
  await getAgentDatabase().run(`
    INSERT OR REPLACE INTO conversation_checkpoints (
      conversation_id, kind, root_blob_ids_json, turn_blob_ids_json,
      summary_archive_ids_json, used_tokens, max_tokens, mode, updated_at, write_token
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    checkpoint.conversationId,
    checkpoint.kind,
    JSON.stringify(checkpoint.rootBlobIds),
    JSON.stringify(checkpoint.turnBlobIds),
    JSON.stringify(checkpoint.summaryArchiveIds),
    checkpoint.tokenDetails.usedTokens,
    checkpoint.tokenDetails.maxTokens,
    checkpoint.mode,
    checkpoint.updatedAt,
    writeToken,
  ])
}

describe('conversation-wide checkpoint CAS', () => {
  it.each<CheckpointKind>(['committed', 'draft'])('rejects stale writes to either kind after an external %s write', async (winningKind) => {
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const staleScope = await beginCheckpointWriteScope(conversationId)
    const externalScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('B', winningKind), undefined, externalScope)
    const winnerRows = await readRawRows()

    for (const losingKind of ['committed', 'draft'] as const) {
      await expect(persistConversationCheckpoint(makeCheckpoint('C', losingKind), undefined, staleScope))
        .rejects
        .toBeInstanceOf(CheckpointConflictError)
    }
    expect(await readRawRows()).toEqual(winnerRows)
  })

  it('advances one writer scope across rolling drafts, final commit, and draft cleanup', async () => {
    const scope = await beginCheckpointWriteScope(conversationId)
    const staleScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    const snapshotSpy = vi.spyOn(getCheckpointDatabase(), 'all')
    await persistConversationCheckpoint(makeCheckpoint('rolling-1', 'draft'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('rolling-2', 'draft'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('final'), undefined, scope)
    await clearDraftCheckpoint(conversationId, undefined, scope)
    expect(snapshotSpy).not.toHaveBeenCalled()
    expect(scope.committedCheckpoint).toBeNull()
    expect(scope.draftCheckpoint).toBeNull()
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('final'))
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    await expect(externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('stale'), undefined, staleScope))
      .rejects
      .toMatchObject({ name: 'CheckpointConflictError', conversationId })
  })

  it('does not reserve ownership by run arrival order', async () => {
    const earlierScope = await beginCheckpointWriteScope(conversationId)
    const laterScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('earlier-winner'), undefined, earlierScope)
    await expect(externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('later-loser'), undefined, laterScope))
      .rejects
      .toMatchObject({ name: 'CheckpointConflictError' })
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('earlier-winner'))
  })

  it('accepts exactly one simultaneous cross-connection writer based on the same snapshot', async () => {
    const localScope = await beginCheckpointWriteScope(conversationId)
    const externalScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    const results = await Promise.allSettled([
      persistConversationCheckpoint(makeCheckpoint('local', 'draft'), undefined, localScope),
      externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('external'), undefined, externalScope),
    ])
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1)
    const rejectedResult = results.find(result => result.status === 'rejected')
    expect(rejectedResult).toMatchObject({ reason: { name: 'CheckpointConflictError' } })
    const activeRows = await getAgentDatabase().all('SELECT * FROM conversation_checkpoints WHERE conversation_id = ? AND is_deleted = 0', [conversationId])
    expect(activeRows).toHaveLength(1)
  })

  it('keeps independent conversations valid across concurrent connections', async () => {
    const otherCheckpoint = { ...makeCheckpoint('other'), conversationId: 'independent-conversation' }
    const scope = await beginCheckpointWriteScope(conversationId)
    const otherScope = await externalCheckpoints.beginCheckpointWriteScope(otherCheckpoint.conversationId)
    await Promise.all([
      persistConversationCheckpoint(makeCheckpoint('local'), undefined, scope),
      externalCheckpoints.persistConversationCheckpoint(otherCheckpoint, undefined, otherScope),
    ])
    await persistConversationCheckpoint(makeCheckpoint('local-next', 'draft'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(otherCheckpoint.conversationId)).toEqual(otherCheckpoint)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toEqual(makeCheckpoint('local-next', 'draft'))
  })

  it('rejects a scope belonging to a different conversation before issuing SQL', async () => {
    const scope = await beginCheckpointWriteScope('another-conversation')
    const runSpy = vi.spyOn(getCheckpointDatabase(), 'run')
    await expect(persistConversationCheckpoint(makeCheckpoint('A'), undefined, scope)).rejects.toThrow('different conversation')
    expect(runSpy).not.toHaveBeenCalled()
  })

  it('keeps the one-argument API as a one-shot CAS and generates fresh versions for identical payloads', async () => {
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const scope = await beginCheckpointWriteScope(conversationId)
    const previousRows = await readRawRows()
    await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('A'))
    expect(await readRawRows()).not.toEqual(previousRows)
    await expect(persistConversationCheckpoint(makeCheckpoint('B'), undefined, scope)).rejects.toBeInstanceOf(CheckpointConflictError)
  })
})

describe('committed checkpoint draft retirement', () => {
  it('retires draft B with final C in one CAS, changes the active baseline, and invalidates sibling scopes', async () => {
    const scope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('A'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('B', 'draft'), undefined, scope)
    const siblingScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    expect(siblingScope.draftCheckpoint).toEqual(makeCheckpoint('B', 'draft'))
    const runSpy = vi.spyOn(getCheckpointDatabase(), 'run')

    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    expect(runSpy).toHaveBeenCalledTimes(1)
    const finalRows = await readRawRows()
    expect(finalRows).toEqual([
      expect.objectContaining({ kind: 'committed', root_blob_ids_json: '["root-C"]', is_deleted: 0, write_token: expect.stringMatching(/^ccursor-cas-v1:/) }),
      expect.objectContaining({ kind: 'draft', root_blob_ids_json: '[]', turn_blob_ids_json: '[]', summary_archive_ids_json: '[]', is_deleted: 1, write_token: expect.stringMatching(/^ccursor-cas-v1:/) }),
    ])
    const laterScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    expect(laterScope.draftCheckpoint).toBeNull()
    expect(laterScope.draftCheckpoint ?? laterScope.committedCheckpoint).toEqual(makeCheckpoint('C'))
    await expect(externalCheckpoints.beginCheckpointWriteScope(conversationId, {
      expectedCommittedCheckpoint: makeCheckpoint('B', 'draft'),
    })).rejects.toMatchObject({ name: 'CheckpointConflictError' })
    for (const kind of ['committed', 'draft'] as const) {
      await expect(externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('stale', kind), undefined, siblingScope))
        .rejects
        .toMatchObject({ name: 'CheckpointConflictError' })
    }
    expect(await readRawRows()).toEqual(finalRows)
    // Reusing the writer proves that both of its expected tokens advanced at commit.
    await persistConversationCheckpoint(makeCheckpoint('next', 'draft'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toEqual(makeCheckpoint('next', 'draft'))
  })

  it.each([true, false])('preserves both rows and scope on draft retirement failure (active draft: %s)', async (hasActiveDraft) => {
    const database = getAgentDatabase()
    const scope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('A'), undefined, scope)
    if (hasActiveDraft)
      await persistConversationCheckpoint(makeCheckpoint('B', 'draft'), undefined, scope)
    const previousRows = await readRawRows()
    await database.exec(`
      CREATE TRIGGER reject_final_draft_retirement BEFORE INSERT ON conversation_checkpoints
      WHEN NEW.kind = 'draft' AND NEW.is_deleted = 1
      BEGIN SELECT RAISE(ABORT, 'injected draft retirement failure'); END;
    `)

    await expect(persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)).rejects.toThrow('draft retirement failure')
    expect(await readRawRows()).toEqual(previousRows)
    expect(await externalCheckpoints.getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('A'))
    expect(await externalCheckpoints.getPersistedConversationCheckpoint(conversationId, 'draft'))
      .toEqual(hasActiveDraft ? makeCheckpoint('B', 'draft') : null)
    await database.exec('DROP TRIGGER reject_final_draft_retirement')
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
  })
})

describe('checkpoint autocommit isolation', () => {
  it('cannot report a checkpoint committed inside an unrelated transaction that later rolls back', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const scope = await beginCheckpointWriteScope(conversationId)
    await expect(database.transaction(async () => {
      // A deferred transaction on the shared connection must not capture this write.
      await persistConversationCheckpoint(makeCheckpoint('B'), undefined, scope)
      expect(await externalCheckpoints.getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
      throw new Error('unrelated transaction failed')
    })).rejects.toThrow('unrelated transaction failed')
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('C'))
  })

  it('snapshots committed versions instead of uncommitted edits on the shared connection', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    await database.exec('BEGIN')
    try {
      await database.run(`UPDATE conversation_checkpoints SET write_token = 'uncommitted-token' WHERE conversation_id = ?`, [conversationId])
      const scope = await beginCheckpointWriteScope(conversationId)
      await database.exec('ROLLBACK')
      await persistConversationCheckpoint(makeCheckpoint('B'), undefined, scope)
      expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('B'))
    }
    finally {
      await database.exec('ROLLBACK').catch(() => {})
    }
  })
})

describe('checkpoint deletion and restart', () => {
  it.each([true, false])('retains deletion versions across reopen and rejects pre-reset scopes (existing: %s)', async (hasPreviousCheckpoint) => {
    if (hasPreviousCheckpoint) {
      await persistConversationCheckpoint(makeCheckpoint('A'))
      await persistConversationCheckpoint(makeCheckpoint('draft-A', 'draft'))
    }
    const staleScope = await beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.clearPersistedConversationCheckpoint(conversationId)
    const tombstones = await readRawRows()
    expect(tombstones).toEqual([
      expect.objectContaining({ kind: 'committed', is_deleted: 1, write_token: expect.any(String) }),
      expect.objectContaining({ kind: 'draft', is_deleted: 1, write_token: expect.any(String) }),
    ])
    await closeAgentDatabase()
    await initDatabase()
    expect(await readRawRows()).toEqual(tombstones)
    for (const kind of ['committed', 'draft'] as const) {
      expect(await getPersistedConversationCheckpoint(conversationId, kind)).toBeNull()
      await expect(persistConversationCheckpoint(makeCheckpoint('stale', kind), undefined, staleScope))
        .rejects
        .toBeInstanceOf(CheckpointConflictError)
    }
    const resetScope = await beginCheckpointWriteScope(conversationId, { expectedCommittedCheckpoint: null })
    await persistConversationCheckpoint(makeCheckpoint('new'), undefined, resetScope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('new'))
  })

  it('never returns to an old version after delete, recreate-identical, and delete again', async () => {
    const absentScope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const originalScope = await beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.clearPersistedConversationCheckpoint(conversationId)
    const deletedScope = await beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('A'))
    await externalCheckpoints.clearPersistedConversationCheckpoint(conversationId)
    for (const staleScope of [absentScope, originalScope, deletedScope]) {
      await expect(persistConversationCheckpoint(makeCheckpoint('resurrected'), undefined, staleScope))
        .rejects
        .toBeInstanceOf(CheckpointConflictError)
    }
    expect(await getPersistedConversationCheckpoint(conversationId)).toBeNull()
  })

  it('clears even an absent draft while invalidating old committed writers and preserving committed data', async () => {
    await persistConversationCheckpoint(makeCheckpoint('A'))
    const staleScope = await beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.clearDraftCheckpoint(conversationId)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('A'))
    await expect(persistConversationCheckpoint(makeCheckpoint('stale'), undefined, staleScope))
      .rejects
      .toBeInstanceOf(CheckpointConflictError)
  })

  it('rejects stale cleanup rather than deleting a newer run checkpoint', async () => {
    const staleScope = await beginCheckpointWriteScope(conversationId)
    await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('new-draft', 'draft'))
    await expect(clearDraftCheckpoint(conversationId, undefined, staleScope)).rejects.toBeInstanceOf(CheckpointConflictError)
    await expect(clearPersistedConversationCheckpoint(conversationId, undefined, staleScope)).rejects.toBeInstanceOf(CheckpointConflictError)
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toEqual(makeCheckpoint('new-draft', 'draft'))
  })

  it('atomically clears both kinds and advances the same scope for an intentional reset', async () => {
    const scope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('A'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('draft-A', 'draft'), undefined, scope)
    await clearPersistedConversationCheckpoint(conversationId, undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toBeNull()
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    await persistConversationCheckpoint(makeCheckpoint('after-reset'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('after-reset'))
  })

  it('rolls back the entire clear statement if its second row fails, without advancing its scope', async () => {
    const database = getAgentDatabase()
    const scope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('A'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('draft-A', 'draft'), undefined, scope)
    const previousRows = await readRawRows()
    await database.exec(`
      CREATE TRIGGER reject_draft_tombstone BEFORE INSERT ON conversation_checkpoints
      WHEN NEW.kind = 'draft' AND NEW.is_deleted = 1
      BEGIN SELECT RAISE(ABORT, 'injected second-row failure'); END;
    `)
    await expect(clearPersistedConversationCheckpoint(conversationId, undefined, scope)).rejects.toThrow('second-row failure')
    expect(await readRawRows()).toEqual(previousRows)
    await database.exec('DROP TRIGGER reject_draft_tombstone')
    await persistConversationCheckpoint(makeCheckpoint('after-failure'), undefined, scope)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('after-failure'))
  })
})

describe('run-start reference precondition', () => {
  it('rejects old input even when its run begins after the winning run completed', async () => {
    await persistConversationCheckpoint(makeCheckpoint('A'))
    await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('B'))
    await expect(beginCheckpointWriteScope(conversationId, { expectedCommittedCheckpoint: makeCheckpoint('A') }))
      .rejects
      .toBeInstanceOf(CheckpointConflictError)
    const scope = await beginCheckpointWriteScope(conversationId, { expectedCommittedCheckpoint: makeCheckpoint('B') })
    expect(scope.committedCheckpoint).toEqual(makeCheckpoint('B'))
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
  })

  it('compares all reference arrays exactly but not incidental token usage or timestamps', async () => {
    const checkpoint = makeCheckpoint('A')
    checkpoint.rootBlobIds.push('second-root')
    await persistConversationCheckpoint(checkpoint)
    for (const referenceField of ['rootBlobIds', 'turnBlobIds', 'summaryArchiveIds'] as const) {
      await expect(beginCheckpointWriteScope(conversationId, {
        expectedCommittedCheckpoint: { ...checkpoint, [referenceField]: ['different'] },
      })).rejects.toBeInstanceOf(CheckpointConflictError)
    }
    await expect(beginCheckpointWriteScope(conversationId, {
      expectedCommittedCheckpoint: { ...checkpoint, rootBlobIds: [...checkpoint.rootBlobIds].reverse() },
    })).rejects.toBeInstanceOf(CheckpointConflictError)
    const input = { ...checkpoint, updatedAt: 0, tokenDetails: { usedTokens: 0, maxTokens: 0 } }
    await expect(beginCheckpointWriteScope(conversationId, { expectedCommittedCheckpoint: input })).resolves.toBeDefined()
    await expect(beginCheckpointWriteScope(conversationId, { expectedCommittedCheckpoint: null })).rejects.toBeInstanceOf(CheckpointConflictError)
  })
})

describe('legacy checkpoint provenance', () => {
  it('flags a divergent legacy draft then final pair after migration without changing either recovery candidate', async () => {
    const legacyDraft = makeCheckpoint('legacy-D', 'draft')
    const legacyFinal = { ...makeCheckpoint('legacy-C'), updatedAt: legacyDraft.updatedAt + 100 }
    await seedLegacyCheckpoint(legacyDraft, 'legacy-draft-token')
    await seedLegacyCheckpoint(legacyFinal, 'legacy-final-token')
    const originalRows = await readRawRows()
    await externalDatabaseModule.closeAgentDatabase()
    await getAgentDatabase().exec('ALTER TABLE conversation_checkpoints DROP COLUMN is_deleted')
    await closeAgentDatabase()
    await initDatabase()
    await externalDatabaseModule.initDatabase()
    expect(await readRawRows()).toEqual(originalRows)
    const writeSpies = [
      vi.spyOn(getCheckpointDatabase(), 'run'),
      vi.spyOn(getAgentDatabase(), 'run'),
      vi.spyOn(getAgentDatabase(), 'exec'),
      vi.spyOn(externalDatabaseModule.getCheckpointDatabase(), 'run'),
    ]

    const scope = await beginCheckpointWriteScope(conversationId)
    expect(scope.hasAmbiguousLegacyPair).toBe(true)
    expect(scope.draftCheckpoint).toEqual(legacyDraft)
    expect(scope.committedCheckpoint).toEqual(legacyFinal)
    const externalScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    expect(externalScope.hasAmbiguousLegacyPair).toBe(true)
    expect(await readRawRows()).toEqual(originalRows)
    for (const writeSpy of writeSpies)
      expect(writeSpy).not.toHaveBeenCalled()
  })

  it.each<CheckpointKind>(['committed', 'draft'])('flags a mixed-provenance divergent pair when %s has a legacy token', async (legacyKind) => {
    await persistConversationCheckpoint(makeCheckpoint('C'))
    await persistConversationCheckpoint(makeCheckpoint('D', 'draft'))
    await getAgentDatabase().run(`UPDATE conversation_checkpoints SET write_token = ? WHERE conversation_id = ? AND kind = ?`, [
      `legacy-${legacyKind}`,
      conversationId,
      legacyKind,
    ])
    const originalRows = await readRawRows()
    expect((await beginCheckpointWriteScope(conversationId)).hasAmbiguousLegacyPair).toBe(true)
    expect(await readRawRows()).toEqual(originalRows)
  })

  it.each<CheckpointKind>(['committed', 'draft'])('allows a single active legacy %s checkpoint', async (kind) => {
    const checkpoint = makeCheckpoint('legacy-only', kind)
    await seedLegacyCheckpoint(checkpoint, '')
    const originalRows = await readRawRows()
    const scope = await beginCheckpointWriteScope(conversationId)
    expect(scope.hasAmbiguousLegacyPair).toBe(false)
    expect(scope.draftCheckpoint ?? scope.committedCheckpoint).toEqual(checkpoint)
    expect(await readRawRows()).toEqual(originalRows)
  })

  it('allows identical legacy reference triples despite different tokens, usage, and timestamps', async () => {
    const checkpoint = makeCheckpoint('same')
    await seedLegacyCheckpoint(checkpoint, 'legacy-committed')
    await seedLegacyCheckpoint({
      ...checkpoint,
      kind: 'draft',
      updatedAt: checkpoint.updatedAt - 100,
      tokenDetails: { usedTokens: 1, maxTokens: 20 },
    }, 'legacy-draft')
    const originalRows = await readRawRows()
    expect((await beginCheckpointWriteScope(conversationId)).hasAmbiguousLegacyPair).toBe(false)
    expect(await readRawRows()).toEqual(originalRows)
  })

  it('allows a divergent tagged pair produced by the atomic retirement protocol', async () => {
    expect((await beginCheckpointWriteScope(conversationId)).hasAmbiguousLegacyPair).toBe(false)
    const scope = await beginCheckpointWriteScope(conversationId)
    await persistConversationCheckpoint(makeCheckpoint('C'), undefined, scope)
    await persistConversationCheckpoint(makeCheckpoint('D', 'draft'), undefined, scope)
    const originalRows = await readRawRows()
    expect(originalRows).toEqual([
      expect.objectContaining({ kind: 'committed', is_deleted: 0, write_token: expect.stringMatching(/^ccursor-cas-v1:/) }),
      expect.objectContaining({ kind: 'draft', is_deleted: 0, write_token: expect.stringMatching(/^ccursor-cas-v1:/) }),
    ])
    const externalScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    expect(externalScope.hasAmbiguousLegacyPair).toBe(false)
    expect(externalScope.draftCheckpoint ?? externalScope.committedCheckpoint).toEqual(makeCheckpoint('D', 'draft'))
    expect(await readRawRows()).toEqual(originalRows)
  })
})

describe('checkpoint version schema migration', () => {
  it('adds tokens and tombstones without rewriting checkpoint metadata, legacy blobs, or usage', async () => {
    const database = getAgentDatabase()
    await persistConversationCheckpoint(makeCheckpoint('A'))
    await database.run(`UPDATE conversation_checkpoints SET root_blob_ids_json = ' [ "root-A" , "root-A" ] ', turn_blob_ids_json = '[\n "turn-A"\n]'`)
    await database.exec(`
      CREATE TABLE agent_blobs (blob_id TEXT PRIMARY KEY, data BLOB NOT NULL);
      INSERT INTO agent_blobs VALUES ('legacy-blob', X'0001ff');
    `)
    await recordModelUsage({
      providerId: 'migration-provider',
      providerName: 'Migration Provider',
      modelId: 'migration-model',
      apiModel: 'migration-model',
      usage: { inputTokens: 10, outputTokens: 4, cacheReadTokens: 0, cacheWriteTokens: 0 },
    })
    const legacyBlobs = await database.all('SELECT * FROM agent_blobs')
    const usageRows = await database.all('SELECT * FROM model_usage_stats')
    const previousRows = await readRawRows()
    await externalDatabaseModule.closeAgentDatabase()
    await database.exec('ALTER TABLE conversation_checkpoints DROP COLUMN write_token; ALTER TABLE conversation_checkpoints DROP COLUMN is_deleted;')
    await closeAgentDatabase()
    await initDatabase()

    expect(await readRawRows()).toEqual(previousRows.map(row => ({ ...row as object, write_token: '', is_deleted: 0 })))
    expect(await getAgentDatabase().all('SELECT * FROM agent_blobs')).toEqual(legacyBlobs)
    expect(await getAgentDatabase().all('SELECT * FROM model_usage_stats')).toEqual(usageRows)
    await persistConversationCheckpoint(makeCheckpoint('after-migration'))
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('after-migration'))
  })

  it('adds version columns after the old single-kind migration without changing its insert count', async () => {
    const database = getAgentDatabase()
    await externalDatabaseModule.closeAgentDatabase()
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
    await database.run('INSERT INTO conversation_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)', [
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
    expect(await readRawRows()).toEqual([{
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
      is_deleted: 0,
      terminal_receipt_json: '',
    }])
    await persistConversationCheckpoint(makeCheckpoint('new', 'draft'))
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toEqual(makeCheckpoint('new', 'draft'))
  })
})

describe('recovery preservation before conditional adoption', () => {
  it('keeps byte-exact legacy candidates and invalidates old writers without replacing the conversation', async () => {
    await seedLegacyCheckpoint(makeCheckpoint('committed'), 'old-committed')
    await seedLegacyCheckpoint(makeCheckpoint('draft', 'draft'), 'old-draft')
    const originalRows = await readRawRows()
    const scope = await beginCheckpointWriteScope(conversationId)
    const olderScope = await externalCheckpoints.beginCheckpointWriteScope(conversationId)
    await adoptConversationCheckpoint(makeCheckpoint('selected'), scope, 'user-selected')
    const snapshots = await getCheckpointDatabase().all<{ checkpoint_rows_json: string }>(
      'SELECT checkpoint_rows_json FROM conversation_checkpoint_recovery WHERE conversation_id = ?',
      [conversationId],
    )
    expect(snapshots).toHaveLength(1)
    expect(JSON.parse(snapshots[0]!.checkpoint_rows_json)).toEqual(originalRows)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('selected'))
    expect(await getPersistedConversationCheckpoint(conversationId, 'draft')).toBeNull()
    await expect(externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('late'), undefined, olderScope))
      .rejects
      .toMatchObject({ name: 'CheckpointConflictError' })
  })

  it('never adopts if preserving the original candidates fails', async () => {
    await persistConversationCheckpoint(makeCheckpoint('original'))
    const originalRows = await readRawRows()
    const scope = await beginCheckpointWriteScope(conversationId)
    vi.spyOn(getCheckpointDatabase(), 'run').mockRejectedValueOnce(new Error('Recovery disk full'))
    await expect(adoptConversationCheckpoint(makeCheckpoint('selected'), scope, 'user-selected')).rejects.toThrow('Recovery disk full')
    expect(await readRawRows()).toEqual(originalRows)
  })

  it('retains a harmless snapshot but refuses adoption if another connection wins after preservation', async () => {
    await persistConversationCheckpoint(makeCheckpoint('original'))
    const originalRows = await readRawRows()
    const scope = await beginCheckpointWriteScope(conversationId)
    const database = getCheckpointDatabase()
    const executeRun = database.run.bind(database)
    vi.spyOn(database, 'run').mockImplementation(async (sql, parameters) => {
      const result = await executeRun(sql, parameters)
      if (sql.includes('INSERT INTO conversation_checkpoint_recovery'))
        await externalCheckpoints.persistConversationCheckpoint(makeCheckpoint('winner'))
      return result
    })
    await expect(adoptConversationCheckpoint(makeCheckpoint('selected'), scope, 'user-selected')).rejects.toBeInstanceOf(CheckpointConflictError)
    expect(await getPersistedConversationCheckpoint(conversationId)).toEqual(makeCheckpoint('winner'))
    const snapshot = await database.get<{ checkpoint_rows_json: string }>(
      'SELECT checkpoint_rows_json FROM conversation_checkpoint_recovery WHERE conversation_id = ?',
      [conversationId],
    )
    expect(JSON.parse(snapshot!.checkpoint_rows_json)).toEqual(originalRows)
  })
})
