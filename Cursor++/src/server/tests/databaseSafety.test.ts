import type { AsyncDatabase } from '../database/sqlite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { closeAgentDatabase, getAgentDatabase, initDatabase, resolveAgentDatabasePath } from '../database/sqlite'

let temporaryDirectory = ''
let previousDatabasePath: string | undefined

beforeEach(async () => {
  await closeAgentDatabase()
  previousDatabasePath = process.env.BYOK_AGENT_DB_PATH
  temporaryDirectory = mkdtempSync(join(tmpdir(), 'ccursor-database-safety-'))
  process.env.BYOK_AGENT_DB_PATH = join(temporaryDirectory, 'cursor.db')
})

afterEach(async () => {
  vi.restoreAllMocks()
  await closeAgentDatabase()
  if (previousDatabasePath === undefined)
    delete process.env.BYOK_AGENT_DB_PATH
  else process.env.BYOK_AGENT_DB_PATH = previousDatabasePath
  rmSync(temporaryDirectory, { recursive: true, force: true })
})

async function readPersistedState(database: AsyncDatabase) {
  return {
    blobs: await database.all('SELECT * FROM agent_blobs ORDER BY blob_id'),
    checkpoints: await database.all('SELECT * FROM conversation_checkpoints ORDER BY conversation_id, kind'),
    summaries: await database.all('SELECT * FROM conversation_summaries ORDER BY conversation_id, kind'),
    usage: await database.all('SELECT * FROM model_usage_stats ORDER BY day, hour, provider_id, model_id'),
    blobSchema: await database.all(`
      SELECT type, name, sql FROM sqlite_master
      WHERE tbl_name = 'agent_blobs' ORDER BY type, name
    `),
  }
}

describe('database startup safety', () => {
  it('preserves legacy blobs, checkpoints, summaries, and usage during ordinary schema initialization', async () => {
    await initDatabase()
    const fixtureDatabase = getAgentDatabase()
    expect(await fixtureDatabase.get(`SELECT name FROM sqlite_master WHERE name = 'agent_blobs'`)).toBeUndefined()

    await fixtureDatabase.exec(`
      CREATE TABLE agent_blobs (
        blob_id TEXT PRIMARY KEY NOT NULL,
        blob_data TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX idx_agent_blobs_updated_at ON agent_blobs(updated_at);
      INSERT INTO agent_blobs VALUES
        ('legacy-root', 'AAECA//+AA==', 1700000000001),
        ('unreferenced-legacy-blob', 'a2VlcCB0aGlzIHRvbw==', 1700000000002);

      INSERT INTO conversation_checkpoints (
        conversation_id, kind, root_blob_ids_json, turn_blob_ids_json,
        summary_archive_ids_json, used_tokens, max_tokens, mode, updated_at
      ) VALUES
        ('preserved-conversation', 'committed', '["legacy-root"]', '["turn-blob"]',
         '["summary-archive"]', 1234, 200000, 'AGENT_MODE_AGENT', 1700000000003),
        ('preserved-conversation', 'speculative', '["legacy-root"]', '[]',
         '["speculative-archive"]', 567, 200000, 'AGENT_MODE_AGENT', 1700000000004);

      INSERT INTO conversation_summaries (
        conversation_id, kind, truncation_bubble_id_inclusive, resume_bubble_id_inclusive,
        previous_summary_bubble_id, summary_text, includes_tool_results, strategy, updated_at
      ) VALUES (
        'preserved-conversation', 'primary', 'boundary-bubble', 'resume-bubble',
        'previous-summary', 'Retain this conversation summary verbatim.',
        1, 'plain_text_summary', 1700000000005
      );

      INSERT INTO model_usage_stats (
        day, hour, provider_id, provider_name, model_id, api_model, request_count,
        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, last_used_at
      ) VALUES (
        '2026-09-08', 10, 'preserved-provider', 'Preserved Provider', 'preserved-model',
        'preserved-api-model', 7, 1001, 202, 303, 404, 1700000000006
      );
    `)
    const persistedBeforeRestart = await readPersistedState(fixtureDatabase)

    await closeAgentDatabase()
    await initDatabase()

    expect(await readPersistedState(getAgentDatabase())).toEqual(persistedBeforeRestart)
  })

  it('does not publish a failed schema initialization and retries the same database after repair', async () => {
    await initDatabase()
    const fixtureDatabase = getAgentDatabase()
    await fixtureDatabase.exec(`
      DROP TABLE conversation_checkpoints;
      CREATE TABLE conversation_checkpoints (conversation_id TEXT PRIMARY KEY);
    `)

    // Keep a separate fixture connection so the invalid schema can be repaired
    // without relying on access to a failed runtime database initialization.
    vi.resetModules()
    const retryingDatabaseModule = await import('../database/sqlite')
    try {
      await expect(retryingDatabaseModule.initDatabase()).rejects.toThrow(/no such column: updated_at/)
      expect(retryingDatabaseModule.getAgentDatabase).toThrow('Database not initialized')
      await expect(retryingDatabaseModule.initDatabase()).rejects.toThrow(/no such column: updated_at/)
      expect(retryingDatabaseModule.getAgentDatabase).toThrow('Database not initialized')

      await fixtureDatabase.exec('DROP TABLE conversation_checkpoints')
      await closeAgentDatabase()
      await retryingDatabaseModule.initDatabase()

      const repairedDatabase = retryingDatabaseModule.getAgentDatabase()
      expect(await repairedDatabase.get(`
        SELECT name FROM sqlite_master WHERE name = 'idx_conversation_checkpoints_updated_at'
      `)).toEqual({ name: 'idx_conversation_checkpoints_updated_at' })
      expect(await repairedDatabase.all('SELECT kind, turn_blob_ids_json FROM conversation_checkpoints')).toEqual([])
      await retryingDatabaseModule.initDatabase()
      expect(retryingDatabaseModule.getAgentDatabase()).toBe(repairedDatabase)
    }
    finally {
      await retryingDatabaseModule.closeAgentDatabase()
    }
  })

  it('retains the usable database when closing it fails and allows a path-switch retry', async () => {
    await initDatabase()
    const originalDatabase = getAgentDatabase()
    const closeError = new Error('simulated close failure')
    const closeDatabaseSpy = vi.spyOn(originalDatabase, 'close').mockRejectedValueOnce(closeError)
    process.env.BYOK_AGENT_DB_PATH = join(temporaryDirectory, 'replacement.db')

    await expect(initDatabase()).rejects.toBe(closeError)
    expect(getAgentDatabase()).toBe(originalDatabase)
    expect(await originalDatabase.get('SELECT 1 AS available')).toEqual({ available: 1 })

    await initDatabase()
    expect(closeDatabaseSpy).toHaveBeenCalledTimes(2)
    expect(getAgentDatabase()).not.toBe(originalDatabase)
    expect(resolveAgentDatabasePath()).toBe(join(temporaryDirectory, 'replacement.db'))
    expect(await getAgentDatabase().all('SELECT * FROM model_usage_stats')).toEqual([])
  })
})
