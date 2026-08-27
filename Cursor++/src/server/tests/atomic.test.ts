import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { readJsonOrNull, replaceFile, writeJsonAtomic } from '../config/atomic'

describe('atomic json writes', () => {
  const dirs: string[] = []

  afterEach(() => {
    for (const dir of dirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  function scratch(): string {
    const dir = mkdtempSync(join(tmpdir(), 'ccursor-atomic-'))
    dirs.push(dir)
    return dir
  }

  it('overwrites an existing file on a second write (Windows rename cannot replace dest)', () => {
    const path = join(scratch(), 'providers.json')
    writeJsonAtomic(path, { n: 1 })
    writeJsonAtomic(path, { n: 2 })
    expect(readJsonOrNull<{ n: number }>(path)).toEqual({ n: 2 })
  })

  it('replaceFile leaves dest with source contents and removes the source', () => {
    const dir = scratch()
    const from = join(dir, 'from.json')
    const to = join(dir, 'to.json')
    writeJsonAtomic(from, { from: true })
    writeJsonAtomic(to, { to: true })
    replaceFile(from, to)
    expect(readJsonOrNull(to)).toEqual({ from: true })
    expect(existsSync(from)).toBe(false)
    expect(readFileSync(to, 'utf-8')).toContain('"from": true')
  })
})
