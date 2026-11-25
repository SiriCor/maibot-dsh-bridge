import { readFile, rename, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const START = '<!-- MAIBOT_BRIDGE_RULES_START -->'
const END = '<!-- MAIBOT_BRIDGE_RULES_END -->'

/** Keep caller-authored instructions byte-for-byte outside the bridge-owned section. */
export async function ensureWorkspaceRules(cwd, rules) {
  const block = `${START}\n${rules.trim()}\n${END}`
  // AGENT.md is the requested visible rule file. DSH natively loads AGENTS.md,
  // so mirror the same owned section there instead of asking the model to read a file per task.
  for (const name of ['AGENT.md', 'AGENTS.md']) {
    const path = join(cwd, name)
    let old = ''
    try {
      const bytes = await readFile(path)
      new TextDecoder('utf-8', { fatal: true }).decode(bytes)
      old = bytes.toString('utf8')
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
    const begin = old.indexOf(START)
    const end = old.indexOf(END)
    if ((begin < 0) !== (end < 0) || (begin >= 0 && end < begin)
        || (begin >= 0 && old.indexOf(START, begin + START.length) >= 0)
        || (end >= 0 && old.indexOf(END, end + END.length) >= 0)) {
      throw new Error(`Malformed bridge instruction section: ${path}`)
    }
    const updated = begin < 0 ? `${old}${old && !old.endsWith('\n') ? '\n' : ''}${block}\n`
      : old.slice(0, begin) + block + old.slice(end + END.length)
    if (updated === old) continue
    const temporary = `${path}.${process.pid}.tmp`
    await writeFile(temporary, updated, 'utf8')
    await rename(temporary, path)
  }
}
