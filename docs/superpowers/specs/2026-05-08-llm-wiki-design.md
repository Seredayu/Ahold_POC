# LLM Wiki — Design Spec
*2026-05-08*

## Context

The Ahold POC accumulates research in `research/` (architecture docs, meeting notes, vendor docs, ADRs). There is no structured way to query or onboard from this material. This spec describes a Karpathy-pattern LLM wiki: sources are automatically ingested into a set of Obsidian-compatible markdown topic pages using the Claude API. The wiki compounds over time as new sources arrive.

## Architecture

```
research/                    ← sources drop here (watched)
  sources/
  meeting-notes/
  vendor-docs/
  architecture-decisions/
  notebooklm-exports/

wiki/                        ← Obsidian vault subfolder
  Index.md                   ← master topic index + backlinks
  Architecture-Overview.md
  Data-Integration.md
  ML-Models.md
  Replenishment-Engine.md
  SAP-Integration.md
  Frontend.md
  Infrastructure.md
  .manifest.json             ← {filepath: {hash, timestamp, pages_updated[]}}
  .errors.log                ← failed extractions (non-blocking)

wiki/ingest.py               ← main script: extract → Claude API → write pages
wiki/watcher.py              ← watchdog file watcher (calls ingest.py on change)
wiki/extract.py              ← file type handlers
wiki/requirements.txt        ← python deps
```

## Ingest Logic (`ingest.py`)

Per source file:

1. Hash file → check `.manifest.json`. Unchanged hash → skip.
2. Extract text via `extract.py`:
   - `.md` / `.txt` → read direct
   - `.pdf` → `pdfplumber`
   - `.docx` → `python-docx`
   - `.pptx` → `python-pptx`
   - `.xlsx` → `openpyxl` (sheet names + cell values)
   - `.png` / `.jpg` → Claude vision (describe + extract text)
3. Call Claude API (`claude-sonnet-4-6`):
   - System: "You maintain a wiki for the Ahold Delhaize Freshness Sprint POC. Given a source document and the current content of relevant wiki pages, return JSON: `{pages: [{name, full_markdown_content}]}` merging new knowledge into existing pages. Preserve prior content; add, correct, or cross-reference as needed."
   - User: `<extracted source text>\n\n<existing page contents for pages likely to be affected>`
4. Write each returned page to `wiki/<name>.md`. Update `Index.md`.
5. Write manifest entry `{hash, timestamp, pages: [...]}`.

**Error handling:** failed extract → append to `.errors.log`, skip (don't block watcher). API failure → don't update manifest so retry runs on next trigger.

## Wiki Page Format

```markdown
# <Topic>

> Last updated: YYYY-MM-DD | Sources: [file1.md, file2.pdf]

## Summary
...

## Key Points
- ...

## Related
[[Architecture-Overview]] [[SAP-Integration]]
```

## Automation

**File watcher (`watcher.py`):**
- `watchdog` library watches `research/` recursively
- Debounce 3s to avoid partial-write triggers
- Fires on `created` + `modified` events → calls `ingest.py <filepath>`
- User starts once per work session: `python wiki/watcher.py &`

**SessionStart hook** (added to `~/.claude/settings.json`):
```json
{
  "type": "command",
  "command": "python C:/Projects/Ahold_POC/wiki/ingest.py --check",
  "timeout": 60,
  "statusMessage": "Syncing wiki..."
}
```
`--check` mode scans all `research/**` files and ingests any absent from manifest or with changed hash. Silent if nothing new.

## Dependencies

`wiki/requirements.txt`:
```
anthropic
watchdog
pdfplumber
python-docx
python-pptx
openpyxl
```

`ANTHROPIC_API_KEY` read from environment. Never hardcoded.

## Verification

1. Drop a markdown file into `research/meeting-notes/` → confirm watcher fires → confirm page appears in `wiki/`
2. Open new Claude Code session → confirm SessionStart hook runs `ingest.py --check` silently
3. Drop same file again (unchanged) → confirm manifest hash check skips re-ingest
4. Drop a PDF → confirm `pdfplumber` extraction + wiki page created
5. Open Obsidian → confirm `[[wikilinks]]` resolve between pages
