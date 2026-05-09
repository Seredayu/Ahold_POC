# Watcher

> Last updated: 2026-05-08 | Sources: [wiki/watcher.py]

## Summary

`wiki/watcher.py` is a file system daemon that watches `research/` for new or modified files and automatically triggers wiki ingestion. Run it manually during active research sessions to keep wiki pages in sync without restarting Claude Code.

## How to Run

```bash
uv run --project C:/Projects/Ahold_POC/wiki python C:/Projects/Ahold_POC/wiki/watcher.py
```

Ctrl-C to stop. Logs each ingestion to stdout.

## Watched Directory

`research/` (recursive) — all subdirectories:
- `notebooklm-exports/`
- `sources/`
- `architecture-decisions/`
- `meeting-notes/`
- `vendor-docs/`

## Supported File Types

Mirrors `ingest.ALL_SUPPORTED_EXTENSIONS`: `.md`, `.txt`, `.pdf`, `.docx`, `.pptx`, `.xlsx`, `.png`, `.jpg`, `.jpeg`

## Debounce Logic

- Window: **3 seconds** idle before ingest fires
- Mechanism: in-memory dict `{path → timestamp}`, flushed every 1 second
- Prevents multiple rapid saves from triggering redundant API calls

## Retry on Failure

If `ingest.py` returns non-zero exit code, the file is re-enqueued with a fresh timestamp — retried after another 3-second idle window.

## Subprocess Isolation

Watcher runs in the main Python env; ingest runs via `uv run --project wiki/` to ensure wiki venv (with `anthropic`, `pdfplumber`, etc.) is used:

```python
subprocess.run(
    ['uv', 'run', '--project', str(WIKI_DIR), 'python', str(INGEST_SCRIPT), path],
    check=False,
)
```

## Relation to SessionStart Hook

| Trigger | Mechanism | When |
|---|---|---|
| Session start | `~/.claude/settings.json` SessionStart hook | Each new Claude Code session |
| Runtime file change | `watcher.py` daemon | Continuous, while watcher is running |

The hook runs `--check` (scans all research files for changes); the watcher passes individual file paths directly.

## Related

[[Index]] [[Architecture-Overview]] [[Data-Integration]]
