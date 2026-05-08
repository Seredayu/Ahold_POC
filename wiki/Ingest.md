# Ingest System

> Last updated: 2026-05-08 | Sources: [wiki/ingest.py, wiki/extract.py]

## Summary

`wiki/ingest.py` extracts text from research documents, sends them to Claude (`claude-sonnet-4-6`), and merges the response into wiki pages. It tracks processed files via a content-hash manifest so unchanged files are never re-processed.

## Entry Points

| Command | Behaviour |
|---|---|
| `ingest.py --check` | Scan all `research/**` for new/changed files, ingest each |
| `ingest.py <filepath>` | Ingest one specific file (used by watcher) |

## API Key

Loaded from `wiki/.env` via `python-dotenv`. File must contain:

```
ANTHROPIC_API_KEY=sk-ant-...
```

`wiki/.env` is in `.gitignore` — never committed.

## Processing Flow

```
ingest_file(filepath)
  ├─ is_changed()? → skip if hash matches manifest
  ├─ image (.png/.jpg/.jpeg) → _ingest_image() — base64 vision API call
  └─ text file → extract_text() via extract.py
       ↓
  _call_api_and_write()
       ↓
  Claude API (claude-sonnet-4-6, max_tokens=8096)
  System: merge into wiki, return JSON only
  User: source text + all current wiki page contents
       ↓
  _process_response()
  ├─ parse JSON → [{name, full_markdown_content}, ...]
  ├─ write each page to wiki/<name>.md
  └─ update manifest (hash + timestamp + pages list)
```

## Manifest

`wiki/.manifest.json` — tracks every processed file:

```json
{
  "c:\\path\\to\\file.md": {
    "hash": "37dd4d24b8...",
    "timestamp": "2026-05-08T11:31:21.267351",
    "pages": ["Architecture-Overview", "Data-Integration", ...]
  }
}
```

- Hash = SHA256 of raw file bytes
- If API call fails, manifest is NOT updated → automatic retry on next run

## Supported File Types

| Extensions | Handler |
|---|---|
| `.md`, `.txt` | Direct UTF-8 read |
| `.pdf` | `pdfplumber` |
| `.docx` | `python-docx` |
| `.pptx` | `python-pptx` |
| `.xlsx` | `openpyxl` (read-only + data-only) |
| `.png`, `.jpg`, `.jpeg` | Base64 vision encoding (Claude multimodal) |

## Wiki Pages

Pages the API may write to (controlled by `WIKI_PAGES` list and system prompt):

`Architecture-Overview`, `Data-Integration`, `ML-Models`, `Replenishment-Engine`, `SAP-Integration`, `Frontend`, `Infrastructure`, `Watcher`, `Index`

Only pages that need updating are returned in the API response.

## Error Handling

| Failure | Behaviour |
|---|---|
| Extract fails | Log to `wiki/.errors.log`, skip file, manifest not updated |
| API call fails | Log to `wiki/.errors.log`, manifest not updated → retry next run |
| Bad JSON response | Log `"Bad API response: ..."`, skip |

`wiki/.errors.log` is append-only, non-blocking — never halts the watcher or hook.

## Merge Strategy

System prompt instructs Claude to:
> "Merge new knowledge into existing pages. Preserve prior content; add, correct, cross-reference as needed."

All existing page contents are sent with every API call so Claude has full context for cross-referencing.

## Dependencies

Defined in `wiki/pyproject.toml`, isolated in `wiki/.venv` via `uv`:

```
anthropic>=0.40.0
python-dotenv>=1.0.0
pdfplumber>=0.11.0
python-docx>=1.1.0
python-pptx>=1.0.0
openpyxl>=3.1.0
watchdog>=4.0.0
```

## Related

[[Index]] [[Watcher]] [[Architecture-Overview]]
