# Extract Module

> Last updated: 2026-05-08 | Sources: [wiki/extract.py]

## Summary

`wiki/extract.py` converts research documents into plain text for the ingest pipeline. Single public function `extract_text()` dispatches by file extension. Libraries are imported lazily (inside each handler) to keep startup fast.

## API

```python
from extract import extract_text, UnsupportedFileType

text = extract_text(Path("research/sources/doc.pdf"))
```

Raises `UnsupportedFileType` for unrecognised extensions. All other exceptions propagate — callers (`ingest.py`) catch and log them.

## Handlers

| Extension | Function | Library | Notes |
|---|---|---|---|
| `.md`, `.txt` | inline | built-in | UTF-8 read, returned as-is |
| `.pdf` | `_extract_pdf()` | `pdfplumber` | Page-by-page, joined with `\n\n`; empty pages skipped |
| `.docx` | `_extract_docx()` | `python-docx` | Paragraph text, blank paragraphs skipped |
| `.pptx` | `_extract_pptx()` | `python-pptx` | All slide shapes with `.text`, joined with `\n` |
| `.xlsx` | `_extract_xlsx()` | `openpyxl` | Sheet name header + TSV rows; `read_only=True`, `data_only=True`; workbook closed in `finally` |

Images (`.png`, `.jpg`, `.jpeg`) are **not** handled here — `ingest.py` routes them directly to the Claude vision API via base64 encoding.

## Error Handling

```python
class UnsupportedFileType(Exception):
    pass
```

- Unknown extension → raises `UnsupportedFileType`
- Extract failure (corrupt file, missing dependency) → propagates exception; `ingest.py` catches, logs to `wiki/.errors.log`, skips file

## Tests

`tests/wiki/test_extract.py` — 3 tests:
- `test_extract_markdown()` — reads `tests/wiki/fixtures/sample.md`
- `test_extract_txt()` — reads `tests/wiki/fixtures/sample.txt`
- `test_unsupported_type_raises()` — confirms `.xyz` raises `UnsupportedFileType`

## Related

[[Index]] [[Ingest]] [[Watcher]]
