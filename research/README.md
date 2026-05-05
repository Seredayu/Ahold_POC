# Research

Discovery phase materials for the Ahold Delhaize Freshness Sprint POC.

## Folder Structure

| Folder | Purpose |
|--------|---------|
| `sources/` | Raw input docs — SAP specs, Databricks docs, Ahold process docs, PDFs |
| `notebooklm-exports/` | Summaries, FAQs, and notes exported from NotebookLM sessions |
| `architecture-decisions/` | ADRs (Architecture Decision Records) — why we chose X over Y |
| `meeting-notes/` | Stakeholder meetings, SAP Basis discussions, supplier EDI calls |
| `vendor-docs/` | Vendor-provided specs — EDI partner guides, SAP BAPI references, Databricks accelerator docs |

## Workflow

1. Upload source docs to **NotebookLM** → ask questions, generate summaries
2. Export summaries as markdown → save to `notebooklm-exports/`
3. Reference in Claude Code with `@research/notebooklm-exports/filename.md`
4. Claude Code turns research into implementation plans and code

## Naming Convention

- `notebooklm-exports/` → `YYYY-MM-DD_topic.md`
- `architecture-decisions/` → `ADR-001_topic.md`
- `meeting-notes/` → `YYYY-MM-DD_meeting-topic.md`
