# Ahold Delhaize Freshness Sprint POC — Wiki Index

> Last updated: 2026-05-08

## Project Summary

The **Ahold Delhaize Freshness Sprint POC** is an AI-driven inventory optimization system targeting **€350M+ annual EBITDA improvement**. The POC scope is Fresh Produce & Bakery at Albert Heijn NL, 50 pilot stores, with a **90% No-Touch ordering** goal and a hard **08:15 AM EDI 850 release** deadline.

## Wiki Pages

| Page | Description |
|------|-------------|
| [[Architecture-Overview]] | 8-Layer AI Inventory Optimization Architecture, system summary, KPIs, daily timeline |
| [[Data-Integration]] | Three ingestion streams (SAP ECC, Symphony Gold, S/4HANA), Medallion layers, Rosetta Stone |
| [[ML-Models]] | Five ML engines (Demand, Waste, Pricing, Replenishment, Supplier), Feature Store, MLOps |
| [[Replenishment-Engine]] | Three autonomous engines (Phantom Stock, Freshness MILP, Sweeper), EDI 850 delivery |
| [[SAP-Integration]] | Clean Core mandate, BAPIs, SAP BTP AI Core, EDI, pre-sprint dependencies |
| [[Frontend]] | React Field App, SHAP Waterfall, Power BI, SAP Analytics Cloud |
| [[Infrastructure]] | Azure + Databricks platform, security, DevOps, implementation roadmap |
| [[Watcher]] | `watcher.py` file system daemon for automatic wiki ingestion |
| [[Ingest]] | `ingest.py` pipeline — extract, Claude API call, manifest tracking |
| [[Extract]] | `extract.py` — document-to-text conversion for ingest pipeline |

## Research Folder Structure

| Folder | Purpose |
|--------|---------|
| `sources/` | Raw input docs — SAP specs, Databricks docs, Ahold process docs, PDFs |
| `notebooklm-exports/` | Summaries, FAQs, and notes exported from NotebookLM sessions |
| `architecture-decisions/` | ADRs (Architecture Decision Records) — why we chose X over Y |
| `meeting-notes/` | Stakeholder meetings, SAP Basis discussions, supplier EDI calls |
| `vendor-docs/` | Vendor-provided specs — EDI partner guides, SAP BAPI references, Databricks accelerator docs |

## Research Workflow

1. Upload source docs to **NotebookLM** → ask questions, generate summaries
2. Export summaries as markdown → save to `research/notebooklm-exports/`
3. Reference in Claude Code with `@research/notebooklm-exports/filename.md`
4. Claude Code turns research into implementation plans and code
5. `watcher.py` detects new/changed files and auto-triggers wiki ingestion

## Naming Conventions

| Location | Pattern |
|----------|---------|
| `notebooklm-exports/` | `YYYY-MM-DD_topic.md` |
| `architecture-decisions/` | `ADR-001_topic.md` |
| `meeting-notes/` | `YYYY-MM-DD_meeting-topic.md` |

## Critical Deadlines & Constraints

- **08:15 AM CET**: Hard EDI 850 release deadline (daily)
- **SAP Basis RFC access**: 3-week lead time — must initiate before sprint Week 1
- **EDI supplier partner agreement**: 4–6 week lead time — must initiate in Week 1
- **Rosetta Stone match rate**: ≥ 95% required before model training begins

## Related

[[Architecture-Overview]] [[Data-Integration]] [[ML-Models]] [[Replenishment-Engine]] [[SAP-Integration]] [[Frontend]] [[Infrastructure]] [[Watcher]] [[Ingest]] [[Extract]]
