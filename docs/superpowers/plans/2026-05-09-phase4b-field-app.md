# Phase 4B: FastAPI Backend + React Field App — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the human-in-the-loop exception review layer — FastAPI backend reading `gold.replenishment.exception_queue` via Databricks SQL connector, React field app for store managers to approve/reject ESCALATE exceptions between 06:15–08:10 AM.

**Architecture:** FastAPI on Azure Container Apps connects to Databricks SQL warehouse via `databricks-sql-connector`. React SPA on Azure Static Web Apps polls the API every 15s. Manager decisions write back to `gold.replenishment.exception_queue` before `sweeper_finalize` runs at 08:10 AM. Azure AD SSO via Static Web Apps built-in auth.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2, `databricks-sql-connector>=2.9.0`, React 18 + TypeScript + Vite, Vitest + Testing Library, Docker (non-root), Azure Container Apps, Azure Static Web Apps

**Status:** ✅ Implemented on `feature/phase4b-field-app` (merged to master 2026-05-09)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/medallion/gold/exception_queue.py` | Create | `ExceptionQueueSchema` + `build_exception_id()` |
| `src/medallion/gold/__init__.py` | Modify | Export schema + function |
| `src/api/routers/exceptions.py` | Rewrite | 4 endpoints + Databricks SQL implementation |
| `src/api/main.py` | Modify | Health endpoint + CORS with `ALLOW_ORIGINS` env var |
| `requirements.txt` | Modify | Add `databricks-sql-connector>=2.9.0`, `httpx>=0.26.0` |
| `frontend/src/App.tsx` | Rewrite | Polling, `X-Manager-Id` header, error banner |
| `frontend/src/components/ExceptionQueue.tsx` | Rewrite | Override inputs, status badges, mobile layout |
| `frontend/src/components/ExceptionQueue.css` | Create | Responsive styles extracted from inline |
| `frontend/src/components/ShapWaterfall.tsx` | Modify | Empty state guard, integer units, `data-testid` |
| `frontend/src/components/__tests__/ShapWaterfall.test.tsx` | Create | 6 Vitest tests |
| `frontend/src/test-setup.ts` | Create | `@testing-library/jest-dom` import |
| `frontend/.env.example` | Create | `VITE_API_BASE`, `VITE_POLLING_INTERVAL_MS`, `VITE_MANAGER_ID` |
| `frontend/package.json` | Modify | Add Vitest + Testing Library |
| `frontend/vite.config.ts` | Modify | Add `test` block |
| `Dockerfile` | Create | `python:3.11-slim`, non-root `appuser` |
| `.dockerignore` | Create | Exclude secrets, tests, worktrees |
| `azure-container-apps.yml` | Create | ACA manifest, stable API `2024-03-01`, health probes |
| `staticwebapp.config.json` | Create | AAD SSO, security headers, nav fallback |
| `tests/unit/test_exceptions_router.py` | Create | 16+ unit tests with mocked Databricks |
| `tests/integration/test_exception_workflow.py` | Create | 7 smoke tests via TestClient |

---

## Task 1 — Gold Layer Schema + FastAPI Databricks Integration

**Scope:** Backend only.

- [ ] Create `src/medallion/gold/exception_queue.py`:
  - `ExceptionQueueSchema` — Pydantic v2 model. Use `loaded_at: str` (NOT `_loaded_at` — Pydantic v2 treats leading underscore as private)
  - `build_exception_id(werks, unified_sku_id, loaded_at) -> str` — returns `f"{werks}|{unified_sku_id}|{loaded_at}"` (pipe delimiter, safe for SKU IDs with underscores)
  
- [ ] Implement `src/api/routers/exceptions.py`:
  - `get_databricks_connection()` — lazy `import databricks.sql`, reads `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` env vars, raises `RuntimeError` if missing, returns `conn` (not cursor)
  - `_parse_exception_id(exception_id)` — splits on `|` maxsplit=2, raises `HTTPException(422)` if not 3 parts
  - `_decision_to_status()` — `None→"PENDING"`, `"APPROVED"→"APPROVED"`, `"REJECTED"→"BLOCKED"`
  - All 4 handlers: `conn = get_databricks_connection(); cursor = conn.cursor(); try: ... finally: cursor.close(); conn.close()`
  - `list_exceptions`: translate `status="BLOCKED"` → `db_status="REJECTED"` before SQL; `COALESCE(manager_decision,'PENDING') = ?`; ORDER BY `quantity_deviation_pct DESC`
  - `approve_exception` / `reject_exception`: compute `decision_ts = datetime.utcnow().strftime(...)` before UPDATE; pass as SQL param (single clock for DB + response)

- [ ] Update `src/api/main.py`:
  - `GET /health` → `{"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}`
  - CORS: `ALLOW_ORIGINS = os.environ.get("ALLOW_ORIGINS", "http://localhost:5173").split(",")`, methods=GET/POST, headers=Content-Type+X-Manager-Id

- [ ] Create `tests/unit/test_exceptions_router.py` — 16+ tests, mock `get_databricks_connection`

- [ ] Commit

---

## Task 2 — React Field App

**Scope:** Frontend only.

- [ ] `frontend/src/App.tsx`:
  - `MANAGER_ID = import.meta.env.VITE_MANAGER_ID ?? "dev-manager-01"`
  - `loadExceptions(isInitial)` — initial call sorts by `quantity_deviation_pct` desc; subsequent polls append only new IDs
  - `setInterval` every `parseInt(VITE_POLLING_INTERVAL_MS) || 15000` with `clearInterval` on unmount
  - `approve(id, overrideQty?, note?)` and `reject(id, note?)` — POST with `X-Manager-Id` header; catch errors → `setError`
  - Error banner: `{error && <div role="alert">...</div>}`

- [ ] `frontend/src/components/ExceptionQueue.tsx`:
  - Override inputs: `valueAsNumber + Number.isFinite()` guard on qty
  - Status badge column
  - `import "./ExceptionQueue.css"` (CSS extracted from inline)
  - `useEffect([exceptions])` cleans stale Map keys on row removal

- [ ] `frontend/src/components/ExceptionQueue.css` — responsive styles, 44px button min-height, mobile stacking

- [ ] `frontend/src/components/ShapWaterfall.tsx` — empty state guard, `Math.round(predictedValue)`, `data-testid="shap-bar"`

- [ ] `frontend/src/components/__tests__/ShapWaterfall.test.tsx` — 6 tests: title, bar count, green class, red class, empty state, units

- [ ] Commit

---

## Task 3 — Deployment Config + Integration Tests

**Scope:** Infra files + smoke tests.

- [ ] `Dockerfile` — `python:3.11-slim`, `adduser --system --no-create-home appuser`, `USER appuser`

- [ ] `azure-container-apps.yml` — API version `2024-03-01`, 3 secrets via secretRef, liveness+readiness probes on `/health`, min=1 max=3

- [ ] `staticwebapp.config.json` — AAD SSO, `/api/*` requires authenticated, 401→/login, 5 security headers

- [ ] `tests/integration/test_exception_workflow.py` — 7 smoke tests: list→approve→verify APPROVED→reject→verify BLOCKED→health→approve without X-Manager-Id

- [ ] Commit

---

## Key Bugs Fixed During Implementation

| Bug | Fix |
|-----|-----|
| Pydantic v2 drops `_loaded_at` | Renamed to `loaded_at` in schema; SQL keeps `_loaded_at` |
| `_` delimiter breaks reverse-parse on SKU IDs | Changed to `\|` in `build_exception_id` |
| `get_databricks_connection()` returned cursor | Fixed to return `conn`; handlers call `conn.cursor()` |
| 500 on missing exception | Added `if row is None: raise HTTPException(404)` |
| `DATABRICKS_HTTP_PATH` hardcoded | Changed to env var with RuntimeError guard |
| `status="BLOCKED"` passed to SQL | Added `db_status = "REJECTED" if status == "BLOCKED" else status` |
| Two-clock `decision_timestamp` | Unified to pre-computed `decision_ts` passed as SQL param |
| NaN on override qty input | Changed to `valueAsNumber + Number.isFinite()` guard |
| Silent approve/reject failure | Added error handling with `setError` + error banner |
| Re-sort on every poll disrupts review | Stable: initial fetch sorts, polls append new IDs only |
| Inline `<style>` in ExceptionQueue | Extracted to `ExceptionQueue.css` |
| Dockerfile runs as root | Added `adduser appuser; USER appuser` |
| ACA preview API unstable | Changed to `2024-03-01` stable |
| CORS `allow_origins=["*"]` | Changed to `ALLOW_ORIGINS` env var |

## GSTACK REVIEW REPORT

| Run | Date | Verdict |
|-----|------|---------|
| Task 1 | 2026-05-09 | ✅ Both stages passed after 1 review loop |
| Task 2 | 2026-05-09 | ✅ Both stages passed after 2 review loops |
| Task 3 | 2026-05-09 | ✅ Both stages passed after 1 review loop |
