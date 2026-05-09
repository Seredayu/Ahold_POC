# Phase 4B: FastAPI Backend + React Field App — Design Spec

**Date:** 2026-05-09  
**Phase:** 4B (Weeks 14–15)  
**Status:** Implemented ✅

---

## 1. Goal

Provide store managers at Albert Heijn NL with a mobile-friendly web UI to review ESCALATE exceptions between 06:15–08:10 AM. Decisions (approve/reject with optional quantity override) write back to `gold.replenishment.exception_queue` before `sweeper_finalize` runs at 08:10 AM.

---

## 2. Context

At 06:15 AM, `sweeper_load_exceptions` writes ~10% of replenishment recommendations to `gold.replenishment.exception_queue` with `sweeper_action = 'ESCALATE'`. These are cases where the MILP solver's recommended quantity has high uncertainty or large deviation from the previous order. Managers have until 08:10 AM to act — 1h55m review window.

If no decision is made, `sweeper_finalize` treats the exception as auto-rejected (no PO created for that SKU/store).

---

## 3. Architecture

```
React SPA (Azure Static Web Apps)
    ↓ polls every 15s via HTTPS
FastAPI (Azure Container Apps, port 8000)
    ↓ Databricks SQL connector
gold.replenishment.exception_queue (Delta Lake)
    ↑ written by sweeper_load_exceptions (06:15 AM)
    ↓ read by sweeper_finalize (08:10 AM)
```

**Auth:** Azure AD SSO via Azure Static Web Apps built-in auth (`/.auth/login/aad`). POC uses `X-Manager-Id` request header; production replaces with `X-MS-CLIENT-PRINCIPAL` token claim.

---

## 4. Data Model

### `gold.replenishment.exception_queue` (Delta table)

| Column | Type | Notes |
|--------|------|-------|
| `werks` | STRING | SAP plant/store code |
| `unified_sku_id` | STRING | Rosetta Stone unified SKU |
| `recommended_qty` | INT | MILP solver output |
| `transit_to_life_ratio` | FLOAT | Delivery transit days / shelf life days |
| `quantity_deviation_pct` | FLOAT | Deviation from previous order |
| `sweeper_action` | STRING | `ESCALATE` or `AUTO_APPROVE` |
| `exception_type` | STRING | Reason for escalation |
| `manager_decision` | STRING | `APPROVED`, `REJECTED`, or NULL |
| `manager_id` | STRING | Manager identifier |
| `decision_timestamp` | STRING | ISO 8601 UTC |
| `override_reason` | STRING | Manager's note |
| `override_qty` | INT | Manager's quantity override |
| `shap_values` | STRING | JSON: feature → SHAP value dict |
| `_loaded_at` | STRING | Partition key, ISO compact UTC |

### Exception ID format

`{werks}|{unified_sku_id}|{loaded_at}` — `|` delimiter safe for SKU IDs containing underscores.

---

## 5. API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Returns `{"status": "ok", "timestamp": "..."}` |
| GET | `/exceptions/` | List exceptions, filter by `status` + `store_id`, sorted by `quantity_deviation_pct` DESC |
| GET | `/exceptions/{id}` | Single exception by composite ID |
| POST | `/exceptions/{id}/approve` | Write APPROVED decision + optional override |
| POST | `/exceptions/{id}/reject` | Write REJECTED decision |

**Status mapping:**
- DB `NULL` → API `"PENDING"`
- DB `APPROVED` → API `"APPROVED"`
- DB `REJECTED` → API `"BLOCKED"`

---

## 6. Frontend Behaviour

- **Polling:** `setInterval` every `VITE_POLLING_INTERVAL_MS` (default 15s). Initial load sorts by `quantity_deviation_pct` DESC. Subsequent polls append only new IDs — avoids re-sorting active review rows.
- **Row expansion:** Click row → `ShapWaterfall` expands below with SHAP feature bars.
- **Override inputs:** Number input (qty) + textarea (reason) in expanded row.
- **Optimistic update:** Row removed from list immediately on approve/reject; error banner on failure.
- **Error banner:** `role="alert"` div shown when any API call fails.
- **Mobile layout:** `@media (max-width: 640px)` stacks table rows; buttons `min-height: 44px`.

---

## 7. Security

- **CORS:** `ALLOW_ORIGINS` env var (default `http://localhost:5173`; production `*.azurestaticapps.net`). Methods: GET, POST only. Headers: Content-Type, X-Manager-Id.
- **AAD:** `staticwebapp.config.json` requires `authenticated` role for `/api/*`. 401 → redirect to `/login` → `/.auth/login/aad`.
- **Secrets:** Databricks credentials in ACA built-in secret store (secretRef). Not in environment variables directly.
- **Non-root container:** `appuser` in Dockerfile — required by Azure Container Apps security policy.

---

## 8. Key Implementation Decisions

| Decision | Rationale |
|----------|-----------|
| `loaded_at` (not `_loaded_at`) in Pydantic schema | Pydantic v2 treats leading-underscore fields as private — silently drops them. SQL still uses `_loaded_at`. |
| `get_databricks_connection()` returns `conn` | Each handler calls `conn.cursor()` and closes both in `finally`. Prevents connection pool exhaustion. |
| Single clock for `decision_timestamp` | `decision_ts = datetime.utcnow()` computed before UPDATE, passed as SQL param. DB value and HTTP response identical. |
| Polling in `App.tsx` not `ExceptionQueue.tsx` | Keeps fetch + decision logic co-located. `ExceptionQueue` is pure display. |
| `_parse_exception_id()` helper | Returns HTTP 422 on malformed IDs instead of unhandled ValueError → 500. |
