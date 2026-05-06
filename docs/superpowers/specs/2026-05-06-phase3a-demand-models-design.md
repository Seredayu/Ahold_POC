# Phase 3A — Demand Models (M1/M2/M4) Design

**Phase:** 3A — Weeks 9–10  
**Builds on:** Phase 2A Feature Store (`feature_store.stock`, `feature_store.velocity`, `feature_store.phantom`), Phase 1 Gold tables (`silver.sap.enriched_movements`, `silver.master.unified_sku_registry`)  
**Consumed by:** Phase 3B Freshness Orchestrator (MILP solver)

---

## Goal

Produce daily per-SKU/site probabilistic demand forecasts (P10/P50/P90) for all 50 Albert Heijn pilot stores. Three chained models: M1 baseline (LightGBM + Prophet), M2 short-horizon correction (promo + weather lift), M4 uncertainty quantification (quantile regression). Outputs land in the Databricks Feature Engineering in Unity Catalog (FEU) for consumption by the MILP solver and the React ShapWaterfall explainability component.

---

## Architecture

### M1 — LightGBM Baseline Demand Sensing

**Training data:** 104 weeks (2 years) of `silver.sap.enriched_movements` filtered to `BWART = '601'` (goods issues = sales), grouped by `(WERKS, unified_sku_id, BUDAT)` to produce daily demand.

**Feature engineering:**
- Prophet seasonal decomposition on the 104-week series per SKU/site: extracts `trend`, `weekly`, `yearly` components as numeric features
- Calendar features: `day_of_week` (0–6), `week_of_year` (1–53), `is_public_holiday` (NL calendar)
- Stock context from Phase 2A: `stock_qty`, `days_of_cover`

**Model:** `lightgbm.LGBMRegressor` with `objective='regression'`, `n_estimators=500`, `num_leaves=63`. Trained per horizon: 7d, 14d, 28d forecasts as separate targets (multi-output via three model instances).

**Output table:** `feature_store.demand.m1_forecast`

| Column | Type | Description |
|---|---|---|
| werks | string | SAP plant code |
| unified_sku_id | string | Rosetta Stone SKU ID |
| forecast_7d | double | 7-day demand forecast (units) |
| forecast_14d | double | 14-day demand forecast |
| forecast_28d | double | 28-day demand forecast |
| _computed_at | timestamp | FEU write timestamp |

**Primary keys:** `werks`, `unified_sku_id`  
**MLflow registration:** `demand_m1` → Production alias after validation (MAE vs. naive baseline)

---

### M2 — Short-Horizon Correction (Promo + Weather Lift)

**Purpose:** Multiplicative correction of M1 forecasts for near-term signals not visible in historical movement patterns.

**Promo lift:**
- Source: `silver.promo.calendar` — populated from SAP SD condition records (promotion type, start/end date, affected SKUs/sites)
- Lift coefficient per `(werks, unified_sku_id, date)`: derived from historical uplift ratio during past promotions vs. baseline M1 forecast
- Stored in `silver.promo.lift_coefficients` table, updated weekly at training time

**Weather lift:**
- Source: Open-Meteo API (free tier, no authentication required) — daily temperature (°C) and precipitation (mm) for Amsterdam (52.37°N, 4.90°E) covering the 28-day forecast window
- Ingested daily to `bronze.weather.daily_forecast` by the `ingest_weather` DABs task
- Lift coefficient: LGBMRegressor trained on historical (weather delta from seasonal norm) → (demand delta from M1 baseline) per category; Fresh Produce more weather-sensitive than Bakery
- Stored in MLflow as `demand_m2_weather_lift`

**Formula:** `m2_corrected = m1_forecast × promo_lift × weather_lift`

Lift factors stored alongside the corrected forecast so the React ShapWaterfall component can attribute them per exception.

**Output table:** `feature_store.demand.m2_corrected`

| Column | Type | Description |
|---|---|---|
| werks | string | |
| unified_sku_id | string | |
| corrected_7d | double | M2-adjusted 7-day forecast |
| corrected_14d | double | M2-adjusted 14-day forecast |
| corrected_28d | double | M2-adjusted 28-day forecast |
| promo_lift | double | Promo multiplier applied |
| weather_lift | double | Weather multiplier applied |
| _computed_at | timestamp | |

**Primary keys:** `werks`, `unified_sku_id`  
**MLflow registration:** `demand_m2` (weather lift sub-model), `demand_m2_promo` (promo lift sub-model)

---

### M4 — Probabilistic P10/P50/P90 (Uncertainty Quantification)

**Purpose:** Convert M2 point forecast into a probability distribution so the MILP solver can optimise against demand uncertainty.

**Method:** LightGBM quantile regression — three separate `LGBMRegressor` instances with `objective='quantile'` at `alpha = 0.1, 0.5, 0.9`. Trained on M1 residuals (actual − M1 forecast) over 104 weeks; M2 corrected forecast used as the base; residual distribution calibrated per SKU category.

**Inputs:** `feature_store.demand.m2_corrected` + 104-week M1 residual history

**Output table:** `feature_store.demand.m4_probabilistic`

| Column | Type | Description |
|---|---|---|
| werks | string | |
| unified_sku_id | string | |
| p10 | double | 10th percentile demand (pessimistic) |
| p50 | double | 50th percentile demand (central estimate) |
| p90 | double | 90th percentile demand (optimistic) |
| uncertainty_spread | double | P90 − P10 (absolute spread) |
| _computed_at | timestamp | |

**Primary keys:** `werks`, `unified_sku_id`  
**MLflow registration:** `demand_m4`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/engines/demand/prophet_features.py` | Create | Prophet seasonal decomposition → feature DataFrame |
| `src/engines/demand/m1_model.py` | Create | LightGBM baseline; `train()`, `score()`, `_entry_train_m1()`, `_entry_score_m1()` |
| `src/engines/demand/weather_ingest.py` | Create | Open-Meteo API → `bronze.weather.daily_forecast`; `_entry_ingest_weather()` |
| `src/engines/demand/m2_model.py` | Create | Promo + weather lift; `train()`, `score()`, `_entry_train_m2()`, `_entry_score_m2()` |
| `src/engines/demand/m4_model.py` | Create | Quantile regression P10/P50/P90; `train()`, `score()`, `_entry_train_m3()`, `_entry_score_m4()` |
| `tests/unit/test_demand_models.py` | Create | 6 unit tests (see Testing section) |
| `resources/jobs/demand_forecast_train.yml` | Create | Weekly Sunday 01:00 AM training job |
| `setup.py` | Modify | Add 6 `demand_*` console script entry points |

---

## DABs Jobs

### `demand_forecast_train` (weekly, Sunday 01:00 AM Amsterdam)

```
train_m1 → train_m2 → train_m4
```

Runs before `phantom_stock_train` (03:00 AM). Weather lift sub-model retraining is part of `train_m2`.

### Morning Pipeline Integration

`demand_forecast_score` is a task group within `morning_pipeline.yml` (see Phase 3B spec for full pipeline definition):

```
feature_store_refresh
├── phantom_stock_score      (parallel)
└── demand_forecast_score    (parallel)
    ├── ingest_weather
    ├── score_m1  (depends_on: ingest_weather)
    ├── score_m2  (depends_on: score_m1)
    └── score_m4  (depends_on: score_m2)
        └── freshness_score  (Phase 3B, depends_on: score_m4)
```

---

## Testing

6 unit tests in `tests/unit/test_demand_models.py`:

1. `test_prophet_features_produces_trend_weekly_yearly` — assert three columns present and non-null for a 104-row synthetic series
2. `test_m1_forecast_increases_with_promo_week` — inject a promo week row; assert forecast_7d > baseline
3. `test_m2_weather_lift_above_one_for_heat_wave` — temperature delta +10°C → weather_lift > 1.0
4. `test_m2_promo_lift_above_one_for_active_promo` — active promo flag → promo_lift > 1.0
5. `test_m4_p90_greater_than_p50_greater_than_p10` — invariant check on quantile ordering
6. `test_m4_uncertainty_spread_equals_p90_minus_p10` — computed column correctness

---

## Error Handling

- **Open-Meteo API unavailable:** `ingest_weather` raises `WeatherIngestError`; morning pipeline task fails; `demand_forecast_score` is blocked (no stale weather data silently used)
- **M1 training convergence failure:** MLflow run logged with `status=FAILED`; Production alias not updated; previous model version remains active
- **Insufficient history (< 52 weeks for a SKU):** SKU excluded from M1 training; falls back to category-median forecast; flagged in `gold.demand.forecast_quality` table

---

## Go/No-Go Gate (Week 10)

M1 MAE on held-out 4-week test set must be ≤ 15% of mean daily demand per SKU. Measured via MLflow metric `m1_mae_pct` logged at training time. Gate evaluated before Phase 3B implementation begins.
