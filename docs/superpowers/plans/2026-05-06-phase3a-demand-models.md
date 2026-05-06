# Phase 3A — Demand Models (M1/M2/M4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three chained LightGBM demand models (M1 baseline, M2 promo+weather correction, M4 P10/P50/P90 quantile) that write daily per-SKU/site probabilistic forecasts to the Feature Store for consumption by the Phase 3B MILP solver.

**Architecture:** M1 uses Prophet seasonal decomposition as input features to three LightGBMRegressor instances (7d/14d/28d horizons). M2 applies multiplicative weather-lift (LightGBM) and promo-lift (table lookup from `silver.promo.lift_coefficients`) corrections. M4 runs three quantile regressors (α=0.1/0.5/0.9) on M2 residuals to produce P10/P50/P90. All models are registered in MLflow under the `demand_m1_*`, `demand_m2_weather_lift`, and `demand_m4_*` names; outputs land in `feature_store.demand.*` Delta tables for FEU consumption by Phase 3B.

**Tech Stack:** LightGBM, Prophet, MLflow (lightgbm flavour), PySpark, Databricks Feature Engineering in Unity Catalog (FEU), Open-Meteo free API (Amsterdam forecast), Quartz cron via DABs

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/engines/demand/__init__.py` | Create | Package init (empty) |
| `src/engines/demand/prophet_features.py` | Create | `build_prophet_features()` — pandas-only Prophet decomposition → trend/weekly/yearly |
| `src/engines/demand/m1_model.py` | Create | `M1DemandModel` + `_entry_train_m1` + `_entry_score_m1` |
| `src/engines/demand/weather_ingest.py` | Create | `WeatherIngestError` + `fetch_weather_forecast()` + `_entry_ingest_weather` |
| `src/engines/demand/m2_model.py` | Create | `M2CorrectionModel` + `_entry_train_m2` + `_entry_score_m2` |
| `src/engines/demand/m4_model.py` | Create | `M4UncertaintyModel` + `_entry_train_m4` + `_entry_score_m4` |
| `tests/unit/test_demand_models.py` | Create | 6 unit tests (see Tasks 1–5) |
| `resources/jobs/demand_forecast_train.yml` | Create | Weekly Sunday 01:00 AM training job: train_m1 → train_m2 → train_m4 |
| `setup.py` | Modify | Add 7 `demand_*` console script entry points |
| `databricks.yml` | Modify | Register `demand_forecast_train` job |

---

### Task 1: `prophet_features.py` + test #1

**Files:**
- Create: `src/engines/demand/__init__.py`
- Create: `src/engines/demand/prophet_features.py`
- Create: `tests/unit/test_demand_models.py`

- [ ] **Step 1: Create the package init**

```python
# src/engines/demand/__init__.py
```

(empty file)

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_demand_models.py
import pandas as pd
import numpy as np


def test_prophet_features_produces_trend_weekly_yearly():
    from engines.demand.prophet_features import build_prophet_features

    np.random.seed(42)
    dates = pd.date_range("2022-01-01", periods=104, freq="W")
    values = np.abs(np.random.normal(100, 10, 104))
    series = pd.DataFrame({"ds": dates, "y": values})

    result = build_prophet_features(series)

    assert "trend" in result.columns
    assert "weekly" in result.columns
    assert "yearly" in result.columns
    assert result["trend"].notna().all()
    assert result["weekly"].notna().all()
    assert result["yearly"].notna().all()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_demand_models.py::test_prophet_features_produces_trend_weekly_yearly -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engines.demand'`

- [ ] **Step 4: Write `prophet_features.py`**

```python
# src/engines/demand/prophet_features.py
import pandas as pd


def build_prophet_features(series: pd.DataFrame) -> pd.DataFrame:
    """
    Accept a two-column DataFrame: 'ds' (datetime) and 'y' (float daily/weekly demand).
    Returns the same rows with three added columns: trend, weekly, yearly.
    Raises ValueError if fewer than 14 rows (Prophet minimum).
    """
    from prophet import Prophet

    if len(series) < 14:
        raise ValueError(f"Prophet requires ≥14 rows; got {len(series)}")

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
    )
    model.fit(series[["ds", "y"]])
    forecast = model.predict(series[["ds"]])

    result = series.copy()
    result["trend"] = forecast["trend"].values
    result["weekly"] = forecast["weekly"].values
    result["yearly"] = forecast["yearly"].values
    return result
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/test_demand_models.py::test_prophet_features_produces_trend_weekly_yearly -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/engines/demand/__init__.py src/engines/demand/prophet_features.py tests/unit/test_demand_models.py
git commit -m "feat: add prophet_features.py with build_prophet_features() + test"
```

---

### Task 2: `m1_model.py` + test #2

**Files:**
- Create: `src/engines/demand/m1_model.py`
- Modify: `tests/unit/test_demand_models.py` (append test #2)

- [ ] **Step 1: Write the failing test — append to `tests/unit/test_demand_models.py`**

```python
def test_m1_forecast_increases_with_promo_week(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.demand.m1_model import M1DemandModel

    model = M1DemandModel()

    def predict_with_holiday_uplift(X):
        return np.where(X["is_public_holiday"].values == 1, 150.0, 100.0)

    for horizon in M1DemandModel.HORIZONS:
        mock_lgbm = MagicMock()
        mock_lgbm.predict.side_effect = predict_with_holiday_uplift
        model._models[horizon] = mock_lgbm

    schema = [
        "werks", "unified_sku_id",
        "trend", "weekly", "yearly",
        "day_of_week", "week_of_year", "is_public_holiday",
        "stock_qty", "days_of_cover",
    ]
    baseline_row = ("1000", "SKU001", 5.0, 1.0, 2.0, 1, 10, 0, 100.0, 7.0)
    promo_row    = ("1000", "SKU001", 5.0, 1.0, 2.0, 1, 10, 1, 100.0, 7.0)

    baseline = model.score(spark.createDataFrame([baseline_row], schema)).collect()
    promo    = model.score(spark.createDataFrame([promo_row],    schema)).collect()

    assert promo[0]["forecast_7d"] > baseline[0]["forecast_7d"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_demand_models.py::test_m1_forecast_increases_with_promo_week -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engines.demand.m1_model'`

- [ ] **Step 3: Write `m1_model.py`**

```python
# src/engines/demand/m1_model.py
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class M1DemandModel:
    """LightGBM baseline demand sensing: 7d/14d/28d point forecasts per SKU/site."""

    MODEL_NAME_PREFIX = "demand_m1"
    HORIZONS = ["7d", "14d", "28d"]
    FEATURE_COLUMNS = [
        "trend", "weekly", "yearly",
        "day_of_week", "week_of_year", "is_public_holiday",
        "stock_qty", "days_of_cover",
    ]
    MIN_HISTORY_ROWS = 52 * 7

    def __init__(self, model_version: str = "Production"):
        self._models: dict = {}
        self._model_version = model_version

    def train(self, training_df: pd.DataFrame, experiment_name: str = "demand_m1") -> None:
        """
        training_df columns: FEATURE_COLUMNS + demand_7d + demand_14d + demand_28d.
        Logs m1_mae_pct (7d horizon) to MLflow — Week 10 gate: must be ≤ 0.15.
        """
        import lightgbm as lgb

        mlflow.set_experiment(experiment_name)
        with mlflow.start_run():
            for horizon in self.HORIZONS:
                target_col = f"demand_{horizon}"
                X = training_df[self.FEATURE_COLUMNS]
                y = training_df[target_col]

                model = lgb.LGBMRegressor(
                    objective="regression",
                    n_estimators=500,
                    num_leaves=63,
                )
                model.fit(X, y)

                preds = model.predict(X)
                mae = float(np.mean(np.abs(preds - y.values)))
                mlflow.log_metric(f"m1_mae_{horizon}", mae)
                if horizon == "7d" and y.mean() > 0:
                    mlflow.log_metric("m1_mae_pct", mae / float(y.mean()))

                mlflow.lightgbm.log_model(
                    model,
                    f"model_{horizon}",
                    registered_model_name=f"{self.MODEL_NAME_PREFIX}_{horizon}",
                )

    def load(self) -> None:
        for horizon in self.HORIZONS:
            self._models[horizon] = mlflow.lightgbm.load_model(
                f"models:/{self.MODEL_NAME_PREFIX}_{horizon}/{self._model_version}"
            )

    def score(self, features: DataFrame) -> DataFrame:
        """
        Accept Spark DataFrame with FEATURE_COLUMNS + werks + unified_sku_id.
        Returns Spark DataFrame with forecast_7d/14d/28d (clipped ≥0) + _computed_at.
        """
        if not self._models:
            raise RuntimeError("Models not loaded. Call load() first.")

        pandas_df = features.toPandas()
        for horizon in self.HORIZONS:
            pandas_df[f"forecast_{horizon}"] = (
                self._models[horizon]
                .predict(pandas_df[self.FEATURE_COLUMNS])
                .clip(0)
                .astype(float)
            )

        result_cols = ["werks", "unified_sku_id", "forecast_7d", "forecast_14d", "forecast_28d"]
        result = SparkSession.getActiveSession().createDataFrame(pandas_df[result_cols])
        return result.withColumn("_computed_at", F.current_timestamp())


def _entry_train_m1() -> None:
    import datetime
    import holidays as hols
    from databricks.feature_engineering import FeatureEngineeringClient
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from engines.demand.prophet_features import build_prophet_features

    spark = SparkSession.getActiveSession()
    nl_holidays = hols.Netherlands()

    movements = (
        spark.table("silver.sap.enriched_movements")
        .filter(F.col("BWART") == "601")
        .filter(F.col("BUDAT") >= F.date_sub(F.current_date(), 104 * 7))
        .groupBy("WERKS", "unified_sku_id", "BUDAT")
        .agg(F.sum("MENGE").alias("y"))
        .withColumnRenamed("WERKS", "werks")
        .withColumnRenamed("BUDAT", "ds")
        .toPandas()
    )
    movements["ds"] = pd.to_datetime(movements["ds"])

    fs = FeatureEngineeringClient()
    stock = (
        fs.read_table("feature_store.stock.sku_site_positions")
        .select("werks", "unified_sku_id", "stock_qty", "days_of_cover")
        .toPandas()
    )

    training_rows = []
    for (werks, sku_id), group in movements.groupby(["werks", "unified_sku_id"]):
        group = group.sort_values("ds").reset_index(drop=True)
        if len(group) < M1DemandModel.MIN_HISTORY_ROWS:
            continue
        try:
            feat = build_prophet_features(group[["ds", "y"]])
        except ValueError:
            continue

        feat["day_of_week"] = feat["ds"].dt.dayofweek
        feat["week_of_year"] = feat["ds"].dt.isocalendar().week.astype(int)
        feat["is_public_holiday"] = feat["ds"].apply(
            lambda d: 1 if d.date() in nl_holidays else 0
        )

        stock_row = stock[(stock["werks"] == werks) & (stock["unified_sku_id"] == sku_id)]
        if stock_row.empty:
            continue
        feat["stock_qty"] = float(stock_row["stock_qty"].iloc[0])
        feat["days_of_cover"] = float(stock_row["days_of_cover"].iloc[0])

        feat["y"] = feat["y"].clip(lower=0)
        for horizon_days, col in [(7, "demand_7d"), (14, "demand_14d"), (28, "demand_28d")]:
            feat[col] = (
                feat["y"]
                .rolling(horizon_days, min_periods=horizon_days)
                .sum()
                .shift(-horizon_days)
            )
        feat["werks"] = werks
        feat["unified_sku_id"] = sku_id
        training_rows.append(feat)

    if not training_rows:
        raise RuntimeError(
            "No training rows — silver.sap.enriched_movements needs ≥52 weeks per SKU/site."
        )

    train_df = pd.concat(training_rows, ignore_index=True).dropna(
        subset=["demand_7d", "demand_14d", "demand_28d"] + M1DemandModel.FEATURE_COLUMNS
    )
    M1DemandModel().train(train_df)


def _entry_score_m1() -> None:
    import datetime
    import holidays as hols
    from databricks.feature_engineering import FeatureEngineeringClient
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from engines.demand.prophet_features import build_prophet_features

    spark = SparkSession.getActiveSession()
    nl_holidays = hols.Netherlands()
    today = datetime.date.today()

    movements = (
        spark.table("silver.sap.enriched_movements")
        .filter(F.col("BWART") == "601")
        .filter(F.col("BUDAT") >= F.date_sub(F.current_date(), 104 * 7))
        .groupBy("WERKS", "unified_sku_id", "BUDAT")
        .agg(F.sum("MENGE").alias("y"))
        .withColumnRenamed("WERKS", "werks")
        .withColumnRenamed("BUDAT", "ds")
        .toPandas()
    )
    movements["ds"] = pd.to_datetime(movements["ds"])

    fs = FeatureEngineeringClient()
    stock = (
        fs.read_table("feature_store.stock.sku_site_positions")
        .select("werks", "unified_sku_id", "stock_qty", "days_of_cover")
        .toPandas()
    )

    score_rows = []
    for (werks, sku_id), group in movements.groupby(["werks", "unified_sku_id"]):
        group = group.sort_values("ds").reset_index(drop=True)
        if len(group) < 14:
            continue
        try:
            feat = build_prophet_features(group[["ds", "y"]])
        except ValueError:
            continue

        stock_row = stock[(stock["werks"] == werks) & (stock["unified_sku_id"] == sku_id)]
        if stock_row.empty:
            continue

        last = feat.iloc[-1]
        score_rows.append({
            "werks": werks,
            "unified_sku_id": sku_id,
            "trend": float(last["trend"]),
            "weekly": float(last["weekly"]),
            "yearly": float(last["yearly"]),
            "day_of_week": today.weekday(),
            "week_of_year": today.isocalendar()[1],
            "is_public_holiday": 1 if today in nl_holidays else 0,
            "stock_qty": float(stock_row["stock_qty"].iloc[0]),
            "days_of_cover": float(stock_row["days_of_cover"].iloc[0]),
        })

    if not score_rows:
        raise RuntimeError("No scoring rows — check silver.sap.enriched_movements.")

    features_df = spark.createDataFrame(pd.DataFrame(score_rows))
    model = M1DemandModel()
    model.load()
    forecast = model.score(features_df)
    forecast.write.format("delta").mode("overwrite").saveAsTable(
        "feature_store.demand.m1_forecast"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_demand_models.py::test_m1_forecast_increases_with_promo_week -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engines/demand/m1_model.py tests/unit/test_demand_models.py
git commit -m "feat: add M1DemandModel LightGBM baseline + train/score entry points + test"
```

---

### Task 3: `weather_ingest.py`

**Files:**
- Create: `src/engines/demand/weather_ingest.py`

- [ ] **Step 1: Write `weather_ingest.py`**

```python
# src/engines/demand/weather_ingest.py
import datetime

import requests


class WeatherIngestError(Exception):
    """Raised when the Open-Meteo API is unavailable or returns unexpected structure."""


_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_AMSTERDAM_LAT = 52.37
_AMSTERDAM_LON = 4.90
_FORECAST_DAYS = 28


def fetch_weather_forecast() -> list[dict]:
    """
    Fetch 28-day daily max temperature + precipitation from Open-Meteo for Amsterdam.
    Returns a list of dicts: forecast_date (str ISO), temperature_max (float), precipitation (float).
    Raises WeatherIngestError on API unavailability or unexpected response structure.
    """
    params = {
        "latitude": _AMSTERDAM_LAT,
        "longitude": _AMSTERDAM_LON,
        "daily": ["temperature_2m_max", "precipitation_sum"],
        "forecast_days": _FORECAST_DAYS,
        "timezone": "Europe/Amsterdam",
    }
    try:
        response = requests.get(_OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise WeatherIngestError(f"Open-Meteo API unavailable: {exc}") from exc

    data = response.json()
    try:
        dates = data["daily"]["time"]
        temps = data["daily"]["temperature_2m_max"]
        precip = data["daily"]["precipitation_sum"]
    except KeyError as exc:
        raise WeatherIngestError(f"Unexpected Open-Meteo response structure: missing key {exc}") from exc

    ingest_ts = datetime.datetime.utcnow().isoformat()
    return [
        {
            "forecast_date": dates[i],
            "temperature_max": float(temps[i]) if temps[i] is not None else None,
            "precipitation": float(precip[i]) if precip[i] is not None else None,
            "_ingest_ts": ingest_ts,
        }
        for i in range(len(dates))
    ]


def _entry_ingest_weather() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    records = fetch_weather_forecast()
    if not records:
        raise WeatherIngestError("Open-Meteo returned zero forecast rows.")
    df = spark.createDataFrame(records)
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.weather.daily_forecast")
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `python -c "from engines.demand.weather_ingest import WeatherIngestError, fetch_weather_forecast; print('ok')"` (from the `src/` directory)
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/engines/demand/weather_ingest.py
git commit -m "feat: add weather_ingest.py — Open-Meteo → bronze.weather.daily_forecast"
```

---

### Task 4: `m2_model.py` + tests #3 and #4

**Files:**
- Create: `src/engines/demand/m2_model.py`
- Modify: `tests/unit/test_demand_models.py` (append tests #3 and #4)

- [ ] **Step 1: Write the failing tests — append to `tests/unit/test_demand_models.py`**

```python
import pytest


def test_m2_weather_lift_above_one_for_heat_wave(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.demand.m2_model import M2CorrectionModel

    model = M2CorrectionModel()
    mock_weather = MagicMock()
    mock_weather.predict.return_value = np.array([1.35])
    model._weather_model = mock_weather

    m1_df = spark.createDataFrame(
        [("1000", "SKU001", 100.0, 200.0, 400.0)],
        ["werks", "unified_sku_id", "forecast_7d", "forecast_14d", "forecast_28d"],
    )
    weather_rows = [
        {"forecast_date": f"2026-05-{i+1:02d}", "temperature_max": 25.0,
         "precipitation": 0.0, "_ingest_ts": "ts"}
        for i in range(28)
    ]
    weather_df = spark.createDataFrame(weather_rows)
    promo_df = spark.createDataFrame(
        [], spark.createDataFrame([("x", "y", 1.0)], ["werks", "unified_sku_id", "promo_lift"]).schema
    )

    result = model.score(m1_df, weather_df, promo_df).collect()
    assert result[0]["weather_lift"] == pytest.approx(1.35)
    assert result[0]["weather_lift"] > 1.0
    assert result[0]["corrected_7d"] > 100.0


def test_m2_promo_lift_above_one_for_active_promo(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.demand.m2_model import M2CorrectionModel

    model = M2CorrectionModel()
    mock_weather = MagicMock()
    mock_weather.predict.return_value = np.array([1.0])
    model._weather_model = mock_weather

    m1_df = spark.createDataFrame(
        [("1000", "SKU001", 100.0, 200.0, 400.0)],
        ["werks", "unified_sku_id", "forecast_7d", "forecast_14d", "forecast_28d"],
    )
    weather_rows = [
        {"forecast_date": f"2026-05-{i+1:02d}", "temperature_max": 15.0,
         "precipitation": 2.0, "_ingest_ts": "ts"}
        for i in range(28)
    ]
    weather_df = spark.createDataFrame(weather_rows)
    promo_df = spark.createDataFrame(
        [("1000", "SKU001", 1.3)],
        ["werks", "unified_sku_id", "promo_lift"],
    )

    result = model.score(m1_df, weather_df, promo_df).collect()
    assert result[0]["promo_lift"] == pytest.approx(1.3)
    assert result[0]["corrected_7d"] == pytest.approx(130.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_demand_models.py::test_m2_weather_lift_above_one_for_heat_wave tests/unit/test_demand_models.py::test_m2_promo_lift_above_one_for_active_promo -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engines.demand.m2_model'`

- [ ] **Step 3: Write `m2_model.py`**

```python
# src/engines/demand/m2_model.py
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


_AMSTERDAM_SEASONAL_TEMP_NORM = 15.0
_AMSTERDAM_SEASONAL_PRECIP_NORM = 2.0
_WEATHER_LIFT_CLIP_MIN = 0.5
_WEATHER_LIFT_CLIP_MAX = 2.0


class M2CorrectionModel:
    """
    Multiplicative correction on M1: m2_corrected = m1_forecast × promo_lift × weather_lift.
    Weather lift is a LightGBM model trained on (temperature_delta, precipitation_delta) → lift ratio.
    Promo lift comes from a direct table lookup (silver.promo.lift_coefficients).
    """

    WEATHER_MODEL_NAME = "demand_m2_weather_lift"
    _WEATHER_FEATURE_COLUMNS = ["temperature_delta", "precipitation_delta"]

    def __init__(self, model_version: str = "Production"):
        self._weather_model = None
        self._model_version = model_version

    def train(self, weather_lift_df: pd.DataFrame, experiment_name: str = "demand_m2") -> None:
        """
        weather_lift_df columns: temperature_delta (float), precipitation_delta (float),
        actual_lift (float — historical actual_demand / m1_forecast_demand ratio).
        """
        import lightgbm as lgb

        mlflow.set_experiment(experiment_name)
        with mlflow.start_run():
            model = lgb.LGBMRegressor(
                objective="regression",
                n_estimators=200,
                num_leaves=31,
            )
            model.fit(
                weather_lift_df[self._WEATHER_FEATURE_COLUMNS],
                weather_lift_df["actual_lift"],
            )
            mlflow.lightgbm.log_model(
                model, "weather_model", registered_model_name=self.WEATHER_MODEL_NAME
            )

    def load(self) -> None:
        self._weather_model = mlflow.lightgbm.load_model(
            f"models:/{self.WEATHER_MODEL_NAME}/{self._model_version}"
        )

    def score(
        self,
        m1_forecast: DataFrame,
        weather_df: DataFrame,
        promo_df: DataFrame,
    ) -> DataFrame:
        """
        m1_forecast: Spark DF — werks, unified_sku_id, forecast_7d/14d/28d
        weather_df:  Spark DF — forecast_date, temperature_max, precipitation (28 rows)
        promo_df:    Spark DF — werks, unified_sku_id, promo_lift

        Returns Spark DF with corrected_7d/14d/28d, promo_lift, weather_lift, _computed_at.
        """
        if self._weather_model is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        weather_pdf = weather_df.orderBy("forecast_date").limit(7).toPandas()
        if weather_pdf.empty:
            raise RuntimeError("No weather forecast rows in bronze.weather.daily_forecast.")

        temp_mean = float(weather_pdf["temperature_max"].dropna().mean())
        precip_mean = float(weather_pdf["precipitation"].fillna(0.0).mean())

        weather_features = pd.DataFrame({
            "temperature_delta": [temp_mean - _AMSTERDAM_SEASONAL_TEMP_NORM],
            "precipitation_delta": [precip_mean - _AMSTERDAM_SEASONAL_PRECIP_NORM],
        })
        weather_lift = float(
            np.clip(
                self._weather_model.predict(weather_features),
                _WEATHER_LIFT_CLIP_MIN,
                _WEATHER_LIFT_CLIP_MAX,
            )[0]
        )

        m1_pdf = m1_forecast.toPandas()
        promo_pdf = promo_df.toPandas()

        merged = m1_pdf.merge(
            promo_pdf[["werks", "unified_sku_id", "promo_lift"]],
            on=["werks", "unified_sku_id"],
            how="left",
        )
        merged["promo_lift"] = merged["promo_lift"].fillna(1.0).astype(float)
        merged["weather_lift"] = weather_lift

        for horizon in ["7d", "14d", "28d"]:
            merged[f"corrected_{horizon}"] = (
                merged[f"forecast_{horizon}"] * merged["promo_lift"] * merged["weather_lift"]
            ).clip(lower=0).astype(float)

        result_cols = [
            "werks", "unified_sku_id",
            "corrected_7d", "corrected_14d", "corrected_28d",
            "promo_lift", "weather_lift",
        ]
        result = SparkSession.getActiveSession().createDataFrame(merged[result_cols])
        return result.withColumn("_computed_at", F.current_timestamp())


def _entry_train_m2() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from engines.demand.m2_model import M2CorrectionModel

    spark = SparkSession.getActiveSession()

    m1_forecast = spark.table("feature_store.demand.m1_forecast").toPandas()
    actuals = (
        spark.table("silver.sap.enriched_movements")
        .filter(F.col("BWART") == "601")
        .filter(F.col("BUDAT") >= F.date_sub(F.current_date(), 7))
        .groupBy("WERKS", "unified_sku_id")
        .agg(F.sum("MENGE").alias("actual_7d"))
        .withColumnRenamed("WERKS", "werks")
        .toPandas()
    )

    merged = m1_forecast.merge(actuals, on=["werks", "unified_sku_id"], how="inner")
    merged["actual_lift"] = (
        merged["actual_7d"] / merged["forecast_7d"].clip(lower=1.0)
    ).clip(0.5, 2.0)

    weather_hist = spark.table("bronze.weather.daily_forecast").toPandas()
    temp_delta = float(weather_hist["temperature_max"].dropna().mean()) - 15.0
    precip_delta = float(weather_hist["precipitation"].fillna(0.0).mean()) - 2.0

    weather_lift_df = pd.DataFrame({
        "temperature_delta": [temp_delta] * len(merged),
        "precipitation_delta": [precip_delta] * len(merged),
        "actual_lift": merged["actual_lift"].values,
    })

    # Promo lift coefficients — write to silver table for score_m2 lookup
    try:
        promo_calendar = spark.table("silver.promo.calendar").toPandas()
        promo_merged = promo_calendar.merge(actuals, on=["werks", "unified_sku_id"], how="inner")
        promo_merged["promo_lift"] = (
            promo_merged["actual_7d"] / promo_merged["baseline_demand"].clip(lower=1.0)
        ).clip(0.5, 2.0)
        lift_coef = promo_merged.groupby(["werks", "unified_sku_id"])["promo_lift"].mean().reset_index()
    except Exception:
        lift_coef = (
            spark.table("feature_store.demand.m1_forecast")
            .select("werks", "unified_sku_id")
            .withColumn("promo_lift", F.lit(1.0))
            .toPandas()
        )

    spark.createDataFrame(lift_coef).write.format("delta").mode("overwrite").saveAsTable(
        "silver.promo.lift_coefficients"
    )

    M2CorrectionModel().train(weather_lift_df)


def _entry_score_m2() -> None:
    from pyspark.sql import SparkSession
    from engines.demand.m2_model import M2CorrectionModel

    spark = SparkSession.getActiveSession()
    m1_forecast = spark.table("feature_store.demand.m1_forecast")
    weather_df = spark.table("bronze.weather.daily_forecast")
    promo_df = spark.table("silver.promo.lift_coefficients").select(
        "werks", "unified_sku_id", "promo_lift"
    )

    model = M2CorrectionModel()
    model.load()
    corrected = model.score(m1_forecast, weather_df, promo_df)
    corrected.write.format("delta").mode("overwrite").saveAsTable(
        "feature_store.demand.m2_corrected"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_demand_models.py::test_m2_weather_lift_above_one_for_heat_wave tests/unit/test_demand_models.py::test_m2_promo_lift_above_one_for_active_promo -v`
Expected: both PASS

- [ ] **Step 5: Commit**

```bash
git add src/engines/demand/m2_model.py tests/unit/test_demand_models.py
git commit -m "feat: add M2CorrectionModel (weather + promo lift) + train/score entry points + tests"
```

---

### Task 5: `m4_model.py` + tests #5 and #6

**Files:**
- Create: `src/engines/demand/m4_model.py`
- Modify: `tests/unit/test_demand_models.py` (append tests #5 and #6)

- [ ] **Step 1: Write the failing tests — append to `tests/unit/test_demand_models.py`**

```python
def test_m4_p90_greater_than_p50_greater_than_p10(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.demand.m4_model import M4UncertaintyModel

    model = M4UncertaintyModel()
    mock_p10 = MagicMock()
    mock_p50 = MagicMock()
    mock_p90 = MagicMock()
    mock_p10.predict.return_value = np.array([-50.0])
    mock_p50.predict.return_value = np.array([0.0])
    mock_p90.predict.return_value = np.array([50.0])
    model._models = {"p10": mock_p10, "p50": mock_p50, "p90": mock_p90}

    m2_df = spark.createDataFrame(
        [("1000", "SKU001", 100.0, 200.0, 400.0, 1.0, 1.0)],
        ["werks", "unified_sku_id", "corrected_7d", "corrected_14d",
         "corrected_28d", "promo_lift", "weather_lift"],
    )

    result = model.score(m2_df).collect()
    assert result[0]["p90"] > result[0]["p50"]
    assert result[0]["p50"] > result[0]["p10"]


def test_m4_uncertainty_spread_equals_p90_minus_p10(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.demand.m4_model import M4UncertaintyModel

    model = M4UncertaintyModel()
    mock_p10 = MagicMock()
    mock_p50 = MagicMock()
    mock_p90 = MagicMock()
    mock_p10.predict.return_value = np.array([-50.0])
    mock_p50.predict.return_value = np.array([0.0])
    mock_p90.predict.return_value = np.array([50.0])
    model._models = {"p10": mock_p10, "p50": mock_p50, "p90": mock_p90}

    m2_df = spark.createDataFrame(
        [("1000", "SKU001", 100.0, 200.0, 400.0, 1.0, 1.0)],
        ["werks", "unified_sku_id", "corrected_7d", "corrected_14d",
         "corrected_28d", "promo_lift", "weather_lift"],
    )

    result = model.score(m2_df).collect()
    expected_spread = result[0]["p90"] - result[0]["p10"]
    assert result[0]["uncertainty_spread"] == pytest.approx(expected_spread)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_demand_models.py::test_m4_p90_greater_than_p50_greater_than_p10 tests/unit/test_demand_models.py::test_m4_uncertainty_spread_equals_p90_minus_p10 -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engines.demand.m4_model'`

- [ ] **Step 3: Write `m4_model.py`**

```python
# src/engines/demand/m4_model.py
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class M4UncertaintyModel:
    """
    Three LightGBM quantile regressors (α=0.1/0.5/0.9) trained on M2 residuals.
    Produces P10/P50/P90 demand estimates; uncertainty_spread = P90 − P10.
    Quantile ordering is enforced via sort: P10 ≤ P50 ≤ P90 always holds.
    """

    MODEL_NAME_PREFIX = "demand_m4"
    ALPHAS = {"p10": 0.1, "p50": 0.5, "p90": 0.9}
    FEATURE_COLUMNS = [
        "corrected_7d", "corrected_14d", "corrected_28d",
        "promo_lift", "weather_lift",
    ]

    def __init__(self, model_version: str = "Production"):
        self._models: dict = {}
        self._model_version = model_version

    def train(self, residuals_df: pd.DataFrame, experiment_name: str = "demand_m4") -> None:
        """
        residuals_df columns: FEATURE_COLUMNS + residual_7d (actual_7d − corrected_7d).
        Trains p10/p50/p90 quantile models; registers each in MLflow.
        """
        import lightgbm as lgb

        mlflow.set_experiment(experiment_name)
        with mlflow.start_run():
            for quantile_name, alpha in self.ALPHAS.items():
                model = lgb.LGBMRegressor(
                    objective="quantile",
                    alpha=alpha,
                    n_estimators=300,
                    num_leaves=31,
                )
                model.fit(
                    residuals_df[self.FEATURE_COLUMNS],
                    residuals_df["residual_7d"],
                )
                mlflow.lightgbm.log_model(
                    model,
                    f"model_{quantile_name}",
                    registered_model_name=f"{self.MODEL_NAME_PREFIX}_{quantile_name}",
                )

    def load(self) -> None:
        for quantile_name in self.ALPHAS:
            self._models[quantile_name] = mlflow.lightgbm.load_model(
                f"models:/{self.MODEL_NAME_PREFIX}_{quantile_name}/{self._model_version}"
            )

    def score(self, m2_forecast: DataFrame) -> DataFrame:
        """
        Accept M2 corrected forecast Spark DataFrame with FEATURE_COLUMNS + werks/unified_sku_id.
        Returns Spark DataFrame with p10, p50, p90, uncertainty_spread, _computed_at.
        Quantile ordering is enforced: p10 ≤ p50 ≤ p90.
        """
        if not self._models:
            raise RuntimeError("Models not loaded. Call load() first.")

        pandas_df = m2_forecast.toPandas()
        base = pandas_df["corrected_7d"].values.astype(float)

        for quantile_name in self.ALPHAS:
            residual = self._models[quantile_name].predict(pandas_df[self.FEATURE_COLUMNS])
            pandas_df[quantile_name] = (base + residual).clip(0).astype(float)

        sorted_q = np.sort(pandas_df[["p10", "p50", "p90"]].values, axis=1)
        pandas_df["p10"] = sorted_q[:, 0]
        pandas_df["p50"] = sorted_q[:, 1]
        pandas_df["p90"] = sorted_q[:, 2]
        pandas_df["uncertainty_spread"] = (pandas_df["p90"] - pandas_df["p10"]).astype(float)

        result_cols = ["werks", "unified_sku_id", "p10", "p50", "p90", "uncertainty_spread"]
        result = SparkSession.getActiveSession().createDataFrame(pandas_df[result_cols])
        return result.withColumn("_computed_at", F.current_timestamp())


def _entry_train_m4() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from engines.demand.m4_model import M4UncertaintyModel

    spark = SparkSession.getActiveSession()

    m2_corrected = spark.table("feature_store.demand.m2_corrected").toPandas()
    actuals = (
        spark.table("silver.sap.enriched_movements")
        .filter(F.col("BWART") == "601")
        .filter(F.col("BUDAT") >= F.date_sub(F.current_date(), 7))
        .groupBy("WERKS", "unified_sku_id")
        .agg(F.sum("MENGE").alias("actual_7d"))
        .withColumnRenamed("WERKS", "werks")
        .toPandas()
    )

    residuals = m2_corrected.merge(actuals, on=["werks", "unified_sku_id"], how="inner")
    residuals["residual_7d"] = residuals["actual_7d"] - residuals["corrected_7d"]
    residuals_df = residuals.dropna(
        subset=M4UncertaintyModel.FEATURE_COLUMNS + ["residual_7d"]
    )

    if residuals_df.empty:
        raise RuntimeError(
            "No M2 residuals for training — run score_m2 before train_m4."
        )

    M4UncertaintyModel().train(residuals_df)


def _entry_score_m4() -> None:
    from pyspark.sql import SparkSession
    from engines.demand.m4_model import M4UncertaintyModel

    spark = SparkSession.getActiveSession()
    m2_corrected = spark.table("feature_store.demand.m2_corrected")

    model = M4UncertaintyModel()
    model.load()
    probabilistic = model.score(m2_corrected)
    probabilistic.write.format("delta").mode("overwrite").saveAsTable(
        "feature_store.demand.m4_probabilistic"
    )
```

- [ ] **Step 4: Run all 6 tests to verify they all pass**

Run: `pytest tests/unit/test_demand_models.py -v`
Expected: 6 tests pass, 0 failures

- [ ] **Step 5: Commit**

```bash
git add src/engines/demand/m4_model.py tests/unit/test_demand_models.py
git commit -m "feat: add M4UncertaintyModel P10/P50/P90 quantile regression + train/score entry points + tests"
```

---

### Task 6: DABs job + `setup.py` + `databricks.yml`

**Files:**
- Create: `resources/jobs/demand_forecast_train.yml`
- Modify: `setup.py`
- Modify: `databricks.yml`

- [ ] **Step 1: Write the failing test — verify entry points import cleanly**

Run after each sub-step:
```bash
python -c "
from engines.demand.m1_model import _entry_train_m1, _entry_score_m1
from engines.demand.weather_ingest import _entry_ingest_weather
from engines.demand.m2_model import _entry_train_m2, _entry_score_m2
from engines.demand.m4_model import _entry_train_m4, _entry_score_m4
print('all entry points importable')
"
```
(run from `src/` directory)
Expected: `all entry points importable`

- [ ] **Step 2: Create `resources/jobs/demand_forecast_train.yml`**

```yaml
name: demand-forecast-train-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 1 ? * SUN"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: train_m1
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_train_m1
    timeout_seconds: 7200
  - task_key: train_m2
    depends_on:
      - task_key: train_m1
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_train_m2
    timeout_seconds: 3600
  - task_key: train_m4
    depends_on:
      - task_key: train_m2
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_train_m4
    timeout_seconds: 3600
```

- [ ] **Step 3: Add 7 `demand_*` entry points to `setup.py`**

Replace the `entry_points` block in `setup.py` with:

```python
    entry_points={
        "console_scripts": [
            # Phase 1 — Gold aggregates
            "gold_daily_positions=medallion.gold.aggregates:_entry_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:_entry_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:_entry_open_orders",
            # Phase 2A — Feature Store
            "fs_velocity_features=medallion.feature_store.velocity_features:_entry_velocity_features",
            "fs_stock_features=medallion.feature_store.stock_features:_entry_stock_features",
            "fs_collapse_signals=medallion.feature_store.collapse_signals:_entry_collapse_signals",
            # Phase 2B — Phantom Stock Detector
            "phantom_generate_labels=engines.phantom_stock.label_generator:_entry_generate_labels",
            "phantom_train_model=engines.phantom_stock.classifier:_entry_train_model",
            "phantom_score_batch=engines.phantom_stock.feature_pipeline:_entry_score_batch",
            "phantom_write_alerts=engines.phantom_stock.feature_pipeline:_entry_write_alerts",
            "phantom_bapi_writeback=engines.phantom_stock.feature_pipeline:_entry_bapi_writeback",
            # Phase 3A — Demand Models (M1/M2/M4)
            "demand_train_m1=engines.demand.m1_model:_entry_train_m1",
            "demand_score_m1=engines.demand.m1_model:_entry_score_m1",
            "demand_ingest_weather=engines.demand.weather_ingest:_entry_ingest_weather",
            "demand_train_m2=engines.demand.m2_model:_entry_train_m2",
            "demand_score_m2=engines.demand.m2_model:_entry_score_m2",
            "demand_train_m4=engines.demand.m4_model:_entry_train_m4",
            "demand_score_m4=engines.demand.m4_model:_entry_score_m4",
        ],
    },
```

- [ ] **Step 4: Register the training job in `databricks.yml`**

Add `demand_forecast_train` to the `jobs:` section in `databricks.yml`:

```yaml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
    phantom_stock_train:
      source: resources/jobs/phantom_stock_train.yml
    phantom_stock_score:
      source: resources/jobs/phantom_stock_score.yml
    demand_forecast_train:
      source: resources/jobs/demand_forecast_train.yml
```

- [ ] **Step 5: Run all 6 demand model tests to confirm nothing broke**

Run: `pytest tests/unit/test_demand_models.py -v`
Expected: 6 PASS, 0 failures

- [ ] **Step 6: Commit**

```bash
git add resources/jobs/demand_forecast_train.yml setup.py databricks.yml
git commit -m "feat: add demand_forecast_train DABs job + 7 setup.py entry points"
```

---

## Spec Coverage Self-Review

| Spec Requirement | Covered By |
|---|---|
| `prophet_features.py` — trend/weekly/yearly columns | Task 1 |
| `m1_model.py` — LGBMRegressor × 3, MLflow `m1_mae_pct` metric | Task 2 |
| `feature_store.demand.m1_forecast` schema | Task 2 `_entry_score_m1` |
| `weather_ingest.py` — Open-Meteo → `bronze.weather.daily_forecast` | Task 3 |
| `WeatherIngestError` raised on API failure | Task 3 |
| `m2_model.py` — weather lift LGBMRegressor + promo table lookup | Task 4 |
| `feature_store.demand.m2_corrected` schema (promo_lift, weather_lift columns) | Task 4 |
| `silver.promo.lift_coefficients` write in `_entry_train_m2` | Task 4 |
| `m4_model.py` — 3 quantile regressors + quantile ordering enforcement | Task 5 |
| `feature_store.demand.m4_probabilistic` schema (p10/p50/p90/uncertainty_spread) | Task 5 |
| `demand_forecast_train` job: train_m1 → train_m2 → train_m4, Sunday 01:00 AM | Task 6 |
| 7 `demand_*` console script entry points | Task 6 |
| 6 unit tests matching spec names | Tasks 1–5 |
| MLflow registration: `demand_m1_7d/14d/28d`, `demand_m2_weather_lift`, `demand_m4_p10/p50/p90` | Tasks 2, 4, 5 |
| Week 10 gate: `m1_mae_pct` ≤ 0.15 logged to MLflow | Task 2 `train()` |
