# Phase 2B — Engine 1: Phantom Stock Detector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phantom Stock Detector — an XGBoost classifier that identifies SKUs where SAP shows positive stock but sales have stopped, triggers SAP corrections via BAPI_GOODSMVT_CREATE, and writes daily alerts to `gold.phantom_stock.alerts` for store manager review.

**Architecture:** `generate_labels()` applies a velocity-collapse heuristic to Gold tables to produce weekly training labels; `PhantomStockClassifier` trains XGBoost (with SHAP explainability) and scores daily feature batches loaded from the Phase 2A Feature Store; `BAPIClient` posts movement-type 562 corrections to SAP ECC via SAP BTP AI Core for high-confidence phantom detections. Two DABs jobs wire the pipeline: weekly train (Sunday 03:00 AM) and daily score (06:00 AM, after feature-store-refresh).

**Tech Stack:** XGBoost, SHAP, MLflow (model registry + artifacts), Databricks Feature Engineering in Unity Catalog (FEU), `requests` (HTTP + retry), Databricks Asset Bundles (DABs), PySpark, pytest + `unittest.mock`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/engines/phantom_stock/label_generator.py` | Create | `generate_labels()` pure function + `_entry_generate_labels()` wheel entry point |
| `src/engines/phantom_stock/classifier.py` | Expand existing stub | Add `FEATURE_COLUMNS`, rewrite `predict()` for Spark DataFrames, add SHAP to `train()`, add `_entry_train_model()` |
| `src/engines/phantom_stock/bapi_client.py` | Create | `BAPIClient` + `BAPIError` — thin HTTP client, no business logic |
| `src/engines/phantom_stock/feature_pipeline.py` | Create | `load_features()` via FEU + `_entry_score_batch()`, `_entry_write_alerts()`, `_entry_bapi_writeback()` |
| `tests/unit/test_phantom_stock.py` | Create | 6 unit tests across all components |
| `resources/jobs/phantom_stock_train.yml` | Create | DABs job: weekly Sunday 03:00 AM, 2 sequential tasks |
| `resources/jobs/phantom_stock_score.yml` | Create | DABs job: daily 06:00 AM, 3 sequential tasks |
| `databricks.yml` | Modify | Register 2 new jobs under `resources.jobs` |
| `setup.py` | Modify | Add 5 `phantom_*` console script entry points |

---

## Task 1: `label_generator.py` + label tests (TDD)

**Files:**
- Create: `src/engines/phantom_stock/label_generator.py`
- Create: `tests/unit/test_phantom_stock.py`

- [ ] **Step 1: Create the test file with 3 failing label tests**

Create `tests/unit/test_phantom_stock.py`:

```python
import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

INVENTORY_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("stock_qty", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

VELOCITY_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("sales_7d", DecimalType(13, 3), True),
    StructField("sales_14d", DecimalType(13, 3), True),
    StructField("sales_28d", DecimalType(13, 3), True),
    StructField("sales_90d", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

# All 8 XGBoost feature columns + identity columns + shelf_life context
FEATURES_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("sales_7d", DoubleType(), True),
    StructField("sales_14d", DoubleType(), True),
    StructField("sales_28d", DoubleType(), True),
    StructField("sales_90d", DoubleType(), True),
    StructField("stock_qty", DoubleType(), True),
    StructField("days_of_cover", DoubleType(), True),
    StructField("velocity_collapse_ratio", DoubleType(), True),
    StructField("days_since_last_sale", IntegerType(), True),
    StructField("shelf_life_days", IntegerType(), True),
])


def test_generate_labels_labels_phantom(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # stock_qty > 0, sales_7d = 0, sales_28d > 5 → phantom
    inventory = spark.createDataFrame(
        [("1000", "SKU001", date(2024, 1, 1), Decimal("10.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU001", Decimal("0.000"), Decimal("0.000"),
          Decimal("20.000"), Decimal("60.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert len(result) == 1
    assert result[0]["is_phantom"] == 1


def test_generate_labels_ignores_zero_stock(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # stock_qty = 0 → not phantom even if sales also stopped
    inventory = spark.createDataFrame(
        [("1000", "SKU002", date(2024, 1, 1), Decimal("0.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU002", Decimal("0.000"), Decimal("0.000"),
          Decimal("20.000"), Decimal("60.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert result[0]["is_phantom"] == 0


def test_generate_labels_ignores_slow_movers(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # sales_28d ≤ 5 → slow-mover guard, not phantom
    inventory = spark.createDataFrame(
        [("1000", "SKU003", date(2024, 1, 1), Decimal("10.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU003", Decimal("0.000"), Decimal("0.000"),
          Decimal("3.000"), Decimal("5.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert result[0]["is_phantom"] == 0
```

- [ ] **Step 2: Run to verify 3 tests fail with import error**

```bash
pytest tests/unit/test_phantom_stock.py -v
```
Expected: `ModuleNotFoundError: No module named 'engines.phantom_stock.label_generator'`

- [ ] **Step 3: Create `src/engines/phantom_stock/label_generator.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_labels(
    inventory_df: DataFrame,   # gold.inventory.daily_positions
    velocity_df: DataFrame,    # gold.sales.velocity
) -> DataFrame:
    """
    Label (werks, unified_sku_id) as phantom if:
      stock_qty > 0 AND sales_7d = 0 AND sales_28d > 5

    Returns DataFrame with columns: werks, unified_sku_id, is_phantom (int 0/1), label_date
    """
    vel = velocity_df.select("werks", "unified_sku_id", "sales_7d", "sales_28d")
    return (
        inventory_df
        .join(vel, on=["werks", "unified_sku_id"], how="inner")
        .withColumn(
            "is_phantom",
            F.when(
                (F.col("stock_qty") > 0)
                & (F.col("sales_7d") == 0)
                & (F.col("sales_28d") > 5),
                F.lit(1),
            ).otherwise(F.lit(0)).cast("int"),
        )
        .withColumn("label_date", F.current_date())
        .select("werks", "unified_sku_id", "is_phantom", "label_date")
    )


def _entry_generate_labels() -> None:
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    labels = generate_labels(
        spark.table("gold.inventory.daily_positions"),
        spark.table("gold.sales.velocity"),
    )
    labels.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.training_labels"
    )
```

- [ ] **Step 4: Run tests — verify 3 pass**

```bash
pytest tests/unit/test_phantom_stock.py::test_generate_labels_labels_phantom \
       tests/unit/test_phantom_stock.py::test_generate_labels_ignores_zero_stock \
       tests/unit/test_phantom_stock.py::test_generate_labels_ignores_slow_movers -v
```
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/engines/phantom_stock/label_generator.py tests/unit/test_phantom_stock.py
git commit -m "feat(phantom-stock): label_generator + 3 label tests"
```

---

## Task 2: Expand `classifier.py` + predict tests (TDD)

**Files:**
- Modify: `src/engines/phantom_stock/classifier.py` (existing stub — full rewrite)
- Modify: `tests/unit/test_phantom_stock.py` (append tests 4–5)

- [ ] **Step 1: Append 2 failing predict tests to `tests/unit/test_phantom_stock.py`**

Append to the bottom of `tests/unit/test_phantom_stock.py`:

```python
def test_predict_auto_corrected_for_high_score(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.phantom_stock.classifier import PhantomStockClassifier

    clf = PhantomStockClassifier()
    mock_model = MagicMock()
    mock_model.predict.return_value = np.array([0.97])
    clf._model = mock_model

    # Row with all 8 feature columns + identity + shelf_life context
    row = ("1000", "SKU001", 0.0, 0.0, 20.0, 60.0, 50.0, None, 0.0, 14, 7)
    features_df = spark.createDataFrame([row], FEATURES_SCHEMA)

    result = clf.predict(features_df).collect()
    assert result[0]["action"] == "AUTO_CORRECTED"
    assert result[0]["phantom_score"] == pytest.approx(0.97, abs=1e-6)
    assert result[0]["is_phantom"] is True


def test_predict_pending_review_for_mid_score(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.phantom_stock.classifier import PhantomStockClassifier

    clf = PhantomStockClassifier()
    mock_model = MagicMock()
    mock_model.predict.return_value = np.array([0.88])
    clf._model = mock_model

    row = ("1000", "SKU002", 0.0, 0.0, 10.0, 40.0, 50.0, None, 0.0, 21, 5)
    features_df = spark.createDataFrame([row], FEATURES_SCHEMA)

    result = clf.predict(features_df).collect()
    assert result[0]["action"] == "PENDING_REVIEW"
    assert result[0]["is_phantom"] is True
```

- [ ] **Step 2: Run to verify 2 tests fail**

```bash
pytest tests/unit/test_phantom_stock.py::test_predict_auto_corrected_for_high_score \
       tests/unit/test_phantom_stock.py::test_predict_pending_review_for_mid_score -v
```
Expected: `AttributeError` or `TypeError` — `predict()` currently accepts Pandas, not Spark DataFrame

- [ ] **Step 3: Rewrite `src/engines/phantom_stock/classifier.py`**

Replace the full file:

```python
import mlflow
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class PhantomStockClassifier:
    """XGBoost classifier detecting invisible stockouts (stock > 0 but sales stopped).

    Target: precision ≥85%, +4% OSA improvement.
    Registered in MLflow under 'phantom_stock_detector'.
    """

    MODEL_NAME = "phantom_stock_detector"
    CONFIDENCE_THRESHOLD = 0.85
    AUTO_CORRECT_THRESHOLD = 0.95

    FEATURE_COLUMNS = [
        "sales_7d",
        "sales_14d",
        "sales_28d",
        "sales_90d",
        "stock_qty",
        "days_of_cover",
        "velocity_collapse_ratio",
        "days_since_last_sale",
    ]

    def __init__(self, model_version: str = "Production"):
        self._model = None
        self._model_version = model_version

    def load(self) -> None:
        self._model = mlflow.pyfunc.load_model(
            f"models:/{self.MODEL_NAME}/{self._model_version}"
        )

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        experiment_name: str = "phantom_stock",
    ) -> None:
        import shap
        import xgboost as xgb
        import matplotlib.pyplot as plt

        mlflow.set_experiment(experiment_name)
        with mlflow.start_run():
            model = xgb.XGBClassifier(
                n_estimators=300,
                max_depth=6,
                learning_rate=0.05,
                eval_metric="logloss",
                use_label_encoder=False,
            )
            model.fit(X_train, y_train)
            mlflow.xgboost.log_model(
                model, "model", registered_model_name=self.MODEL_NAME
            )

            # SHAP explainability — logged as artifact for React ShapWaterfall component
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_train)
            shap.summary_plot(shap_values, X_train, show=False)
            plt.savefig("/tmp/shap_summary.png", bbox_inches="tight")
            plt.close()
            mlflow.log_artifact("/tmp/shap_summary.png", "explainability")

    def predict(self, features: DataFrame) -> DataFrame:
        """
        Accept Spark DataFrame with FEATURE_COLUMNS + werks/unified_sku_id/shelf_life_days.
        Returns Spark DataFrame with phantom_score, is_phantom, action, shelf_life_days, _scored_at.
        """
        if self._model is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        pandas_df = features.toPandas()
        scores = self._model.predict(pandas_df[self.FEATURE_COLUMNS])
        pandas_df["phantom_score"] = scores.astype(float)
        pandas_df["is_phantom"] = pandas_df["phantom_score"] >= self.CONFIDENCE_THRESHOLD

        def _action(score: float) -> str:
            if score >= self.AUTO_CORRECT_THRESHOLD:
                return "AUTO_CORRECTED"
            if score >= self.CONFIDENCE_THRESHOLD:
                return "PENDING_REVIEW"
            return "BELOW_THRESHOLD"

        pandas_df["action"] = pandas_df["phantom_score"].apply(_action)

        result_cols = [
            "werks", "unified_sku_id", "phantom_score",
            "is_phantom", "action", "shelf_life_days",
        ]
        result = features.sparkSession.createDataFrame(pandas_df[result_cols])
        return result.withColumn("_scored_at", F.current_timestamp())


def _entry_train_model() -> None:
    from pyspark.sql import SparkSession
    from engines.phantom_stock.feature_pipeline import load_features  # lazy — avoids circular import

    spark = SparkSession.getActiveSession()
    labels = spark.table("gold.phantom_stock.training_labels")
    features = load_features(spark)

    training_data = (
        labels.select("werks", "unified_sku_id", "is_phantom")
        .join(features, on=["werks", "unified_sku_id"], how="inner")
        .toPandas()
    )

    X = training_data[PhantomStockClassifier.FEATURE_COLUMNS]
    y = training_data["is_phantom"]

    clf = PhantomStockClassifier()
    clf.train(X, y)
```

- [ ] **Step 4: Run tests — verify 2 pass**

```bash
pytest tests/unit/test_phantom_stock.py::test_predict_auto_corrected_for_high_score \
       tests/unit/test_phantom_stock.py::test_predict_pending_review_for_mid_score -v
```
Expected: `2 passed`

- [ ] **Step 5: Run all 5 tests to confirm no regressions**

```bash
pytest tests/unit/test_phantom_stock.py -v
```
Expected: `5 passed`

- [ ] **Step 6: Commit**

```bash
git add src/engines/phantom_stock/classifier.py tests/unit/test_phantom_stock.py
git commit -m "feat(phantom-stock): expand classifier — FEATURE_COLUMNS, Spark predict, SHAP train"
```

---

## Task 3: `bapi_client.py` + BAPIError test (TDD)

**Files:**
- Create: `src/engines/phantom_stock/bapi_client.py`
- Modify: `tests/unit/test_phantom_stock.py` (append test 6)

- [ ] **Step 1: Append 1 failing BAPIError test to `tests/unit/test_phantom_stock.py`**

Append to the bottom of `tests/unit/test_phantom_stock.py`:

```python
def test_bapi_error_raised_on_http_500():
    from unittest.mock import patch, MagicMock
    from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    # Patch requests.post AND time.sleep to skip real delays
    with patch("engines.phantom_stock.bapi_client.requests.post", return_value=mock_response), \
         patch("engines.phantom_stock.bapi_client.time.sleep"):
        client = BAPIClient("https://fake-btp-endpoint", "fake-token")
        with pytest.raises(BAPIError) as exc_info:
            client.post_goods_movement("1000", "SKU001")

    assert "500" in str(exc_info.value)
```

- [ ] **Step 2: Run to verify 1 test fails**

```bash
pytest tests/unit/test_phantom_stock.py::test_bapi_error_raised_on_http_500 -v
```
Expected: `ModuleNotFoundError: No module named 'engines.phantom_stock.bapi_client'`

- [ ] **Step 3: Create `src/engines/phantom_stock/bapi_client.py`**

```python
import time

import requests


class BAPIError(Exception):
    """Raised when SAP BTP AI Core returns a non-2xx response after all retries."""


_RETRY_DELAYS = [1, 2, 4]  # seconds before retry 1, 2, 3 — total 4 attempts


class BAPIClient:
    """
    Posts goods movement corrections to SAP ECC via SAP BTP AI Core HTTP endpoint.
    Credentials read from Databricks Secrets: scope "sap-btp", key "ai-core-token".
    Every call logged to MLflow as a run artifact for full auditability.
    """

    def __init__(self, endpoint_url: str, token: str):
        self._endpoint = endpoint_url
        self._token = token

    def post_goods_movement(
        self,
        werks: str,
        unified_sku_id: str,
        movement_type: str = "562",  # inventory difference posting — phantom removal
        quantity: float = 0.0,       # zero-out the phantom stock
    ) -> dict:
        """POST to SAP BTP AI Core. Raises BAPIError on HTTP 4xx/5xx after 3 retries."""
        payload = {
            "WERKS": werks,
            "MATNR": unified_sku_id,
            "BWART": movement_type,
            "MENGE": quantity,
        }
        response = None
        for attempt in range(1 + len(_RETRY_DELAYS)):
            if attempt > 0:
                time.sleep(_RETRY_DELAYS[attempt - 1])
            response = requests.post(
                self._endpoint,
                json=payload,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=30,
            )
            if response.ok:
                return response.json()

        raise BAPIError(
            f"BAPI_GOODSMVT_CREATE failed after {1 + len(_RETRY_DELAYS)} attempts: "
            f"HTTP {response.status_code} — {response.text}"
        )
```

- [ ] **Step 4: Run all 6 tests**

```bash
pytest tests/unit/test_phantom_stock.py -v
```
Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add src/engines/phantom_stock/bapi_client.py tests/unit/test_phantom_stock.py
git commit -m "feat(phantom-stock): BAPIClient + BAPIError + retry + test 6"
```

---

## Task 4: `feature_pipeline.py` + score pipeline entry points

**Files:**
- Create: `src/engines/phantom_stock/feature_pipeline.py`

No additional tests required by spec for this file. The 6 spec tests are complete after Task 3.

- [ ] **Step 1: Create `src/engines/phantom_stock/feature_pipeline.py`**

```python
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_features(spark: SparkSession) -> DataFrame:
    """
    Load all three Phase 2A feature tables from FEU and join on (werks, unified_sku_id).
    Returns a single DataFrame ready for PhantomStockClassifier.predict().

    Feature columns produced (8 model inputs + identity + shelf_life context):
      sales_7d, sales_14d, sales_28d, sales_90d,
      stock_qty, days_of_cover,
      velocity_collapse_ratio, days_since_last_sale,
      shelf_life_days (context only — not a model input)
    """
    fs = FeatureEngineeringClient()
    velocity = fs.read_table("feature_store.velocity.sku_site_velocity")
    stock = fs.read_table("feature_store.stock.sku_site_positions")
    collapse = fs.read_table("feature_store.phantom.collapse_signals")
    return (
        velocity
        .join(stock.drop("uom", "_computed_at"), on=["werks", "unified_sku_id"], how="inner")
        .join(collapse.drop("_computed_at"), on=["werks", "unified_sku_id"], how="inner")
    )


def _entry_score_batch() -> None:
    from pyspark.sql import SparkSession
    from engines.phantom_stock.classifier import PhantomStockClassifier

    spark = SparkSession.getActiveSession()
    clf = PhantomStockClassifier()
    clf.load()
    features = load_features(spark)
    scores = clf.predict(features)
    # Persist scores for downstream write_alerts task
    scores.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.scores"
    )


def _entry_write_alerts() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    scores = spark.table("gold.phantom_stock.scores")

    # Full overwrite — consumed daily by Phase 4 React app exception queue
    scores.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.alerts"
    )

    # Append-only review queue — manager resolves via Phase 4 app
    review = scores.filter(F.col("action") == "PENDING_REVIEW").select(
        "werks",
        "unified_sku_id",
        "phantom_score",
        "_scored_at",
        F.lit(None).cast("timestamp").alias("_resolved_at"),
        F.lit(None).cast("string").alias("_resolution"),
    )
    review.write.format("delta").mode("append").saveAsTable(
        "gold.phantom_stock.review_queue"
    )


def _entry_bapi_writeback() -> None:
    import mlflow
    from pyspark.sql import SparkSession
    from databricks.sdk.runtime import dbutils
    from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

    spark = SparkSession.getActiveSession()
    token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
    endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
    client = BAPIClient(endpoint_url=endpoint, token=token)

    auto_rows = (
        spark.table("gold.phantom_stock.alerts")
        .filter(F.col("action") == "AUTO_CORRECTED")
        .collect()
    )

    mlflow.set_experiment("phantom_stock_bapi_writeback")
    with mlflow.start_run():
        results = []
        for row in auto_rows:
            try:
                response = client.post_goods_movement(
                    werks=row["werks"],
                    unified_sku_id=row["unified_sku_id"],
                )
                results.append({"werks": row["werks"], "sku": row["unified_sku_id"],
                                 "status": "ok", "response": response})
            except BAPIError as exc:
                results.append({"werks": row["werks"], "sku": row["unified_sku_id"],
                                 "status": "error", "message": str(exc)})
        mlflow.log_dict({"writeback_audit": results}, "bapi_audit.json")
        mlflow.log_metric("auto_corrected_count", len(auto_rows))
        error_count = sum(1 for r in results if r["status"] == "error")
        mlflow.log_metric("bapi_error_count", error_count)
```

- [ ] **Step 2: Run all 6 tests to confirm nothing broken**

```bash
pytest tests/unit/test_phantom_stock.py -v
```
Expected: `6 passed`

- [ ] **Step 3: Commit**

```bash
git add src/engines/phantom_stock/feature_pipeline.py
git commit -m "feat(phantom-stock): feature_pipeline — load_features + score/write/bapi entry points"
```

---

## Task 5: DABs jobs + `setup.py` + `databricks.yml`

**Files:**
- Create: `resources/jobs/phantom_stock_train.yml`
- Create: `resources/jobs/phantom_stock_score.yml`
- Modify: `databricks.yml` (add 2 job entries)
- Modify: `setup.py` (add 5 `phantom_*` console scripts)

- [ ] **Step 1: Create `resources/jobs/phantom_stock_train.yml`**

```yaml
name: phantom-stock-train-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 3 ? * SUN"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: generate_labels
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_generate_labels
    timeout_seconds: 3600
  - task_key: train_model
    depends_on:
      - task_key: generate_labels
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_train_model
    timeout_seconds: 7200
```

- [ ] **Step 2: Create `resources/jobs/phantom_stock_score.yml`**

```yaml
name: phantom-stock-score-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 6 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: score_batch
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_score_batch
    timeout_seconds: 1800
  - task_key: write_alerts
    depends_on:
      - task_key: score_batch
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_write_alerts
    timeout_seconds: 900
  - task_key: bapi_writeback
    depends_on:
      - task_key: write_alerts
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_bapi_writeback
    timeout_seconds: 1800
```

- [ ] **Step 3: Add 2 job entries to `databricks.yml`**

In `databricks.yml`, under `resources.jobs`, add after the last existing job entry:

```yaml
    phantom_stock_train:
      source: resources/jobs/phantom_stock_train.yml
    phantom_stock_score:
      source: resources/jobs/phantom_stock_score.yml
```

The full `resources.jobs` block should look like:

```yaml
resources:
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
    feature_store_refresh:
      source: resources/jobs/feature_store_refresh.yml
    phantom_stock_train:
      source: resources/jobs/phantom_stock_train.yml
    phantom_stock_score:
      source: resources/jobs/phantom_stock_score.yml
```

> Note: `feature_store_refresh` lands when Phase 2A merges. If it is not yet present in `databricks.yml`, add only the two phantom entries. Do not add `feature_store_refresh` in this task — that belongs to the Phase 2A merge.

- [ ] **Step 4: Add 5 entry points to `setup.py`**

`setup.py` must already contain the gold and feature-store entries from Phases 1 and 2A. Append the 5 phantom entries to the `console_scripts` list:

```python
from setuptools import setup, find_packages

setup(
    name="ahold_freshness_poc",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
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
        ],
    },
    python_requires=">=3.8",
    install_requires=[],
)
```

> Note: If `setup.py` does not yet exist (Phase 1/2A not merged), create the file with this full content. If it exists, add only the 5 `phantom_*` lines.

- [ ] **Step 5: Run all 6 tests one final time**

```bash
pytest tests/unit/test_phantom_stock.py -v
```
Expected: `6 passed`

- [ ] **Step 6: Commit**

```bash
git add resources/jobs/phantom_stock_train.yml \
        resources/jobs/phantom_stock_score.yml \
        databricks.yml \
        setup.py
git commit -m "feat(phantom-stock): DABs train/score jobs + wheel entry points"
```

---

## Verification Checklist (post-deploy)

Before marking Phase 2B complete, confirm on Databricks:

- [ ] `phantom-stock-train` job runs green; model visible in MLflow under `phantom_stock_detector/Production`
- [ ] Backtest precision on 90-day historical labels ≥ 80% (Week 8 Go/No-Go gate)
- [ ] `phantom-stock-score` runs daily; `gold.phantom_stock.alerts` populated with `_scored_at`
- [ ] At least one `AUTO_CORRECTED` row in `gold.phantom_stock.alerts` within 7 days of live run
- [ ] Corresponding `BAPI_GOODSMVT_CREATE` call confirmed in SAP QA movement log (movement type 562)
- [ ] `gold.phantom_stock.review_queue` populated for `PENDING_REVIEW` rows
