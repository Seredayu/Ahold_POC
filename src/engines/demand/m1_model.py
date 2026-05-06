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
