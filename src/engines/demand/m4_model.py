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

    spark = SparkSession.getActiveSession()
    m2_corrected = spark.table("feature_store.demand.m2_corrected")

    model = M4UncertaintyModel()
    model.load()
    probabilistic = model.score(m2_corrected)
    probabilistic.write.format("delta").mode("overwrite").saveAsTable(
        "feature_store.demand.m4_probabilistic"
    )
