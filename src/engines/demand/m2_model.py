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
    Multiplicative correction: m2_corrected = m1_forecast × promo_lift × weather_lift.
    Weather lift is a LightGBM model. Promo lift is a direct lookup from silver.promo.lift_coefficients.
    """

    WEATHER_MODEL_NAME = "demand_m2_weather_lift"
    _WEATHER_FEATURE_COLUMNS = ["temperature_delta", "precipitation_delta"]

    def __init__(self, model_version: str = "Production"):
        self._weather_model = None
        self._model_version = model_version

    def train(self, weather_lift_df: pd.DataFrame, experiment_name: str = "demand_m2") -> None:
        """
        weather_lift_df columns: temperature_delta (float), precipitation_delta (float),
        actual_lift (float — historical actual_demand / m1_forecast ratio).
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

        Returns Spark DF: corrected_7d/14d/28d, promo_lift, weather_lift, _computed_at.
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

    try:
        promo_calendar = spark.table("silver.promo.calendar").toPandas()
        promo_merged = promo_calendar.merge(actuals, on=["werks", "unified_sku_id"], how="inner")
        promo_merged["promo_lift"] = (
            promo_merged["actual_7d"] / promo_merged["baseline_demand"].clip(lower=1.0)
        ).clip(0.5, 2.0)
        lift_coef = promo_merged.groupby(["werks", "unified_sku_id"])["promo_lift"].mean().reset_index()
    except Exception:
        # silver.promo.calendar not yet populated — default all lift coefficients to 1.0
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
