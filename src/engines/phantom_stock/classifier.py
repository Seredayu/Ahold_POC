import mlflow
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
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
        self._model = mlflow.xgboost.load_model(
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
        if features.rdd.isEmpty():
            raise ValueError("predict() received an empty DataFrame — check upstream feature pipeline.")

        pandas_df = features.toPandas()
        scores = self._model.predict_proba(pandas_df[self.FEATURE_COLUMNS])[:, 1]
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
        result = SparkSession.getActiveSession().createDataFrame(pandas_df[result_cols])
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
