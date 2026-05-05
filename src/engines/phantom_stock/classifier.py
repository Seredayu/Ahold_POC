import mlflow
import pandas as pd
from dataclasses import dataclass


@dataclass
class PhantomStockPrediction:
    sku_id: str
    site_id: str
    is_phantom: bool
    confidence: float


class PhantomStockClassifier:
    """XGBoost classifier detecting invisible stockouts (stock > 0 but sales stopped).

    Target: precision ≥85%, +4% OSA improvement.
    Registered in MLflow under 'phantom_stock_detector'.
    """

    MODEL_NAME = "phantom_stock_detector"
    CONFIDENCE_THRESHOLD = 0.85

    def __init__(self, model_version: str = "Production"):
        self._model = None
        self._model_version = model_version

    def load(self) -> None:
        self._model = mlflow.pyfunc.load_model(
            f"models:/{self.MODEL_NAME}/{self._model_version}"
        )

    def predict(self, features: pd.DataFrame) -> list[PhantomStockPrediction]:
        if self._model is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        raw = self._model.predict(features)
        predictions = []
        for i, row in features.iterrows():
            confidence = float(raw[i])
            predictions.append(PhantomStockPrediction(
                sku_id=str(row["sku_id"]),
                site_id=str(row["site_id"]),
                is_phantom=confidence >= self.CONFIDENCE_THRESHOLD,
                confidence=confidence,
            ))
        return predictions

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, experiment_name: str = "phantom_stock") -> None:
        import xgboost as xgb

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
            mlflow.xgboost.log_model(model, "model", registered_model_name=self.MODEL_NAME)
