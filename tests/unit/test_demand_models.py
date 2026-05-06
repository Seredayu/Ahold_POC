import pandas as pd
import numpy as np
import pytest


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


def test_prophet_features_raises_on_short_series():
    from engines.demand.prophet_features import build_prophet_features

    short = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=13, freq="D"), "y": range(13)})
    with pytest.raises(ValueError, match="Prophet requires"):
        build_prophet_features(short)


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
