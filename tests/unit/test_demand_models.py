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
