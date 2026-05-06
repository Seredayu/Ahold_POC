import pandas as pd


def build_prophet_features(series: pd.DataFrame) -> pd.DataFrame:
    """
    Accept a two-column DataFrame: 'ds' (datetime) and 'y' (float daily/weekly demand).
    Returns the same rows with three added columns: trend, weekly, yearly.
    Raises ValueError if fewer than 14 rows (Prophet minimum).
    """
    # Prophet has a 2–3s import overhead; lazy-load to avoid penalising every Databricks task that imports this module
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
