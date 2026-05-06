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
