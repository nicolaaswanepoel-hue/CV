from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Iterable, Tuple
import logging
import os

import pandas as pd
import numpy as np
import psycopg2
import requests

from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.python import PythonOperator  # type: ignore[attr-defined]

log = logging.getLogger("backfill_history")

# ------------------------
# Config via environment
# ------------------------
PG = dict(host="postgres", dbname="airflow", user="airflow", password="airflow")

CITY = os.environ.get("CITY", "Cape Town")
LAT = float(os.environ.get("LAT", "-33.9258"))
LON = float(os.environ.get("LON", "18.4233"))

# Backfill window (UTC calendar days)
BACKFILL_START = os.environ.get("BACKFILL_START")  # e.g. "2025-07-15"
BACKFILL_END = os.environ.get("BACKFILL_END")  # e.g. "2025-08-16"

# How we treat negative horizons when computing metrics
ALLOW_NEGATIVE = os.environ.get("METRICS_ALLOW_NEGATIVE", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}

# APIs
ARCHIVE_OBS_URL = "https://archive-api.open-meteo.com/v1/archive"
HIST_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# Variables we care about (must match your DB schema)
HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m"]


# ---------------------------------
# Helpers
# ---------------------------------
def _coerce_window() -> Tuple[str, str]:
    """Resolve the backfill window as strings YYYY-MM-DD."""
    # If unset, default to the last 14 days (inclusive)
    if not BACKFILL_START or not BACKFILL_END:
        end = pd.Timestamp.utcnow().normalize()  # today UTC
        start = end - pd.Timedelta(days=13)
        return (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    return (BACKFILL_START, BACKFILL_END)


def _daterange_inclusive(start: str, end: str) -> Iterable[date]:
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    d = s
    while d <= e:
        yield d
        d = d + timedelta(days=1)


def _snap_to_6h_model_run(ts_utc: pd.Timestamp) -> pd.Timestamp:
    """Heuristic model_run: floor to previous 6-hour boundary (00,06,12,18 UTC)."""
    h = (ts_utc.hour // 6) * 6
    return ts_utc.replace(hour=h, minute=0, second=0, microsecond=0)


def ensure_base_tables():
    """Create schemas/tables/indexes if they don't exist."""
    log.info("ensure_base_tables: start")
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS weather;")

    # Observations (hourly)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather.observation_hourly(
          city text NOT NULL,
          valid_time timestamptz NOT NULL,
          temperature_2m double precision,
          precipitation double precision,
          wind_speed_10m double precision
        );
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS ix_observation_city_time ON weather.observation_hourly (city, valid_time);"
    )

    # Forecasts (hourly)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather.forecast_hourly(
          city text NOT NULL,
          model_run timestamptz NOT NULL,
          valid_time timestamptz NOT NULL,
          temperature_2m double precision,
          precipitation double precision,
          wind_speed_10m double precision
        );
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS ix_forecast_city_valid_model ON weather.forecast_hourly (city, valid_time, model_run);"
    )

    # Metrics (daily) — matches your compute_metrics DAG
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather.metrics_daily(
          day date NOT NULL,
          city text NOT NULL,
          horizon_hours int NOT NULL,
          var text NOT NULL,
          mae double precision,
          rmse double precision,
          bias double precision
        );
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS ix_metrics_daily_day_city_var_h ON weather.metrics_daily(day, city, var, horizon_hours);"
    )

    cur.close()
    conn.close()
    log.info("ensure_base_tables: done")


def _get_json(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def backfill_observations():
    """Load historical observations (ERA5/ERA5-Land via Open-Meteo Archive API)."""
    start, end = _coerce_window()
    log.info("backfill_observations: %s → %s city=%s", start, end, CITY)

    params = dict(
        latitude=LAT,
        longitude=LON,
        start_date=start,
        end_date=end,
        hourly=",".join(HOURLY_VARS),
        timezone="UTC",
    )
    data = _get_json(ARCHIVE_OBS_URL, params)
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        log.warning("backfill_observations: no data returned")
        return

    df = pd.DataFrame({"valid_time": pd.to_datetime(times, utc=True)})
    for v in HOURLY_VARS:
        df[v] = hourly.get(v, [None] * len(df))

    # Delete + bulk insert for the window (idempotent)
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM weather.observation_hourly
                 WHERE city = %s
                   AND (valid_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (CITY, start, end),
            )
            cur.executemany(
                """
                INSERT INTO weather.observation_hourly (city, valid_time, temperature_2m, precipitation, wind_speed_10m)
                VALUES (%s,%s,%s,%s,%s);
                """,
                [
                    (
                        CITY,
                        vt.to_pydatetime(),
                        (
                            float(x["temperature_2m"])
                            if pd.notna(x["temperature_2m"])
                            else None
                        ),
                        (
                            float(x["precipitation"])
                            if pd.notna(x["precipitation"])
                            else None
                        ),
                        (
                            float(x["wind_speed_10m"])
                            if pd.notna(x["wind_speed_10m"])
                            else None
                        ),
                    )
                    for vt, x in zip(df["valid_time"], df.to_dict(orient="records"))
                ],
            )
        conn.commit()
    finally:
        conn.close()
    log.info("backfill_observations: inserted %d rows", len(df))


def backfill_forecasts_day0():
    """
    Load historical forecasts (concatenated first hours of each model update).
    We approximate model_run by flooring each valid_time to the previous 6-hour boundary.
    This seeds short lead-time horizons (0–5/6h).
    """
    start, end = _coerce_window()
    log.info("backfill_forecasts_day0: %s → %s city=%s", start, end, CITY)

    params = dict(
        latitude=LAT,
        longitude=LON,
        start_date=start,
        end_date=end,
        hourly=",".join(HOURLY_VARS),
        timezone="UTC",
    )
    data = _get_json(HIST_FORECAST_URL, params)
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        log.warning("backfill_forecasts_day0: no data returned")
        return

    vt = pd.to_datetime(times, utc=True)
    mr = vt.map(_snap_to_6h_model_run)

    df = pd.DataFrame({"valid_time": vt, "model_run": mr})
    for v in HOURLY_VARS:
        df[v] = hourly.get(v, [None] * len(df))

    # Delete + bulk insert (idempotent)
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM weather.forecast_hourly
                 WHERE city = %s
                   AND (valid_time AT TIME ZONE 'UTC')::date BETWEEN %s AND %s;
                """,
                (CITY, start, end),
            )
            cur.executemany(
                """
                INSERT INTO weather.forecast_hourly
                  (city, model_run, valid_time, temperature_2m, precipitation, wind_speed_10m)
                VALUES (%s,%s,%s,%s,%s,%s);
                """,
                [
                    (
                        CITY,
                        mr_i.to_pydatetime(),
                        vt_i.to_pydatetime(),
                        (
                            float(x["temperature_2m"])
                            if pd.notna(x["temperature_2m"])
                            else None
                        ),
                        (
                            float(x["precipitation"])
                            if pd.notna(x["precipitation"])
                            else None
                        ),
                        (
                            float(x["wind_speed_10m"])
                            if pd.notna(x["wind_speed_10m"])
                            else None
                        ),
                    )
                    for mr_i, vt_i, x in zip(
                        df["model_run"], df["valid_time"], df.to_dict(orient="records")
                    )
                ],
            )
        conn.commit()
    finally:
        conn.close()
    log.info("backfill_forecasts_day0: inserted %d rows", len(df))


# -------------------------------
# Compute metrics per day in range
# -------------------------------
def _compute_for_day(conn, target_day: str):
    """Compute daily metrics for a single day (copied from your compute logic, parameterized)."""
    where_city = "AND o.city = %(city)s" if CITY else ""
    params = {"day": target_day}
    if CITY:
        params["city"] = CITY

    q = f"""
      WITH y AS (SELECT %(day)s::date AS day)
      SELECT
        o.city,
        o.valid_time,
        EXTRACT(EPOCH FROM (o.valid_time - f.model_run))/3600.0 AS horizon_hours,
        f.temperature_2m AS f_temp, o.temperature_2m AS o_temp,
        f.precipitation  AS f_prec, o.precipitation  AS o_prec,
        f.wind_speed_10m AS f_wind, o.wind_speed_10m AS o_wind
      FROM weather.observation_hourly o
      JOIN weather.forecast_hourly f
        ON f.city = o.city
       AND f.valid_time = o.valid_time
      JOIN y ON date_trunc('day', o.valid_time AT TIME ZONE 'UTC')::date = y.day
      WHERE 1=1
      {where_city}
      ORDER BY o.valid_time, f.model_run;
    """
    df = pd.read_sql(q, conn, params=params)
    log.info("compute_for_day: %s rows=%d", target_day, len(df))
    if df.empty:
        return

    df["h"] = np.round(df["horizon_hours"]).astype("Int64")
    if not ALLOW_NEGATIVE:
        df = df[df["h"] >= 0]
    if df.empty:
        log.warning("compute_for_day: %s no rows after horizon filter", target_day)
        return

    rows = []

    def agg_err(fcol, ocol, var_name):
        tmp = df[["city", "h", fcol, ocol]].dropna()
        for (city, h), g in tmp.groupby(["city", "h"], dropna=True):
            err = g[fcol].to_numpy(dtype=float) - g[ocol].to_numpy(dtype=float)
            mae = float(np.mean(np.abs(err)))
            rmse = float(np.sqrt(np.mean(err**2)))
            bias = float(np.mean(err))
            rows.append(
                {
                    "day": pd.to_datetime(target_day).date(),
                    "city": city,
                    "horizon_hours": int(h),
                    "var": var_name,
                    "mae": mae,
                    "rmse": rmse,
                    "bias": bias,
                }
            )

    agg_err("f_temp", "o_temp", "temperature_2m")
    agg_err("f_prec", "o_prec", "precipitation")
    agg_err("f_wind", "o_wind", "wind_speed_10m")

    mdf = pd.DataFrame(rows)
    if mdf.empty:
        log.warning("compute_for_day: %s produced no metric rows", target_day)
        return

    with conn.cursor() as cur:
        if CITY:
            cur.execute(
                "DELETE FROM weather.metrics_daily WHERE day=%s AND city=%s;",
                (pd.to_datetime(target_day).date(), CITY),
            )
        else:
            cur.execute(
                "DELETE FROM weather.metrics_daily WHERE day=%s;",
                (pd.to_datetime(target_day).date(),),
            )
        cur.executemany(
            """
            INSERT INTO weather.metrics_daily (day, city, horizon_hours, var, mae, rmse, bias)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
            """,
            list(
                mdf[
                    ["day", "city", "horizon_hours", "var", "mae", "rmse", "bias"]
                ].itertuples(index=False, name=None)
            ),
        )
        conn.commit()
    log.info("compute_for_day: %s insert done (%d rows)", target_day, len(mdf))


def compute_metrics_range():
    """Compute metrics for every day in the backfill window."""
    start, end = _coerce_window()
    log.info("compute_metrics_range: %s → %s", start, end)
    conn = psycopg2.connect(**PG)
    try:
        for d in _daterange_inclusive(start, end):
            _compute_for_day(conn, d.isoformat())
    finally:
        conn.close()
    log.info("compute_metrics_range: done")


def export_latest_and_history():
    """
    Reuse your existing export_csvs() to write:
      - metrics_latest.csv
      - metrics_YYYY-MM-DD.csv
      - metrics_history.csv (window controlled by METRICS_HISTORY_DAYS)
    """
    # Lazy import to avoid double-registering a DAG at import time
    import importlib

    cm = importlib.import_module("compute_metrics")
    cm.export_csvs()  # your function


# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="backfill_history",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,  # manual / on-demand
    catchup=False,
    default_args={"retries": 0},
    description="One-off backfill: historical obs + historical forecast (short lead), compute metrics per day, export CSVs.",
) as dag:
    ensure = PythonOperator(task_id="ensure_tables", python_callable=ensure_base_tables)
    obs = PythonOperator(
        task_id="backfill_observations", python_callable=backfill_observations
    )
    fcst = PythonOperator(
        task_id="backfill_forecasts_day0", python_callable=backfill_forecasts_day0
    )
    comp = PythonOperator(
        task_id="compute_metrics_range", python_callable=compute_metrics_range
    )
    expo = PythonOperator(
        task_id="export_csvs", python_callable=export_latest_and_history
    )

    ensure >> obs >> fcst >> comp >> expo
