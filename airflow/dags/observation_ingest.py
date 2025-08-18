from datetime import datetime, timedelta, date
from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.python import PythonOperator  # type: ignore[attr-defined]
import os
import logging
import psycopg2
import pandas as pd
import numpy as np
from meteostat import Hourly, Point

# ---------- config ----------
PG = dict(host="postgres", dbname="airflow", user="airflow", password="airflow")

CITY = os.environ.get("CITY", "Cape Town")
LAT = float(os.environ.get("LAT", "-33.9249"))
LON = float(os.environ.get("LON", "18.4241"))
LOOKBACK_DAYS = int(os.environ.get("OBS_LOOKBACK_DAYS", "7"))

log = logging.getLogger("observation_ingest")


# ---------- helpers ----------
def ensure_table():
    log.info("ensure_table: start (schema=weather, table=observation_hourly)")
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS weather;")
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
        """
      CREATE UNIQUE INDEX IF NOT EXISTS uq_obs_city_time
      ON weather.observation_hourly (city, valid_time);
    """
    )
    cur.close()
    conn.close()
    log.info("ensure_table: done")


def _fetch_range(start_d: date, end_d: date) -> pd.DataFrame:
    log.info(
        "fetch_range: %s -> %s (UTC), lat=%.4f lon=%.4f city=%s",
        start_d,
        end_d,
        LAT,
        LON,
        CITY,
    )
    loc = Point(LAT, LON)
    start_dt = datetime.combine(start_d, datetime.min.time())
    end_dt = datetime.combine(end_d, datetime.min.time())
    # Meteostat returns a pandas DataFrame with 'time' index
    df = Hourly(loc, start_dt, end_dt, timezone="UTC").fetch().reset_index()
    log.info("fetch_range: fetched %d raw rows", len(df))

    if df.empty:
        return pd.DataFrame(
            columns=[
                "valid_time",
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "city",
            ]
        )

    out = pd.DataFrame(
        {
            "valid_time": pd.to_datetime(df["time"], utc=True, errors="coerce"),
            "temperature_2m": df.get("temp"),
            "precipitation": df.get("prcp"),
            "wind_speed_10m": df.get("wspd"),
        }
    )
    out["city"] = CITY
    out = out.replace({np.nan: None}).dropna(subset=["valid_time"])
    log.info("fetch_range: normalized %d rows after cleaning", len(out))
    return out


def _upsert(df: pd.DataFrame):
    if df.empty:
        log.warning("upsert: nothing to insert (empty frame)")
        return
    log.info("upsert: inserting %d rows", len(df))
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.executemany(
        """
      INSERT INTO weather.observation_hourly
        (city, valid_time, temperature_2m, precipitation, wind_speed_10m)
      VALUES (%s,%s,%s,%s,%s)
      ON CONFLICT (city, valid_time) DO NOTHING;
    """,
        list(
            df[
                [
                    "city",
                    "valid_time",
                    "temperature_2m",
                    "precipitation",
                    "wind_speed_10m",
                ]
            ].itertuples(index=False, name=None)
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    log.info("upsert: done")


def load_initial_history():
    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    log.info("load_initial_history: start (lookback_days=%d)", LOOKBACK_DAYS)
    df = _fetch_range(start, end)
    _upsert(df)
    log.info("load_initial_history: done")


def load_yesterday():
    end = date.today()
    start = end - timedelta(days=1)
    log.info("load_yesterday: start")
    df = _fetch_range(start, end)
    _upsert(df)
    log.info("load_yesterday: done")


# ---------- DAG ----------
with DAG(
    dag_id="observation_ingest",
    start_date=datetime(2025, 8, 1),
    schedule_interval="20 1 * * *",  # daily ~01:20 UTC
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    description="Import hourly observations from Meteostat (UTC) with logging.",
) as dag:
    t0 = PythonOperator(task_id="ensure_table", python_callable=ensure_table)
    t1 = PythonOperator(
        task_id="load_initial_history", python_callable=load_initial_history
    )
    t2 = PythonOperator(task_id="load_yesterday", python_callable=load_yesterday)
    t0 >> t1 >> t2
