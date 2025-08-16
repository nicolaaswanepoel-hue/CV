from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import requests
import psycopg2
import pandas as pd

# Use the Airflow metadata DB (service 'postgres') for demo simplicity
PG = dict(host="postgres", dbname="airflow", user="airflow", password="airflow")

CITY = os.environ.get("CITY", "Johannesburg")
LAT = os.environ.get("LAT", "-26.2041")
LON = os.environ.get("LON", "28.0473")


def ensure_schema_and_table():
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS weather;")
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
    cur.close()
    conn.close()


def ingest_forecast():
    # Pull 7-day hourly forecast (no API key needed)
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}&longitude={LON}"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m"
        f"&forecast_days=7&timezone=auto"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    hourly = r.json()["hourly"]

    df = pd.DataFrame(
        {
            "valid_time": pd.to_datetime(hourly["time"]),
            "temperature_2m": hourly.get("temperature_2m"),
            "precipitation": hourly.get("precipitation"),
            "wind_speed_10m": hourly.get("wind_speed_10m"),
        }
    )
    df["model_run"] = pd.Timestamp.utcnow()
    df["city"] = CITY

    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.executemany(
        """
      INSERT INTO weather.forecast_hourly
        (city, model_run, valid_time, temperature_2m, precipitation, wind_speed_10m)
      VALUES (%s,%s,%s,%s,%s,%s);
    """,
        list(
            df[
                [
                    "city",
                    "model_run",
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


with DAG(
    dag_id="forecast_ingest",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 * * * *",  # hourly
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    description="Pull hourly forecast from Open-Meteo and store in Postgres (weather schema).",
) as dag:
    t1 = PythonOperator(
        task_id="ensure_schema_and_table", python_callable=ensure_schema_and_table
    )
    t2 = PythonOperator(task_id="ingest_forecast", python_callable=ingest_forecast)
    t1 >> t2
