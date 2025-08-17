from datetime import datetime, timedelta
from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.python import PythonOperator  # type: ignore[attr-defined]
import logging
import os
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

log = logging.getLogger("compute_metrics")

ALLOW_NEGATIVE = os.environ.get("METRICS_ALLOW_NEGATIVE", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
PG = dict(host="postgres", dbname="airflow", user="airflow", password="airflow")
CITY = os.environ.get("CITY")  # if set, filter metrics to this city only (optional)


def ensure_table():
    log.info("ensure_table: start")
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS weather;")
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
    # helpful composite index for fast dedupe/query
    cur.execute(
        """
      CREATE INDEX IF NOT EXISTS ix_metrics_daily_day_city_var_h
      ON weather.metrics_daily(day, city, var, horizon_hours);
    """
    )
    cur.close()
    conn.close()
    log.info("ensure_table: done")


def _latest_overlap_day(conn):
    qry = """
      SELECT LEAST(
        (SELECT (MAX(valid_time) AT TIME ZONE 'UTC')::date FROM weather.observation_hourly),
        (SELECT (MAX(valid_time) AT TIME ZONE 'UTC')::date FROM weather.forecast_hourly)
      ) AS day
    """
    with conn.cursor() as c:
        c.execute(qry)
        return c.fetchone()[0]


def compute_for_yesterday():
    conn = psycopg2.connect(**PG)
    try:
        target_day = _latest_overlap_day(conn)
        log.info("compute_for_yesterday: target_day=%s", target_day)

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
        log.info("compute_for_yesterday: joined rows=%d", len(df))

        if df.empty:
            log.warning(
                "compute_for_yesterday: no overlapping rows for %s (city=%s)",
                target_day,
                CITY,
            )
            return

        df["h"] = np.round(df["horizon_hours"]).astype("Int64")
        log.info(
            "ALLOW_NEGATIVE=%s; rows before horizon filter=%d", ALLOW_NEGATIVE, len(df)
        )
        if not ALLOW_NEGATIVE:
            df = df[df["h"] >= 0]
        log.info("rows after horizon filter=%d", len(df))
        if df.empty:
            log.warning(
                "no rows after horizon filtering (likely all overlaps were negative)"
            )
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
                        "day": target_day,
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
        log.info("compute_for_yesterday: metric rows=%d", len(mdf))
        if mdf.empty:
            log.warning("compute_for_yesterday: no metric rows to insert")
            return

        cur = conn.cursor()
        if CITY:
            cur.execute(
                "DELETE FROM weather.metrics_daily WHERE day=%s AND city=%s;",
                (target_day, CITY),
            )
            log.info("deleted existing rows for day=%s city=%s", target_day, CITY)
        else:
            cur.execute(
                "DELETE FROM weather.metrics_daily WHERE day=%s;", (target_day,)
            )
            log.info("deleted existing rows for day=%s (all cities)", target_day)

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
        cur.close()
        log.info("insert done")
    finally:
        conn.close()


def export_latest_csv():
    log.info("export_latest_csv: start")
    conn = psycopg2.connect(**PG)
    try:
        q = """
        WITH d AS (
          SELECT MAX(mday) AS day
          FROM (SELECT (day) AS mday FROM weather.metrics_daily) AS x
        )
        SELECT
          m.day,
          m.city,
          m.var,
          m.horizon_hours,
          m.mae,
          m.rmse,
          m.bias
        FROM weather.metrics_daily AS m
        JOIN d ON m.day = d.day
        WHERE m.horizon_hours >= 0      -- keep negatives out of the CSV
        ORDER BY m.var, m.horizon_hours;
        """
        df = pd.read_sql(q, conn)
    finally:
        conn.close()

    if df.empty:
        log.warning("export_latest_csv: no rows to export (after horizon filter)")
        return

    from datetime import datetime as dt

    df.insert(0, "generated_at", dt.utcnow().isoformat(timespec="seconds") + "Z")

    outdir = Path("/opt/site/data")
    outdir.mkdir(parents=True, exist_ok=True)

    day_str = pd.to_datetime(df["day"].max()).strftime("%Y-%m-%d")
    latest = outdir / "metrics_latest.csv"
    dated = outdir / f"metrics_{day_str}.csv"

    df.to_csv(latest, index=False)
    df.to_csv(dated, index=False)

    log.info("export_latest_csv: wrote %d rows to %s and %s", len(df), latest, dated)


with DAG(
    dag_id="compute_metrics",
    start_date=datetime(2025, 8, 1),
    schedule_interval="30 2 * * *",  # daily 02:30 UTC (after obs/forecast)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Compute daily MAE/RMSE/Bias by city, variable, and forecast horizon.",
) as dag:
    ensure = PythonOperator(task_id="ensure_table", python_callable=ensure_table)
    compute = PythonOperator(
        task_id="compute_for_yesterday", python_callable=compute_for_yesterday
    )
    export = PythonOperator(
        task_id="export_latest_csv", python_callable=export_latest_csv
    )

    ensure >> compute >> export
