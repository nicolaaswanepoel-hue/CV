from datetime import datetime, timedelta
from pathlib import Path
import logging
import os

import numpy as np
import pandas as pd
import psycopg2

from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.python import PythonOperator  # type: ignore[attr-defined]
from airflow.operators.bash import BashOperator  # type: ignore[attr-defined]
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("compute_metrics")

# ------------------- Config -------------------
ALLOW_NEGATIVE = os.environ.get("METRICS_ALLOW_NEGATIVE", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
PG = dict(host="postgres", dbname="airflow", user="airflow", password="airflow")
CITY = os.environ.get("CITY")  # optional city filter for both exports

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/opt/airflow/docs/data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Keep dbt artefacts OFF the git bind mount
DBT_LOG_PATH = os.environ.get("DBT_LOG_PATH", "/tmp/dbt/logs")
DBT_TARGET_PATH = os.environ.get("DBT_TARGET_PATH", "/tmp/dbt/target")

DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    "DBT_HOST": PG["host"],
    "DBT_PORT": "5432",
    "DBT_DB": PG["dbname"],
    "DBT_SCHEMA": "analytics",
    "DBT_USER": PG["user"],
    "DBT_PASSWORD": PG["password"],
    "DBT_THREADS": "4",
    # keep dbt logs/targets in /tmp
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
    # for psql \copy
    "PGPASSWORD": PG["password"],
    # ensure dbt/psql in PATH
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
}


# ------------------- Helpers -------------------
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


# ------------------- Existing metrics (unchanged) -------------------
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


def export_csvs():
    log.info("export_csvs: start")
    ndays = int(os.environ.get("METRICS_HISTORY_DAYS", "30"))  # rolling window
    conn = psycopg2.connect(**PG)
    try:
        q_max = "SELECT MAX(day) AS max_day FROM weather.metrics_daily"
        max_day = pd.read_sql(q_max, conn).iloc[0]["max_day"]
        if pd.isna(max_day):
            log.warning("export_csvs: no metrics yet")
            return

        q_latest = """
            SELECT day, city, var, horizon_hours, mae, rmse, bias
            FROM weather.metrics_daily
            WHERE day = %(day)s AND horizon_hours >= 0
            ORDER BY var, horizon_hours;
        """
        df_latest = pd.read_sql(q_latest, conn, params={"day": max_day})

        min_day = pd.to_datetime(max_day) - pd.Timedelta(days=ndays - 1)
        q_hist = """
            SELECT day, city, var, horizon_hours, mae, rmse, bias
            FROM weather.metrics_daily
            WHERE day >= %(min_day)s AND day <= %(max_day)s
              AND horizon_hours >= 0
            ORDER BY day, city, var, horizon_hours;
        """
        df_hist = pd.read_sql(
            q_hist, conn, params={"min_day": min_day, "max_day": max_day}
        )
    finally:
        conn.close()

    if df_latest.empty or df_hist.empty:
        log.warning(
            "export_csvs: nothing to export (latest=%d, hist=%d)",
            len(df_latest),
            len(df_hist),
        )
        return

    gen = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    df_latest.insert(0, "generated_at", gen)
    df_hist.insert(0, "generated_at", gen)

    day_str = pd.to_datetime(df_latest["day"].max()).strftime("%Y-%m-%d")
    (OUTPUT_DIR / "metrics_latest.csv").write_text(df_latest.to_csv(index=False))
    (OUTPUT_DIR / f"metrics_{day_str}.csv").write_text(df_latest.to_csv(index=False))
    (OUTPUT_DIR / "metrics_history.csv").write_text(df_hist.to_csv(index=False))

    log.info(
        "export_csvs: wrote latest (%d rows) and history (%d rows)",
        len(df_latest),
        len(df_hist),
    )


# ------------------- NEW: Forecast vs Observed (daily) -------------------
def export_daily_forecast_vs_obs():
    """
    Build a tidy daily table of forecast vs observed per city and variable.
    For each hourly valid_time, pick the forecast with the smallest non-negative horizon.
    Then aggregate to day-level:
      - temperature_2m: daily mean
      - precipitation:  daily sum
      - wind_speed_10m: daily mean
    """
    log.info("export_daily_forecast_vs_obs: start")
    ndays = int(os.environ.get("COMPARE_HISTORY_DAYS", "30"))

    conn = psycopg2.connect(**PG)
    try:
        q_max_obs = "SELECT MAX(valid_time) AT TIME ZONE 'UTC' AS max_obs FROM weather.observation_hourly"
        max_obs = pd.read_sql(q_max_obs, conn).iloc[0]["max_obs"]
        if pd.isna(max_obs):
            log.warning("export_daily_forecast_vs_obs: no observations yet")
            return
        min_day = (
            pd.to_datetime(max_obs).normalize() - pd.Timedelta(days=ndays - 1)
        ).to_pydatetime()

        where_city = "AND o.city = %(city)s" if CITY else ""
        params = {"min_dt": min_day, "max_dt": max_obs}
        if CITY:
            params["city"] = CITY

        q = f"""
        WITH joined AS (
            SELECT
                o.city,
                o.valid_time AT TIME ZONE 'UTC' AS valid_time_utc,
                EXTRACT(EPOCH FROM ((o.valid_time AT TIME ZONE 'UTC') - (f.model_run AT TIME ZONE 'UTC'))) / 3600.0 AS horizon_hours,
                f.temperature_2m AS f_temp, o.temperature_2m AS o_temp,
                f.precipitation  AS f_prec, o.precipitation  AS o_prec,
                f.wind_speed_10m AS f_wind, o.wind_speed_10m AS o_wind
            FROM weather.observation_hourly o
            JOIN weather.forecast_hourly f
              ON f.city = o.city
             AND f.valid_time = o.valid_time
            WHERE (o.valid_time AT TIME ZONE 'UTC') >= %(min_dt)s
              AND (o.valid_time AT TIME ZONE 'UTC') <= %(max_dt)s
              {where_city}
        ),
        best_nonneg AS (
            SELECT DISTINCT ON (city, valid_time_utc)
                city, valid_time_utc,
                horizon_hours,
                f_temp, o_temp,
                f_prec, o_prec,
                f_wind, o_wind
            FROM joined
            WHERE horizon_hours >= 0
            ORDER BY city, valid_time_utc, horizon_hours
        )
        SELECT
            (valid_time_utc::date) AS day,
            city,
            AVG(o_temp)  AS o_temp_mean,
            AVG(f_temp)  AS f_temp_mean,
            SUM(o_prec)  AS o_prec_sum,
            SUM(f_prec)  AS f_prec_sum,
            AVG(o_wind)  AS o_wind_mean,
            AVG(f_wind)  AS f_wind_mean
        FROM best_nonneg
        GROUP BY 1,2
        ORDER BY 1,2;
        """

        df = pd.read_sql(q, conn, params=params)
        log.info("export_daily_forecast_vs_obs: aggregated daily rows=%d", len(df))
        if df.empty:
            log.warning("export_daily_forecast_vs_obs: no rows in window")
            return

    finally:
        conn.close()

    long_rows = []
    for _, r in df.iterrows():
        day = pd.to_datetime(r["day"]).date()
        city = r["city"]

        long_rows.append(
            {
                "day": day,
                "city": city,
                "var": "temperature_2m",
                "forecast_value": float(r["f_temp_mean"]),
                "observed_value": float(r["o_temp_mean"]),
            }
        )
        long_rows.append(
            {
                "day": day,
                "city": city,
                "var": "precipitation",
                "forecast_value": float(r["f_prec_sum"]),
                "observed_value": float(r["o_prec_sum"]),
            }
        )
        long_rows.append(
            {
                "day": day,
                "city": city,
                "var": "wind_speed_10m",
                "forecast_value": float(r["f_wind_mean"]),
                "observed_value": float(r["o_wind_mean"]),
            }
        )

    out = pd.DataFrame(long_rows)
    gen = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    out.insert(0, "generated_at", gen)
    (OUTPUT_DIR / "forecast_vs_obs.csv").write_text(out.to_csv(index=False))
    log.info(
        "export_daily_forecast_vs_obs: wrote %d tidy rows to %s",
        len(out),
        OUTPUT_DIR / "forecast_vs_obs.csv",
    )


# ------------------- DAG definition -------------------
with DAG(
    dag_id="compute_metrics",
    start_date=datetime(2025, 8, 1),
    schedule_interval="30 2 * * *",  # daily 02:30 UTC
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Compute daily MAE/RMSE/Bias by horizon + export daily forecast vs observed + dbt build.",
    tags=["weather", "dbt", "analytics"],
) as dag:
    # Ensure target table exists
    ensure = PythonOperator(task_id="ensure_table", python_callable=ensure_table)

    # --- dbt orchestration ---

    # Keep /tmp dirs ready for dbt logs/targets
    prep_dbt_dirs = BashOperator(
        task_id="prep_dbt_dirs",
        bash_command="""
          set -e
          install -d -m 775 "${DBT_LOG_PATH}" "${DBT_TARGET_PATH}"
        """,
        env=DBT_ENV,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt source freshness -s 'source:weather' --profiles-dir . --project-dir ."
        ),
        env=DBT_ENV,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir . --project-dir . && "
            "dbt build --profiles-dir . --project-dir ."
        ),
        env=DBT_ENV,
    )

    export_dbt_daily_kpis_csv = BashOperator(
        task_id="export_dbt_daily_kpis_csv",
        bash_command=(
            "mkdir -p /opt/airflow/docs/data && "
            "psql -h postgres -U airflow -d airflow "
            '-c "\\copy ('
            "select * from analytics_mart.daily_kpis order by day desc, city"
            ") to '/opt/airflow/docs/data/daily_kpis.csv' csv header\""
        ),
        env=DBT_ENV,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt docs generate --profiles-dir . --project-dir . && "
            "mkdir -p /opt/airflow/docs/dbt && "
            "cp -r ${DBT_TARGET_PATH}/* /opt/airflow/docs/dbt/"
        ),
        env=DBT_ENV,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Python metrics pipeline
    compute = PythonOperator(
        task_id="compute_for_yesterday", python_callable=compute_for_yesterday
    )
    export_metrics = PythonOperator(task_id="export_csvs", python_callable=export_csvs)
    export_compare = PythonOperator(
        task_id="export_daily_forecast_vs_obs",
        python_callable=export_daily_forecast_vs_obs,
    )

    # ----- Wiring -----
    ensure >> prep_dbt_dirs >> dbt_source_freshness >> dbt_build
    dbt_build >> export_dbt_daily_kpis_csv >> dbt_docs_generate
    dbt_build >> compute >> export_metrics >> export_compare
