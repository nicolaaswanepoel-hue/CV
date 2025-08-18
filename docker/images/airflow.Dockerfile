FROM apache/airflow:2.9.3

# Install extra libs honoring Airflow's constraints (no hard pins)
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    pandas \
    psycopg2-binary \
    meteostat \
    duckdb

#DBT install
RUN pip install --no-cache-dir "dbt-postgres>=1.7,<2.0"

