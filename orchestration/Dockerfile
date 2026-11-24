FROM apache/airflow:2.10.5-python3.11

# Keep dbt's Python dependencies separate from the Airflow scheduler.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.10.15 dbt-duckdb==1.10.0 duckdb==1.4.3 \
    && /home/airflow/dbt-venv/bin/python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL icu')"
