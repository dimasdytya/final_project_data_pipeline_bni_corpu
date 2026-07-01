"""
dag_etl_date.py
===============

ETL Pipeline

dim_date.csv
        ↓
stg_date
        ↓
dim_date
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

CONN_ID = "learning_etl"

SOURCE_FILE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "include",
    "dataset",
    "dim_date.csv",
)

DDL_STATEMENTS = """
DROP TABLE IF EXISTS stg_date CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE stg_date (

    date_id         INTEGER,
    full_date       VARCHAR(20),
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week_of_year    INTEGER,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN

);

CREATE TABLE dim_date (

    date_id         INTEGER PRIMARY KEY,

    full_date       DATE,

    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),

    week_of_year    INTEGER,

    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),

    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,

    etl_loaded_at   TIMESTAMP DEFAULT NOW()

);
"""


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

@dag(
    dag_id="dag_etl_date",
    description="ETL dim_date.csv → stg_date → dim_date",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["etl", "date", "dimension"],
    template_searchpath=["/opt/airflow/include/sql/date"],
)

def dag_etl_date():

    # -------------------------------------------------------------------------
    # Create Tables
    # -------------------------------------------------------------------------

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql=DDL_STATEMENTS,
    )

    # -------------------------------------------------------------------------
    # Extract & Load
    # -------------------------------------------------------------------------

    @task()
    def extract_load():

        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(CONN_ID)

        engine = create_engine(
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )

        df = pd.read_csv(SOURCE_FILE)

        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE stg_date"))

        df.to_sql(
            name="stg_date",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        engine.dispose()

        return len(df)

    # -------------------------------------------------------------------------
    # Transform
    # -------------------------------------------------------------------------

    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id=CONN_ID,
        sql="01_transform.sql",
    )

    create_tables >> extract_load() >> transform


dag_etl_date()