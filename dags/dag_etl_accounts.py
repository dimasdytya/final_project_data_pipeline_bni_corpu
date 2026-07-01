"""
dag_etl_channels.py
===================

ETL Pipeline:
channels.csv
        ↓
stg_channels
        ↓
dim_channels
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
    "accounts.csv",
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_accounts (
    account_id      INTEGER,
    account_no      VARCHAR(20),
    account_type    VARCHAR(20),
    product_name    VARCHAR(100),
    currency        VARCHAR(10),
    open_date       VARCHAR(20),
    close_date      VARCHAR(20),
    status          VARCHAR(20),
    interest_rate   NUMERIC(5,2),
    customer_id     INTEGER,
    branch_id       INTEGER
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id          INTEGER PRIMARY KEY,
    account_no          VARCHAR(20),
    account_type        VARCHAR(20),
    product_name        VARCHAR(100),
    currency            VARCHAR(10),
    open_date           DATE,
    close_date          DATE,
    status              VARCHAR(20),
    interest_rate       NUMERIC(5,2),
    customer_id         INTEGER,
    branch_id           INTEGER,
    account_age_years   INTEGER,
    is_closed           BOOLEAN,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);
"""

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

@dag(
    dag_id="dag_etl_accounts",
    description="ETL accounts.csv → stg_accounts → dim_accounts",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["etl", "accounts", "postgresql"],
    template_searchpath=["/opt/airflow/include/sql/accounts"],
)

def dag_etl_accounts():

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
            connection.execute(text("TRUNCATE TABLE stg_accounts"))

        df.to_sql(
            name="stg_accounts",
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

    # -------------------------------------------------------------------------
    # Dependency
    # -------------------------------------------------------------------------

    create_tables >> extract_load() >> transform


dag_etl_channels()