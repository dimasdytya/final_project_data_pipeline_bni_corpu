"""
dag_etl_transactions.py
=======================

ETL Pipeline

transactions.csv
        ↓
stg_transactions
        ↓
fact_transactions
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
    "transactions.csv",
)

DDL_STATEMENTS = """
DROP TABLE IF EXISTS stg_transactions CASCADE;
DROP TABLE IF EXISTS fact_transactions CASCADE;

CREATE TABLE stg_transactions (

    transaction_id      BIGINT,
    transaction_code    VARCHAR(30),
    account_id          INTEGER,
    customer_id         INTEGER,
    branch_id           INTEGER,
    channel_id          INTEGER,
    transaction_date    VARCHAR(20),
    transaction_at      VARCHAR(30),
    transaction_type    VARCHAR(30),
    amount              NUMERIC(18,2),
    balance_before      NUMERIC(18,2),
    balance_after       NUMERIC(18,2),
    status              VARCHAR(20),
    reference_no        VARCHAR(50)

);

CREATE TABLE fact_transactions (

    transaction_id      BIGINT PRIMARY KEY,

    date_id             INTEGER NOT NULL,

    account_id          INTEGER NOT NULL,
    customer_id         INTEGER NOT NULL,
    branch_id           INTEGER NOT NULL,
    channel_id          INTEGER NOT NULL,

    transaction_code    VARCHAR(30),
    transaction_at      TIMESTAMP,
    transaction_type    VARCHAR(30),

    amount              NUMERIC(18,2),
    balance_before      NUMERIC(18,2),
    balance_after       NUMERIC(18,2),

    status              VARCHAR(20),
    reference_no        VARCHAR(50),

    is_fraud            BOOLEAN DEFAULT FALSE,
    fraud_type          VARCHAR(50),
    fraud_score         NUMERIC(5,4),
    flagged_at          TIMESTAMP,

    etl_loaded_at       TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_date
        FOREIGN KEY(date_id)
        REFERENCES dim_date(date_id),

    CONSTRAINT fk_customer
        FOREIGN KEY(customer_id)
        REFERENCES dim_customers(customer_id),

    CONSTRAINT fk_account
        FOREIGN KEY(account_id)
        REFERENCES dim_accounts(account_id),

    CONSTRAINT fk_branch
        FOREIGN KEY(branch_id)
        REFERENCES dim_branches(branch_id),

    CONSTRAINT fk_channel
        FOREIGN KEY(channel_id)
        REFERENCES dim_channels(channel_id)

);
"""

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

@dag(
    dag_id="dag_etl_transactions",
    description="ETL transactions.csv → stg_transactions → fact_transactions",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "email_on_failure":False,
    },
    tags=["etl","fact","transactions"],
    template_searchpath=["/opt/airflow/include/sql/transactions"],
)

def dag_etl_transactions():

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql=DDL_STATEMENTS,
    )

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
            connection.execute(text("TRUNCATE TABLE stg_transactions"))

        df.to_sql(
            "stg_transactions",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        engine.dispose()

        return len(df)

    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id=CONN_ID,
        sql="01_transform.sql",
    )

    create_tables >> extract_load() >> transform


dag_etl_transactions()