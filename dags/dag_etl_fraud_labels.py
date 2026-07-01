"""
dag_etl_fraud_labels.py

ETL Pipeline

fraud_labels.csv
        ↓
stg_fraud_labels
        ↓
UPDATE fact_transactions
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
    "fraud_labels.csv",
)

DDL_STATEMENTS = """
DROP TABLE IF EXISTS stg_fraud_labels;

CREATE TABLE stg_fraud_labels (

    transaction_id      BIGINT,
    transaction_code    VARCHAR(30),
    is_fraud            BOOLEAN,
    fraud_type          VARCHAR(50),
    fraud_score         NUMERIC(5,4),
    flagged_at          VARCHAR(30)

);
"""

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

@dag(
    dag_id="dag_etl_fraud_labels",
    description="Update Fraud Label ke Fact Transactions",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "email_on_failure":False,
    },
    tags=["etl","fraud","transactions"],
    template_searchpath=[
        "/opt/airflow/include/sql/fraud_labels"
    ],
)

def dag_etl_fraud_labels():

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
            connection.execute(text("TRUNCATE TABLE stg_fraud_labels"))

        df.to_sql(
            name="stg_fraud_labels",
            con=engine,
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


dag_etl_fraud_labels()