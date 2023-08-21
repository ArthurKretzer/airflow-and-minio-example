from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    "owner": "ark",
    "start_date": datetime(2023, 8, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_porfolio_data",
    default_args=default_args,
    description="DAG to run a Python script",
    schedule_interval="0 0 * * *",  # Run once per day at midnight
)


def process_portfolio_data(
    stock_symbol_key, minio_endpoint, minio_api_access_key, minio_api_access_secret_key
):
    from stock_portfolio_data.main import main

    main(
        stock_symbol_key=stock_symbol_key,
        minio_endpoint=minio_endpoint,
        minio_api_access_key=minio_api_access_key,
        minio_api_access_secret_key=minio_api_access_secret_key,
    )


def pre_process_operations(minio_endpoint, minio_api_access_key, minio_api_access_secret_key):
    from stock_portfolio_data.pre_process import process_raw_operations

    process_raw_operations(
        minio_endpoint=minio_endpoint,
        minio_api_access_key=minio_api_access_key,
        minio_api_access_secret_key=minio_api_access_secret_key,
    )


# I could use DockerOperator or KubernetesPodOperator to avoid installation overhead by having a pre-built image with the dependencies installed.
process_portfolio_data_task = PythonVirtualenvOperator(
    task_id="process_portfolio_data",
    python_callable=process_portfolio_data,
    dag=dag,
    provide_context=True,
    op_args=[],
    op_kwargs={
        "stock_symbol_key": Variable.get("stock_symbol_key"),
        "minio_endpoint": Variable.get("minio_endpoint"),
        "minio_api_access_key": Variable.get("minio_api_access_key"),
        "minio_api_access_secret_key": Variable.get("minio_api_access_secret_key"),
    },
    requirements=[
        "/opt/airflow/src/dist/stock_portfolio_data-0.0.0-py3-none-any.whl",
        "minio",
        "pandas",
        "yfinance",
        "stocksymbol",
        "pytz",
        "requests",
        "requests_cache",
        "requests_ratelimiter",
    ],
)

pre_process_operations_task = PythonVirtualenvOperator(
    task_id="pre_process_operations",
    python_callable=pre_process_operations,
    dag=dag,
    provide_context=True,
    op_args=[],
    op_kwargs={
        "minio_endpoint": Variable.get("minio_endpoint"),
        "minio_api_access_key": Variable.get("minio_api_access_key"),
        "minio_api_access_secret_key": Variable.get("minio_api_access_secret_key"),
    },
    requirements=[
        "/opt/airflow/src/dist/stock_portfolio_data-0.0.0-py3-none-any.whl",
        "minio",
        "pandas",
    ],
)

pre_process_operations_task >> process_portfolio_data_task
