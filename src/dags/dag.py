from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    "owner": "ark",
    "start_date": datetime(2023, 8, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env": {"stock_symbol_key": Variable.get("stock_symbol_key")},
}

dag = DAG(
    "stock_porfolio_data",
    default_args=default_args,
    description="DAG to run a Python script",
    schedule_interval="0 0 * * *",  # Run once per day at midnight
)


def run_python_script(stock_symbol_key):
    from stock_portfolio_data.main import main

    print("stock_symbol_key", stock_symbol_key)

    main(stock_symbol_key=stock_symbol_key)


# I could use DockerOperator or KubernetesPodOperator to avoid installation overhead by having a pre-built image with the dependencies installed.
run_script_task = PythonVirtualenvOperator(
    task_id="stock_porfolio_data_script",
    python_callable=run_python_script,
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
        "python-dotenv",
        "pytz",
        "requests",
        "requests_cache",
        "requests_ratelimiter",
    ],
)

run_script_task
