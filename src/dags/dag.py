from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    "owner": "ark",
    "start_date": datetime(2023, 7, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_porfolio_data",
    default_args=default_args,
    description="DAG to run a Python script",
    schedule_interval="0 0 * * *",  # Run once per day at midnight
)


def run_python_script():
    from stock_portfolio_data.main import main

    main()


run_script_task = PythonVirtualenvOperator(
    task_id="stock_porfolio_data_script",
    python_callable=run_python_script,
    dag=dag,
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
