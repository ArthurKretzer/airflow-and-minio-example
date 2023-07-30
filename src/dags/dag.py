from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from stock_portfolio_data.main import main

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
    main()


run_script_task = PythonOperator(
    task_id="stock_porfolio_data_script", python_callable=run_python_script, dag=dag
)

run_script_task
