pip install --upgrade --force-reinstall --no-cache-dir dist/stock_portfolio_data-0.0.0-py3-none-any.whl

airflow dags reserialize

python3 setup.py clean --all

docker exec -it stock-portfolio-data-airflow-scheduler-1 bash

python setup.py bdist_wheel

source .venv/Scripts/activate