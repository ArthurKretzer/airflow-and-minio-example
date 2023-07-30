import os
from threading import Thread

import pandas as pd
from stock_portfolio_data.stock import Stock

import sys

sys.path.append("opt/airflow/dags/data/interim/")


def main():
    operations = pd.read_parquet("./operações.parquet")
    tickers = operations["ativo"].unique()

    thread_list = [
        Thread(target=Stock(ticker).stock_calculations) for ticker in tickers
    ]

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()


if __name__ == "__main__":
    main()
