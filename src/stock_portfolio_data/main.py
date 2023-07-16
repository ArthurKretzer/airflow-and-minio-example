import os
from threading import Thread

import pandas as pd
from stock import Stock

if __name__ == "__main__":
    operations = pd.read_parquet(os.path.join("data", "interim", "operações.parquet"))
    tickers = operations["ativo"].unique()

    thread_list = [
        Thread(target=Stock(ticker).stock_calculations) for ticker in tickers
    ]

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()
