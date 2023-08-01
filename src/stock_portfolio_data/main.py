from threading import Thread

from minio.error import S3Error
import io
import pandas as pd
from stock_portfolio_data.stock import Stock
from stock_portfolio_data.minio import minio_client

# import sys

# sys.path.append("opt/airflow/dags/data/interim/")

bucket_name = "interim"

object_name = "operacoes.parquet"


def main():
    try:
        object_data = minio_client.get_object(bucket_name, object_name)
        # Convert the object data to a DataFrame
        operations = pd.read_parquet(io.BytesIO(object_data.read()))
    except S3Error as err:
        print(err)

    # operations = pd.read_parquet("./operações.parquet")
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
