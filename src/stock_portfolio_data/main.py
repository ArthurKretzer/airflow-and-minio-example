import io
import os
from threading import Thread

import pandas as pd

from stock_portfolio_data.minio_client import minio_client
from stock_portfolio_data.stock import Stock

bucket_name = "interim"

object_name = "operacoes.parquet"


def main(*args, **kwargs):
    os.environ["stock_symbol_key"] = kwargs["stock_symbol_key"]
    os.environ["minio_endpoint"] = kwargs["minio_endpoint"]
    os.environ["minio_api_access_key"] = kwargs["minio_api_access_key"]
    os.environ["minio_api_access_secret_key"] = kwargs["minio_api_access_secret_key"]
    try:
        object_data = minio_client.get_object(bucket_name, object_name)
        # Convert the object data to a DataFrame
        operations = pd.read_parquet(io.BytesIO(object_data.read()))
    except Exception as err:
        print(err)

    # operations = pd.read_parquet("./operações.parquet")
    tickers = operations["ativo"].unique()

    thread_list = [Thread(target=Stock(ticker).stock_calculations) for ticker in tickers]

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()


if __name__ == "__main__":
    main(
        stock_symbol_key="f5892249-b4e8-448b-8b91-6a861f02c311",
        minio_endpoint="localhost:9000",
        minio_api_access_key="XBJhoPKpkIqIPYmrgPGh",
        minio_api_access_secret_key="UJQb2VWWystUFv6WffVtCxWvakYW0HTlrF9tz0yq",
    )
