import io
import os

import pandas as pd
from minio.error import S3Error
from pytz import timezone

from utils.logger import log

logger = log(f"{__name__}")


def process_raw_operations(*args, **kwargs) -> pd.DataFrame:
    os.environ["minio_endpoint"] = kwargs["minio_endpoint"]
    os.environ["minio_api_access_key"] = kwargs["minio_api_access_key"]
    os.environ["minio_api_access_secret_key"] = kwargs["minio_api_access_secret_key"]
    from stock_portfolio_data.minio_client import minio_client

    try:
        bucket_name = "raw"

        object_name = "operacoes.csv"

        object_data = minio_client.get_object(bucket_name, object_name)
        # Convert the object data to a DataFrame
        operations = pd.read_csv(
            io.BytesIO(object_data.read()),
            sep=";",
            decimal=",",
            encoding="CP1252",
        )
        process_csv_file(operations)
        save_parquet_operations(operations)
        return 0

    except S3Error as err:
        logger.error(f"{err}")
        return -1


def process_csv_file(csv_operations: pd.DataFrame) -> pd.DataFrame:
    csv_operations["lucro_pct"] = (
        csv_operations["lucro_pct"]
        .fillna("0")
        .apply(lambda x: x.replace(",", ".").replace("%", ""))
        .astype(float)
    )
    csv_operations["fluxoCx"] = (
        csv_operations["fluxoCx"]
        .apply(lambda x: x.replace(".", "").replace(",", "."))
        .astype(float)
    )
    csv_operations["vlrInvest"] = (
        csv_operations["vlrInvest"]
        .apply(lambda x: x.replace(".", "").replace(",", "."))
        .astype(float)
    )
    csv_operations["pmAnt"] = (
        csv_operations["pmAnt"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float)
    )
    csv_operations["pmAtual"] = (
        csv_operations["pmAtual"]
        .apply(lambda x: x.replace(".", "").replace(",", "."))
        .astype(float)
    )
    csv_operations["vol"] = (
        csv_operations["vol"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float)
    )
    csv_operations["lucro"] = (
        csv_operations["lucro"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float)
    )

    csv_operations["date"] = pd.to_datetime(csv_operations["date"], dayfirst=True)
    csv_operations["date"] = pd.to_datetime(csv_operations["date"]).dt.tz_localize(
        timezone("America/Sao_Paulo")
    )

    csv_operations = (
        csv_operations.groupby("ativo").apply(_forward_fill_on_date_ini).reset_index(drop=True)
    )
    csv_operations = csv_operations.sort_values(by=["date"])

    return csv_operations


def _forward_fill_on_date_ini(df):
    df["date_ini"] = df["date_ini"].fillna(method="ffill")
    df["date_ini"] = pd.to_datetime(df["date_ini"], dayfirst=True)
    return df


def save_parquet_operations(csv_operations: pd.DataFrame):
    from stock_portfolio_data.minio_client import minio_client

    logger.info("Saving results...")
    parquet_data = csv_operations.to_parquet()
    try:
        minio_client.put_object(
            "interim", f"operacoes.parquet", io.BytesIO(parquet_data), len(parquet_data)
        )
        logger.info(f"Successfully uploaded to interim on MinIO.")
    except S3Error as e:
        logger.error(f"Error occurred: {e}")


if __name__ == "__main__":
    process_raw_operations(
        minio_endpoint="localhost:9000",
        minio_api_access_key="XBJhoPKpkIqIPYmrgPGh",
        minio_api_access_secret_key="UJQb2VWWystUFv6WffVtCxWvakYW0HTlrF9tz0yq",
    )
