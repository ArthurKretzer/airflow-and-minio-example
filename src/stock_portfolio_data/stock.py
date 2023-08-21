import io
from datetime import datetime

import numpy as np
import pandas as pd
from minio.error import S3Error

from stock_portfolio_data.minio_client import minio_client
from stock_portfolio_data.yf_api import YFApi
from utils.logger import log

logger = log(f"{__name__}")


class Stock(YFApi):
    def __init__(self, ticker):
        super().__init__(ticker)
        self.ticker = ticker

    def _get_operations(self):
        try:
            bucket_name = "interim"

            object_name = "operacoes.parquet"

            object_data = minio_client.get_object(bucket_name, object_name)
            # Convert the object data to a DataFrame
            operations = pd.read_parquet(io.BytesIO(object_data.read()))

        except S3Error as err:
            logger.error(f"{type(err).__name__} - {err}")
            operations = pd.DataFrame()

        if operations.empty:
            raise ValueError(f"No operations found for ticker {self.ticker}.")

        return operations[operations["ativo"] == f"{self.ticker}"]

    def _prepare_stock(self):
        logger.info(f"Building stock infos for ticker {self.ticker}...")
        _ticker_operations = self._get_operations()

        start_date = _ticker_operations["date"].min()
        end_date = datetime.now().date()

        logger.info(f"Searching for {self.ticker} data from {start_date} to {end_date}...")
        _ticker_history = self.get_ticker_history(start_date, end_date)

        logger.info(f"Getting {self.ticker} dividends...")
        _ticker_dividends = _ticker_history[_ticker_history["Dividends"] != 0]["Dividends"]
        _ticker_dividends = _ticker_dividends.reset_index()
        _ticker_dividends = pd.DataFrame(_ticker_dividends).rename(columns={"Date": "date"})

        logger.info(f"Getting {self.ticker} splits...")
        _ticker_splits = _ticker_history[_ticker_history["Stock Splits"] != 0]["Stock Splits"]
        _ticker_splits = pd.DataFrame(_ticker_splits).rename(columns={"Date": "date"})

        _ticker_dividends_and_ops = (
            pd.concat([_ticker_operations, _ticker_dividends])
            .sort_values(by=["date"])
            .reset_index(drop=True)
        )

        _ticker_dividends_and_ops["tipoOP"].fillna("D", inplace=True)
        logger.info(f"Processed {self.ticker} dividends...")

        _ticker_dividends_and_ops = (
            pd.concat([_ticker_dividends_and_ops, _ticker_splits])
            .sort_values(by=["date"])
            .reset_index(drop=True)
        )

        _ticker_dividends_and_ops["Stock Splits"].fillna(1, inplace=True)

        _ticker_dividends_and_ops["tipoOP"] = np.where(
            _ticker_dividends_and_ops["Stock Splits"] > 1,
            "S",
            _ticker_dividends_and_ops["tipoOP"],
        )

        _ticker_dividends_and_ops["tipoOP"] = np.where(
            _ticker_dividends_and_ops["Stock Splits"] < 1,
            "I",
            _ticker_dividends_and_ops["tipoOP"],
        )
        logger.info(f"Processed {self.ticker} splits...")

        _ticker_dividends_and_ops["ativo"].fillna(f"{self.ticker}", inplace=True)

        _ticker_dividends_and_ops.loc[
            :,
            [
                "preco",
                "qtd",
                "lucro_pct",
                "lucro",
                "irrf",
                "vlrInvest",
                "taxas",
                "Dividends",
            ],
        ].fillna(0, inplace=True)

        _ticker_dividends_and_ops.loc[
            :,
            [
                "pmAtual",
                "date_ini",
                "pmAtual_usd",
                "pmAtual_ptax",
                "classe",
                "moeda",
                "qtdAtual",
                "qtdAnt",
                "pmAnt",
            ],
        ].ffill(inplace=True)

        _ticker_dividends_and_ops["ptax"].fillna(1, inplace=True)

        if "Dividends" not in _ticker_dividends_and_ops.columns:
            _ticker_dividends_and_ops["Dividends"] = 0

        if "Stock Splits" not in _ticker_dividends_and_ops.columns:
            _ticker_dividends_and_ops["Stock Splits"] = 1
        logger.info(f"Filled {self.ticker} missing data...")

        return _ticker_dividends_and_ops

    def _adjusted_stock_quantity(self, stock_df):
        # Stock Quantity
        stock_df["qtd_acum"] = stock_df["qtd"].cumsum()

        insignificant_amounts = stock_df["qtd_acum"] < 0.0001

        stock_df["qtd_acum"] = np.where(insignificant_amounts, 0, stock_df["qtd_acum"])
        stock_df["qtd_acum_ant"] = stock_df["qtd_acum"].shift(1).fillna(0)

        # Adjusted Stock Quantity for Stock Splits
        stock_df["adjusted_qtd_acum"] = stock_df["qtd_acum_ant"]

        split_operations = stock_df["Stock Splits"] != 1

        splits = stock_df.loc[split_operations, ["Stock Splits", "qtd_acum"]]

        split_adjust = (splits["Stock Splits"] * splits["qtd_acum"]) - splits["qtd_acum"]

        stock_df.loc[split_operations, "adjusted_qtd_acum"] += split_adjust

        return stock_df

    def _adjust_currency(self, stock_df):
        # Original Currency
        invested_usd = ((stock_df["preco"] * stock_df["qtd"]) + stock_df["taxas"]).cumsum()
        stock_df["invested_usd"] = invested_usd
        sell_ops = stock_df["tipoOP"] == "V"

        stock_df["pm_atual_usd"] = np.where(sell_ops, np.nan, invested_usd / stock_df["qtd_acum"])

        # sometimes 0/0 division occur and they tend to result in np.inf
        inf_values = (stock_df["pm_atual_usd"] == np.inf) | (stock_df["pm_atual_usd"] == -np.inf)

        stock_df["pm_atual_usd"] = np.where(
            inf_values,
            0,
            stock_df["pm_atual_usd"],
        )

        stock_df["pm_atual_usd"].ffill(inplace=True)

        stock_df["pm_ant_usd"] = stock_df["pm_atual_usd"].shift(1).fillna(0)

        return stock_df

    def _calculate_raw_profits(self, stock_df):
        # Currency exchange rate considered
        # Disconsiders dividends
        amount_operated = stock_df["preco"] * stock_df["qtd"]

        amount_operated_plus_taxes = amount_operated + stock_df["taxas"]

        adjusted_amount_operated_plus_taxes = amount_operated_plus_taxes * stock_df["ptax"]

        stock_df["volume"] = adjusted_amount_operated_plus_taxes
        stock_df["fluxo_cx"] = -adjusted_amount_operated_plus_taxes

        amount_invested = adjusted_amount_operated_plus_taxes.cumsum()
        stock_df["invested"] = amount_invested

        dividend_operations = stock_df["tipoOP"] == "D"
        sell_ops = stock_df["tipoOP"] == "V"

        dividends_received = stock_df["Dividends"] * stock_df["ptax"] * stock_df["qtd_acum"]

        stock_df["fluxo_cx"] = np.where(
            dividend_operations,
            dividends_received,
            stock_df["fluxo_cx"],
        )

        stock_df["pm_atual"] = np.where(sell_ops, np.nan, amount_invested / stock_df["qtd_acum"])

        # sometimes 0/0 division occur and they tend to result in np.inf
        inf_values = (stock_df["pm_atual"] == np.inf) | (stock_df["pm_atual"] == -np.inf)

        stock_df["pm_atual"] = np.where(
            inf_values,
            0,
            stock_df["pm_atual"],
        )

        stock_df["pm_atual"].ffill(inplace=True)

        stock_df["pm_ant"] = stock_df["pm_atual"].shift(1).fillna(0)

        price_minus_taxes = stock_df["preco"] - stock_df["taxas"]
        adjusted_price_minus_taxes = price_minus_taxes * stock_df["ptax"]

        stock_price_delta = adjusted_price_minus_taxes - stock_df["pm_ant"]

        invested_delta = stock_df["qtd"] * stock_price_delta

        absolute_price_delta = np.abs(stock_price_delta)

        invested_delta = stock_df["qtd"] * stock_price_delta

        absolute_invested_delta = stock_df["qtd"] * absolute_price_delta

        stock_df["profits"] = np.where(
            sell_ops & (stock_df["pm_atual"] > 0.0),
            -invested_delta,
            np.where(
                sell_ops & (stock_df["pm_atual"] <= 0.0),
                absolute_invested_delta,
                0,
            ),
        )

        stock_price_delta_percentual = (stock_price_delta / stock_df["pm_ant"]) * 100

        stock_df["profits_pct"] = stock_price_delta_percentual

        return stock_df

    def _calculate_adjusted_profits(self, stock_df):
        amount_operated = stock_df["preco"] * stock_df["qtd"]

        amount_operated_plus_taxes = amount_operated + stock_df["taxas"]

        dividends_received = stock_df["Dividends"] * stock_df["qtd_acum"]

        # Considers dividends
        invested_adjusted = (
            (amount_operated_plus_taxes - dividends_received) * stock_df["ptax"]
        ).cumsum()

        stock_df["invested_adjusted"] = invested_adjusted

        sell_ops = stock_df["tipoOP"] == "V"

        stock_df["pm_atual_adjusted"] = np.where(
            sell_ops,
            np.nan,
            invested_adjusted / stock_df["qtd_acum"],
        )

        # sometimes 0/0 division occur and they tend to result in np.inf
        inf_values = (stock_df["pm_atual_adjusted"] == np.inf) | (
            stock_df["pm_atual_adjusted"] == -np.inf
        )

        stock_df["pm_atual_adjusted"] = np.where(
            inf_values,
            0,
            stock_df["pm_atual_adjusted"],
        )

        stock_df["pm_atual_adjusted"].ffill(inplace=True)

        stock_df["pm_ant_adjusted"] = stock_df["pm_atual_adjusted"].shift(1).fillna(0)

        price_minus_taxes = stock_df["preco"] - stock_df["taxas"]

        adjusted_price_minus_taxes = price_minus_taxes * stock_df["ptax"]

        stock_price_delta = adjusted_price_minus_taxes - stock_df["pm_ant_adjusted"]

        absolute_price_delta = np.abs(stock_price_delta)

        invested_delta = stock_df["qtd"] * stock_price_delta

        absolute_invested_delta = stock_df["qtd"] * absolute_price_delta

        stock_df["profits_adjusted"] = np.where(
            sell_ops & (stock_df["pm_atual_adjusted"] > 0.0),
            -invested_delta,
            np.where(
                sell_ops & (stock_df["pm_atual_adjusted"] <= 0.0),
                absolute_invested_delta,
                0,
            ),
        )

        stock_price_delta_percentual = (stock_price_delta / stock_df["pm_ant"]) * 100

        stock_df["profits_adjusted_pct"] = stock_price_delta_percentual

        return stock_df

    def _calculate_current_profits(self, stock_df):
        stock_df = self._calculate_raw_profits(stock_df)
        stock_df = self._calculate_adjusted_profits(stock_df)
        return stock_df

    def _save_results(self, stock_df: pd.DataFrame):
        logger.info("Saving results...")
        csv_data = stock_df.to_csv(sep=";", decimal=",", index=False)
        parquet_data = stock_df.to_parquet()
        try:
            minio_client.put_object(
                "processed",
                f"{self.ticker}.csv",
                io.BytesIO(csv_data.encode("utf-8")),
                len(csv_data),
            )
            minio_client.put_object(
                "processed", f"{self.ticker}.parquet", io.BytesIO(parquet_data), len(parquet_data)
            )
            logger.info(f"Successfully uploaded {self.ticker} to processed on MinIO.")
        except S3Error as e:
            logger.error(f"Error occurred: {type(e).__name__} - {repr(e)}")

    def stock_calculations(self):
        logger.info(f"Calculating for {self.ticker}...")
        try:
            stock_df = self._prepare_stock()
        except ValueError as e:
            logger.error(
                f"Error while calculating {self.ticker}. Exception: {type(e).__name__} - {repr(e)}"
            )
            return None

        logger.info("Adjusting stock quantity...")
        stock_df = self._adjusted_stock_quantity(stock_df)
        logger.info("Adjusting stock currency...")
        stock_df = self._adjust_currency(stock_df)
        logger.info("Calculating profits...")
        stock_df = self._calculate_current_profits(stock_df)
        self._save_results(stock_df)
        logger.info("Done!")

        return stock_df
