from datetime import timedelta

import yfinance as yf
from custom_session import CustomSession
from symbols import Symbols

from utils.logger import log

logger = log("ingestao")


class YFApi:
    def __init__(self, ticker):
        self.ticker = ticker
        self.session = CustomSession().get_session()
        self.ticker_info = None

    def _is_ticker_valid(self):
        if Symbols("US").check_symbol(self.ticker):
            logger.info(f"{self.ticker} is in US market")
            return True
        elif Symbols("BR").check_symbol(self.ticker):
            logger.info(f"{self.ticker} is in SA market")
            self.ticker = self.ticker + ".SA"
            return True
        else:
            logger.error(f"Ticker {self.ticker} not found in US and SA markets.")
            return False

    def get_ticker(self):
        if not self._is_ticker_valid():
            raise ValueError(f"Ticker {self.ticker} not found in US and SA markets.")

        logger.info("Requesting ticker info...")
        ticker = yf.Ticker(self.ticker, session=self.session)
        self.ticker_info = ticker
        return ticker

    def get_ticker_history(self, start_date, end_date, interval="1d"):
        if self.ticker_info is None:
            self.get_ticker()
        # History end date on yf is exclusive, but we want it to be virtualy inclusive
        end_date += timedelta(days=1)
        history = self.ticker_info.history(
            start=start_date, end=end_date, interval=interval
        )
        if history.empty:
            raise ValueError(
                f"No history found for {self.ticker} between {start_date} and {end_date}"
            )
        return history
