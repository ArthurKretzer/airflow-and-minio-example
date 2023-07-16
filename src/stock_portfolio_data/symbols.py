import os

import dotenv
from custom_session import CustomSession
from stocksymbol import StockSymbol

from utils.logger import log

logger = log(f"{__name__}")

dotenv.load_dotenv()


class Symbols(StockSymbol):
    def __init__(self, market="US", symbols_only=True):
        api_key = os.environ["stock_symbol_key"]
        super().__init__(api_key)
        self.market = market

        self.symbols_only = symbols_only
        self.session = CustomSession().get_session()
        self.symbol_list = self._get_symbol_list()

    def _get_symbol_list(self):
        symbol_list = self.get_symbol_list(
            market=self.market, symbols_only=self.symbols_only
        )
        if self.market != "US":
            symbol_list = [symbol.split(".")[0] for symbol in symbol_list]
        return symbol_list

    def check_symbol(self, symbol):
        if symbol in self.symbol_list:
            return True
        else:
            return False
