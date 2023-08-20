from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

from utils.logger import log

logger = log(f"{__name__}")


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


class CustomSession:
    def __init__(self) -> None:
        self.session = self.create_session()

    def create_session(self):
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND * 5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        return session

    def get_session(self):
        return self.session
