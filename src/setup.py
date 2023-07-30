import setuptools

setuptools.setup(
    name="stock_portfolio_data",
    packages=setuptools.find_packages(exclude=("dags",)),
    install_requires=[
        "pandas",
        "yfinance",
        "stocksymbol",
        "python-dotenv",
        "pytz",
        "requests",
        "requests_cache",
        "requests_ratelimiter",
    ],
)
