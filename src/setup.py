import setuptools

setuptools.setup(
    name="stock_portfolio_data",
    packages=setuptools.find_packages(exclude=("dags",)),
)
