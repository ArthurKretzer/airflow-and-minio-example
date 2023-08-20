# Data processing for stock-portfolio project

## Activate venv in Linux

```bash
source .venv/Scripts/activate
```

## Activate venv in windows

```powershell
./.venv/Scripts/Activate.ps1
```

## Install requirements

```bash
pip install -r requirements.txt
```

## Build stock_portfolio_data module

```bash
cd src
python3 setup.py clean --all
python3 setup.py bdist_wheel
pip install --upgrade --force-reinstall --no-cache-dir dist/stock_portfolio_data-0.0.0-py3-none-any.whl
```

## Access container

``` bash
docker exec -it stock-portfolio-data-airflow-scheduler-1 bash
```

### Reload airflow dags

```bash
airflow dags reserialize
```

### Export variables

```bash
airflow variables export my_variables.json
```
