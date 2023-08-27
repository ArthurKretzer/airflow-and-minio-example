import csv
import random
from datetime import datetime, timedelta


def generate_mocked_ops():
    # Lista de tickers de ações fictícias
    brl_tickers = [
        "BRDT3",
        "PETR4",
        "VALE3",
        "ITUB4",
        "BBDC4",
        "NEOE3",
        "BABA34",
        "ALZR11",
        "BTLG11",
        "EZTC3",
    ]

    usd_tickers = ["YY", "VZ", "HUYA", "DIBS", "AMZN", "CEPU"]

    # Data inicial para as operações
    start_date = datetime(2022, 1, 1)

    # Lista de tipos de operações
    tipos_op = ["C", "V"]

    # Abrir arquivo CSV para escrita
    with open("dados_operacoes.csv", mode="w", newline="") as file:
        writer = csv.writer(file, delimiter=";")

        # Escrever cabeçalho
        writer.writerow(
            ["ativo", "date", "tipoOP", "qtd", "preco", "taxas", "corretora", "moeda", "ptax"]
        )

        # Gerar 100 linhas de dados
        for _ in range(100):
            ativo = random.choice(brl_tickers)
            date = start_date + timedelta(days=random.randint(1, 547))
            tipo_op = random.choice(tipos_op)
            qtd = random.randint(10, 100) * (-1 if tipo_op == "V" else 1)
            preco = round(random.uniform(10, 100), 2)
            taxas = round(random.uniform(1, 5), 2)
            corretora = random.choice(["Corretora A", "Corretora B", "Corretora C"])
            moeda = "BRL"
            ptax = round(random.uniform(4, 5), 2) if moeda == "USD" else 1

            preco_str = str(preco).replace(".", ",")
            taxas_str = str(taxas).replace(".", ",")
            ptax_str = str(ptax).replace(".", ",")

            # Escrever linha no CSV
            writer.writerow(
                [
                    ativo,
                    date.strftime("%d/%m/%Y"),
                    tipo_op,
                    qtd,
                    preco_str,
                    taxas_str,
                    corretora,
                    moeda,
                    ptax_str,
                ]
            )

        for _ in range(100):
            ativo = random.choice(usd_tickers)
            date = start_date + timedelta(days=random.randint(1, 365))
            tipo_op = random.choice(tipos_op)
            qtd = random.randint(10, 100) * (-1 if tipo_op == "V" else 1)
            preco = round(random.uniform(10, 100), 2)
            taxas = round(random.uniform(1, 5), 2)
            corretora = random.choice(["Corretora A", "Corretora B", "Corretora C"])
            moeda = "USD"
            ptax = round(random.uniform(4, 5), 2) if moeda == "USD" else 1

            preco_str = str(preco).replace(".", ",")
            taxas_str = str(taxas).replace(".", ",")
            ptax_str = str(ptax).replace(".", ",")

            # Escrever linha no CSV
            writer.writerow(
                [
                    ativo,
                    date.strftime("%d/%m/%Y"),
                    tipo_op,
                    qtd,
                    preco_str,
                    taxas_str,
                    corretora,
                    moeda,
                    ptax_str,
                ]
            )

    print("Dados gerados e escritos no arquivo dados_operacoes.csv")
