import sqlite3
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"

table_attribs = ["Rank", "Bank_name", "MC_USD_Billion"]
db_name = "Largest_banks.db"
table_name = "banks"
exchange_rate = "./exchange_rate.csv"
csv_path = "./largest_bank.csv"


def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    tables = soup.find_all("table")
    rows = tables[0].find_all("tr")
    df = pd.DataFrame(columns=table_attribs)
    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 0:

            data = [col.get_text(strip=True) for col in cols]

            df.loc[len(df)] = data
    return df


def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_banks_project_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


def transform(df, exchange_rate):
    df["MC_USD_Billion"] = df["MC_USD_Billion"].map(
        lambda x: float(str(x).replace(",", ""))
    )
    rates = pd.read_csv(exchange_rate, index_col="Currency").squeeze()
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * rates["GBP"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * rates["EUR"]).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * rates["INR"]).round(2)
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, exchange_rate)

log_progress("Data transformation complete. Initiating loading process")

load_to_csv(df, csv_path)

log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)

log_progress("SQL Connection initiated.")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as table. Running the query")

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Bank_name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

sql_connection.close()

log_progress("Server Connection closed.")
