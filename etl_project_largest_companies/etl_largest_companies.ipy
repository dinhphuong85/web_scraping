import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

url = "https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue"
table_attribs = [
    "Rank ",
    "Name",
    "Industry",
    "Revenue_USD_millions",
    "Revenue_growth_%",
    "Employees",
    "Headquarters",
]
db_name = "Largest_Companies.db"
table_name = "companies"
csv_path = "./Largest_companies.csv"


def extract(url, table_attribs):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    tables = soup.find_all("table")
    df = pd.DataFrame(columns=table_attribs)
    rows = tables[1].find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 0:
            data = [col.text.strip() for col in cols]
            df.loc[len(df)] = data
    return df


def transform(df):
    for col in df.columns[3:6]:
        df[col] = df[col].map(lambda x: float(str(x).replace(",", "").replace("%", "")))
    return df


def load_to_csv(df, csv_path):
    df.to_csv("Largest_companies.csv", index=False)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df)

log_progress("Data transformation complete. Initiating loading process")

load_to_csv(df, csv_path)

log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)

log_progress("SQL Connection initiated.")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as table. Running the query")

query_statement = f"SELECT * from {table_name} WHERE [Revenue_growth_%] > 30"
run_query(query_statement, sql_connection)

query_statement = f"SELECT * from {table_name} WHERE Revenue_USD_millions > 100000"
run_query(query_statement, sql_connection)

log_progress("Process Complete.")

sql_connection.close()
