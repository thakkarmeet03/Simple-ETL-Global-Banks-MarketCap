# Importing the required libraries
import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now() # Gets current system timestamp 
    timestamp = now.strftime(timestamp_format)
    # Appending log message to the log file with timestamp
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    response = requests.get(url)
    data = BeautifulSoup(response.text, 'html.parser')
    tables = data.find_all('tbody') # Locating all table bodies in the page
    rows = tables[0].find_all('tr') # Extracting rows from the first table
    entries = []
    # Extracting bank name and market capitalization from each row
    for row in rows[1:]:  # Skipping header row
        cols = row.find_all('td')
        if len(cols) >= 3:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip()
            entries.append([bank_name, float(market_cap)])
    # Creating a DataFrame from the extracted data
    df = pd.DataFrame(entries, columns=table_attribs)
    return df

def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    # Creating a dictionary as {Currency: Rate}
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    # Extracting individual exchange rates
    gbp_rate = float(exchange_rate['GBP'])
    eur_rate = float(exchange_rate['EUR'])
    inr_rate = float(exchange_rate['INR'])
    # Converting market capitalization from USD to other currencies and storing in new columns
    df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    # Loading the transformed DataFrame to a CSV file.
    log_progress('Loading data to a CSV file')
    df.to_csv(csv_path, index=False)

def load_to_db(df, conn, table_name):
    # Loading the transformed DataFrame to a SQL database as a table.
    log_progress('Loading data to the database')
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def run_queries(query, conn):
    # Executing SQL query and printing the result.
    query_output = pd.read_sql(query, conn)
    print('\nQuery: ', query, '\n', query_output)

# Initializing preliminary data
data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'LargestBanks.db'
table_name = 'Largest_banks_marketcaps'
csv_path = 'Largest_banks_marketcaps.csv'
log_progress('Preliminaries complete. Initiating ETL process')

# Extract
df = extract(data_url, table_attribs)
print('Resulting dataframe after extraction:')
print(df)
log_progress('Data extraction complete. Initiating Transformation process.')

# Transform
df = transform(df, './exchange_rate.csv')
print('\nResulting dataframe after transformation:')
print(df)
log_progress('Data transformation complete. Initiating loading process.')

# Load
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file.')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, executing queries')

# Running SQL queries on the loaded data
query_statement1 = "SELECT * FROM Largest_banks_marketcaps"
run_queries(query_statement1, sql_connection)
'''query_statement2 = "SELECT AVG(MC_USD_Billion) FROM Largest_banks_marketcaps"
run_queries(query_statement2, sql_connection)
query_statement3 = "SELECT Name FROM Largest_banks_marketcaps WHERE MC_EUR_Billion > 200"
run_queries(query_statement3, sql_connection)'''

log_progress('Process Complete')
sql_connection.close()
log_progress('Server connection closed')