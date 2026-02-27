## ETL Pipeline - Global Banks Market Capitalization
This project implements a simple ETL pipeline using Python. It extracts market capitalization data of the world's largest banks from a Wikipedia archive, converts it into multiple currencies, and loads it into a CSV file and an SQLite databse. It also includes a logging function that records progress at each stage and demonstrates running SQL queries on the loaded data.

### Workflow
1. **Extract:** Scrapes bank names and market cap from the archived Wikipedia page - List of largest banks
2. **Transform:** Converts market cap from USD to other currencies and stores them in new columns
3. **Load:** Stores final data into CSV & SQLite database (also includes example SQL queries)

### Stack / Dependencies
Python, BeautifulSoup, Pandas, NumPy, SQLite

### Outputs Generated
- Largest_banks_marketcaps.csv - final transformed dataset
- LargestBanks.db - SQLite database with table
- code_log.txt - log file tracking the ETL process