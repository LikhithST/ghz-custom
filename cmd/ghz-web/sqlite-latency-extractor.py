import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Connect to the SQLite database
conn = sqlite3.connect('/work/data/ghz.db')
cursor = conn.cursor()

# Load data from the "details" table into a DataFrame
query = 'SELECT * FROM details WHERE "set_id"="0"'
# query = 'SELECT * FROM details'

df = pd.read_sql_query(query, conn)

print(len(df))
print((df))



# Connect to the SQLite database
conn = sqlite3.connect('/work/data/ghz-publisher-info.db')
cursor = conn.cursor()

# Load data from the "details" table into a DataFrame
query = 'SELECT * FROM details WHERE "set_id"="0"'
# query = 'SELECT * FROM details'

df = pd.read_sql_query(query, conn)

print(len(df))
print((df))




# Close the database connection
conn.close()
