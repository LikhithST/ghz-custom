import sqlite3
import pandas as pd

# Path to your SQLite database
db_path = './data/new.db'

# Connect to the SQLite database
connection = sqlite3.connect(db_path)

# Query to list all tables in the database
tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
tables_df = pd.read_sql_query(tables_query, connection)

# Display the list of tables
print("List of tables:")
print(tables_df)

# Create a DataFrame to hold the columns and their types
columns_info_list = []

# Iterate over each table and get the columns with their types
for table_name in tables_df['name']:
    
    # Query to get column information for the current table
    columns_query = f"PRAGMA table_info({table_name});"
    columns_df = pd.read_sql_query(columns_query, connection)
    
    # Add the table name, column name, and column type to the list
    for _, row in columns_df.iterrows():
        columns_info_list.append({
            'Table': table_name,
            'Column': row['name'],
            'Type': row['type']
        })

# Convert the list to a DataFrame
columns_info_df = pd.DataFrame(columns_info_list)

# Display the DataFrame with columns and their types
print("\nColumns and their types:")
print(columns_info_df)

# Close the connection
connection.close()
