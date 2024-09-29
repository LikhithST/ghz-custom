import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def calculate_latency_microseconds(start_time_str, additional_time_ns, time_diffs_ns, additional_nanoseconds):
    # Convert the start time to a datetime object
    start_time = datetime.fromisoformat(start_time_str[:-6])
    
    # Convert the single additional time in nanoseconds to timedelta
    total_microseconds = additional_time_ns / 1000
    additional_timedelta = timedelta(microseconds=total_microseconds)
    
    # Add the additional time to the start time
    new_time = start_time + additional_timedelta
    
    # Add the time differences provided in nanoseconds
    for diff in time_diffs_ns:
        time1 = datetime.fromisoformat(diff[0])
        time2 = datetime.fromisoformat(diff[1])
        time_difference = time1 - time2
        new_time += time_difference
    
    # Convert additional nanoseconds to microseconds and add to the new time
    microseconds_to_add = additional_nanoseconds / 1000
    final_time = new_time + timedelta(microseconds=microseconds_to_add)
    
    # Calculate the latency
    latency = final_time - start_time
    
    # Convert latency to microseconds
    latency_microseconds = latency.total_seconds() * 1_000_000
    
    return latency_microseconds

# Connect to the SQLite database
conn = sqlite3.connect('./data/ghz.db')
cursor = conn.cursor()

# Load data from the "details" table into a DataFrame
query = "SELECT * FROM details"
df = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Group the DataFrame by 'subscription_id' and 'request_id'
grouped = df.groupby(['set_id', 'request_id'])

# Prepare a list to collect results
results = []

# Iterate through each group to find the publisher and extract the required values
for (set_id, request_id), group in grouped:
    publisher_row = group[group['databroker_enter_timestamp'] != '0001-01-01 00:00:00+00:00']
    if not publisher_row.empty:
        publisher_row = publisher_row.iloc[0]  # Take the first matching row
        begin_timestamp_publisher = publisher_row['begin_timestamp']
        client_to_broker_ts_publisher = publisher_row['client_to_broker_ts']
        request_process_timestamp_for_publisher = publisher_row['request_process_ts']
        databroker_enter_timestamp_for_publisher = publisher_row['databroker_enter_timestamp']
        
        non_publisher_rows = group[group['id'] != publisher_row['id']]
        
        # Loop through non-publisher rows and collect details
        for index, row in non_publisher_rows.iterrows():
            databroker_exit_timestamp_subscriber = row['databroker_exit_timestamp']
            broker_to_client_ts_subscriber = row['broker_to_client_ts']
            broker_client_set_id = row['set_id']
            
            # Calculate latency
            start_time_str = begin_timestamp_publisher
            additional_time_ns = client_to_broker_ts_publisher 
            time_diffs_ns = [
                (databroker_exit_timestamp_subscriber, databroker_enter_timestamp_for_publisher)
            ]
            additional_nanoseconds = broker_to_client_ts_subscriber

            latency = calculate_latency_microseconds(start_time_str, additional_time_ns, time_diffs_ns, additional_nanoseconds)
            
            # Collect the result
            results.append({
                'set_id': broker_client_set_id,
                'request_id': request_id,
                'latency_microseconds': latency
            })

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Save the results to a CSV file
results_df.to_csv('latency_results.csv', index=False)

print("Results have been written to latency_results.csv")
