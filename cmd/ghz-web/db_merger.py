import sqlite3
import argparse

def create_table_if_not_exists(cursor):
    # Create the table in the new database if it does not exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS details (
        id INTEGER,
        created_at DATETIME,
        updated_at DATETIME,
        report_id INTEGER,
        timestamp DATETIME,
        latency BIGINT,
        error VARCHAR(255),
        status VARCHAR(255),
        begin_timestamp DATETIME,
        databroker_enter_timestamp DATETIME,
        databroker_exit_timestamp DATETIME,
        request_process_ts BIGINT,
        client_to_broker_ts BIGINT,
        broker_to_client_ts BIGINT,
        cpu_utilisation REAL,
        mem_utilisation REAL,
        subscription_id VARCHAR(255),
        request_id VARCHAR(255),
        set_id VARCHAR(255)
    );
    """)

def combine_databases(main_db_path, attach_db_path, new_db_path):
    # Connect to the new database (create it if it doesn't exist)
    new_conn = sqlite3.connect(new_db_path)
    new_cursor = new_conn.cursor()

    # Create the table in the new database
    create_table_if_not_exists(new_cursor)

    # Attach the original databases
    new_cursor.execute(f"ATTACH '{main_db_path}' AS main_db")
    new_cursor.execute(f"ATTACH '{attach_db_path}' AS attach_db")

    # Copy data from the main database to the new database
    new_cursor.execute("""
    INSERT INTO details (
        created_at, updated_at, report_id, timestamp, latency, error, status,
        begin_timestamp, databroker_enter_timestamp, databroker_exit_timestamp,
        request_process_ts, client_to_broker_ts, broker_to_client_ts,
        cpu_utilisation, mem_utilisation, subscription_id, request_id, set_id
    )
    SELECT
        created_at, updated_at, report_id, timestamp, latency, error, status,
        begin_timestamp, databroker_enter_timestamp, databroker_exit_timestamp,
        request_process_ts, client_to_broker_ts, broker_to_client_ts,
        cpu_utilisation, mem_utilisation, subscription_id, request_id, set_id
    FROM main_db.details;
    """)

    # Copy data from the attach database to the new database
    new_cursor.execute("""
    INSERT INTO details (
        created_at, updated_at, report_id, timestamp, latency, error, status,
        begin_timestamp, databroker_enter_timestamp, databroker_exit_timestamp,
        request_process_ts, client_to_broker_ts, broker_to_client_ts,
        cpu_utilisation, mem_utilisation, subscription_id, request_id, set_id
    )
    SELECT
        created_at, updated_at, report_id, timestamp, latency, error, status,
        begin_timestamp, databroker_enter_timestamp, databroker_exit_timestamp,
        request_process_ts, client_to_broker_ts, broker_to_client_ts,
        cpu_utilisation, mem_utilisation, subscription_id, request_id, set_id
    FROM attach_db.details;
    """)

    # Commit the changes
    new_conn.commit()

    # Detach the databases
    new_cursor.execute("DETACH DATABASE main_db")
    new_cursor.execute("DETACH DATABASE attach_db")

    # Close the connections
    new_conn.close()

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Combine two SQLite databases into a new database.")
    parser.add_argument('main_db', help="Path to the main database (e.g., databases/main.db)")
    parser.add_argument('attach_db', help="Path to the database to attach (e.g., databases/attach.db)")
    parser.add_argument('new_db', help="Path to the new database (e.g., databases/new_combined.db)")
    args = parser.parse_args()

    # Run the combine function
    combine_databases(args.main_db, args.attach_db, args.new_db)