import os
import pandas as pd
from dotenv import load_dotenv
from eptr2 import EPTR2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

client = EPTR2(
    username=os.getenv("EPTR_USERNAME"),
    password=os.getenv("EPTR_PASSWORD")
)

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def fetch_and_load(endpoint: str, table_name: str, date_ranges: list):
    all_dfs = []
    
    for start_date, end_date in date_ranges:
        print(f"Fetching {endpoint} from {start_date} to {end_date}...")
        df = client.call(endpoint, start_date=start_date, end_date=end_date)
        all_dfs.append(df)
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.columns = [col.upper() for col in combined_df.columns]
    combined_df['DATE'] = combined_df['DATE'].astype(str)
    combined_df['LOADED_AT'] = pd.Timestamp.now().isoformat()
    
    print(f"Loading {len(combined_df)} rows to {table_name}...")
    conn = get_snowflake_conn()
    write_pandas(conn, combined_df, table_name, auto_create_table=True, overwrite=True)
    conn.close()
    print(f"Done: {table_name}")

if __name__ == "__main__":
    yearly_ranges = [
        ("2023-01-01", "2023-12-31"),
        ("2024-01-01", "2024-12-31"),
    ]

    quarterly_ranges = [
        ("2023-01-01", "2023-03-31"),
        ("2023-04-01", "2023-06-30"),
        ("2023-07-01", "2023-09-30"),
        ("2023-10-01", "2023-12-31"),
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
        ("2024-10-01", "2024-12-31"),
    ]
    
    fetch_and_load("mcp",     "RAW_PRICES",      yearly_ranges)
    fetch_and_load("rt-gen",  "RAW_GENERATION",  quarterly_ranges)
    fetch_and_load("rt-cons", "RAW_CONSUMPTION", quarterly_ranges)