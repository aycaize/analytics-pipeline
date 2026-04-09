import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from eptr2 import EPTR2
import snowflake.connector
import time
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

def get_last_loaded_date(table_name: str) -> str:
    """Snowflake'teki tablonun en son tarihini döndürür."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MAX(DATE) FROM {table_name}")
        result = cursor.fetchone()[0]
        if result:
            # son tarihten bir gün sonrasını başlangıç yap
            last_date = pd.to_datetime(result).date()
            next_date = last_date + timedelta(days=1)
            return str(next_date)
        else:
            return "2023-01-01"
    except Exception as e:
        print(f"Error getting last date: {e}")
        return "2023-01-01"
    finally:
        cursor.close()
        conn.close()

def fetch_and_append(endpoint: str, table_name: str, start_date: str, end_date: str, max_retries: int = 3):
    """Belirtilen tarih aralığını çekip tabloya ekler, hata durumunda yeniden dener."""
    print(f"Fetching {endpoint} from {start_date} to {end_date}...")
    
    if start_date > end_date:
        print(f"No new data needed for {table_name} — already up to date.")
        return
    
    for attempt in range(max_retries):
        try:
            df = client.call(endpoint, start_date=start_date, end_date=end_date)
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 30  # 30, 60, 90 saniye bekle
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed for {endpoint}. Skipping.")
                return
    
    if df.empty:
        print(f"No data returned for {table_name}")
        return
    
    df.columns = [col.upper() for col in df.columns]
    df['DATE'] = df['DATE'].astype(str)
    df['LOADED_AT'] = pd.Timestamp.now().isoformat()
    
    print(f"Appending {len(df)} rows to {table_name}...")
    conn = get_snowflake_conn()
    write_pandas(conn, df, table_name, auto_create_table=False, overwrite=False)
    conn.close()
    print(f"Done: {table_name}")

    """Belirtilen tarih aralığını çekip tabloya ekler (overwrite=False)."""
    print(f"Fetching {endpoint} from {start_date} to {end_date}...")
    
    if start_date > end_date:
        print(f"No new data needed for {table_name} — already up to date.")
        return
    
    df = client.call(endpoint, start_date=start_date, end_date=end_date)
    
    if df.empty:
        print(f"No data returned for {table_name}")
        return
    
    df.columns = [col.upper() for col in df.columns]
    df['DATE'] = df['DATE'].astype(str)
    df['LOADED_AT'] = pd.Timestamp.now().isoformat()
    
    print(f"Appending {len(df)} rows to {table_name}...")
    conn = get_snowflake_conn()
    write_pandas(conn, df, table_name, auto_create_table=False, overwrite=False)
    conn.close()
    print(f"Done: {table_name}")

if __name__ == "__main__":
    today = str(datetime.today().date())
    yesterday = str((datetime.today() - timedelta(days=1)).date())

    # her tablo için son yüklenen tarihten itibaren çek
    prices_start = get_last_loaded_date("RAW_PRICES")
    generation_start = get_last_loaded_date("RAW_GENERATION")
    consumption_start = get_last_loaded_date("RAW_CONSUMPTION")

    print(f"RAW_PRICES start: {prices_start}")
    print(f"RAW_GENERATION start: {generation_start}")
    print(f"RAW_CONSUMPTION start: {consumption_start}")

    fetch_and_append("mcp",     "RAW_PRICES",      prices_start,     yesterday)
    fetch_and_append("rt-gen",  "RAW_GENERATION",  generation_start, yesterday)
    fetch_and_append("rt-cons", "RAW_CONSUMPTION", consumption_start, yesterday)