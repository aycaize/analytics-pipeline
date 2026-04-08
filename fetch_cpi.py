import os
import pandas as pd
from dotenv import load_dotenv
from evds import evdsAPI
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

api_key = os.getenv("EVDS_API_KEY")
evds = evdsAPI(api_key)

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def fetch_and_load_cpi(startdate: str, enddate: str):
    print("Fetching CPI data from EVDS...")
    
    df = evds.get_data(
        series=["TP.FG.J0"],
        startdate=startdate,
        enddate=enddate,
        frequency="5"
    )
    
    # kolon isimlerini düzenle
    df.columns = ["DATE_RAW", "CPI_INDEX"]
    
    # yıl ve ay ayrı kolonlar
    df["YEAR"] = df["DATE_RAW"].str.split("-").str[0].astype(int)
    df["MONTH"] = df["DATE_RAW"].str.split("-").str[1].astype(int)
    
    # ham tarihi kaldır
    df = df.drop(columns=["DATE_RAW"])
    
    # metadata ekle
    df["LOADED_AT"] = pd.Timestamp.now().isoformat()
    
    print(df.head())
    print(f"Loading {len(df)} rows to RAW_CPI...")
    
    conn = get_snowflake_conn()
    write_pandas(conn, df, "RAW_CPI", auto_create_table=True, overwrite=True)
    conn.close()
    print("Done: RAW_CPI")

if __name__ == "__main__":
    fetch_and_load_cpi("01-01-2023", "01-04-2026")