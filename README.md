# Turkey Energy Market Analytics Pipeline

An end-to-end analytics pipeline that collects, models, and visualizes Turkey's electricity market data — from raw API ingestion to executive dashboards.

## Overview

This project analyzes Turkey's electricity market by integrating hourly generation, consumption, and price data from EPIAS (Energy Markets Operating Company) with monthly inflation data from TCMB (Central Bank of Turkey). The pipeline enables both nominal and inflation-adjusted price analysis across 2023–2026.

**Key business questions answered:**
- How have real electricity prices (inflation-adjusted) evolved over time?
- What is the relationship between renewable energy penetration and market clearing prices?
- Which seasons and hours show the highest price volatility?
- How does Turkey's generation-consumption balance change across periods?

## Architecture

```
EPIAS API (Hourly)         TCMB EVDS API (Monthly)
        │                              │
        ▼                              ▼
  Python Ingestion            Python Ingestion
  (GitHub Actions)            (GitHub Actions)
        │                              │
        └──────────────┬───────────────┘
                       ▼
            Snowflake RAW Schema
                       │
                       ▼
          dbt Transformations
          ├── Staging Layer
          ├── Intermediate Layer
          ├── Fact Tables
          └── Mart Layer
                       │
                       ▼
            Power BI Dashboard
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, eptr2, evds |
| Orchestration | GitHub Actions |
| Storage | Snowflake |
| Transformation | dbt Core |
| Testing & CI | dbt tests, GitHub Actions |
| Visualization | Power BI |

## Data Sources

**EPIAS Transparency Platform**
- Hourly Market Clearing Price (MCP/PTF) — USD, EUR, TRY
- Hourly electricity generation by source (wind, solar, hydro, gas, coal...)
- Hourly electricity consumption
- Coverage: 2023–2026

**TCMB EVDS**
- Monthly CPI index (TP.FG.J0)
- Used for inflation adjustment of nominal prices
- Coverage: 2023–2026

## Project Structure

```
analytics-pipeline/
├── fetch_prices_incremental.py
├── fetch_prices_full.py
├── fetch_cpi.py
├── requirements.txt
└── .github/
    └── workflows/
        ├── daily_ingestion.yml
        └── monthly_ingestion.yml
```

## dbt Data Model

```
Snowflake Layers
│
├── RAW
│   ├── raw_prices
│   ├── raw_generation
│   ├── raw_consumption
│   └── raw_cpi
│
├── STAGING
│   ├── stg_prices
│   ├── stg_generation
│   ├── stg_consumption
│   └── stg_cpi
│
├── INTERMEDIATE
│   └── int_hourly_market
│
└── MARTS
    ├── dim_date
    ├── dim_generation_source
    ├── fact_hourly_prices
    ├── fact_hourly_generation
    ├── fact_hourly_consumption
    ├── mart_price_volatility
    ├── mart_renewable_share
    ├── mart_price_seasonal
    └── mart_cpi
```

## Key Insights

- Real electricity prices (inflation-adjusted) have declined ~15% since 2023 peak despite nominal increases
- Renewable energy share reached 55%+ in spring months, driven by hydro and wind
- Price volatility (CV%) is highest in summer months due to cooling demand and reduced hydro capacity
- Strong negative correlation between renewable share and MCP during peak generation hours

## Automation

| Job | Schedule | Description |
|-----|----------|-------------|
| daily_ingestion | Daily 09:00 TR | Incremental EPIAS data load |
| monthly_ingestion | 5th of month 09:00 TR | Full CPI data refresh |

## How It Works

**Daily Ingestion (Automated)**
GitHub Actions runs `fetch_prices_incremental.py` every day at 09:00 TR time. 
The script checks the last loaded date in Snowflake and fetches only new data from EPIAS API.

**Monthly CPI Ingestion (Automated)**
GitHub Actions runs `fetch_cpi.py` on the 5th of each month.
Full refresh of CPI data from TCMB EVDS API.

**dbt Transformations**
After ingestion, dbt models transform raw data through staging → intermediate → marts layers.
46 data quality tests run across all models.

**Local Setup (requires Snowflake + EPIAS + EVDS credentials)**
```bash
git clone https://github.com/aycaize/analytics-pipeline.git
cd analytics-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Create .env file with your credentials
# Run ingestion
python fetch_prices_incremental.py
```
