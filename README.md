# Kenya Agri-Market & Climate Impact Pipeline
An end-to-end data engineering pipeline for analyzing the relationship between climate variability and agricultural commodity prices in Kenya.

## Objective
Agriculture is the backbone of Kenya's economy, yet it is highly vulnerable to extreme climate events. This data engineering pipeline extracts 20 years of historical weather data across key agricultural counties and cross-references it with staple commodity market prices. The goal is to provide a structured data warehouse that analysts can use to determine how droughts, floods, and temperature spikes impact food inflation and supply chains.

## Architecture & Tech Stack

```
kenya-agri-climate-pipeline/
├── README.md               # Your master documentation
├── extract/                # Python scripts for pulling API data
│   ├── weather_extract.py  # Your improved Open-Meteo script
│   └── market_extract.py   # Your new script for agricultural prices
├── orchestration/          # Kestra YAML flows
│   └── main_pipeline.yml
├── warehouse/              # BigQuery SQL scripts (Your Module 3 work)
│   ├── 1_external_tables.sql
│   └── 2_partitioned_clustered.sql
└── analytics/              # Placeholder for Module 4 (dbt models)
```


*   **Data Sources:** Open-Meteo API (Climate Data), [Insert Market Data API] (Commodity Prices)
*   **Orchestration:** Kestra
*   **Data Lake:** Google Cloud Storage (GCS)
*   **Data Warehouse:** Google BigQuery
*   **Transformations:** dbt (Data Build Tool) - *Coming Soon*

##  Pipeline Flow
1.  **Extract:** Python scripts pull daily weather and monthly market prices.
2.  **Load (Lake):** Raw data is loaded into GCS as Parquet/CSV files.
3.  **Load (Warehouse):** External tables are created in BigQuery and materialized into native, partitioned tables.
4.  **Transform:** Raw data is modeled into a Star Schema (Fact and Dimension tables) using dbt for downstream analytics. 