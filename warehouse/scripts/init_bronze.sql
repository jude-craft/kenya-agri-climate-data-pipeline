/*
  INITIAL BRONZE LAYER SETUP & HISTORICAL BACKFILL
  ------------------------------------------------
  This script transitions the architecture from external tables 
  to native BigQuery tables. It drops the external references and 
  physically loads the 20-year historical CSVs from GCS into BigQuery.
  
  Moving forward, daily incremental loads are handled automatically 
  via Kestra using the WRITE_APPEND disposition.
*/

-- 1. Drop the external tablesi had initially
DROP EXTERNAL TABLE IF EXISTS `kestra-sandbox-504410.kenya_agri_market.raw_weather`;
DROP EXTERNAL TABLE IF EXISTS `kestra-sandbox-504410.kenya_agri_market.raw_market`;

-- 2. Physically copy the weather data into a native table
LOAD DATA OVERWRITE `kestra-sandbox-504410.kenya_agri_market.raw_weather`
FROM FILES (
  format = 'CSV',
  uris = ['gs://kenya-agri-climate-lake-bucket/raw_weather/*.csv'],
  skip_leading_rows = 1
);

-- 3. Physically copy the market data into a native table
LOAD DATA OVERWRITE `kestra-sandbox-504410.kenya_agri_market.raw_market`
FROM FILES (
  format = 'CSV',
  uris = ['gs://kenya-agri-climate-lake-bucket/raw_market/*.csv'],
  skip_leading_rows = 1
);