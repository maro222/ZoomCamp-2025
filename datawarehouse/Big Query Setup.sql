-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi_tripdata.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3___2025/yellow_tripdata_2024-*.parquet']
);


-- Creating regular table
CREATE OR REPLACE TABLE `taxi_tripdata.regular_yellow_tripdata`
AS 
SELECT * 
FROM `taxi_tripdata.external_yellow_tripdata`;
