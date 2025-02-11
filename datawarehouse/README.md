# THIS IS ANWERS SHOWD BY QUERIES:

## SETUP CODE
```
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
```

### 1)  20,332,093
``` 
select count(*) as NOofRecodrs from taxi_tripdata.external_yellow_tripdata
 ```
----
### 2)  0 MB for the External Table and 155.12 MB for the Materialized Table
```
 SELECT 
  COUNT(DISTINCT PULocationID) as count_for_external_table
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`;

SELECT 
   COUNT(DISTINCT PULocationID) as count_for_regular_table
from `zoomcamp-week3.taxi_tripdata.regular_yellow_tripdata` ;
```
----
# 3)  BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
``` SELECT 
   PULocationID ,DOLocationID
from `zoomcamp-week3.taxi_tripdata.regular_yellow_tripdata`
```
-----
### 4)  8,333
```
 select 
  count(fare_amount)
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`
where fare_amount = 0
```
----
### 5)  Partition by tpep_dropoff_datetime and Cluster on VendorID
```
create or replace table `zoomcamp-week3.taxi_tripdata.partitioned_clustered_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY (VendorID)
AS 
select * 
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`
```
----
### 6)  310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
```
select VendorID
from `zoomcamp-week3.taxi_tripdata.regular_yellow_tripdata`
where DATE(tpep_dropoff_datetime) between DATE('2024-03-01') and DATE('2024-03-15');


select VendorID
from `zoomcamp-week3.taxi_tripdata.partitioned_clustered_yellow_tripdata`
where DATE(tpep_dropoff_datetime) between DATE('2024-03-01') and DATE('2024-03-15');
```

### 7)  GCP Bucket
External tables in BigQuery do not store data inside BigQuery itself. Instead, they reference data that is stored in Google Cloud Storage (GCP Bucket).
This allows BigQuery to query data directly from the storage bucket without importing it into BigQuery’s internal storage.

### 8)  False
 clustering is unnecessary:
❌ Small datasets where full table scans are already fast.
❌ If queries do not filter or group by clustered columns.
