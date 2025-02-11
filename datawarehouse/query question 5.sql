create or replace table `zoomcamp-week3.taxi_tripdata.partitioned_clustered_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY (VendorID)
AS 
select * 
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`