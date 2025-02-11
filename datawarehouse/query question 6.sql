select VendorID
from `zoomcamp-week3.taxi_tripdata.regular_yellow_tripdata`
where DATE(tpep_dropoff_datetime) between DATE('2024-03-01') and DATE('2024-03-15');


select VendorID
from `zoomcamp-week3.taxi_tripdata.partitioned_clustered_yellow_tripdata`
where DATE(tpep_dropoff_datetime) between DATE('2024-03-01') and DATE('2024-03-15');
