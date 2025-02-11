SELECT 
  COUNT(DISTINCT PULocationID) as count_for_external_table
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`;


SELECT 
   COUNT(DISTINCT PULocationID) as count_for_regular_table
from `zoomcamp-week3.taxi_tripdata.regular_yellow_tripdata` ;

