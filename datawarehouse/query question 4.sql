select 
  count(fare_amount)
from `zoomcamp-week3.taxi_tripdata.external_yellow_tripdata`
where fare_amount = 0