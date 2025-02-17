# Solutions

* 1)dlt version: 1.6.1
 
* 2)
```
@dlt.resource(name="rides")   
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page
```


* 3) 10000
```
df = pipeline.dataset(dataset_type="default").rides.df()
len(df)            # or
df.count().max()   # or
df.shape[0]
```


* 4) [(12.3049,)] minutes
```
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
```
