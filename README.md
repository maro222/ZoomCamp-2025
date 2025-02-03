# HWweek2-kestra-orchestration

### First question:
##### answer: 128.3 MB
##### We commented the year and month inputs and use triggers instead and do backfills to the specific date (year:2020, month:12) and also commented the purgefiles property to be able to see the size of file in kestra outputfiles



### Second question:
##### answer: green_tripdata_2020-04.csv
##### this how it's wrutten as code in yml file (file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv")


### Third question:
##### answer: 
##### We had to sync it with google cloud as it was big data and doing it locally will crash the OS, We backfill with the mention date from 2020-1-1 to 2021-1-1 and since the trigger do it in 9 am so it will bring all the month of this year


### Fourth Question:
##### answer: 1,734,051
##### Since green taxi data is small we do it locally , we Backfill it for the same data as previous question and answered with the records appear in PgAdmin


### Fifth Question:
##### answer: 1,925,152
##### Since it;s only one yellow file we did it locally, We backfill with specific date(year:2021, month:3)

### Sixth Question:
##### answer: Add a timezone property set to America/New_York in the Schedule trigger configuration
##### there is no location property in Schedule trigger configuration so we used timezone and set it to specific region(newyork)
