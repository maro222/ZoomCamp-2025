import dlt
import requests
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import io
import gzip
import time

# Constants
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]  # January to December

# Define the schema for FHV Trip Data
schema = pa.schema([
    ("dispatching_base_num", pa.string()),
    ("pickup_datetime", pa.timestamp('ns')),
    ("dropOff_datetime", pa.timestamp('ns')),
    ("PUlocationID", pa.int64()),
    ("DOlocationID", pa.int64()),
    ("SR_Flag", pa.int64()),  # Shared ride flag (can be null)
    ("Affiliated_base_number", pa.string()),  # Optional field, may not be present in all datasets
])

# Function to generate download URLs
def get_urls():
    return [f"{BASE_URL}fhv_tripdata_{year}-{month}.csv.gz" for year in YEARS for month in MONTHS]

# Function to fetch and process CSV.GZ data with retry logic
def fetch_csv_gz_data(url, retries=3, delay=5):
    print(f"[INFO] Starting download: {url}")

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                print(f"[SUCCESS] Downloaded: {url}")

                # Read compressed CSV
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                    convert_options = pv.ConvertOptions(column_types={col.name: col.type for col in schema})
                    table = pv.read_csv(f, convert_options=convert_options)

                return table

            print(f"[WARNING] Attempt {attempt+1}/{retries} failed for {url} (Status: {response.status_code}). Retrying in {delay} seconds...")

        except requests.RequestException as e:
            print(f"[ERROR] Network error on {url}: {e}. Retrying in {delay} seconds...")

        time.sleep(delay)

    print(f"[ERROR] Failed to download data after {retries} attempts: {url}")
    return None  # Instead of raising an error, return None to allow pipeline continuation

# Define the API resource for NYC green taxi data
@dlt.resource(name="fhv_trip_data")
def ny_taxi():
    for url in get_urls():
        table = fetch_csv_gz_data(url)
        if table:
            yield table.to_pandas().to_dict(orient="records")  # Convert PyArrow Table to list of dictionaries

# Initialize dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="nyc_taxi_pipeline",
    destination="bigquery",  # Change to 'bigquery' or 'duckdb' if needed
    dataset_name="nyc_taxi_data",
)

# Run the pipeline and load data
print("[INFO] Starting data ingestion pipeline...")
load_info = pipeline.run(ny_taxi, loader_file_format="parquet")
print("[INFO] Data ingestion completed.")
print(load_info)
