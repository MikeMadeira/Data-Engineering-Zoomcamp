import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from io import BytesIO

# Define base URL and file prefix
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{}.csv.gz"

# Define date range
date = datetime(2019, 1, 1)
end_date = datetime(2020, 12, 31)

# Create an empty DataFrame to store concatenated data
concatenated_df = pd.DataFrame()

# Loop through the months using a for loop
for year in [2019,2020]:
    for month in range(10, 13):
        if month < 10:
            formatted_month = f'{year}-0{month}'
        else:
            formatted_month = f'{year}-{month}'
        url = base_url.format(formatted_month)
        intermediary_filename = 'data/download/yellow/yellow_tripdata_{}.csv'
        intermediary_filename = intermediary_filename.format(formatted_month)

        # Download the file
        response = requests.get(url)
        print(url,end='\n')
        df = pd.read_csv(BytesIO(response.content), compression='gzip', dtype={'RatecodeID': str}, na_values='')
        # Save intermediary file
        df.to_csv(intermediary_filename, index=False)
        print(df)
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)

# Save the concatenated DataFrame to a new CSV file
concatenated_filename = "data/download/yellow/concatenated_yellow_tripdata_2019-01_to_2020-12.csv"
concatenated_df.to_csv(concatenated_filename, index=False)

# Upload the consolidated file to BigQuery
project_id = "dbt-elt"
dataset_id = "dbt_mikemadeira"
table_id = "yellow_taxi"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Define the BigQuery schema
schema = [
    bigquery.SchemaField(name="VendorID", field_type="FLOAT64"),
    bigquery.SchemaField(name="tpep_pickup_datetime", field_type="STRING"),
    bigquery.SchemaField(name="tpep_dropoff_datetime", field_type="STRING"),
    bigquery.SchemaField(name="passenger_count", field_type="FLOAT64"),
    bigquery.SchemaField(name="trip_distance", field_type="FLOAT64"),
    bigquery.SchemaField(name="RatecodeID", field_type="FLOAT64"),
    bigquery.SchemaField(name="store_and_fwd_flag", field_type="STRING"),
    bigquery.SchemaField(name="PULocationID", field_type="INT64"),
    bigquery.SchemaField(name="DOLocationID", field_type="INT64"),
    bigquery.SchemaField(name="payment_type", field_type="FLOAT64"),
    bigquery.SchemaField(name="fare_amount", field_type="FLOAT64"),
    bigquery.SchemaField(name="extra", field_type="FLOAT64"),
    bigquery.SchemaField(name="mta_tax", field_type="FLOAT64"),
    bigquery.SchemaField(name="tip_amount", field_type="FLOAT64"),
    bigquery.SchemaField(name="tolls_amount", field_type="FLOAT64"),
    bigquery.SchemaField(name="improvement_surcharge", field_type="FLOAT64"),
    bigquery.SchemaField(name="total_amount", field_type="FLOAT64"),
    bigquery.SchemaField(name="congestion_surcharge", field_type="FLOAT64")
]

# Create a BigQuery dataset if it doesn't exist
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
try:
    client.create_dataset(dataset)
except Exception as e:
    print('Dataset already exists')
    pass  # Dataset already exists

# Load the data into BigQuery table
table_ref = client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.CSV)

with open(concatenated_filename, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()  # Wait for the job to complete

print(f"Data loaded into BigQuery table {project_id}.{dataset_id}.{table_id}")
