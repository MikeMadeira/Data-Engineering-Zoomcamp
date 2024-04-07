import io
import os
import requests
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage
from io import BytesIO

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET")
print(BUCKET)

def download_files(taxi,years,filepath):

    # Define base URL and file prefix
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{}/{}_tripdata_{}.csv.gz'

    filepath = filepath+taxi+'/'
    # Create the directory if it doesn't exist
    os.makedirs(filepath, exist_ok=True)
    
    # Loop through the months using a for loop
    for year in years:
        for month in range(1, 13):
            if month < 10:
                formatted_month = f'{year}-0{month}'
            else:
                formatted_month = f'{year}-{month}'
            url = base_url.format(taxi,taxi,formatted_month)
            intermediary_filename = '{}{}_tripdata_{}.csv'
            intermediary_filename = intermediary_filename.format(filepath,taxi,formatted_month)
            print(intermediary_filename)

            # Download the file
            response = requests.get(url)
            print(url,end='\n')
            df = pd.read_csv(BytesIO(response.content), compression='gzip', dtype={'RatecodeID': str}, na_values='')
            # Save intermediary file
            df.to_csv(intermediary_filename, index=False)
            print(df.head())

def get_or_create_bucket(bucket_name):
    client = storage.Client()

    try:
        # Attempt to get the bucket
        bucket = client.get_bucket(bucket_name)
    except:
        # Bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")

    return bucket

def upload_to_gcs(bucket_name, object_path, local_folder):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = get_or_create_bucket(bucket_name)

    for filename in os.listdir(local_folder):
        print(local_folder)
        if filename.endswith('.csv'):
            print(filename)
            local_file_path = os.path.join(local_folder, filename)
            object_name = f'{object_path}/{filename}'
            blob = bucket.blob(object_name)
            blob.upload_from_filename(local_file_path)



def web_parquet_files_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # parquet file_name
        folder = './data/'
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        # request url for week 3 homework
        request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet'
        print(request_url)
        #request_url = f"{init_url}{service}/{file_name}"

        r = requests.get(request_url)

        print(r.content)
        # save files locally
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        df = pq.read_table(file_name)
        #df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")
        # upload it to gcs 
        # upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

if __name__ == '__main__':

    taxi='green'
    years = [2020]
    filepath = './data/download/'
    files = '*.csv'
    # download_files(taxi, years, filepath)
    
    object_path = f'{taxi}_trips'
    upload_to_gcs(BUCKET, object_path, filepath+taxi)
