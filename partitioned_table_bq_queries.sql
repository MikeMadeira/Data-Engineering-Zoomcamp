-- Create external table from parquet files uploaded through python web_files_to_gcs.py script
CREATE EXTERNAL TABLE green_cab_data.green_trip_data_2022
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://parquet_bucket_bq/green/*.parquet'
  ]
) 

-- Check data from external table
SELECT * FROM `green_cab_data.green_trip_data_2022` LIMIT 5;

-- Create the non partitioned table from external table
CREATE OR REPLACE TABLE `green_cab_data.green_trip_data_2022_non_partitoned` 
AS SELECT * FROM `green_cab_data.green_trip_data_2022`;

-- Number of records of table `green_cab_data.green_trip_data_2022`
SELECT COUNT(*) FROM `green_cab_data.green_trip_data_2022_non_partitoned`;

-- Number of distinct PULocationIDs FROM `green_cab_data.green_trip_data_2022`
SELECT COUNT(DISTINCT gtd.PULocationID) FROM `green_cab_data.green_trip_data_2022` AS gtd;

-- Number of distinct PULocationIDs FROM `green_cab_data.green_trip_data_2022_non_partitoned`
SELECT COUNT(DISTINCT gtd.PULocationID) FROM `green_cab_data.green_trip_data_2022_non_partitoned` AS gtd;

-- How many records have a fare_amount of 0?
SELECT 
  COUNT(gtd.VendorID) 
FROM `green_cab_data.green_trip_data_2022_non_partitoned` AS gtd
WHERE gtd.fare_amount = 0;

-- Create a partitioned table with the best strategy
CREATE OR REPLACE TABLE `green_cab_data.green_trip_data_2022_partitoned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS SELECT * FROM `green_cab_data.green_trip_data_2022` as gtd;

-- Select the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 from the table partitioned
SELECT DISTINCT PULocationID 
FROM `green_cab_data.green_trip_data_2022_partitoned`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP("2022-01-06") AND TIMESTAMP("2022-06-30");

-- Select the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 from the table non partitioned
SELECT DISTINCT PULocationID 
FROM `green_cab_data.green_trip_data_2022_non_partitoned`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP("2022-01-06") AND TIMESTAMP("2022-06-30");

