/*
Use this to get data directly from gcs bucket to bigquery
Use this only if you used option 2 for processing i. e. you processed data directly in GCS bucket and wrote results back to GCS bucket.
Replace below variables with your parameters:
  - <DATASET_ID>
  - <your_gcs_bucket>
*/

-- Creating external tables referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<DATASET_ID>.fact_table` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<your_gcs_bucket>/star-schema/fact_table/*']
);

CREATE OR REPLACE EXTERNAL TABLE `<DATASET_ID>.date` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<your_gcs_bucket>/star-schema/date/*']
);

CREATE OR REPLACE EXTERNAL TABLE `<DATASET_ID>.item` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<your_gcs_bucket>/star-schema/item/*']
);

CREATE OR REPLACE EXTERNAL TABLE `<DATASET_ID>.supplier` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<your_gcs_bucket>/star-schema/supplier/*']
);

-- Check retail data
SELECT *
FROM <DATASET_ID>.fact_table
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.date
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.item
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.supplier
limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <DATASET_ID>.fact_table_np AS
SELECT * FROM <DATASET_ID>.fact_table;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <DATASET_ID>.date_np AS
SELECT * FROM <DATASET_ID>.date;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <DATASET_ID>.item_np AS
SELECT * FROM <DATASET_ID>.item;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <DATASET_ID>.supplier_np AS
SELECT * FROM <DATASET_ID>.supplier;


-- Check retail data
SELECT *
FROM <DATASET_ID>.fact_table_np
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.date_np
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.item_np
limit 10;

-- Check retail data
SELECT *
FROM <DATASET_ID>.supplier_np
limit 10;