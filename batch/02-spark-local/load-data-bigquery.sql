/*
Hack to get data directly from gcs bucket to bigquery
Use this during experimentation only e. g. if you processed data directly in GCS bucket and wrote results back to GCS bucket.
*/

-- Creating external tables referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `woven-edge-412500.de_retail_sales.fact_table` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://woven-edge-412500-de-retail-sales-bucket/star-schema/fact_table/*']
);

CREATE OR REPLACE EXTERNAL TABLE `woven-edge-412500.de_retail_sales.date` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://woven-edge-412500-de-retail-sales-bucket/star-schema/date/*']
);

CREATE OR REPLACE EXTERNAL TABLE `woven-edge-412500.de_retail_sales.item` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://woven-edge-412500-de-retail-sales-bucket/star-schema/item/*']
);

CREATE OR REPLACE EXTERNAL TABLE `woven-edge-412500.de_retail_sales.supplier` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://woven-edge-412500-de-retail-sales-bucket/star-schema/supplier/*']
);

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.fact_table
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.date
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.item
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.supplier
limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE woven-edge-412500.de_retail_sales.fact_table_np AS
SELECT * FROM woven-edge-412500.de_retail_sales.fact_table;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE woven-edge-412500.de_retail_sales.date_np AS
SELECT * FROM woven-edge-412500.de_retail_sales.date;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE woven-edge-412500.de_retail_sales.item_np AS
SELECT * FROM woven-edge-412500.de_retail_sales.item;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE woven-edge-412500.de_retail_sales.supplier_np AS
SELECT * FROM woven-edge-412500.de_retail_sales.supplier;


-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.fact_table_np
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.date_np
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.item_np
limit 10;

-- Check retail data
SELECT *
FROM woven-edge-412500.de_retail_sales.supplier_np
limit 10;