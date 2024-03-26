#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


parser = argparse.ArgumentParser()

parser.add_argument('--input_retail', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_retail = args.input_retail
output = args.output


spark = SparkSession.builder \
    .appName("Data processing with Spark") \
    .getOrCreate()

# Replace below with your GCP project_id
project_id = '<your_project_id>'
# Replace below with your temp bucket created by spark dataproc cluter
spark.conf.set('temporaryGcsBucket', '<dataproc-temp-bucket>')

df = spark.read.parquet(input_retail)

def data_modeling(df):
    ### Building the star schema
    df.createOrReplaceTempView("df_view")

    # Supplier Dimension Table Creation
    supplier_df = df.select("supplier").dropDuplicates().withColumnRenamed("supplier", "SUPPLIER").withColumn("supplier_id", monotonically_increasing_id() + 1)
    
    # Item Dimension Table Creation
    item_df = df.selectExpr("item_code", "item_type", "item_description").dropDuplicates().withColumnRenamed("item_code", "ITEM_CODE")
    
    # Date Dimension Table Creation
    date_df = df.select("year", "month").dropDuplicates().withColumnRenamed("year", "YEAR").withColumnRenamed("month", "MONTH").withColumn("DATE_ID", monotonically_increasing_id() + 1)
    
    # Fact Table Creation
    fact_table = df.join(supplier_df, "SUPPLIER") \
        .join(item_df, df["item_code"] == item_df["ITEM_CODE"]) \
        .join(date_df, (df["year"] == date_df["YEAR"]) & (df["month"] == date_df["MONTH"])) \
        .select(df["item_code"], supplier_df["supplier_id"], date_df["DATE_ID"], df["retail_sales"], df["retail_transfers"], df["warehouse_sales"]) \
        .dropDuplicates()

    # Lowercase column names for all DataFrames
    supplier_df = supplier_df.toDF(*[col.lower() for col in supplier_df.columns])
    item_df = item_df.toDF(*[col.lower() for col in item_df.columns])
    date_df = date_df.toDF(*[col.lower() for col in date_df.columns])
    fact_table = fact_table.toDF(*[col.lower() for col in fact_table.columns])

    return {
        "supplier": supplier_df,
        "item": item_df,
        "date": date_df,
        "fact_table": fact_table
    }

# Call the data_modeling function and store the result
star_schema = data_modeling(df)

# Define BigQuery dataset and table names
dataset_id = 'de_retail_sales_data'  
table_dataframes = {
    "supplier": star_schema["supplier"],
    "item": star_schema["item"],
    "date": star_schema["date"],
    "fact_table": star_schema["fact_table"]
}

# Load data into BigQuery tables using Spark
for table_name, dataframe in table_dataframes.items():
    full_table_name = f'{project_id}.{dataset_id}.{table_name}'
    dataframe.write \
        .format('bigquery') \
        .option('table', full_table_name) \
        .mode('overwrite') \
        .save()

    print(f"Data loaded into BigQuery table: {full_table_name}")
