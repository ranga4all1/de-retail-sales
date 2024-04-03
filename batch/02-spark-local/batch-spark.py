#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import monotonically_increasing_id

# Replace below with you parameters
credentials_location = '/workspaces/de-retail-sales/creds/my-creds.json'
project_id = 'PROJECT_ID'
input_retail = 'gs://gs://<your-gcs-bucket>/retail_data/*'
output = 'gs://gs://<your-gcs-bucket>/star-schema/'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('gcs_bigquery') \
    .set("spark.jars", "/home/codespace/bin/gcs-connector-hadoop3-latest.jar, /home/codespace/bin/spark-3.1-bigquery-0.36.1.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .set("spark.hadoop.google.cloud.auth.project.id", project_id)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .config("spark.sql.extensions", "com.google.cloud.spark.bigquery.BigQuerySparkRegistrator") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.project.id", project_id) \
    .getOrCreate()


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

# Save each table to GCS as Parquet files
for table_name, dataframe in star_schema.items():
    # Define the full GCS path for the table
    table_gcs_path = f"{output}{table_name}/"
    
    # Write the DataFrame to GCS as Parquet files
    dataframe.write.parquet(table_gcs_path, mode="overwrite")

    print(f"Table '{table_name}' saved to GCS at: {table_gcs_path}")
