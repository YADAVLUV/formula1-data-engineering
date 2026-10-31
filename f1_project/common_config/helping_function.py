# Databricks notebook source
from  pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df
            .withColumn('ingestion_timestap',F.current_timestamp())
            .withColumn('source_file', F.col('_metadata.file_path'))
    )


# COMMAND ----------


