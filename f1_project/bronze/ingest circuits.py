# Databricks notebook source
# MAGIC %md
# MAGIC Ingest circuit file

# COMMAND ----------

# MAGIC %md
# MAGIC Read file using spark dataframe reader API

# COMMAND ----------

# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/helping_function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

circuit_Schema = StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)

])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .schema(circuit_Schema)
    .load(f"{source_file}/circuits.csv")
    )

# COMMAND ----------

circuit_final_df = add_ingestion_metadata(circuits_df)

display(circuit_final_df)

# COMMAND ----------

display(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1.gold;

# COMMAND ----------

(
    circuit_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable(f"{catalog_name}.{bronze_schema}.circuits")
)

# COMMAND ----------

display(spark.table(f"{catalog_name}.{bronze_schema}.circuits"))