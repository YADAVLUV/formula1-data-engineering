# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df =circuits_df.select(
    F.col("circuitId").alias("circuit_Id"),
    F.col("circuitName").alias("circuit_Name"),
    F.col("lat").alias("latitude"),
    F.col("long").alias("longitude"),
    F.col("locality"),
    F.col("country").alias("country_name"),
    F.col("ingestion_timestap"),
    F.col("source_file")
)

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_valid_df = circuits_selected_df.filter(
    F.col("circuit_Id").isNotNull() 
)

# COMMAND ----------

circuits_distinct_df=circuits_valid_df.distinct()


# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df.withColumn('circuit_Name', F.initcap(F.col('circuit_Name')))
    .withColumn('country_name', F.initcap(F.col('country_name')))
    .withColumn('locality', F.initcap(F.col('locality')))
)

# COMMAND ----------

display(circuits_final_df)


# COMMAND ----------

(
    circuits_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))