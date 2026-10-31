# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

races_df = spark.read.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_selected_df =  races_df.select(
    F.col("season").alias("season"),
    F.col("round"),
    F.col("circuitId").alias("circuit_Id"),
    F.col("raceName").alias("race_Name"),
    F.col("date").alias("race_date"),
    F.col("ingestion_timestap"),
    F.col("source_file")
    
)

# COMMAND ----------

races_distinct_df = races_selected_df.distinct()
display(races_distinct_df)

# COMMAND ----------

races_final_df = (
    races_distinct_df.withColumn('race_Name', F.initcap(F.col('race_Name')))
    
)

# COMMAND ----------

display(races_final_df)


# COMMAND ----------

(
    races_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(silver_table)