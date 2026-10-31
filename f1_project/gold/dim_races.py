# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_race"

# COMMAND ----------

circuit_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
race_df = spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

dim_race_df =     (
        race_df.join(circuit_df, race_df.circuit_id == circuit_df.circuit_id, "inner")
        .select(
            race_df.season,
            race_df.round,
            race_df.race_name,
            race_df.race_date,
            circuit_df.circuit_name,
            circuit_df.locality,
            circuit_df.country_name,
        )
    )
display(dim_race_df)

# COMMAND ----------

(dim_race_df
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(target_table)
 )