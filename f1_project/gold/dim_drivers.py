# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

driver_df = spark.table(f"{catalog_name}.{silver_schema}.driver")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_driver_df =(
    driver_df
    .join(
        ref_nationality_region_df,
        driver_df.nationality == ref_nationality_region_df.nationality,"left"
    )
    .select(
        driver_df.driver_id,
        driver_df.driver_name,
        driver_df.date_of_birth,
        driver_df.nationality,
        ref_nationality_region_df.region
    )
    )
display(dim_driver_df)

# COMMAND ----------

(dim_driver_df
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(target_table)
 )

# COMMAND ----------

display(spark.table(target_table))