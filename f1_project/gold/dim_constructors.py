# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructor_df = spark.table(f"{catalog_name}.{silver_schema}.constructor")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructor_df =(
    constructor_df
    .join(
        ref_nationality_region_df,
        constructor_df.nationality == ref_nationality_region_df.nationality,"left"
    )
    .select(
        constructor_df.constructor_Id,
        constructor_df.constructor_name,
        constructor_df.nationality,
        ref_nationality_region_df.region
    )
    )
display(dim_constructor_df)

# COMMAND ----------

(dim_constructor_df
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(target_table)
 )

# COMMAND ----------

display(spark.table(target_table))