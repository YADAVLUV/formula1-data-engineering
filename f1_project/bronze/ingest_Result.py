# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/helping_function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


# COMMAND ----------




        

# COMMAND ----------

result_df = (
    spark.read
    .format('json')
    .option("inferSchema","true")
    .load(f"{source_file}/results")
    )

# COMMAND ----------

result_final_df = add_ingestion_metadata(result_df)

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table 
# MAGIC

# COMMAND ----------

(
    result_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable(f"{catalog_name}.{bronze_schema}.result")
)

# COMMAND ----------

display(spark.table(f"{catalog_name}.{bronze_schema}.result"))