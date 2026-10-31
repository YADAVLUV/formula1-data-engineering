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

constructor_Schema = "constructorId STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = (
    spark.read
    .format('json')
    .schema(constructor_Schema)
    .load(f"{source_file}/constructors.json")
    )

# COMMAND ----------

constructor_final_df = add_ingestion_metadata(constructor_df)

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 10
(
    constructor_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .option('overwriteSchema', 'true')
        .saveAsTable(f"{catalog_name}.{bronze_schema}.constructor")
)

# COMMAND ----------

display(spark.table(f"{catalog_name}.{bronze_schema}.constructor"))