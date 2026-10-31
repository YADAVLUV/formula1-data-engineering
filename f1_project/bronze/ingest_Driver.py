# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/helping_function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


# COMMAND ----------


name_schema =  StructType([
    StructField('givenName',StringType()),
    StructField('familyName',StringType())
])

driver_Schema = StructType([
    StructField('driverId',StringType()),
    StructField('name',name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType())
])

        

# COMMAND ----------

driver_df = (
    spark.read
    .format('json')
    .schema(driver_Schema)
    .load(f"{source_file}/drivers.json")
    )

# COMMAND ----------

driver_final_df = add_ingestion_metadata(driver_df)

display(driver_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table 
# MAGIC

# COMMAND ----------

(
    driver_final_df
        .write
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .format('delta')
        .saveAsTable(f"{catalog_name}.{bronze_schema}.driver")
)

# COMMAND ----------

display(spark.table(f"{catalog_name}.{bronze_schema}.driver"))