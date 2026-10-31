# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.driver"
silver_table = f"{catalog_name}.{silver_schema}.driver"


# COMMAND ----------

driver_df = spark.read.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

driver_selected_df = driver_df.select(
    F.col("driverId").alias("driver_Id"),
    F.col("name").alias("name"),
    F.col("dateOfBirth").alias("date_of_birth"),
    F.col("nationality").alias("nationality"),
    F.col("ingestion_timestap"),
    F.col("source_file")
    
)

display(driver_selected_df)


# COMMAND ----------

driver_distinct_df = driver_selected_df.distinct()
display(driver_distinct_df)

# COMMAND ----------

driver_concatenated_df = (
    driver_distinct_df.withColumn("driver_name", F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName")))
)

# COMMAND ----------

driver_final_df = (
    driver_concatenated_df.withColumn('nationality', F.initcap(F.col('nationality')))
    
)

# COMMAND ----------

display(driver_final_df)


# COMMAND ----------

# DBTITLE 1,Cell 9
(
    driver_final_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(driver_final_df)