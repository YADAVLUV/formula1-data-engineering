# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.result"
silver_table = f"{catalog_name}.{silver_schema}.result"

# COMMAND ----------

result_df = spark.read.table(bronze_table)

# COMMAND ----------

result_selected_df = result_df.drop("url")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql import functions as F

result_selected_df = result_df.select(
    F.col("constructorId").alias("constructor_Id"),
    F.col("date").alias("race_date"),
    F.col("driverId").alias("driver_Id"),
    F.col("grid").alias("grid_position"),
    F.col("laps").alias("completed_laps"),
    F.col("number").alias("car_number"),
    F.col("points").alias("points"),
    F.col("position").alias("position"),
    F.col("positionText").alias("final_position_Text"),
    F.col("raceName").alias("race_Name"),
    F.col("round").alias("round"),
    F.col("season").alias("season"),
    F.col("status").alias("status"),
    F.col("ingestion_timestap").alias("ingestion_timestamp"),
    F.col("source_file").alias("source_file")
)

# COMMAND ----------

display(result_selected_df)

# COMMAND ----------

result_valid_df = result_selected_df.filter(
    (F.col("driver_Id").isNotNull()) &
    (F.col("season").isNotNull()) &
    (F.col("round").isNotNull())
)

# COMMAND ----------

result_distinct_df=result_valid_df.distinct()


# COMMAND ----------

display(result_distinct_df)

# COMMAND ----------

result_final_df = (
   result_distinct_df.withColumn('race_Name', F.initcap(F.col('race_Name')))
    
)

# COMMAND ----------

display(result_final_df)


# COMMAND ----------

(
    result_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))