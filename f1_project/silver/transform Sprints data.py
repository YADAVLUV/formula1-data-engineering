# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprint"
silver_table = f"{catalog_name}.{silver_schema}.sprint"

# COMMAND ----------

sprint_df = spark.read.table(bronze_table)

# COMMAND ----------

sprint_update_df = sprint_df.drop("url")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql import functions as F

sprint_selected_df = sprint_update_df.select(
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

display(sprint_selected_df)

# COMMAND ----------

sprint_valid_df = sprint_selected_df.filter(
    (F.col("driver_Id").isNotNull()) &
    (F.col("season").isNotNull()) &
    (F.col("round").isNotNull())
)

# COMMAND ----------

sprint_distinct_df=sprint_valid_df.distinct()


# COMMAND ----------

display(sprint_distinct_df)

# COMMAND ----------

sprint_final_df = (
   sprint_distinct_df.withColumn('race_Name', F.initcap(F.col('race_Name')))
    
)

# COMMAND ----------

display(sprint_final_df)


# COMMAND ----------

(
    sprint_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))