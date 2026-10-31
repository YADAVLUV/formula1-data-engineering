# Databricks notebook source
# MAGIC %run /Workspace/Users/yadavlove909@gmail.com/f1_project/common_config/env_config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.Constructor"
silver_table = f"{catalog_name}.{silver_schema}.Constructor"


# COMMAND ----------

Constructor_df = spark.read.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

Constructor_selected_df =  Constructor_df.select(
    F.col("ConstructorId").alias("constructor_Id"),
    F.col("name").alias("Constructor_name"),
    F.col("nationality").alias("nationality"),
    F.col("ingestion_timestap"),
    F.col("source_file")
    
)
display(Constructor_selected_df)


# COMMAND ----------

Constructor_distinct_df = Constructor_selected_df.distinct()
display(Constructor_distinct_df)

# COMMAND ----------

Constructor_final_df = (
    Constructor_distinct_df.withColumn('nationality', F.initcap(F.col('nationality')))
    
)

# COMMAND ----------

display(Constructor_final_df)


# COMMAND ----------

# DBTITLE 1,Cell 9
(
    Constructor_final_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(Constructor_final_df)