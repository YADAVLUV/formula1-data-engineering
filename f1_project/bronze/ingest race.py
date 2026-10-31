# Databricks notebook source
# MAGIC %md
# MAGIC Ingest circuit file

# COMMAND ----------

# MAGIC %md
# MAGIC Read file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F

# COMMAND ----------

Race_Schema = StructType([
    StructField('season', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('raceName', StringType(), True),
    StructField('date', StringType(), True),
    StructField('circuitId', StringType(), True)
])


# COMMAND ----------

Races_df = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .schema(Race_Schema)
    .load('/Volumes/formula1/landing/files/races.csv')
    )

# COMMAND ----------

Race_final_df = (
    Races_df
        .withColumn('ingestion_timestap',F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
 )
display(Race_final_df)

# COMMAND ----------

display(Races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table 
# MAGIC

# COMMAND ----------



# COMMAND ----------

(
    Race_final_df
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable('formula1.bronze.Races')
)

# COMMAND ----------

display(spark.table('formula1.bronze.Races'))