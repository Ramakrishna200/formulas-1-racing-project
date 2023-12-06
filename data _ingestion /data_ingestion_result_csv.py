# Databricks notebook source
display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])



# COMMAND ----------

df=spark.read.option("header",True).schema(results_schema).json("dbfs:/mnt/<rama12/formula1/results.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

results_with_columns_df = df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 



# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

df4=results_with_columns_df.drop('statusId')

# COMMAND ----------

display(df4)

# COMMAND ----------

result_csv=df4.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/<rama12/ingesdata/results") 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/<rama12/ingesdata/results
