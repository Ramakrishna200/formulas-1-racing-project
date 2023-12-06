# Databricks notebook source
display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

df=spark.read.option("header",True).schema(pit_stops_schema).json("dbfs:/mnt/<rama12/formula1/pit_stops.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

inal_df = df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())




# COMMAND ----------

display(inal_df)

# COMMAND ----------

pitstop_csv=inal_df.write.mode("overwrite").parquet("/mnt/<rama12/ingesdata/pitstop") 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/<rama12/ingesdata/pitstop
