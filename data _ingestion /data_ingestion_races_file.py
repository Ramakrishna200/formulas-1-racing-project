# Databricks notebook source
display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/mnt/<rama12/formula1/races.csv")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StringType,StructField,IntegerType,StringType,DoubleType,StructType,DataType

# COMMAND ----------

from pyspark.sql.types import StringType,StructField,IntegerType,StringType,DoubleType,StructType,DataType
sch=StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(),True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                ])

# COMMAND ----------

df=spark.read.option("header",True).schema(sch).csv("dbfs:/mnt/<rama12/formula1/races.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,lit,col,concat

# COMMAND ----------

f = df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

display(f)

# COMMAND ----------

races_df = f.select(col('raceId').alias('race_id'), col('year').alias('race_year'),col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_main=races_df.write.mode("overwrite").partitionBy("race_year").parquet("mnt/<rama12/ingesdata/races")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/<rama12/ingesdata/races

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/<rama12/ingesdata/races"))
