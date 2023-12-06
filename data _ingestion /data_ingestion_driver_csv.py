# Databricks notebook source
display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

rivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

df=spark.read.option("header",True).schema(rivers_schema).json("dbfs:/mnt/<rama12/formula1/drivers.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

df3=df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
        .withColumn("ingestion_data",current_timestamp())\
        .withColumn("name",concat(col("name.forename"),lit(''),col("name.surname")))

# COMMAND ----------

df4=df3.drop('url')

# COMMAND ----------

display(df4)

# COMMAND ----------

driver_csv=df4.write.mode("overwrite").parquet("/mnt/<rama12/ingesdata/drivers") 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/<rama12/ingesdata/drivers
