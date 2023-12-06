# Databricks notebook source
display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

df=spark.read.option("header",True).schema(schema).json("dbfs:/mnt/<rama12/formula1/constructors.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1=df.drop('url')
display(df1)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df3=df1.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
        .withColumn("ingestion_data",current_timestamp())

# COMMAND ----------

display(df3)

# COMMAND ----------

constructor_csv=df3.write.mode("overwrite").parquet("/mnt/<rama12/ingesdata/constructors") 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/<rama12/ingesdata/constructors
