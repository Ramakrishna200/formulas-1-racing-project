# Databricks notebook source
# DBTITLE 1,### Access Azure Data Lake using Service Principal
# MAGIC %md
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.
# MAGIC Call file system utlity mount to mount the storage
# MAGIC 5. Explore other file system utlities related to mount (list all mounts, unmount) 
# MAGIC
# MAGIC

# COMMAND ----------

client="b3059092-16af-4aa5-964e-81cae5fd536c" 
tenant_id="d69eceed-e651-44a4-bb73-6c7937aa7c50"
secrateid="szb8Q~py0R-AP0h6MnOKkVisoQf8PAqEaMpI8aH8"


# COMMAND ----------

con={"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client,
          "fs.azure.account.oauth2.client.secret": secrateid,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://formula1@rama12.dfs.core.windows.net/",
  mount_point = "/mnt/<rama12/formula1",
  extra_configs=con )

# COMMAND ----------

con={"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client,
          "fs.azure.account.oauth2.client.secret": secrateid,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://formula1@rama12.dfs.core.windows.net/",
  mount_point = "/mnt/<rama12/ingesdata",
  extra_configs=con )

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<rama12/ingesdata"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

s=StructType(fields=[StructField('circuitId',IntegerType(),False),\
                         StructField('circuitRef',StringType(),True),\
                         StructField('name',StringType(),True),\
                         StructField('location',StringType(),True),\
                         StructField('country',StringType(),True),\
                         StructField('lat',DoubleType(),True),\
                         StructField('lng',DoubleType(),True),\
                         StructField('alt',IntegerType(),True),\
                         StructField('url',StringType(),True)
])
    

# COMMAND ----------

# MAGIC %md
# MAGIC reading the csv file using Dataframe redar api 

# COMMAND ----------

circuits_csv=spark.read.option("header",True).schema(s).csv("dbfs:/mnt/<rama12/formula1/circuits.csv")
display(circuits_csv)

# COMMAND ----------

circuits_csv.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC select the columns in csv file

# COMMAND ----------

circut_select=circuits_csv.select('circuitId','circuitRef','name','location','country','lat','lng','alt')
display(circut_select)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_select1=circuits_csv.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))
display(circuits_select1)

# COMMAND ----------

# MAGIC %md
# MAGIC rename 

# COMMAND ----------

# MAGIC %md
# MAGIC rename the columns in Circuit_csv

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

circuit_rename=circuits_select1.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuit_rename)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC add the column in circuit csv file

# COMMAND ----------

circuit_int=circuit_rename.withColumn("ingestion_data",current_timestamp())
display(circuit_int)

# COMMAND ----------

df=circuit_int.write.parquet("mnt/<rama12/ingesdata/circuits") 

