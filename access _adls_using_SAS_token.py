# Databricks notebook source
# DBTITLE 1,Access Azure Data Lake using access keys
##1. Set the spark config fs.azure.SAS.token
##2. List files from demo container
##3. Read data from covid19.csv file


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.rama12.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.rama12.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl.dfs.core.windows.net", "sp=racwdlp&st=2023-11-29T13:33:36Z&se=2023-11-29T21:33:36Z&spr=https&sv=2022-11-02&sr=c&sig=5dUXuBz0di6b6FQs6N3UxGQCQQdG3o53RX8tqaD42jk%3D")



# COMMAND ----------

display(dbutils.fs.ls("abfss://rawdata@rama12.dfs.core.windows.net"))

# COMMAND ----------

df=spark.read.csv("abfss://rawdata@rama12.dfs.core.windows.net/covid_data.csv",header=True)

# COMMAND ----------

display(df)
