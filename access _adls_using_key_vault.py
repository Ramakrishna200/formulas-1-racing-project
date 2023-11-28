# Databricks notebook source
# DBTITLE 1,Access Azure Data Lake using access keys
##1. Set the spark config fs.azure.account.key
##2. List files from demo container
##3. Read data from circuits.csv file


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.rama12.dfs.core.windows.net",
    "yvxxLf7sdhOCFzNi5uoG/eXNl9OVr341NPjm4tMEImxm6kbQiCvS299NKskn188badUB1VU8cfop+ASt7CFMCw==")


# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1@rama12.dfs.core.windows.net"))

# COMMAND ----------

df=spark.read.csv("abfss://formula1@rama12.dfs.core.windows.net/Athletes.csv")

# COMMAND ----------

display(df)
