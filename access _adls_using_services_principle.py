# Databricks notebook source
# DBTITLE 1,### Access Azure Data Lake using Service Principal
#### Steps to follow
1. Register Azure AD Application / Service Principal
2. Generate a secret/ password for the Application
3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 



# COMMAND ----------

client="b3059092-16af-4aa5-964e-81cae5fd536c" 
tenant_id="d69eceed-e651-44a4-bb73-6c7937aa7c50"
secrateid="szb8Q~py0R-AP0h6MnOKkVisoQf8PAqEaMpI8aH8"


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.rama12.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rama12.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rama12.dfs.core.windows.net", client)
spark.conf.set("fs.azure.account.oauth2.client.secret.rama12.dfs.core.windows.net", secrateid)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rama12.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://rawdata@rama12.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1@rama12.dfs.core.windows.net"))

# COMMAND ----------

df2=spark.read.csv("abfss://formula1@rama12.dfs.core.windows.net/Athletes.csv",header=True)
display(df2)
