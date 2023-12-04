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
  source = "abfss://rawdata@rama12.dfs.core.windows.net/",
  mount_point = "/mnt/<rama12/rawadata",
  extra_configs=con )

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

display(dbutils.fs.ls("/mnt/<rama12/rawadata"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/<rama12/formula1"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1@rama12.dfs.core.windows.net"))

# COMMAND ----------

df2=spark.read.csv("abfss://formula1@rama12.dfs.core.windows.net/Athletes.csv",header=True)
display(df2)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/<rama12/rawadata")
