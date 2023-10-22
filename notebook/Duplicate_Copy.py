# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/Data_Ingestion"

# COMMAND ----------

df1 = maindata.alias("copy")
df2 = tockendatasets.alias("copy")
metadata = metadata.alias("copy")
