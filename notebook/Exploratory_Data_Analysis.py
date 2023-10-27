# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/Data_Preparation"

# COMMAND ----------

df1.count()

# COMMAND ----------

df1.columns

# COMMAND ----------

len(df1.columns)

# COMMAND ----------

df1.describe().display()

# COMMAND ----------

df1.select("Sales").agg(sum('Sales').alias('Net Sales')).show(truncate=False)

# COMMAND ----------

df1.groupBy(col("Shipping Mode")).count().show()

# COMMAND ----------

df1.groupBy(col("Type")).count().show()

# COMMAND ----------

df1.groupBy(col("Delivery Status")).count().show()

# COMMAND ----------

df1.groupBy(col("Category Name")).count().show()

# COMMAND ----------

df1.groupBy(col("Customer City")).count().show()

# COMMAND ----------

df1.groupBy(col("Customer Country")).count().show()

# COMMAND ----------

df1.groupBy(col("Customer Segment")).count().show()

# COMMAND ----------

df1.groupBy(col("Customer State")).count().show()

# COMMAND ----------

df1.groupBy(col("Department Name")).count().show()

# COMMAND ----------

df1.groupBy(col("Order Country"),col("Order City")).count().show()

# COMMAND ----------

df1.groupBy("Category Name").agg(sum("Sales").alias("Net Sales")).show()

# COMMAND ----------

df1.sort(df1["Customer City"]).select("Customer City").show()

# COMMAND ----------

df1.display()

# COMMAND ----------


