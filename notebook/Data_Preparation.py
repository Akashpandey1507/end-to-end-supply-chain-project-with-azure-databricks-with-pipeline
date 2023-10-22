# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/Duplicate_Copy"

# COMMAND ----------

# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/classes_of_pyspark"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Cleaning on main Datasets
# MAGIC
# MAGIC 1. df1

# COMMAND ----------

df1.count() 
# check the datasize

# COMMAND ----------

df1.columns
# checking the columns

# COMMAND ----------

len(df1.columns)
# checking the lenghth of columns

# COMMAND ----------

df1.select([sum(col(i).isNull().cast('int')).alias(i) for i in df1.columns]).display()
# checking the null values

# COMMAND ----------

# delete the unwated columns
df1 = df1.drop("Order Zipcode","Product Description")
df1 = df1.dropna()

# COMMAND ----------

df1.select([sum(col(i).isNull().cast('int')).alias(i) for i in df1.columns]).display()
# checking again after the delete the unwanted columns

# COMMAND ----------

[column_name for column_name, data_type in df1.dtypes if data_type in ["string"]]
# checking the all string columns

# COMMAND ----------

# show the string columns 
df1.select([column_name for column_name, data_type in df1.dtypes if data_type in ["string"]]).display()

# COMMAND ----------

[column_name for column_name, data_type in df1.dtypes if data_type in ["double", "int"]]

# COMMAND ----------

display(df1.select([column_name for column_name, data_type in df1.dtypes if data_type in ["double", "int"]]))

# COMMAND ----------

dateColumn = df1.select("shipping date (DateOrders)", "order date (DateOrders)")
dateColumn.show()

# COMMAND ----------

df1 = df1.withColumnRenamed("shipping date (DateOrders)", "shipping date").withColumnRenamed("order date (DateOrders)", "order date")

# COMMAND ----------

dateColumn = df1.select("shipping date", "order date")
dateColumn.show()

# COMMAND ----------

dateColumn.groupBy(col("shipping date")).count().show()

# COMMAND ----------

date_format = "M/d/yyyy H:mm"

# Use to_timestamp to convert the string to datetime
df1 = df1.withColumn("shipping date", to_timestamp(df1["shipping date"], date_format)).withColumn("order date", to_timestamp(df1["order date"], date_format))

# COMMAND ----------

dateColumn = df1.select("shipping date", "order date")
dateColumn.show()

# COMMAND ----------

dateColumn.printSchema()

# COMMAND ----------

[column_name for column_name, datatype in df1.dtypes if datatype in ['timestamp']]

# COMMAND ----------

df1.select([column_name for column_name, datatype in df1.dtypes if datatype in ['timestamp']]).show()

# COMMAND ----------

df1.select([column_name for column_name, datatype in df1.dtypes if datatype in ['string']]).display()

# COMMAND ----------

df1.select([column_name for column_name, datatype in df1.dtypes if datatype in ['int', 'double']]).display()

# COMMAND ----------

# Delete the unwanted columns
df1 = df1.drop("Customer Email", "Customer Password")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Working on Datasets Tocket as df2

# COMMAND ----------

df2.count()

# COMMAND ----------

df2.columns

# COMMAND ----------

len(df2.columns)

# COMMAND ----------

[column_name for column_name , datatype in df2.dtypes if datatype in ['string']]

# COMMAND ----------

display(df2.select([column_name for column_name , datatype in df2.dtypes if datatype in ['string']]))

# COMMAND ----------

display(df2.select([column_name for column_name , datatype in df2.dtypes if datatype in ['int', "double"]]))

# COMMAND ----------

df2.groupBy(col("Date")).count().show(50)

# COMMAND ----------

date_format = "M/d/yyyy H:mm"

# Use to_timestamp to convert the string to datetime
df2 = df2.withColumn("Date", to_timestamp(df2["Date"], date_format))

# COMMAND ----------

df2.select("Date").show()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2.display()
