# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/products.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.saveAsTable("products");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Category, ProductName, ListPrice
# MAGIC FROM products
# MAGIC WHERE Category = 'Touring Bikes';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE products;
# MAGIC
