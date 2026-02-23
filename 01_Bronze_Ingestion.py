# Databricks notebook source
# Read raw CSVs from your Volume

suppliers_raw = spark.read.option("header", True).csv(
    "/Volumes/workspace/default/supply_chain_raw/suppliers.csv"
)

products_raw = spark.read.option("header", True).csv(
    "/Volumes/workspace/default/supply_chain_raw/products.csv"
)

shipments_raw = spark.read.option("header", True).csv(
    "/Volumes/workspace/default/supply_chain_raw/shipments.csv"
)

# Quick check
suppliers_raw.show(5)
products_raw.show(5)
shipments_raw.show(5)

# COMMAND ----------

suppliers_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_suppliers")

products_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_products")

shipments_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_shipments")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_products")
spark.sql("DROP TABLE IF EXISTS bronze_suppliers")
spark.sql("DROP TABLE IF EXISTS bronze_shipments")

# COMMAND ----------

suppliers_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_suppliers")

products_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_products")

shipments_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_shipments")

# COMMAND ----------

spark.sql("SHOW TABLES").show()