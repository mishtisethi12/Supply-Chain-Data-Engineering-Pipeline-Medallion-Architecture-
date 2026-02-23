# Databricks notebook source
spark.table("silver_shipments") \
    .selectExpr("min(order_date)", "max(order_date)") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, col

new_batch = spark.table("silver_shipments") \
    .filter(col("order_date") > "2026-01-05")

new_gold_agg = new_batch.groupBy("product_id") \
    .agg(
        sum("revenue").alias("new_total_revenue"),
        sum("quantity").alias("new_total_quantity"),
        avg(col("is_delayed").cast("int")).alias("new_delay_rate"),
        count("*").alias("new_total_orders")
    )

display(new_gold_agg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.gold_shipments;

# COMMAND ----------

from delta.tables import DeltaTable

gold_table = DeltaTable.forName(spark, "workspace.default.gold_shipments")

gold_table.alias("target").merge(
    new_gold_agg.alias("source"),
    "target.product_id = source.product_id"
).whenMatchedUpdate(set={
    "total_revenue": "target.total_revenue + source.new_total_revenue",
    "total_quantity": "target.total_quantity + source.new_total_quantity",
    "total_orders": "target.total_orders + source.new_total_orders",
    "delay_rate": "source.new_delay_rate"
}).whenNotMatchedInsert(values={
    "product_id": "source.product_id",
    "total_revenue": "source.new_total_revenue",
    "total_quantity": "source.new_total_quantity",
    "total_orders": "source.new_total_orders",
    "delay_rate": "source.new_delay_rate"
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.gold_shipments;