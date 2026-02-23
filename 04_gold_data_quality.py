# Databricks notebook source
gold_df = spark.table("workspace.default.gold_shipments")
display(gold_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import Row

total_rows = gold_df.count()
null_product = gold_df.filter(col("product_id").isNull()).count()
negative_revenue = gold_df.filter(col("total_revenue") < 0).count()
invalid_delay = gold_df.filter((col("delay_rate") < 0) | (col("delay_rate") > 1)).count()

dq_data = [Row(
    table_name="gold_shipments",
    total_rows=total_rows,
    null_product_count=null_product,
    negative_revenue_count=negative_revenue,
    invalid_delay_rate_count=invalid_delay
)]

dq_df = spark.createDataFrame(dq_data) \
    .withColumn("run_timestamp", current_timestamp())

display(dq_df)

# COMMAND ----------

dq_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("workspace.default.gold_data_quality_metrics")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.gold_data_quality_metrics;