# Databricks notebook source
silver_df = spark.table("silver_shipments")

# COMMAND ----------

from pyspark.sql.functions import col

gold_base = silver_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

# COMMAND ----------

gold_base.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_shipments")

# COMMAND ----------

from pyspark.sql.functions import when

gold_base = gold_base.withColumn(
    "delay_flag",
    when(col("is_delayed") == True, 1).otherwise(0)
)

gold_shipments = gold_base.groupBy("product_id") \
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        avg("delay_flag").alias("delay_rate"),
        count("*").alias("total_orders")
    )

# COMMAND ----------

display(spark.table("gold_shipments"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, col

gold_base = spark.table("silver_shipments")

gold_shipments = gold_base.groupBy("product_id") \
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        avg(col("is_delayed").cast("int")).alias("delay_rate"),
        count("*").alias("total_orders")
    )

display(gold_shipments)

# COMMAND ----------

gold_shipments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_shipments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.gold_shipments;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED workspace.default.gold_shipments;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.default.gold_shipments;

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, col

initial_silver = spark.table("silver_shipments") \
    .filter(col("order_date") <= "2026-01-05")

gold_initial = initial_silver.groupBy("product_id") \
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        avg(col("is_delayed").cast("int")).alias("delay_rate"),
        count("*").alias("total_orders")
    )

gold_initial.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_shipments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(total_orders), MAX(total_orders) FROM workspace.default.gold_shipments;