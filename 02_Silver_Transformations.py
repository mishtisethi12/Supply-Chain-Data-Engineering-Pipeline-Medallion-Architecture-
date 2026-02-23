# Databricks notebook source
suppliers_bronze = spark.table("bronze_suppliers")
products_bronze = spark.table("bronze_products")
shipments_bronze = spark.table("bronze_shipments")

suppliers_bronze.printSchema()
products_bronze.printSchema()
shipments_bronze.printSchema()


# COMMAND ----------

products_silver = products_bronze.select(
    col("product_id"),      # KEEP STRING
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

# COMMAND ----------

suppliers_silver = suppliers_bronze.select(
    col("supplier_id"),        # keep as string
    col("supplier_name"),
    col("city"),
    col("country")
)

# COMMAND ----------

from pyspark.sql.functions import col, to_date

shipments_silver = shipments_bronze.select(
    col("shipment_id"),     # KEEP STRING
    col("product_id"),      # KEEP STRING
    col("supplier_id"),     # KEEP STRING
    col("quantity").cast("int"),
    to_date(col("shipment_date"), "yyyy-MM-dd").alias("shipment_date")
)

# COMMAND ----------

shipments_enriched = shipments_silver \
    .join(products_silver, "product_id", "left") \
    .join(suppliers_silver, "supplier_id", "left")

shipments_enriched.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

gold_base = shipments_enriched.withColumn(
    "revenue",
    col("quantity") * col("price")
)

gold_base.show(5)

# COMMAND ----------

from pyspark.sql.functions import sum

gold_base.agg(
    sum("revenue").alias("total_revenue")
).show()

# COMMAND ----------

gold_base.groupBy("category").agg(
    sum("revenue").alias("category_revenue")
).orderBy("category_revenue", ascending=False).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, rank

# Step 1: Revenue per supplier
supplier_revenue = gold_base.groupBy("supplier_name").agg(
    sum("revenue").alias("supplier_revenue")
)

# Step 2: Window for ranking
window_spec = Window.orderBy(supplier_revenue["supplier_revenue"].desc())

# Step 3: Apply ranking
ranked_suppliers = supplier_revenue.withColumn(
    "rank",
    rank().over(window_spec)
)

# Step 4: Top 5
ranked_suppliers.filter("rank <= 5").show()

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

spark.table("silver_shipments").printSchema()