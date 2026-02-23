# Supply-Chain-Data-Engineering-Pipeline-Medallion-Architecture-
# Supply Chain Data Pipeline

End-to-end data engineering pipeline built using PySpark and Delta Lake following Medallion Architecture (Bronze, Silver, Gold) with incremental MERGE strategy.

Tech: PySpark | Delta Lake | SQL

1.Project Overview

-This project implements an end-to-end Data Engineering pipeline using a Medallion Architecture (Bronze, Silver, Gold) in Databricks with Delta Lake.

-The pipeline processes shipment-level data, performs transformations, builds business KPIs, and supports incremental processing using Delta MERGE.

🏗 Architecture

-Bronze Layer: Raw data ingestion

-Silver Layer: Data cleaning, revenue calculation, delay metrics

-Gold Layer: Product-level aggregated KPIs

-Incremental Processing: Delta Lake MERGE for upsert logic

-Data Quality Checks: Validation metrics stored in Delta tables

⚙️ Technologies Used

-Apache Spark

-Delta Lake

-Databricks

-SQL

-Python (PySpark)

📊 Business Metrics Generated

-Total Revenue per Product

-Total Quantity Sold

-Total Orders

-Delay Rate

🔁 Incremental Strategy

The Gold layer supports incremental updates using Delta MERGE, ensuring scalability and avoiding full table rewrites.

📈 Future Improvements

-Workflow orchestration

-Partitioning strategy

-Performance optimization using OPTIMIZE & ZORDER
