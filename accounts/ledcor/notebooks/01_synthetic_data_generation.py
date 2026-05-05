# Databricks notebook source
# MAGIC %md
# MAGIC # Ledcor — Synthetic Data Generation
# MAGIC
# MAGIC Construction-themed synthetic datasets (jobsites, equipment, crews, timesheets) using `dbldatagen`.
# MAGIC Output is written to Unity Catalog as Delta tables.

# COMMAND ----------

# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "main"
SCHEMA = "ledcor_synthetic"
SCALE = 1  # multiplier for row counts

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobsites
# MAGIC One row per active construction project.

# COMMAND ----------

JOBSITE_ROWS = 250 * SCALE

provinces = ["AB", "BC", "ON", "SK", "MB", "QC", "NS"]
project_types = ["Commercial", "Industrial", "Infrastructure", "Residential", "Pipeline", "Mining"]
statuses = ["Planning", "Active", "On Hold", "Closing Out", "Complete"]

jobsites = (
    dg.DataGenerator(spark, name="jobsites", rows=JOBSITE_ROWS, partitions=4)
    .withColumn("jobsite_id", "string", prefix="JS", baseColumnType="hash")
    .withColumn("project_name", "string", template=r"\\w \\w \\w")
    .withColumn("province", "string", values=provinces, random=True)
    .withColumn("project_type", "string", values=project_types, random=True, weights=[3, 2, 2, 4, 1, 1])
    .withColumn("status", "string", values=statuses, random=True, weights=[1, 5, 1, 2, 1])
    .withColumn("budget_cad", "double", minValue=500_000, maxValue=250_000_000, random=True)
    .withColumn("start_date", "date", begin="2022-01-01", end="2026-01-01", random=True)
    .withColumn("planned_end_date", "date", begin="2024-01-01", end="2028-12-31", random=True)
    .build()
)

(jobsites.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("jobsites"))

display(spark.table("jobsites").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Equipment
# MAGIC TODO: heavy equipment fleet — excavators, cranes, loaders. FK to jobsite_id (current assignment).

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Crews
# MAGIC TODO: crew roster — foreman, trade, headcount. FK to jobsite_id.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Timesheets
# MAGIC TODO: daily timesheet entries — worker_id, jobsite_id, hours, cost_code.

# COMMAND ----------


