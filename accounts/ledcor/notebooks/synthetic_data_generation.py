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

CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "ledcor_synthetic"
SCALE = 1  # multiplier for row counts

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

JOBSITES_TABLE = f"{CATALOG}.{SCHEMA}.jobsites"

(jobsites.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(JOBSITES_TABLE))

display(spark.table(JOBSITES_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Projects
# MAGIC High-level project register. **Capital Line South LRT** is the featured project for the dashboard;
# MAGIC the others are alternates available for demo selection.

# COMMAND ----------

from datetime import date

PROJECTS_TABLE = f"{CATALOG}.{SCHEMA}.projects"

projects_data = [
    ("PRJ-001", 105, "6210085", "Capital Line South LRT",
     "Civil, Mining & Infrastructure", "City of Edmonton",
     "Active", date(2023, 4, 1), date(2027, 12, 31), date(2025, 12, 27),
     "Marcus Thibault", 1_011_000_000.00),
    ("PRJ-002", 142, "6310221", "Site Alpha Hydro Civil Works",
     "Civil, Mining & Infrastructure", "BC Hydro",
     "Active", date(2022, 7, 15), date(2026, 9, 30), date(2025, 11, 29),
     "Priya Anand", 620_500_000.00),
    ("PRJ-003", 168, "6410112", "Trans Northern Pipeline Loop",
     "Pipeline", "Trans Northern Pipelines Inc.",
     "Active", date(2024, 1, 10), date(2026, 6, 30), date(2025, 12, 27),
     "Daniel Sutherland", 845_000_000.00),
    ("PRJ-004", 173, "6510089", "Royal Inland Hospital Tower",
     "Building", "Interior Health Authority",
     "Closing Out", date(2021, 9, 1), date(2025, 12, 31), date(2025, 11, 29),
     "Annika Berglund", 412_750_000.00),
    ("PRJ-005", 199, "6610344", "Northwest Fiber Backbone",
     "Communications", "Government of the Northwest Territories",
     "Active", date(2024, 5, 6), date(2027, 3, 31), date(2025, 12, 27),
     "Jared Whitehorse", 184_900_000.00),
    ("PRJ-006", 211, "6710076", "Highway 1 Twinning – Phase 3",
     "Civil, Mining & Infrastructure", "BC Ministry of Transportation",
     "Planning", date(2025, 9, 1), date(2028, 11, 30), date(2025, 12, 27),
     "Olivia Tremblay", 312_400_000.00),
]

projects_schema = (
    "project_id STRING, "
    "ineight_project_id INT, "
    "jde_job_number STRING, "
    "project_name STRING, "
    "division STRING, "
    "owner STRING, "
    "status STRING, "
    "start_date DATE, "
    "planned_end_date DATE, "
    "month_end_date DATE, "
    "project_manager STRING, "
    "contract_value_cad DOUBLE"
)

projects_df = spark.createDataFrame(projects_data, projects_schema)

(projects_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PROJECTS_TABLE))

display(spark.table(PROJECTS_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Summaries
# MAGIC One row per project. PRJ-001 uses real Capital Line South LRT numbers; PRJ-002–PRJ-006 are
# MAGIC simulated to match each project's contract value and lifecycle stage (e.g. Closing Out → high
# MAGIC earned revenue, Planning → minimal earned revenue).
# MAGIC
# MAGIC Invariant: `revised_contract_price_cad = original_contract_price_cad + approved_change_revenue_cad + pending_change_revenue_cad`.

# COMMAND ----------

CONTRACT_SUMMARIES_TABLE = f"{CATALOG}.{SCHEMA}.contract_summaries"

contract_summaries_data = [
    # PRJ-001 — Capital Line South LRT (real numbers)
    ("PRJ-001",
     968_883_075.00, 42_381_380.00, 0.00, 1_011_264_455.00,
     122_823_208.05, 128_530_452.78,
     98_894_375.59, 130_094_149.00,
     226_329_075.14),
    # PRJ-002 — Site Alpha Hydro Civil Works (Active, mid-build)
    ("PRJ-002",
     595_680_000.00, 24_820_000.00, 0.00, 620_500_000.00,
     77_438_400.00, 81_285_500.00,
     79_424_000.00, 80_665_000.00,
     279_225_000.00),
    # PRJ-003 — Trans Northern Pipeline Loop (Active, favorable forecast)
    ("PRJ-003",
     819_650_000.00, 25_350_000.00, 0.00, 845_000_000.00,
     114_751_000.00, 119_145_000.00,
     122_525_000.00, 118_300_000.00,
     464_750_000.00),
    # PRJ-004 — Royal Inland Hospital Tower (Closing Out, mostly earned)
    ("PRJ-004",
     387_985_000.00, 24_765_000.00, 0.00, 412_750_000.00,
     42_678_350.00, 47_466_250.00,
     46_228_000.00, 47_053_500.00,
     379_730_000.00),
    # PRJ-005 — Northwest Fiber Backbone (Active, favorable forecast)
    ("PRJ-005",
     180_277_500.00, 4_622_500.00, 0.00, 184_900_000.00,
     27_041_625.00, 28_104_800.00,
     28_659_500.00, 27_919_900.00,
     55_470_000.00),
    # PRJ-006 — Highway 1 Twinning – Phase 3 (Planning, just kicked off)
    ("PRJ-006",
     312_400_000.00, 0.00, 0.00, 312_400_000.00,
     37_488_000.00, 37_488_000.00,
     36_863_200.00, 37_488_000.00,
     6_248_000.00),
]

contract_summaries_schema = (
    "project_id STRING, "
    "original_contract_price_cad DOUBLE, "
    "approved_change_revenue_cad DOUBLE, "
    "pending_change_revenue_cad DOUBLE, "
    "revised_contract_price_cad DOUBLE, "
    "original_contract_margin_cad DOUBLE, "
    "revised_contract_margin_cad DOUBLE, "
    "forecast_final_margin_cad DOUBLE, "
    "last_month_forecast_margin_cad DOUBLE, "
    "earned_revenue_cad DOUBLE"
)

contract_summaries_df = spark.createDataFrame(contract_summaries_data, contract_summaries_schema)

(contract_summaries_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CONTRACT_SUMMARIES_TABLE))

display(spark.table(CONTRACT_SUMMARIES_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pending Changes (Non-Contributing)
# MAGIC Tracks pending and probable change revenue not yet contributing to the revised contract price,
# MAGIC plus the probable contract amount/margin if those changes come through.
# MAGIC PRJ-001 uses real Capital Line South LRT numbers (all zero — no probables on the books).
# MAGIC
# MAGIC Invariants:
# MAGIC - `probable_contract_amount_cad = revised_contract_price_cad + probable_change_revenue_cad`
# MAGIC - `probable_contract_margin_cad ≈ forecast_final_margin_cad + (probable_change_revenue_cad × project_margin_rate)`

# COMMAND ----------

PENDING_CHANGES_TABLE = f"{CATALOG}.{SCHEMA}.pending_changes"

pending_changes_data = [
    # PRJ-001 — Capital Line South LRT (real: nothing pending)
    ("PRJ-001", 0.00, 0.00, 1_011_264_455.00, 98_894_375.59),
    # PRJ-002 — Site Alpha Hydro Civil Works (small probable, ~12.8% margin rate)
    ("PRJ-002", 0.00, 2_500_000.00, 623_000_000.00, 79_744_000.00),
    # PRJ-003 — Trans Northern Pipeline Loop (some pending + probable, ~14.5% margin rate)
    ("PRJ-003", 1_200_000.00, 3_800_000.00, 848_800_000.00, 123_076_000.00),
    # PRJ-004 — Royal Inland Hospital Tower (closing out, nothing new)
    ("PRJ-004", 0.00, 0.00, 412_750_000.00, 46_228_000.00),
    # PRJ-005 — Northwest Fiber Backbone (small probable, ~15.5% margin rate)
    ("PRJ-005", 0.00, 850_000.00, 185_750_000.00, 28_791_250.00),
    # PRJ-006 — Highway 1 Twinning – Phase 3 (planning, several probables, ~11.8% margin rate)
    ("PRJ-006", 2_100_000.00, 4_500_000.00, 316_900_000.00, 37_394_200.00),
]

pending_changes_schema = (
    "project_id STRING, "
    "pending_change_revenue_cad DOUBLE, "
    "probable_change_revenue_cad DOUBLE, "
    "probable_contract_amount_cad DOUBLE, "
    "probable_contract_margin_cad DOUBLE"
)

pending_changes_df = spark.createDataFrame(pending_changes_data, pending_changes_schema)

(pending_changes_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PENDING_CHANGES_TABLE))

display(spark.table(PENDING_CHANGES_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cash Position
# MAGIC Cash flow rollup per project: billings vs. cost, net of holdbacks and accruals.
# MAGIC PRJ-001 uses real Capital Line South LRT numbers; others are scaled to each project's
# MAGIC earned revenue and lifecycle stage. Subcontractor + client holdbacks at ~5% (Canadian lien
# MAGIC standard), with PRJ-004 slightly lower as it's closing out.
# MAGIC
# MAGIC Invariants:
# MAGIC - `total_cash_received_cad = billings_to_date_cad - uncollected_ar_cad - client_holdback_cad`
# MAGIC - `total_cash_paid_cad = total_cost_to_date_cad - accruals_cad - subcontractor_holdback_cad`
# MAGIC - `cash_position_cad = total_cash_received_cad - total_cash_paid_cad`

# COMMAND ----------

CASH_POSITION_TABLE = f"{CATALOG}.{SCHEMA}.cash_position"

cash_position_data = [
    # PRJ-001 — Capital Line South LRT (real numbers)
    ("PRJ-001",
     240_880_714.92, 0.00, 11_003_483.24, 229_877_231.68,
     204_196_164.24, 0.00, 11_162_924.87, 193_033_239.37,
     36_843_992.00),
    # PRJ-002 — Site Alpha Hydro Civil Works (Active, mid-build, slightly overbilled)
    ("PRJ-002",
     290_394_000.00, 0.00, 14_519_700.00, 275_874_300.00,
     243_484_200.00, 0.00, 12_174_210.00, 231_309_990.00,
     44_564_310.00),
    # PRJ-003 — Trans Northern Pipeline Loop (Active, some AR + accruals)
    ("PRJ-003",
     487_987_500.00, 850_000.00, 24_399_375.00, 462_738_125.00,
     397_361_250.00, 1_200_000.00, 21_854_869.00, 374_306_381.00,
     88_431_744.00),
    # PRJ-004 — Royal Inland Hospital Tower (Closing Out, holdback releasing)
    ("PRJ-004",
     372_135_400.00, 0.00, 16_746_093.00, 355_389_307.00,
     337_200_240.00, 0.00, 15_174_011.00, 322_026_229.00,
     33_363_078.00),
    # PRJ-005 — Northwest Fiber Backbone (Active, early)
    ("PRJ-005",
     58_798_200.00, 0.00, 2_939_910.00, 55_858_290.00,
     46_872_150.00, 0.00, 2_343_608.00, 44_528_542.00,
     11_329_748.00),
    # PRJ-006 — Highway 1 Twinning – Phase 3 (Planning, mobilization billings)
    ("PRJ-006",
     6_872_800.00, 125_000.00, 343_640.00, 6_404_160.00,
     5_510_736.00, 250_000.00, 275_537.00, 4_985_199.00,
     1_418_961.00),
]

cash_position_schema = (
    "project_id STRING, "
    "billings_to_date_cad DOUBLE, "
    "uncollected_ar_cad DOUBLE, "
    "client_holdback_cad DOUBLE, "
    "total_cash_received_cad DOUBLE, "
    "total_cost_to_date_cad DOUBLE, "
    "accruals_cad DOUBLE, "
    "subcontractor_holdback_cad DOUBLE, "
    "total_cash_paid_cad DOUBLE, "
    "cash_position_cad DOUBLE"
)

cash_position_df = spark.createDataFrame(cash_position_data, cash_position_schema)

(cash_position_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CASH_POSITION_TABLE))

display(spark.table(CASH_POSITION_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outstanding A/R Aging
# MAGIC Open accounts receivable bucketed by age. PRJ-001 uses real Capital Line South LRT
# MAGIC numbers (fully settled — all zero). Other projects reflect their lifecycle:
# MAGIC - **PRJ-003** carries the most aged AR (slow-paying client / possible billing dispute)
# MAGIC - **PRJ-004** (Closing Out) only has final-invoice Current AR, no overdue
# MAGIC - **PRJ-002 / PRJ-005** healthy aging with most AR within terms
# MAGIC - **PRJ-006** (Planning) small mobilization invoices, a bit creeping into overdue

# COMMAND ----------

OUTSTANDING_AR_TABLE = f"{CATALOG}.{SCHEMA}.outstanding_ar"

outstanding_ar_data = [
    # PRJ-001 — Capital Line South LRT (real: fully settled)
    ("PRJ-001",      0.00,         0.00,         0.00,       0.00,    0.00),
    # PRJ-002 — Site Alpha Hydro Civil Works (healthy, mostly current)
    ("PRJ-002", 8_500_000.00, 1_250_000.00,      0.00,       0.00,    0.00),
    # PRJ-003 — Trans Northern Pipeline Loop (slow-paying client / dispute)
    ("PRJ-003", 11_200_000.00, 2_800_000.00, 650_000.00, 200_000.00,  0.00),
    # PRJ-004 — Royal Inland Hospital Tower (Closing Out, final billings only)
    ("PRJ-004", 1_950_000.00,      0.00,         0.00,       0.00,    0.00),
    # PRJ-005 — Northwest Fiber Backbone (Active early, small overdue)
    ("PRJ-005", 2_400_000.00,   185_000.00,      0.00,       0.00,    0.00),
    # PRJ-006 — Highway 1 Twinning – Phase 3 (Planning, mobilization billings)
    ("PRJ-006",   320_000.00,    95_000.00,  30_000.00,      0.00,    0.00),
]

outstanding_ar_schema = (
    "project_id STRING, "
    "current_not_overdue_cad DOUBLE, "
    "overdue_1_30_cad DOUBLE, "
    "overdue_31_60_cad DOUBLE, "
    "overdue_61_90_cad DOUBLE, "
    "overdue_91_plus_cad DOUBLE"
)

outstanding_ar_df = spark.createDataFrame(outstanding_ar_data, outstanding_ar_schema)

(outstanding_ar_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTSTANDING_AR_TABLE))

display(spark.table(OUTSTANDING_AR_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Budget Performance
# MAGIC Monthly snapshot per project covering the current period, cumulative-to-date, and
# MAGIC period-over-period forecast change. Combines all three reporting sections from the source
# MAGIC into one denormalized row per project per period — easier for downstream AI/BI to query.
# MAGIC
# MAGIC PRJ-001 uses real Capital Line South LRT numbers for the period ending 2025-12-27.
# MAGIC The others reflect the project's stage and forecast direction:
# MAGIC - **PRJ-002** mid-build with mild slippage (period Nov 1 – Nov 29 cycle)
# MAGIC - **PRJ-003** favorable: under control budget, forecast improving
# MAGIC - **PRJ-004** Closing Out: minimal period activity, earlier overruns baked in
# MAGIC - **PRJ-005** favorable: under control budget, forecast improving
# MAGIC - **PRJ-006** Planning: tiny period, mobilization already overrunning slightly
# MAGIC
# MAGIC Invariants:
# MAGIC - `period_gain_loss_cad = period_control_budget_earned_cad - period_actual_cad`
# MAGIC - `total_gain_loss_cad = total_control_budget_earned_cad - total_actual_cad`
# MAGIC - `forecast_final_change_cad = forecast_final_cad - previous_forecast_final_cad`

# COMMAND ----------

BUDGET_PERFORMANCE_TABLE = f"{CATALOG}.{SCHEMA}.budget_performance"

budget_performance_data = [
    # PRJ-001 — Capital Line South LRT (real numbers, period ending 2025-12-27)
    ("PRJ-001", date(2025, 11, 30), date(2025, 12, 27),
     1.49,  7_481_306.04,   8_447_278.75,    -965_972.71,
     14.21, 76_981_159.44,  84_015_434.87,  -7_034_275.43,
     573_193_258.53, -28_133_203.79,
     570_021_037.01, 3_172_221.52),
    # PRJ-002 — Site Alpha Hydro Civil Works (Nov 1 – Nov 29, slight slip)
    ("PRJ-002", date(2025, 11, 1),  date(2025, 11, 29),
     2.10,  7_818_300.00,   7_995_500.00,    -177_200.00,
     42.00, 156_366_000.00, 158_250_000.00, -1_884_000.00,
     377_500_000.00, -5_200_000.00,
     376_800_000.00, 700_000.00),
    # PRJ-003 — Trans Northern Pipeline Loop (Nov 30 – Dec 27, favorable)
    ("PRJ-003", date(2025, 11, 30), date(2025, 12, 27),
     2.30,  10_689_250.00,  10_425_000.00,    264_250.00,
     53.00, 246_317_500.00, 244_800_000.00,  1_517_500.00,
     458_200_000.00,  6_550_000.00,
     459_500_000.00, -1_300_000.00),
    # PRJ-004 — Royal Inland Hospital Tower (Nov 1 – Nov 29, closing out)
    ("PRJ-004", date(2025, 11, 1),  date(2025, 11, 29),
     0.60,  1_609_800.00,   1_615_000.00,      -5_200.00,
     91.00, 244_153_000.00, 250_200_000.00, -6_047_000.00,
     275_800_000.00, -7_500_000.00,
     275_950_000.00, -150_000.00),
    # PRJ-005 — Northwest Fiber Backbone (Nov 30 – Dec 27, favorable)
    ("PRJ-005", date(2025, 11, 30), date(2025, 12, 27),
     2.50,  2_773_500.00,   2_720_000.00,      53_500.00,
     28.00, 31_063_200.00,  30_650_000.00,    413_200.00,
     108_800_000.00,  2_140_000.00,
     109_200_000.00, -400_000.00),
    # PRJ-006 — Highway 1 Twinning – Phase 3 (Nov 30 – Dec 27, planning slip)
    ("PRJ-006", date(2025, 11, 30), date(2025, 12, 27),
     1.00,  1_874_400.00,   1_948_000.00,     -73_600.00,
     1.80,  3_373_920.00,   3_510_000.00,    -136_080.00,
     188_950_000.00, -1_510_000.00,
     188_200_000.00, 750_000.00),
]

budget_performance_schema = (
    "project_id STRING, "
    "period_start_date DATE, "
    "period_end_date DATE, "
    "period_pct_complete DOUBLE, "
    "period_control_budget_earned_cad DOUBLE, "
    "period_actual_cad DOUBLE, "
    "period_gain_loss_cad DOUBLE, "
    "total_pct_complete DOUBLE, "
    "total_control_budget_earned_cad DOUBLE, "
    "total_actual_cad DOUBLE, "
    "total_gain_loss_cad DOUBLE, "
    "forecast_final_cad DOUBLE, "
    "cost_forecast_variance_cad DOUBLE, "
    "previous_forecast_final_cad DOUBLE, "
    "forecast_final_change_cad DOUBLE"
)

budget_performance_df = spark.createDataFrame(budget_performance_data, budget_performance_schema)

(budget_performance_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BUDGET_PERFORMANCE_TABLE))

display(spark.table(BUDGET_PERFORMANCE_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Budget Performance History
# MAGIC Up to 12 prior monthly snapshots per project leading up to (and including) the current period.
# MAGIC Generated using a logistic S-curve for cumulative progress and a trajectory model that drifts
# MAGIC the forecast from the original budget toward the current forecast based on each project's
# MAGIC narrative (deteriorating / favorable / stable). The most recent row is snapped to match the
# MAGIC `budget_performance` snapshot for consistency.
# MAGIC
# MAGIC Enables Genie questions like:
# MAGIC - "Show me PRJ-001's forecast final trend over the last 6 months"
# MAGIC - "Which projects had the largest cost forecast variance growth this year?"
# MAGIC - "Calculate the burn rate trend for PRJ-003"

# COMMAND ----------

import math
import random
from datetime import date, timedelta

BUDGET_PERFORMANCE_HISTORY_TABLE = f"{CATALOG}.{SCHEMA}.budget_performance_history"

def _s_curve(elapsed_ratio):
    """Logistic S-curve, normalized so f(1.0) ≈ 1.0."""
    raw = 1.0 / (1.0 + math.exp(-8.0 * (elapsed_ratio - 0.5)))
    cap = 1.0 / (1.0 + math.exp(-8.0 * 0.5))
    return raw / cap

def generate_history(
    project_id, control_budget, original_budget,
    project_start, current_period_start, current_period_end,
    current_total_pct, current_total_actual, current_forecast,
    trajectory, seed=0, max_periods=12,
):
    rng = random.Random(seed)
    period_len = (current_period_end - current_period_start).days + 1
    total_elapsed_days = max(1, (current_period_end - project_start).days)
    elapsed_periods = max(1, total_elapsed_days // period_len)
    n = min(max_periods, elapsed_periods)

    periods = []
    end = current_period_end
    for _ in range(n):
        start = end - timedelta(days=period_len - 1)
        periods.append((start, end))
        end = start - timedelta(days=1)
    periods.reverse()

    rows = []
    prev_cum_pct = 0.0
    prev_cum_earned = 0.0
    prev_cum_actual = 0.0
    prev_forecast = original_budget

    for i, (pstart, pend) in enumerate(periods):
        elapsed_ratio = (pend - project_start).days / total_elapsed_days
        cum_pct = _s_curve(elapsed_ratio) * current_total_pct
        cum_earned = control_budget * cum_pct / 100.0
        ppct = cum_pct - prev_cum_pct
        pearned = cum_earned - prev_cum_earned

        if trajectory == "deteriorating":
            variance = 1.0 + 0.06 * (i / max(1, n - 1)) + rng.gauss(0, 0.012)
        elif trajectory == "favorable":
            variance = 1.0 - 0.03 * (i / max(1, n - 1)) + rng.gauss(0, 0.010)
        else:
            variance = 1.0 + rng.gauss(0, 0.008)

        pactual = pearned * variance
        cum_actual = prev_cum_actual + pactual
        pgl = pearned - pactual
        cum_gl = cum_earned - cum_actual

        forecast_progress = (i + 1) / n
        forecast = original_budget + (current_forecast - original_budget) * forecast_progress
        forecast_change = forecast - prev_forecast
        cost_forecast_variance = original_budget - forecast

        rows.append((
            project_id, pstart, pend,
            round(ppct, 2), round(pearned, 2), round(pactual, 2), round(pgl, 2),
            round(cum_pct, 2), round(cum_earned, 2), round(cum_actual, 2), round(cum_gl, 2),
            round(forecast, 2), round(cost_forecast_variance, 2),
            round(prev_forecast, 2), round(forecast_change, 2),
        ))

        prev_cum_pct = cum_pct
        prev_cum_earned = cum_earned
        prev_cum_actual = cum_actual
        prev_forecast = forecast

    # Snap final row to known current-period values
    if rows:
        last = list(rows[-1])
        last[7] = round(current_total_pct, 2)
        last[8] = round(control_budget * current_total_pct / 100.0, 2)
        last[9] = round(current_total_actual, 2)
        last[10] = round(last[8] - last[9], 2)
        last[11] = round(current_forecast, 2)
        last[12] = round(original_budget - current_forecast, 2)
        rows[-1] = tuple(last)

    return rows


HISTORY_CONFIGS = [
    # (project_id, control_budget, original_budget, project_start, current_period_start,
    #  current_period_end, current_total_pct, current_total_actual, current_forecast, trajectory, seed)
    ("PRJ-001", 545_060_055.00, 545_060_055.00, date(2023, 4, 1),  date(2025, 11, 30), date(2025, 12, 27),
     14.21,  84_015_434.87, 573_193_258.53, "deteriorating", 1),
    ("PRJ-002", 372_300_000.00, 372_300_000.00, date(2022, 7, 15), date(2025, 11, 1),  date(2025, 11, 29),
     42.00, 158_250_000.00, 377_500_000.00, "deteriorating", 2),
    ("PRJ-003", 464_750_000.00, 464_750_000.00, date(2024, 1, 10), date(2025, 11, 30), date(2025, 12, 27),
     53.00, 244_800_000.00, 458_200_000.00, "favorable", 3),
    ("PRJ-004", 268_300_000.00, 268_300_000.00, date(2021, 9, 1),  date(2025, 11, 1),  date(2025, 11, 29),
     91.00, 250_200_000.00, 275_800_000.00, "deteriorating", 4),
    ("PRJ-005", 110_940_000.00, 110_940_000.00, date(2024, 5, 6),  date(2025, 11, 30), date(2025, 12, 27),
     28.00,  30_650_000.00, 108_800_000.00, "favorable", 5),
    ("PRJ-006", 187_440_000.00, 187_440_000.00, date(2025, 9, 1),  date(2025, 11, 30), date(2025, 12, 27),
     1.80,    3_510_000.00, 188_950_000.00, "deteriorating", 6),
]

history_rows = []
for cfg in HISTORY_CONFIGS:
    history_rows.extend(generate_history(*cfg))

budget_performance_history_schema = (
    "project_id STRING, "
    "period_start_date DATE, "
    "period_end_date DATE, "
    "period_pct_complete DOUBLE, "
    "period_control_budget_earned_cad DOUBLE, "
    "period_actual_cad DOUBLE, "
    "period_gain_loss_cad DOUBLE, "
    "total_pct_complete DOUBLE, "
    "total_control_budget_earned_cad DOUBLE, "
    "total_actual_cad DOUBLE, "
    "total_gain_loss_cad DOUBLE, "
    "forecast_final_cad DOUBLE, "
    "cost_forecast_variance_cad DOUBLE, "
    "previous_forecast_final_cad DOUBLE, "
    "forecast_final_change_cad DOUBLE"
)

budget_performance_history_df = spark.createDataFrame(history_rows, budget_performance_history_schema)

(budget_performance_history_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BUDGET_PERFORMANCE_HISTORY_TABLE))

display(spark.table(BUDGET_PERFORMANCE_HISTORY_TABLE).orderBy("project_id", "period_end_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule Progress
# MAGIC Planned vs. actual cumulative % complete per period per project, with SPI (Schedule Performance Index).
# MAGIC SPI > 1.0 means ahead of schedule, < 1.0 means behind. Planned curve uses the same S-curve as
# MAGIC `budget_performance_history` but assumes a baseline schedule that's slightly more aggressive
# MAGIC than reality (actuals lag planned for "behind schedule" projects, lead for "ahead" projects).
# MAGIC
# MAGIC Schedule narratives:
# MAGIC - PRJ-001: behind schedule (compounds the cost troubles)
# MAGIC - PRJ-002: slightly behind
# MAGIC - PRJ-003: slightly ahead (favorable)
# MAGIC - PRJ-004: on plan (closing out)
# MAGIC - PRJ-005: ahead of schedule
# MAGIC - PRJ-006: on plan (just kicked off)

# COMMAND ----------

SCHEDULE_PROGRESS_TABLE = f"{CATALOG}.{SCHEMA}.schedule_progress"

# Schedule bias: positive = actuals ahead of plan, negative = behind
SCHEDULE_BIAS = {
    "PRJ-001": -0.18,
    "PRJ-002": -0.06,
    "PRJ-003": 0.07,
    "PRJ-004": -0.02,
    "PRJ-005": 0.10,
    "PRJ-006": 0.00,
}

schedule_rows = []
for cfg in HISTORY_CONFIGS:
    project_id = cfg[0]
    project_start = cfg[3]
    current_period_end = cfg[5]
    current_total_pct = cfg[6]
    bias = SCHEDULE_BIAS[project_id]
    rng = random.Random(cfg[10] + 100)

    period_len = (cfg[5] - cfg[4]).days + 1
    elapsed_periods = max(1, (current_period_end - project_start).days // period_len)
    n = min(12, elapsed_periods)

    periods = []
    end = current_period_end
    for _ in range(n):
        start = end - timedelta(days=period_len - 1)
        periods.append((start, end))
        end = start - timedelta(days=1)
    periods.reverse()

    # Planned curve: target slightly higher than actuals based on bias
    # If bias is negative (behind), planned ends ABOVE current_total_pct
    # If bias is positive (ahead), planned ends BELOW current_total_pct
    planned_target_pct = current_total_pct * (1.0 - bias)

    for i, (pstart, pend) in enumerate(periods):
        elapsed_ratio = (pend - project_start).days / max(1, (current_period_end - project_start).days)
        planned_pct = _s_curve(elapsed_ratio) * planned_target_pct
        actual_pct = _s_curve(elapsed_ratio) * current_total_pct + rng.gauss(0, 0.15)
        actual_pct = max(0.0, actual_pct)
        schedule_variance = actual_pct - planned_pct
        spi = (actual_pct / planned_pct) if planned_pct > 0.01 else None

        schedule_rows.append((
            project_id, pstart, pend,
            round(planned_pct, 2), round(actual_pct, 2),
            round(schedule_variance, 2), round(spi, 4) if spi is not None else None,
        ))

schedule_progress_schema = (
    "project_id STRING, "
    "period_start_date DATE, "
    "period_end_date DATE, "
    "planned_pct_complete DOUBLE, "
    "actual_pct_complete DOUBLE, "
    "schedule_variance_pct DOUBLE, "
    "spi DOUBLE"
)

schedule_progress_df = spark.createDataFrame(schedule_rows, schedule_progress_schema)

(schedule_progress_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SCHEDULE_PROGRESS_TABLE))

display(spark.table(SCHEDULE_PROGRESS_TABLE).orderBy("project_id", "period_end_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subcontractor Commitments
# MAGIC Major subcontracts per project with original commitment, approved change orders, and
# MAGIC billing/payment/holdback to date. Sub progress aligns with each project's overall % complete.
# MAGIC
# MAGIC Enables Genie questions like:
# MAGIC - "What's our total subcontractor exposure on PRJ-001?"
# MAGIC - "Which subcontractors are in dispute?"
# MAGIC - "What's outstanding subcontractor holdback by project?"

# COMMAND ----------

SUBCONTRACTOR_COMMITMENTS_TABLE = f"{CATALOG}.{SCHEMA}.subcontractor_commitments"

# (subcontract_id, project_id, subcontractor_name, scope, original, approved_co, billed, paid, holdback, status)
# revised = original + approved_co; holdback typically 5%; paid = billed - holdback (or less if dispute)
subcontractor_data = [
    # PRJ-001 — Capital Line South LRT (~14% complete, struggling)
    ("SUB-001-01", "PRJ-001", "ABC Earthworks Ltd.", "Excavation & Tunneling",
     145_000_000.00,  8_500_000.00, 22_400_000.00, 21_280_000.00, 1_120_000.00, "Active"),
    ("SUB-001-02", "PRJ-001", "Cascade Concrete Forming", "Structural Concrete",
      89_000_000.00,  3_200_000.00, 12_900_000.00, 12_255_000.00,   645_000.00, "Active"),
    ("SUB-001-03", "PRJ-001", "Northland Rail Systems", "Track Laying",
     112_000_000.00,  1_800_000.00,  6_500_000.00,  6_175_000.00,   325_000.00, "Active"),
    ("SUB-001-04", "PRJ-001", "Edmonton Electrical Group", "Electrical & Signaling",
      78_000_000.00,        0.00,   3_200_000.00,  3_040_000.00,   160_000.00, "Active"),
    ("SUB-001-05", "PRJ-001", "Urban Glass & Steel", "Stations Architecture",
      52_000_000.00,        0.00,   1_400_000.00,  1_330_000.00,    70_000.00, "Active"),
    ("SUB-001-06", "PRJ-001", "Prairie Geotechnical Services", "Geotech & Soil Stabilization",
      34_000_000.00,  6_750_000.00, 18_900_000.00, 17_955_000.00,   945_000.00, "Disputed"),
    ("SUB-001-07", "PRJ-001", "Boreal Mechanical", "HVAC & Plumbing",
      28_000_000.00,        0.00,     820_000.00,    779_000.00,    41_000.00, "Active"),

    # PRJ-002 — Site Alpha Hydro Civil Works (~42% complete)
    ("SUB-002-01", "PRJ-002", "Pacific Civil Construction", "Site Prep & Access Roads",
      68_000_000.00,  2_100_000.00, 32_500_000.00, 30_875_000.00, 1_625_000.00, "Active"),
    ("SUB-002-02", "PRJ-002", "Glacier Concrete Works", "Dam Concrete Placement",
     112_000_000.00,  4_800_000.00, 48_200_000.00, 45_790_000.00, 2_410_000.00, "Active"),
    ("SUB-002-03", "PRJ-002", "Westcoast Mechanical Group", "Penstock & Turbine Pads",
      54_000_000.00,        0.00,  18_600_000.00, 17_670_000.00,   930_000.00, "Active"),
    ("SUB-002-04", "PRJ-002", "Mountain Steel Erectors", "Steel Structures",
      38_000_000.00,  1_500_000.00, 14_200_000.00, 13_490_000.00,   710_000.00, "Active"),
    ("SUB-002-05", "PRJ-002", "BC Power Cables Inc.", "Electrical Distribution",
      28_000_000.00,        0.00,   3_400_000.00,  3_230_000.00,   170_000.00, "Active"),

    # PRJ-003 — Trans Northern Pipeline Loop (~53% complete, favorable)
    ("SUB-003-01", "PRJ-003", "Northern Pipeline Welding Co.", "Mainline Pipe Welding",
     185_000_000.00,  6_400_000.00, 102_500_000.00, 97_375_000.00, 5_125_000.00, "Active"),
    ("SUB-003-02", "PRJ-003", "Tundra Earthworks Ltd.", "Right-of-Way Clearing & Trenching",
      92_000_000.00,  2_100_000.00, 53_800_000.00, 51_110_000.00, 2_690_000.00, "Active"),
    ("SUB-003-03", "PRJ-003", "ArcticTech Coatings", "Pipe Coatings & Cathodic Protection",
      48_000_000.00,        0.00,  24_500_000.00, 23_275_000.00, 1_225_000.00, "Active"),
    ("SUB-003-04", "PRJ-003", "Northern Lights Electrical", "SCADA & Monitoring Systems",
      32_000_000.00,        0.00,   8_200_000.00,  7_790_000.00,   410_000.00, "Active"),
    ("SUB-003-05", "PRJ-003", "Stable Crossings Inc.", "River & Road Crossings",
      58_000_000.00,  1_200_000.00, 28_900_000.00, 27_455_000.00, 1_445_000.00, "Active"),

    # PRJ-004 — Royal Inland Hospital Tower (~91% complete, closing out)
    ("SUB-004-01", "PRJ-004", "BC Hospital Builders Inc.", "Structural Construction",
      98_000_000.00,  3_800_000.00, 96_500_000.00, 92_980_000.00, 3_520_000.00, "Completed"),
    ("SUB-004-02", "PRJ-004", "Interior Mechanical Group", "HVAC & Plumbing",
      65_000_000.00,  4_200_000.00, 65_100_000.00, 62_700_000.00, 2_400_000.00, "Active"),
    ("SUB-004-03", "PRJ-004", "Coastal Electrical Services", "Electrical Systems",
      48_000_000.00,  2_100_000.00, 46_800_000.00, 44_460_000.00, 2_340_000.00, "Active"),
    ("SUB-004-04", "PRJ-004", "Northern Glass Curtainwall", "Curtainwall & Glazing",
      22_000_000.00,    850_000.00, 21_800_000.00, 20_710_000.00, 1_090_000.00, "Active"),
    ("SUB-004-05", "PRJ-004", "Specialty Medical Gas Systems", "Medical Gas Infrastructure",
      14_000_000.00,    400_000.00, 13_600_000.00, 12_920_000.00,   680_000.00, "Active"),

    # PRJ-005 — Northwest Fiber Backbone (~28% complete, favorable)
    ("SUB-005-01", "PRJ-005", "Tundra Trenching Ltd.", "Cable Trenching & Conduit",
      52_000_000.00,        0.00,  15_200_000.00, 14_440_000.00,   760_000.00, "Active"),
    ("SUB-005-02", "PRJ-005", "Aurora Fiber Splicing", "Splicing & Terminations",
      34_000_000.00,        0.00,   9_400_000.00,  8_930_000.00,   470_000.00, "Active"),
    ("SUB-005-03", "PRJ-005", "Northern Communications Inc.", "Equipment Installation",
      28_000_000.00,        0.00,   7_500_000.00,  7_125_000.00,   375_000.00, "Active"),
    ("SUB-005-04", "PRJ-005", "Yukon Heavy Hauling", "Logistics & Freight",
      12_000_000.00,        0.00,   3_400_000.00,  3_230_000.00,   170_000.00, "Active"),

    # PRJ-006 — Highway 1 Twinning Phase 3 (~2% complete, just kicked off)
    ("SUB-006-01", "PRJ-006", "BC Highway Construction Ltd.", "Road Grading & Paving",
      98_000_000.00,        0.00,   1_800_000.00,  1_710_000.00,    90_000.00, "Active"),
    ("SUB-006-02", "PRJ-006", "Mountain Bridge Builders", "Overpass Construction",
      58_000_000.00,        0.00,     650_000.00,    618_000.00,    32_000.00, "Active"),
    ("SUB-006-03", "PRJ-006", "Pacific Drainage Systems", "Stormwater Infrastructure",
      24_000_000.00,        0.00,     320_000.00,    304_000.00,    16_000.00, "Active"),
]

subcontractor_commitments_schema = (
    "subcontract_id STRING, "
    "project_id STRING, "
    "subcontractor_name STRING, "
    "scope STRING, "
    "original_commitment_cad DOUBLE, "
    "approved_change_orders_cad DOUBLE, "
    "billed_to_date_cad DOUBLE, "
    "paid_to_date_cad DOUBLE, "
    "holdback_cad DOUBLE, "
    "status STRING"
)

subcontractor_commitments_df = spark.createDataFrame(subcontractor_data, subcontractor_commitments_schema)
subcontractor_commitments_df = subcontractor_commitments_df.withColumn(
    "revised_commitment_cad",
    F.col("original_commitment_cad") + F.col("approved_change_orders_cad"),
)

(subcontractor_commitments_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SUBCONTRACTOR_COMMITMENTS_TABLE))

display(spark.table(SUBCONTRACTOR_COMMITMENTS_TABLE).orderBy("project_id", "subcontract_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Register
# MAGIC Open risks and issues per project. PRJ-001 carries the heaviest risk load (consistent with
# MAGIC its margin fade and forecast slippage); PRJ-005 / PRJ-004 are mostly clean.
# MAGIC
# MAGIC Enables Genie questions like:
# MAGIC - "What are the top risks on PRJ-001 by estimated impact?"
# MAGIC - "Which projects have critical-severity open risks?"
# MAGIC - "Show me all geotech-related risks across the portfolio"

# COMMAND ----------

RISK_REGISTER_TABLE = f"{CATALOG}.{SCHEMA}.risk_register"

risk_data = [
    # PRJ-001 — Capital Line South LRT (5 risks, the troubled project)
    ("RSK-001-01", "PRJ-001", "Geotechnical instability at Tunnel Portal South",
     "Soft clay layer encountered ~30m below grade requires additional shoring and dewatering. "
     "Engineering review in progress; contingency excavation methodology being designed.",
     "Geotechnical", "Critical", "High", "Mitigating", 12_400_000.00,
     "Marcus Thibault", date(2025, 9, 14), date(2026, 4, 30)),
    ("RSK-001-02", "PRJ-001", "Subcontractor schedule slippage — Northland Rail Systems",
     "Track laying behind baseline by ~6 weeks due to delayed delivery of long-lead rail components. "
     "Recovery plan submitted; shift to double-shift operations under negotiation.",
     "Schedule", "High", "High", "Mitigating", 4_200_000.00,
     "Marcus Thibault", date(2025, 10, 22), date(2026, 2, 28)),
    ("RSK-001-03", "PRJ-001", "Material price escalation — structural steel & rebar",
     "Steel index up 18% YoY against contract escalation cap of 8%. Estimated unrecovered exposure "
     "across remaining structural scope.",
     "Cost", "High", "High", "Open", 3_500_000.00,
     "Karen Doyle", date(2025, 8, 1), date(2026, 6, 30)),
    ("RSK-001-04", "PRJ-001", "Pending change order — substation electrical scope",
     "Owner-directed scope addition for backup power redundancy. CO submitted September; "
     "owner approval pending budget cycle.",
     "Scope", "Medium", "Medium", "Open", 1_800_000.00,
     "Marcus Thibault", date(2025, 9, 5), date(2026, 1, 31)),
    ("RSK-001-05", "PRJ-001", "Public safety / community concerns — downtown stations",
     "Noise complaints and pedestrian re-routing escalations at 102 Ave stations. May require "
     "additional traffic control and night-work restrictions.",
     "Stakeholder", "Medium", "Medium", "Mitigating", 800_000.00,
     "Linnea Cho", date(2025, 7, 18), date(2026, 3, 31)),

    # PRJ-002 — Site Alpha Hydro Civil Works (3 risks)
    ("RSK-002-01", "PRJ-002", "Subcontractor performance — Mountain Steel Erectors",
     "Multiple non-conformance reports on weld quality. Performance management plan in place; "
     "potential replacement under evaluation.",
     "Subcontractor", "Medium", "Medium", "Mitigating", 2_000_000.00,
     "Priya Anand", date(2025, 10, 1), date(2026, 2, 15)),
    ("RSK-002-02", "PRJ-002", "Weather window risk — winter pour delays",
     "Concrete pour schedule sensitive to temperature; cold-weather contingency budget exposed if "
     "January pours slip into February.",
     "Schedule", "Medium", "High", "Open", 1_500_000.00,
     "Priya Anand", date(2025, 11, 10), date(2026, 3, 31)),
    ("RSK-002-03", "PRJ-002", "Environmental compliance — fish window restrictions",
     "DFO fish-window restrictions limit in-stream work to a 6-week window. Schedule slip beyond "
     "window would push critical-path work to 2027.",
     "Regulatory", "Medium", "Low", "Open", 950_000.00,
     "Priya Anand", date(2025, 6, 1), date(2026, 7, 31)),

    # PRJ-003 — Trans Northern Pipeline Loop (3 risks, but project favorable overall)
    ("RSK-003-01", "PRJ-003", "Indigenous consultation — northern segment",
     "Ongoing engagement with three Nations along the northern segment. Timeline risk if accommodation "
     "agreements extend beyond Q1 2026.",
     "Stakeholder", "Medium", "Medium", "Mitigating", 1_000_000.00,
     "Daniel Sutherland", date(2025, 5, 15), date(2026, 3, 31)),
    ("RSK-003-02", "PRJ-003", "Environmental permit renewal — water crossings",
     "Existing CEAA permit expires Q3 2026. Renewal application submitted; standard renewal "
     "expected but timing carries minor risk.",
     "Regulatory", "Low", "Low", "Open", 400_000.00,
     "Daniel Sutherland", date(2025, 8, 20), date(2026, 6, 30)),
    ("RSK-003-03", "PRJ-003", "Disputed change order — coatings scope (ArcticTech)",
     "Sub claiming additional scope for cathodic protection at unanticipated soil-resistivity zones. "
     "Under review; merit assessment pending.",
     "Subcontractor", "Low", "Medium", "Open", 750_000.00,
     "Daniel Sutherland", date(2025, 11, 12), date(2026, 2, 28)),

    # PRJ-004 — Royal Inland Hospital Tower (2 risks, closing out)
    ("RSK-004-01", "PRJ-004", "Final commissioning — HVAC balancing",
     "Air balance commissioning behind schedule due to BMS integration issues. Risk to substantial "
     "completion certificate timing.",
     "Schedule", "Medium", "Medium", "Mitigating", 700_000.00,
     "Annika Berglund", date(2025, 10, 25), date(2026, 2, 28)),
    ("RSK-004-02", "PRJ-004", "Punch list extension — interior finishes",
     "Higher-than-typical punch list volume. Owner walkthroughs identifying additional items "
     "beyond contracted scope; negotiation ongoing.",
     "Quality", "Low", "Medium", "Open", 500_000.00,
     "Annika Berglund", date(2025, 11, 1), date(2026, 1, 31)),

    # PRJ-005 — Northwest Fiber Backbone (2 risks, favorable project)
    ("RSK-005-01", "PRJ-005", "Long-haul fiber sourcing — supplier lead time",
     "Single-source supplier for armored fiber spec; lead time extended from 14 to 22 weeks. "
     "Schedule risk on northern segments.",
     "Supply Chain", "Medium", "Medium", "Open", 800_000.00,
     "Jared Whitehorse", date(2025, 9, 8), date(2026, 4, 30)),
    ("RSK-005-02", "PRJ-005", "Right-of-way access — private landowner negotiations",
     "Three holdout landowners in Yellowknife corridor. GNWT support engaged; expropriation backstop available.",
     "Stakeholder", "Low", "Low", "Open", 300_000.00,
     "Jared Whitehorse", date(2025, 7, 22), date(2026, 5, 31)),

    # PRJ-006 — Highway 1 Twinning Phase 3 (3 risks, planning stage)
    ("RSK-006-01", "PRJ-006", "Land acquisition — Hope to Boston Bar corridor",
     "Approximately 14 parcels remaining for acquisition. Two contested; potential expropriation "
     "could push notice to proceed by 4-6 months.",
     "Stakeholder", "High", "Medium", "Mitigating", 4_500_000.00,
     "Olivia Tremblay", date(2025, 10, 1), date(2026, 6, 30)),
    ("RSK-006-02", "PRJ-006", "Environmental assessment — peer review timeline",
     "EA peer review extended for additional cumulative-effects analysis. Permit issuance now "
     "Q2 2026 vs. originally planned Q4 2025.",
     "Regulatory", "Medium", "High", "Open", 1_200_000.00,
     "Olivia Tremblay", date(2025, 11, 20), date(2026, 6, 30)),
    ("RSK-006-03", "PRJ-006", "First Nations consultation — Sto:lo / Nlaka'pamux",
     "Active engagement on cultural and burial sites along corridor. Construction methodology "
     "may require modification at three identified locations.",
     "Stakeholder", "Medium", "Medium", "Mitigating", 900_000.00,
     "Olivia Tremblay", date(2025, 9, 15), date(2026, 8, 31)),
]

risk_register_schema = (
    "risk_id STRING, "
    "project_id STRING, "
    "title STRING, "
    "description STRING, "
    "category STRING, "
    "severity STRING, "
    "probability STRING, "
    "status STRING, "
    "estimated_impact_cad DOUBLE, "
    "owner STRING, "
    "identified_date DATE, "
    "target_resolution_date DATE"
)

risk_register_df = spark.createDataFrame(risk_data, risk_register_schema)

(risk_register_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RISK_REGISTER_TABLE))

display(spark.table(RISK_REGISTER_TABLE).orderBy("project_id", F.desc("estimated_impact_cad")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## WIP Report View
# MAGIC Denormalized Work-in-Progress rollup joining `projects`, `contract_summaries`, `cash_position`,
# MAGIC `pending_changes`, `outstanding_ar`, and current-period `budget_performance`. Adds derived
# MAGIC metrics so Genie can answer common construction-finance questions from a single object:
# MAGIC
# MAGIC - **cpi** — Cost Performance Index (earned / actual; >1.0 favorable)
# MAGIC - **etc_cad** — Estimate to Complete (forecast - actual to date)
# MAGIC - **overbilled_cad** — Billings vs. earned revenue (positive = overbilled, owe work)
# MAGIC - **margin_fade_cad** — Last month forecast margin minus current forecast margin
# MAGIC - **total_overdue_ar_cad** — Sum of all overdue AR buckets
# MAGIC - **forecast_margin_pct** — Forecast margin as % of revised contract
# MAGIC - **open_risk_count** / **open_risk_impact_cad** — Aggregated from `risk_register`
# MAGIC - **subcontract_committed_cad** / **subcontract_holdback_outstanding_cad** — From `subcontractor_commitments`

# COMMAND ----------

WIP_REPORT_VIEW = f"{CATALOG}.{SCHEMA}.vw_wip_report"

spark.sql(f"""
CREATE OR REPLACE VIEW {WIP_REPORT_VIEW} AS
WITH risk_agg AS (
  SELECT
    project_id,
    COUNT(*) FILTER (WHERE status IN ('Open', 'Mitigating')) AS open_risk_count,
    COALESCE(SUM(CASE WHEN status IN ('Open', 'Mitigating') THEN estimated_impact_cad END), 0.0) AS open_risk_impact_cad,
    COUNT(*) FILTER (WHERE severity = 'Critical' AND status IN ('Open', 'Mitigating')) AS critical_risk_count
  FROM {CATALOG}.{SCHEMA}.risk_register
  GROUP BY project_id
),
sub_agg AS (
  SELECT
    project_id,
    COUNT(*) AS subcontract_count,
    SUM(revised_commitment_cad) AS subcontract_committed_cad,
    SUM(billed_to_date_cad) AS subcontract_billed_cad,
    SUM(holdback_cad) AS subcontract_holdback_outstanding_cad,
    COUNT(*) FILTER (WHERE status = 'Disputed') AS disputed_subcontract_count
  FROM {CATALOG}.{SCHEMA}.subcontractor_commitments
  GROUP BY project_id
)
SELECT
  p.project_id, p.project_name, p.division, p.owner, p.status AS project_status,
  p.project_manager, p.start_date, p.planned_end_date, p.month_end_date,
  p.contract_value_cad,
  -- Contract summary
  cs.original_contract_price_cad, cs.approved_change_revenue_cad, cs.revised_contract_price_cad,
  cs.original_contract_margin_cad, cs.revised_contract_margin_cad,
  cs.forecast_final_margin_cad, cs.last_month_forecast_margin_cad, cs.earned_revenue_cad,
  -- Cash
  cp.billings_to_date_cad, cp.uncollected_ar_cad, cp.client_holdback_cad,
  cp.total_cash_received_cad, cp.total_cost_to_date_cad, cp.subcontractor_holdback_cad,
  cp.total_cash_paid_cad, cp.cash_position_cad,
  -- Pending changes (non-contributing)
  pc.pending_change_revenue_cad, pc.probable_change_revenue_cad,
  pc.probable_contract_amount_cad, pc.probable_contract_margin_cad,
  -- AR aging
  ar.current_not_overdue_cad, ar.overdue_1_30_cad, ar.overdue_31_60_cad,
  ar.overdue_61_90_cad, ar.overdue_91_plus_cad,
  -- Budget performance (current period)
  bp.period_start_date, bp.period_end_date,
  bp.period_pct_complete, bp.period_control_budget_earned_cad, bp.period_actual_cad, bp.period_gain_loss_cad,
  bp.total_pct_complete, bp.total_control_budget_earned_cad, bp.total_actual_cad, bp.total_gain_loss_cad,
  bp.forecast_final_cad AS budget_forecast_final_cad, bp.cost_forecast_variance_cad,
  bp.previous_forecast_final_cad, bp.forecast_final_change_cad,
  -- Risks
  COALESCE(r.open_risk_count, 0) AS open_risk_count,
  COALESCE(r.open_risk_impact_cad, 0.0) AS open_risk_impact_cad,
  COALESCE(r.critical_risk_count, 0) AS critical_risk_count,
  -- Subcontractors
  COALESCE(s.subcontract_count, 0) AS subcontract_count,
  COALESCE(s.subcontract_committed_cad, 0.0) AS subcontract_committed_cad,
  COALESCE(s.subcontract_billed_cad, 0.0) AS subcontract_billed_cad,
  COALESCE(s.subcontract_holdback_outstanding_cad, 0.0) AS subcontract_holdback_outstanding_cad,
  COALESCE(s.disputed_subcontract_count, 0) AS disputed_subcontract_count,
  -- Derived metrics
  CASE WHEN bp.total_actual_cad > 0
       THEN bp.total_control_budget_earned_cad / bp.total_actual_cad END AS cpi,
  bp.forecast_final_cad - bp.total_actual_cad AS etc_cad,
  cp.billings_to_date_cad - cs.earned_revenue_cad AS overbilled_cad,
  cs.last_month_forecast_margin_cad - cs.forecast_final_margin_cad AS margin_fade_cad,
  COALESCE(ar.overdue_1_30_cad, 0) + COALESCE(ar.overdue_31_60_cad, 0)
    + COALESCE(ar.overdue_61_90_cad, 0) + COALESCE(ar.overdue_91_plus_cad, 0) AS total_overdue_ar_cad,
  CASE WHEN cs.revised_contract_price_cad > 0
       THEN cs.forecast_final_margin_cad / cs.revised_contract_price_cad END AS forecast_margin_pct
FROM {CATALOG}.{SCHEMA}.projects p
LEFT JOIN {CATALOG}.{SCHEMA}.contract_summaries cs USING (project_id)
LEFT JOIN {CATALOG}.{SCHEMA}.cash_position cp USING (project_id)
LEFT JOIN {CATALOG}.{SCHEMA}.pending_changes pc USING (project_id)
LEFT JOIN {CATALOG}.{SCHEMA}.outstanding_ar ar USING (project_id)
LEFT JOIN {CATALOG}.{SCHEMA}.budget_performance bp USING (project_id)
LEFT JOIN risk_agg r USING (project_id)
LEFT JOIN sub_agg s USING (project_id)
""")

display(spark.table(WIP_REPORT_VIEW))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table & Column Descriptions
# MAGIC Applies metadata comments to every table, view, and column. Genie reads these to interpret
# MAGIC natural-language questions — without descriptions it has to guess from column names alone,
# MAGIC which often produces wrong joins or filters. With descriptions, accuracy improves dramatically.
# MAGIC
# MAGIC Re-run safely: `COMMENT ON` and `ALTER ... ALTER COLUMN ... COMMENT` are idempotent.

# COMMAND ----------

def _esc(s):
    return s.replace("'", "''")

VIEW_NAMES = {"vw_wip_report"}

TABLE_DESCRIPTIONS = {
    "projects": "Curated register of major Ledcor construction projects. Each row is one project with high-level attributes including division, owner, project manager, lifecycle status, dates, and current contract value. Six projects spanning Civil/Mining/Infrastructure, Pipeline, Building, and Communications divisions across BC, AB, and NWT.",
    "jobsites": "Higher-volume synthetic dataset of active construction jobsites generated for general queries and demos. NOT joined to projects — independent rows with random project names, types, statuses, budgets, and dates across Canadian provinces.",
    "contract_summaries": "Per-project contract financial summary. Tracks original/approved/pending revenue, contract margins (original, revised, forecast, last-month), and earned revenue. One row per project. PRJ-001 contains real Ledcor numbers; others simulated.",
    "pending_changes": "Non-contributing pending and probable change orders per project. Tracks change order revenue not yet baked into the revised contract price, plus the probable contract amount/margin if those changes come through.",
    "cash_position": "Per-project cash flow rollup: billings vs. cost net of holdbacks, accruals, and uncollected AR. Yields net cash position. Holdbacks reflect Canadian provincial lien act standards (~5%).",
    "outstanding_ar": "Open accounts receivable bucketed by aging in days past due. Excludes paid invoices. Use for collection risk analysis. Buckets: Current (within terms), 1-30, 31-60, 61-90, 91+ days overdue.",
    "budget_performance": "Most-recent month-end snapshot of budget performance per project. Single current period per project covering current period, cumulative-to-date, and period-over-period forecast change. For trend analysis use budget_performance_history.",
    "budget_performance_history": "Historical monthly snapshots of budget performance per project (up to 12 prior periods including current). Use for trend, burn rate, margin fade, and forecast drift analysis. Most recent row matches budget_performance for each project.",
    "schedule_progress": "Planned vs. actual cumulative % complete per project per period, with Schedule Performance Index (SPI). SPI > 1.0 = ahead of schedule, < 1.0 = behind.",
    "subcontractor_commitments": "Major subcontractor agreements per project. Each row is one subcontract with original commitment, approved change orders, billings, payments, holdback, and status (Active, Completed, Disputed, Default).",
    "risk_register": "Open risks and issues tracked per project. Includes severity, probability, status, estimated dollar impact, owner, and target resolution date for risk-weighted analysis.",
}

VIEW_DESCRIPTIONS = {
    "vw_wip_report": "Denormalized Work-in-Progress (WIP) report joining projects with contract_summaries, cash_position, pending_changes, outstanding_ar, current-period budget_performance, plus aggregated risk_register and subcontractor_commitments. Includes derived metrics (CPI, ETC, overbilled, margin fade, total overdue AR, forecast margin %). PRIMARY table for AI/BI dashboards and Genie natural-language queries — single object answers most cross-cutting questions without joins.",
}

COLUMN_DESCRIPTIONS = {
    "projects": {
        "project_id": "Unique project identifier in PRJ-NNN format. Primary key.",
        "ineight_project_id": "Project ID in the InEight cost-management source system",
        "jde_job_number": "Job number in JD Edwards (JDE) ERP source system",
        "project_name": "Human-readable project name",
        "division": "Ledcor business division: Civil, Mining & Infrastructure / Pipeline / Building / Communications",
        "owner": "Project owner / client (the entity Ledcor is building for)",
        "status": "Current lifecycle status: Planning, Active, On Hold, Closing Out, Complete",
        "start_date": "Project start date (notice to proceed)",
        "planned_end_date": "Planned project completion date per current schedule",
        "month_end_date": "Most recent month-end reporting cutoff date for this project",
        "project_manager": "Ledcor project manager responsible for delivery",
        "contract_value_cad": "Current revised contract value in Canadian dollars",
    },
    "jobsites": {
        "jobsite_id": "Unique synthetic jobsite identifier",
        "project_name": "Random synthetic project name",
        "province": "Canadian province code (AB, BC, ON, SK, MB, QC, NS)",
        "project_type": "Type of construction: Commercial, Industrial, Infrastructure, Residential, Pipeline, Mining",
        "status": "Jobsite status: Planning, Active, On Hold, Closing Out, Complete",
        "budget_cad": "Jobsite budget in Canadian dollars (range $500K to $250M)",
        "start_date": "Jobsite start date",
        "planned_end_date": "Jobsite planned end date",
    },
    "contract_summaries": {
        "project_id": "Foreign key to projects.project_id",
        "original_contract_price_cad": "Original signed contract price in CAD before any change orders",
        "approved_change_revenue_cad": "Sum of approved change order revenue added to original price (CAD)",
        "pending_change_revenue_cad": "Pending change order revenue contributing to revised contract price (CAD)",
        "revised_contract_price_cad": "Current revised contract price = original + approved + contributing pending changes (CAD)",
        "original_contract_margin_cad": "Forecast gross margin at contract signing (CAD)",
        "revised_contract_margin_cad": "Current forecast gross margin against revised contract (CAD)",
        "forecast_final_margin_cad": "Most recent estimate of final gross margin at project completion / EAC margin (CAD)",
        "last_month_forecast_margin_cad": "Forecast final margin from last month-end report (CAD). Compare to forecast_final_margin_cad to detect margin fade.",
        "earned_revenue_cad": "Revenue earned to date based on % complete (CAD). Distinct from billings — billings follow contract milestones, earned revenue follows physical progress.",
    },
    "pending_changes": {
        "project_id": "Foreign key to projects.project_id",
        "pending_change_revenue_cad": "Pending change orders submitted but not approved (CAD). Non-contributing — not yet in revised contract price.",
        "probable_change_revenue_cad": "Estimated revenue from probable change orders (CAD)",
        "probable_contract_amount_cad": "Probable contract amount = revised contract price + probable changes (CAD)",
        "probable_contract_margin_cad": "Probable contract margin if probable changes come through (CAD)",
    },
    "cash_position": {
        "project_id": "Foreign key to projects.project_id",
        "billings_to_date_cad": "Total amount invoiced to client to date (CAD). Note: billings differ from earned revenue.",
        "uncollected_ar_cad": "Past-due accounts receivable not yet collected (CAD)",
        "client_holdback_cad": "Lien holdback withheld by client per Canadian provincial lien acts (typically 5-10%) (CAD)",
        "total_cash_received_cad": "Net cash received = billings - uncollected AR - client holdback (CAD)",
        "total_cost_to_date_cad": "Total project cost incurred to date (CAD)",
        "accruals_cad": "Costs incurred but not yet invoiced or paid (CAD)",
        "subcontractor_holdback_cad": "Lien holdback retained from subcontractor billings (CAD)",
        "total_cash_paid_cad": "Net cash paid out = total cost - accruals - subcontractor holdback (CAD)",
        "cash_position_cad": "Net cash position = total cash received - total cash paid (CAD). Positive = cash positive on the project.",
    },
    "outstanding_ar": {
        "project_id": "Foreign key to projects.project_id",
        "current_not_overdue_cad": "Open invoices within payment terms / not yet overdue (CAD)",
        "overdue_1_30_cad": "Invoices 1-30 days past due (CAD)",
        "overdue_31_60_cad": "Invoices 31-60 days past due (CAD)",
        "overdue_61_90_cad": "Invoices 61-90 days past due (CAD)",
        "overdue_91_plus_cad": "Invoices 91+ days past due (CAD). High collection risk.",
    },
    "budget_performance": {
        "project_id": "Foreign key to projects.project_id",
        "period_start_date": "Reporting period start date",
        "period_end_date": "Reporting period end date (month-end cutoff)",
        "period_pct_complete": "Incremental % complete added during this period (not cumulative)",
        "period_control_budget_earned_cad": "Earned value (control budget basis) for the current period (CAD)",
        "period_actual_cad": "Actual cost incurred during the current period (CAD)",
        "period_gain_loss_cad": "Period gain/loss = earned - actual (CAD). Negative = unfavorable variance / over budget for period.",
        "total_pct_complete": "Cumulative project % complete as of period end",
        "total_control_budget_earned_cad": "Cumulative earned value to date against control budget (CAD)",
        "total_actual_cad": "Cumulative actual cost to date (CAD)",
        "total_gain_loss_cad": "Cumulative gain/loss = total earned - total actual (CAD). Negative = over budget overall.",
        "forecast_final_cad": "Estimate at completion / EAC — forecast final cost at project completion (CAD)",
        "cost_forecast_variance_cad": "Original control budget - forecast final (CAD). Negative = trending over budget.",
        "previous_forecast_final_cad": "Forecast final from prior period (CAD). Used for fade analysis.",
        "forecast_final_change_cad": "Period-over-period change in forecast final (CAD). Positive = forecast worsening / cost trending up.",
    },
    "budget_performance_history": {
        "project_id": "Foreign key to projects.project_id",
        "period_start_date": "Reporting period start date",
        "period_end_date": "Reporting period end date (month-end cutoff)",
        "period_pct_complete": "Incremental % complete added during this period",
        "period_control_budget_earned_cad": "Earned value for the period (CAD)",
        "period_actual_cad": "Actual cost incurred during the period (CAD)",
        "period_gain_loss_cad": "Period gain/loss = earned - actual (CAD)",
        "total_pct_complete": "Cumulative project % complete as of this period end",
        "total_control_budget_earned_cad": "Cumulative earned value to date as of this period (CAD)",
        "total_actual_cad": "Cumulative actual cost to date as of this period (CAD)",
        "total_gain_loss_cad": "Cumulative gain/loss as of this period (CAD)",
        "forecast_final_cad": "Forecast final cost (EAC) as of this period (CAD)",
        "cost_forecast_variance_cad": "Original budget - forecast final at this period (CAD)",
        "previous_forecast_final_cad": "Forecast final from one period prior (CAD)",
        "forecast_final_change_cad": "Period-over-period change in forecast final (CAD)",
    },
    "schedule_progress": {
        "project_id": "Foreign key to projects.project_id",
        "period_start_date": "Reporting period start date",
        "period_end_date": "Reporting period end date",
        "planned_pct_complete": "Planned cumulative % complete per baseline schedule",
        "actual_pct_complete": "Actual cumulative % complete achieved",
        "schedule_variance_pct": "Actual - planned (percentage points). Positive = ahead of schedule, negative = behind.",
        "spi": "Schedule Performance Index = actual / planned. Greater than 1.0 = ahead of schedule, less than 1.0 = behind schedule.",
    },
    "subcontractor_commitments": {
        "subcontract_id": "Unique subcontract identifier",
        "project_id": "Foreign key to projects.project_id",
        "subcontractor_name": "Subcontractor company name",
        "scope": "Scope of work covered by the subcontract (e.g., Excavation & Tunneling, HVAC, Electrical)",
        "original_commitment_cad": "Original commitment value at subcontract signing (CAD)",
        "approved_change_orders_cad": "Approved change orders against the subcontract (CAD)",
        "revised_commitment_cad": "Current revised commitment = original + approved change orders (CAD)",
        "billed_to_date_cad": "Total subcontractor billings received to date (CAD)",
        "paid_to_date_cad": "Total cash paid to subcontractor to date (CAD)",
        "holdback_cad": "Outstanding lien holdback retained from subcontractor billings (CAD)",
        "status": "Subcontract status: Active, Completed, Disputed, Default",
    },
    "risk_register": {
        "risk_id": "Unique risk identifier",
        "project_id": "Foreign key to projects.project_id",
        "title": "Short risk title or summary",
        "description": "Detailed risk description and mitigation context",
        "category": "Risk category: Geotechnical, Schedule, Cost, Subcontractor, Stakeholder, Regulatory, Quality, Supply Chain, Scope",
        "severity": "Severity rating: Low, Medium, High, Critical",
        "probability": "Likelihood of occurrence: Low, Medium, High",
        "status": "Risk status: Open, Mitigating, Closed, Accepted",
        "estimated_impact_cad": "Estimated cost impact if risk materializes (CAD)",
        "owner": "Person accountable for managing the risk",
        "identified_date": "Date risk was identified and logged",
        "target_resolution_date": "Target date for resolution or end of risk window",
    },
    "vw_wip_report": {
        "project_id": "Project identifier (PRJ-NNN). Primary key for the WIP report.",
        "project_name": "Human-readable project name",
        "division": "Ledcor business division",
        "owner": "Project owner / client",
        "project_status": "Current project lifecycle status: Planning, Active, On Hold, Closing Out, Complete",
        "project_manager": "Ledcor project manager",
        "start_date": "Project start date",
        "planned_end_date": "Planned project completion date",
        "month_end_date": "Most recent month-end reporting cutoff",
        "contract_value_cad": "Current revised contract value (CAD)",
        "original_contract_price_cad": "Original signed contract price (CAD)",
        "approved_change_revenue_cad": "Approved change order revenue (CAD)",
        "revised_contract_price_cad": "Revised contract price = original + approved + contributing pending (CAD)",
        "original_contract_margin_cad": "Forecast gross margin at contract signing (CAD)",
        "revised_contract_margin_cad": "Current forecast gross margin against revised contract (CAD)",
        "forecast_final_margin_cad": "Most recent estimate of final gross margin at completion / EAC margin (CAD)",
        "last_month_forecast_margin_cad": "Forecast final margin from last month (CAD). Used for margin fade analysis.",
        "earned_revenue_cad": "Revenue earned to date based on physical % complete (CAD)",
        "billings_to_date_cad": "Total invoiced to client to date (CAD)",
        "uncollected_ar_cad": "Past-due AR not yet collected (CAD)",
        "client_holdback_cad": "Lien holdback withheld by client (CAD)",
        "total_cash_received_cad": "Net cash received from client (CAD)",
        "total_cost_to_date_cad": "Total project cost incurred to date (CAD)",
        "subcontractor_holdback_cad": "Lien holdback retained from subcontractors (CAD)",
        "total_cash_paid_cad": "Net cash paid to subcontractors and suppliers (CAD)",
        "cash_position_cad": "Net cash position = received - paid (CAD). Positive = cash positive.",
        "pending_change_revenue_cad": "Non-contributing pending change orders (CAD)",
        "probable_change_revenue_cad": "Estimated probable change order revenue (CAD)",
        "probable_contract_amount_cad": "Revised contract + probable changes (CAD)",
        "probable_contract_margin_cad": "Probable contract margin if probable changes land (CAD)",
        "current_not_overdue_cad": "Open AR within payment terms (CAD)",
        "overdue_1_30_cad": "AR 1-30 days past due (CAD)",
        "overdue_31_60_cad": "AR 31-60 days past due (CAD)",
        "overdue_61_90_cad": "AR 61-90 days past due (CAD)",
        "overdue_91_plus_cad": "AR 91+ days past due (CAD). High collection risk.",
        "period_start_date": "Current reporting period start date",
        "period_end_date": "Current reporting period end date",
        "period_pct_complete": "Incremental % complete this period",
        "period_control_budget_earned_cad": "Earned value for current period (CAD)",
        "period_actual_cad": "Actual cost for current period (CAD)",
        "period_gain_loss_cad": "Current period gain/loss = earned - actual (CAD). Negative = unfavorable.",
        "total_pct_complete": "Cumulative project % complete",
        "total_control_budget_earned_cad": "Cumulative earned value to date (CAD)",
        "total_actual_cad": "Cumulative actual cost to date (CAD)",
        "total_gain_loss_cad": "Cumulative gain/loss to date (CAD). Negative = over budget overall.",
        "budget_forecast_final_cad": "Estimate at completion / EAC — forecast final cost (CAD)",
        "cost_forecast_variance_cad": "Original budget - forecast final (CAD). Negative = trending over.",
        "previous_forecast_final_cad": "Forecast final from prior period (CAD)",
        "forecast_final_change_cad": "Period-over-period change in EAC (CAD). Positive = forecast worsening.",
        "open_risk_count": "Number of open or mitigating risks for this project",
        "open_risk_impact_cad": "Sum of estimated impact for open/mitigating risks (CAD)",
        "critical_risk_count": "Number of Critical-severity open risks",
        "subcontract_count": "Total number of major subcontracts on the project",
        "subcontract_committed_cad": "Sum of revised commitments across all subcontracts (CAD)",
        "subcontract_billed_cad": "Sum of subcontractor billings received to date (CAD)",
        "subcontract_holdback_outstanding_cad": "Sum of outstanding subcontractor holdback (CAD)",
        "disputed_subcontract_count": "Number of subcontracts in Disputed status",
        "cpi": "Cost Performance Index = total earned / total actual. Greater than 1.0 = under budget (favorable), less than 1.0 = over budget.",
        "etc_cad": "Estimate to Complete = forecast final - actual to date (CAD). Cost remaining to finish project.",
        "overbilled_cad": "Billings - earned revenue (CAD). Positive = overbilled (owe work to client), negative = underbilled (work done not yet billed).",
        "margin_fade_cad": "Last month forecast margin - current forecast margin (CAD). Positive = margin eroded this period.",
        "total_overdue_ar_cad": "Sum of all past-due AR buckets (CAD)",
        "forecast_margin_pct": "Forecast final margin as a percentage of revised contract price (decimal, e.g., 0.10 = 10%)",
    },
}

for tbl, desc in TABLE_DESCRIPTIONS.items():
    spark.sql(f"COMMENT ON TABLE {CATALOG}.{SCHEMA}.{tbl} IS '{_esc(desc)}'")

for vw, desc in VIEW_DESCRIPTIONS.items():
    spark.sql(f"COMMENT ON VIEW {CATALOG}.{SCHEMA}.{vw} IS '{_esc(desc)}'")

for obj, cols in COLUMN_DESCRIPTIONS.items():
    for col, desc in cols.items():
        spark.sql(
            f"COMMENT ON COLUMN {CATALOG}.{SCHEMA}.{obj}.{col} "
            f"IS '{_esc(desc)}'"
        )

print(f"Applied descriptions to {len(TABLE_DESCRIPTIONS)} tables, {len(VIEW_DESCRIPTIONS)} views, "
      f"and {sum(len(c) for c in COLUMN_DESCRIPTIONS.values())} columns.")

