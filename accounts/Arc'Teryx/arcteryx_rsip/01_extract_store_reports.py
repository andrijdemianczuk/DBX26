# Databricks notebook source
# MAGIC %md
# MAGIC # Arc'Teryx RSIP - Store Report Extraction
# MAGIC
# MAGIC This notebook reads weekly store report PDFs from a Unity Catalog Volume,
# MAGIC uses Foundation Model APIs (Claude Sonnet) to extract structured data,
# MAGIC and writes the results to Delta Tables.
# MAGIC
# MAGIC **Input:** `/Volumes/ademianczuk_uc_1_catalog/arcteryx_rsip/store_reports/*.pdf`
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `arcteryx_rsip.store_metrics` — weekly KPIs per store
# MAGIC - `arcteryx_rsip.topic_sentiments` — sentiment by topic (Product, People, Operations, Experience)
# MAGIC - `arcteryx_rsip.negative_sentiments` — flagged negative sentiment indicators
# MAGIC - `arcteryx_rsip.topic_details` — wins and actions per topic

# COMMAND ----------

# MAGIC %pip install pypdf openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import re
from pathlib import Path

from openai import OpenAI
from pypdf import PdfReader
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "arcteryx_rsip"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/store_reports"
MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Foundation Model API Client

# COMMAND ----------

client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .get(),
    base_url=f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/serving-endpoints",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDF Text Extraction

# COMMAND ----------

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text content from a PDF file."""
    reader = PdfReader(pdf_path)
    pages = [page.extract_text() for page in reader.pages]
    return "\n\n".join(pages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Extraction Prompt

# COMMAND ----------

EXTRACTION_PROMPT = """You are a structured data extraction and sentiment analysis agent. Given the text of an Arc'Teryx weekly store report, you must:

1. EXTRACT factual data (store info, metrics, topics with wins/actions, challenges)
2. DERIVE sentiment scores by analyzing the tone and content of each section

Return ONLY valid JSON with this exact schema:

{
  "store_name": "string",
  "region": "string",
  "week_number": integer,
  "metrics": {
    "sales_plan": float,
    "sales_actual": float,
    "attainment_pct": float,
    "weekly_conversion_pct": float
  },
  "store_summary": "string",
  "topics": [
    {
      "topic_name": "string",
      "wins": ["string", ...],
      "actions": ["string", ...],
      "sentiment": {
        "category": "string (positive|negative|neutral)",
        "score": float (0.0-1.0, where 1.0 is most positive),
        "confidence": float (0.0-1.0, how confident you are in the assessment),
        "reasoning": "string (brief explanation of why this score was assigned)"
      }
    }
  ],
  "challenges": [
    {
      "topic": "string",
      "narrative": "string",
      "sentiment": {
        "category": "negative",
        "score": float (0.0-1.0, where 0.0 is most negative),
        "confidence": float (0.0-1.0),
        "reasoning": "string"
      }
    }
  ],
  "overall_sentiment": {
    "category": "string (positive|negative|neutral)",
    "score": float (0.0-1.0),
    "confidence": float (0.0-1.0),
    "reasoning": "string (holistic assessment considering metrics, wins, actions, and challenges)"
  }
}

Rules:
- Extract numbers exactly as they appear (dollar amounts without $ or commas)
- If a field is not present, use null
- Do not add any text outside the JSON object
- DERIVE sentiment by analyzing:
  - The language used (strong/positive words vs concern/negative words)
  - Metrics performance (attainment above/below 100%)
  - Ratio of wins to actions (more wins = more positive)
  - Severity of challenges described
- Score guidelines:
  - 0.8-1.0: Strongly positive (exceeding targets, enthusiastic language, many wins)
  - 0.6-0.8: Moderately positive (meeting targets, generally good with minor concerns)
  - 0.4-0.6: Neutral/mixed (mixed results, some good some bad)
  - 0.2-0.4: Moderately negative (below targets, significant concerns)
  - 0.0-0.2: Strongly negative (major issues, urgent language)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Function

# COMMAND ----------

def extract_report_data(pdf_path: str) -> dict:
    """Extract structured data from a single store report PDF."""
    text = extract_text_from_pdf(pdf_path)

    response = client.chat.completions.create(
        model=MODEL_ENDPOINT,
        messages=[
            {"role": "system", "content": EXTRACTION_PROMPT},
            {"role": "user", "content": f"Extract data from this store report:\n\n{text}"},
        ],
        temperature=0.0,
        max_tokens=4096,
    )

    content = response.choices[0].message.content.strip()
    # Strip optional markdown code fences (```json ... ``` or ``` ... ```).
    # Regex-based so it doesn't depend on fragile literal-backtick handling.
    fence = chr(96) * 3  # ``` without typing literal backticks
    if content.startswith(fence):
        content = re.sub(rf"^{fence}[a-zA-Z]*\n", "", content)
        content = re.sub(rf"\n?{fence}$", "", content)
        content = content.strip()

    return json.loads(content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process All Reports

# COMMAND ----------

pdf_files = sorted(Path(VOLUME_PATH).glob("*.pdf"))
print(f"Found {len(pdf_files)} PDF reports to process")

# COMMAND ----------

results = []
errors = []

for i, pdf_path in enumerate(pdf_files):
    try:
        data = extract_report_data(str(pdf_path))
        data["source_file"] = pdf_path.name
        results.append(data)
        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{len(pdf_files)} reports")
    except Exception as e:
        errors.append({"file": pdf_path.name, "error": str(e)})
        print(f"Error processing {pdf_path.name}: {e}")

print(f"\nCompleted: {len(results)} successful, {len(errors)} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Metrics Table

# COMMAND ----------

metrics_rows = []
for r in results:
    m = r.get("metrics", {})
    metrics_rows.append({
        "store_name": r["store_name"],
        "region": r["region"],
        "week_number": r["week_number"],
        "sales_plan": m.get("sales_plan"),
        "sales_actual": m.get("sales_actual"),
        "attainment_pct": m.get("attainment_pct"),
        "weekly_conversion_pct": m.get("weekly_conversion_pct"),
        "store_summary": r.get("store_summary"),
        "source_file": r["source_file"],
    })

df_metrics = spark.createDataFrame(metrics_rows)
df_metrics.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.store_metrics")
print(f"Wrote {df_metrics.count()} rows to store_metrics")
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topic Sentiments Table (Derived)
# MAGIC Sentiment scores are **derived by the LLM** from the qualitative content of each topic section.

# COMMAND ----------

sentiment_rows = []
for r in results:
    for topic in r.get("topics", []):
        s = topic.get("sentiment", {})
        sentiment_rows.append({
            "store_name": r["store_name"],
            "region": r["region"],
            "week_number": r["week_number"],
            "topic": topic["topic_name"],
            "category": s.get("category"),
            "score": s.get("score"),
            "confidence": s.get("confidence"),
            "reasoning": s.get("reasoning"),
            "source_file": r["source_file"],
        })

df_sentiments = spark.createDataFrame(sentiment_rows)
df_sentiments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.topic_sentiments")
print(f"Wrote {df_sentiments.count()} rows to topic_sentiments")
display(df_sentiments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenges Table (with Derived Sentiment)
# MAGIC Challenges are extracted from the "Challenges & Areas of Concern" section.
# MAGIC Sentiment severity is derived by the LLM based on the narrative language.

# COMMAND ----------

challenge_rows = []
for r in results:
    for ch in r.get("challenges", []):
        s = ch.get("sentiment", {})
        challenge_rows.append({
            "store_name": r["store_name"],
            "region": r["region"],
            "week_number": r["week_number"],
            "topic": ch["topic"],
            "narrative": ch["narrative"],
            "severity_score": s.get("score"),
            "confidence": s.get("confidence"),
            "reasoning": s.get("reasoning"),
            "source_file": r["source_file"],
        })

df_challenges = spark.createDataFrame(challenge_rows)
df_challenges.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.challenges")
print(f"Wrote {df_challenges.count()} rows to challenges")
display(df_challenges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topic Details Table (Wins & Actions)

# COMMAND ----------

detail_rows = []
for r in results:
    for topic in r.get("topics", []):
        for win in topic.get("wins", []):
            detail_rows.append({
                "store_name": r["store_name"],
                "region": r["region"],
                "week_number": r["week_number"],
                "topic": topic["topic_name"],
                "type": "win",
                "detail": win,
                "source_file": r["source_file"],
            })
        for action in topic.get("actions", []):
            detail_rows.append({
                "store_name": r["store_name"],
                "region": r["region"],
                "week_number": r["week_number"],
                "topic": topic["topic_name"],
                "type": "action",
                "detail": action,
                "source_file": r["source_file"],
            })

df_details = spark.createDataFrame(detail_rows)
df_details.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.topic_details")
print(f"Wrote {df_details.count()} rows to topic_details")
display(df_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Store Sentiment Table

# COMMAND ----------

overall_rows = []
for r in results:
    o = r.get("overall_sentiment", {})
    overall_rows.append({
        "store_name": r["store_name"],
        "region": r["region"],
        "week_number": r["week_number"],
        "category": o.get("category"),
        "score": o.get("score"),
        "confidence": o.get("confidence"),
        "reasoning": o.get("reasoning"),
        "source_file": r["source_file"],
    })

df_overall = spark.createDataFrame(overall_rows)
df_overall.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.overall_sentiment")
print(f"Wrote {df_overall.count()} rows to overall_sentiment")
display(df_overall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("EXTRACTION & SENTIMENT ANALYSIS COMPLETE")
print("=" * 60)
print(f"\nReports processed: {len(results)}")
print(f"Errors: {len(errors)}")
print(f"\nTables created in {CATALOG}.{SCHEMA}:")
print(f"  - store_metrics:      {df_metrics.count()} rows")
print(f"  - topic_sentiments:   {df_sentiments.count()} rows (DERIVED)")
print(f"  - challenges:         {df_challenges.count()} rows (DERIVED)")
print(f"  - topic_details:      {df_details.count()} rows")
print(f"  - overall_sentiment:  {df_overall.count()} rows (DERIVED)")

if errors:
    print(f"\nFailed files:")
    for err in errors:
        print(f"  - {err['file']}: {err['error']}")

