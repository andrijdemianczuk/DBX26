# Databricks notebook source
# MAGIC %md
# MAGIC # Corridor Briefing Agent
# MAGIC Governed, single-purpose, MLflow-traced. Given a corridor id (`COR-###`) or a
# MAGIC client name, it returns open detections by severity, recommended work orders
# MAGIC costed from historical averages, affected landowners (PII — masked under policy),
# MAGIC and a Foundation-Model executive summary.
# MAGIC
# MAGIC It queries the governed tables **as you**, so when the ABAC policy is applied the
# MAGIC briefing is automatically scoped (rows) and masked (PII). Open the **MLflow trace**
# MAGIC from the cell output or the Experiments UI.

# COMMAND ----------

# Ensures the notebook runs on any compute (serverless base env may lack mlflow).
%pip install -q mlflow databricks-sdk
dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

SCH = "ademianczuk_uc_1_catalog.stantec_grid_ops"
FM_ENDPOINT = "databricks-claude-sonnet-5"
mlflow.set_experiment("/Users/andrij.demianczuk@databricks.com/grid_corridor_agent")
_w = WorkspaceClient()

# COMMAND ----------

def _scope(target):
    return (f"corridor_id = '{target.upper()}'" if target.upper().startswith("COR-")
            else f"client_name = '{target}'")

@mlflow.trace(span_type="RETRIEVER")
def open_detections_by_severity(target):
    return spark.sql(f"""
        SELECT severity, type, count(*) AS n FROM {SCH}.detections
        WHERE {_scope(target)} AND status IN ('open','dispatched')
        GROUP BY severity, type ORDER BY n DESC""").toPandas().to_dict("records")

@mlflow.trace(span_type="RETRIEVER")
def recommended_work_orders(target):
    df = spark.sql(f"""
        WITH hist AS (
            SELECT d.type, d.severity, avg(w.cost_estimate) AS avg_cost
            FROM {SCH}.work_orders w JOIN {SCH}.detections d ON d.detection_id=w.detection_id
            GROUP BY d.type, d.severity)
        SELECT d.type, d.severity, count(*) AS open_count,
               round(coalesce(h.avg_cost,0),0) AS avg_cost_cad,
               round(count(*)*coalesce(h.avg_cost,0),0) AS est_total_cad
        FROM {SCH}.detections d LEFT JOIN hist h ON h.type=d.type AND h.severity=d.severity
        WHERE {_scope(target)} AND d.status IN ('open','dispatched')
        GROUP BY d.type, d.severity, h.avg_cost ORDER BY est_total_cad DESC""").toPandas()
    return {"line_items": df.to_dict("records"),
            "total_recommended_cad": float(df["est_total_cad"].sum()) if len(df) else 0.0}

@mlflow.trace(span_type="RETRIEVER")
def affected_landowners(target, limit=5):
    return spark.sql(f"""
        SELECT detection_id, severity, landowner_contact FROM {SCH}.detections
        WHERE {_scope(target)} AND status IN ('open','dispatched')
        ORDER BY detected_at DESC LIMIT {limit}""").toPandas().to_dict("records")

@mlflow.trace(span_type="LLM")
def executive_summary(target, sev, wos):
    sysmsg = ("You are a grid-operations briefing assistant. Write ONE tight executive "
              "paragraph (<=90 words) for a Chief Digital Officer. Be concrete about risk "
              "and recommended spend in CAD. If contact data is masked, do not invent it. No preamble.")
    usr = f"Target: {target}\nOpen detections: {sev}\nRecommended work orders: {wos}\nWrite the summary."
    r = _w.serving_endpoints.query(name=FM_ENDPOINT, max_tokens=250,
        messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=sysmsg),
                  ChatMessage(role=ChatMessageRole.USER, content=usr)])
    return r.choices[0].message.content.strip()

@mlflow.trace(span_type="AGENT")
def briefing(target):
    sev = open_detections_by_severity(target)
    wos = recommended_work_orders(target)
    land = affected_landowners(target)
    return {"target": target, "open_detections_by_severity": sev,
            "recommended_work_orders": wos, "affected_landowners": land,
            "executive_summary": executive_summary(target, sev, wos)}

# COMMAND ----------

# MAGIC %md ## Run a briefing — change the target to any corridor id or client name

# COMMAND ----------

result = briefing("COR-007")   # try "Fictional Utility C" after applying the policy
print(result["executive_summary"])
display(spark.createDataFrame(result["affected_landowners"]) if result["affected_landowners"] else spark.range(0))

# COMMAND ----------

import json
print(json.dumps(result["recommended_work_orders"], indent=2, default=str))
