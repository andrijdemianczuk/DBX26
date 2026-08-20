#!/usr/bin/env python3
"""Corridor Briefing Agent — governed, single-purpose, MLflow-traced.

Given a corridor id (COR-###) or a client name, produces a structured
encroachment briefing:
  - open detections by severity
  - recommended work orders with cost estimates from historical averages
  - a one-paragraph executive summary (Foundation Model)

It queries the governed tables AS THE CALLER, so when the ABAC policy is
applied the briefing is automatically scoped (rows) and masked (PII).

Runnable locally (verification) or importable in a Databricks notebook.
"""
from __future__ import annotations
import os
import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

PROFILE = os.environ.get("GRID_PROFILE", "fe-vm-ademianczuk-uc-1")
WAREHOUSE = "c6250844810982c2"
SCH = "ademianczuk_uc_1_catalog.stantec_grid_ops"
FM_ENDPOINT = "databricks-claude-sonnet-5"

_w = WorkspaceClient(profile=PROFILE)


def _sql(stmt: str):
    r = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE, statement=stmt, wait_timeout="50s")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    data = r.result.data_array if r.result and r.result.data_array else []
    return cols, data


def _scope_clause(target: str) -> str:
    if target.upper().startswith("COR-"):
        return f"corridor_id = '{target.upper()}'"
    return f"client_name = '{target}'"


@mlflow.trace(span_type="RETRIEVER")
def open_detections_by_severity(target: str):
    """Open + dispatched detections for the target, grouped by severity."""
    where = _scope_clause(target)
    cols, data = _sql(f"""
        SELECT severity, type, count(*) AS n
        FROM {SCH}.detections
        WHERE {where} AND status IN ('open','dispatched')
        GROUP BY severity, type ORDER BY n DESC""")
    return [dict(zip(cols, row)) for row in data]


@mlflow.trace(span_type="RETRIEVER")
def recommended_work_orders(target: str):
    """Recommend work orders for unresolved detections, costed from historical
    averages by (type, severity). Returns line items + total recommended CAD."""
    where = _scope_clause(target)
    cols, data = _sql(f"""
        WITH hist AS (
            SELECT d.type, d.severity, avg(w.cost_estimate) AS avg_cost
            FROM {SCH}.work_orders w JOIN {SCH}.detections d ON d.detection_id = w.detection_id
            GROUP BY d.type, d.severity
        )
        SELECT d.type, d.severity, count(*) AS open_count,
               round(coalesce(h.avg_cost, 0), 0) AS avg_cost_cad,
               round(count(*) * coalesce(h.avg_cost, 0), 0) AS est_total_cad
        FROM {SCH}.detections d LEFT JOIN hist h ON h.type=d.type AND h.severity=d.severity
        WHERE {where} AND d.status IN ('open','dispatched')
        GROUP BY d.type, d.severity, h.avg_cost
        ORDER BY est_total_cad DESC""")
    items = [dict(zip(cols, row)) for row in data]
    total = round(sum(float(i["est_total_cad"]) for i in items), 0)
    return {"line_items": items, "total_recommended_cad": total}


@mlflow.trace(span_type="RETRIEVER")
def affected_landowners(target: str, limit: int = 5):
    """A few landowner contacts for unresolved detections. PII: masked under policy."""
    where = _scope_clause(target)
    cols, data = _sql(f"""
        SELECT detection_id, severity, landowner_contact
        FROM {SCH}.detections
        WHERE {where} AND status IN ('open','dispatched')
        ORDER BY detected_at DESC LIMIT {limit}""")
    return [dict(zip(cols, row)) for row in data]


@mlflow.trace(span_type="LLM")
def executive_summary(target: str, sev, wos) -> str:
    """One-paragraph executive summary via Foundation Model."""
    sys = ("You are a grid-operations briefing assistant for utility corridor monitoring. "
           "Write ONE tight executive paragraph (<=90 words) for a Chief Digital Officer. "
           "Be concrete about risk (severity, unresolved counts) and recommended spend in CAD. "
           "If contact data is masked, do not invent it. No preamble.")
    usr = (f"Target: {target}\n"
           f"Open detections by severity/type: {sev}\n"
           f"Recommended work orders: {wos}\n"
           f"Write the executive summary.")
    resp = _w.serving_endpoints.query(
        name=FM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=sys),
                  ChatMessage(role=ChatMessageRole.USER, content=usr)],
        max_tokens=250)
    return resp.choices[0].message.content.strip()


@mlflow.trace(span_type="AGENT")
def briefing(target: str) -> dict:
    """Produce the full structured corridor briefing for a corridor or client."""
    sev = open_detections_by_severity(target)
    wos = recommended_work_orders(target)
    landowners = affected_landowners(target)
    summary = executive_summary(target, sev, wos)
    return {
        "target": target,
        "open_detections_by_severity": sev,
        "recommended_work_orders": wos,
        "affected_landowners": landowners,
        "executive_summary": summary,
    }


def render(b: dict) -> str:
    lines = [f"\n=== CORRIDOR BRIEFING: {b['target']} ===\n",
             "Open detections (by severity/type):"]
    for r in b["open_detections_by_severity"]:
        lines.append(f"  - {r['severity']:>8} / {r['type']:<24} : {r['n']}")
    if not b["open_detections_by_severity"]:
        lines.append("  (none)")
    lines.append("\nRecommended work orders (costed from historical averages, CAD):")
    for r in b["recommended_work_orders"]["line_items"]:
        lines.append(f"  - {r['type']:<24} {r['severity']:>8}  x{int(r['open_count']):<3} "
                     f"@ ${float(r['avg_cost_cad']):>9,.0f}  = ${float(r['est_total_cad']):>11,.0f}")
    lines.append(f"  TOTAL RECOMMENDED: ${float(b['recommended_work_orders']['total_recommended_cad']):,.0f} CAD")
    lines.append("\nAffected landowners (PII — governed by column mask):")
    for r in b.get("affected_landowners", []):
        lines.append(f"  - {r['detection_id']}  {r['severity']:>8}  {r['landowner_contact']}")
    if not b.get("affected_landowners"):
        lines.append("  (no unresolved detections visible)")
    lines.append("\nExecutive summary:")
    lines.append(f"  {b['executive_summary']}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    mlflow.set_tracking_uri(f"databricks://{PROFILE}")
    mlflow.set_experiment(f"/Users/andrij.demianczuk@databricks.com/grid_corridor_agent")
    target = sys.argv[1] if len(sys.argv) > 1 else "COR-007"
    result = briefing(target)
    print(render(result))
