#!/usr/bin/env python3
"""Author (create or update) the 'Grid Corridor Intelligence' Genie space.

Idempotent-ish: if setup/genie_space_id.txt exists, PATCHes that space;
otherwise creates a new one and records the id. Round-trips to verify.
"""
import json, os, subprocess, sys

SKILL = "/Users/andrij.demianczuk/.vibe/marketplace/plugins/fe-internal-tools/skills/genie-rooms/resources"
sys.path.insert(0, SKILL)
from genie_space_builder import GenieSpaceBuilder  # noqa: E402

PROFILE = "fe-vm-ademianczuk-uc-1"
WAREHOUSE = "c6250844810982c2"
PARENT = "/Workspace/Users/andrij.demianczuk@databricks.com"
SCH = "ademianczuk_uc_1_catalog.stantec_grid_ops"
HERE = os.path.dirname(os.path.abspath(__file__))
ID_FILE = os.path.join(HERE, "genie_space_id.txt")

INSTRUCTIONS = """\
This space covers satellite-based monitoring of electrical transmission corridors for utility clients.

Business definitions:
- An "encroachment" (a detection) is a change spotted from satellite imagery near a corridor: vegetation growth, construction, excavation, farming, or equipment.
- Severity scale (ascending): low < medium < high < critical.
- "Unresolved" means a detection with status 'open' OR 'dispatched' (i.e., not yet 'resolved').
- "Near critical assets" means the detection's corridor has criticality = 'critical'.
- A work order is remediation triggered by a detection. "Time to close" = work_orders.closed_at minus detections.detected_at (join on detection_id). Only closed work orders have closed_at.
- All costs (work_orders.cost_estimate) are in Canadian dollars (CAD).
- "This quarter" means the current calendar quarter. "Last year" means the last 12 months.
- Join keys: detections.corridor_id = corridors.corridor_id; work_orders.detection_id = detections.detection_id; inspections.corridor_id = corridors.corridor_id.

Answer in business language, lead with the direct answer, and state any filters or time windows you applied. landowner_contact is personal contact information and may be governed/masked — never invent contact values."""


def build():
    s = GenieSpaceBuilder(
        title="Grid Corridor Intelligence",
        description="Governed AI over satellite corridor-monitoring data: encroachment detections, work orders, and inspections across utility clients. Synthetic demo data.",
        warehouse_id=WAREHOUSE,
    )
    s.set_instructions(INSTRUCTIONS)
    for t in ("corridors", "detections", "work_orders", "inspections"):
        s.add_table(f"{SCH}.{t}")

    s.add_example_sql(
        title="Unresolved encroachments this quarter near critical corridors",
        sql=(f"SELECT c.corridor_id, c.name, c.client_name, c.criticality, "
             f"count(*) AS unresolved, "
             f"sum(CASE WHEN d.severity IN ('high','critical') THEN 1 ELSE 0 END) AS high_or_critical "
             f"FROM {SCH}.detections d JOIN {SCH}.corridors c ON c.corridor_id = d.corridor_id "
             f"WHERE d.status IN ('open','dispatched') "
             f"AND d.detected_at >= date_trunc('QUARTER', current_date()) "
             f"GROUP BY c.corridor_id, c.name, c.client_name, c.criticality "
             f"ORDER BY unresolved DESC"),
        description="Ranks corridors by unresolved encroachments this quarter; flags those on critical corridors.",
    )
    s.add_example_sql(
        title="Slowest client by detection-to-work-order-close time",
        sql=(f"SELECT c.client_name, "
             f"round(avg(datediff(w.closed_at, d.detected_at)), 1) AS avg_days_to_close, "
             f"count(*) AS closed_work_orders "
             f"FROM {SCH}.work_orders w "
             f"JOIN {SCH}.detections d ON d.detection_id = w.detection_id "
             f"JOIN {SCH}.corridors c ON c.corridor_id = d.corridor_id "
             f"WHERE w.closed_at IS NOT NULL "
             f"GROUP BY c.client_name ORDER BY avg_days_to_close DESC"),
        description="Average days from detection to work-order close, by client.",
    )
    s.add_example_sql(
        title="Vegetation encroachments by month (last year)",
        sql=(f"SELECT date_format(detected_at, 'yyyy-MM') AS month, count(*) AS veg_detections "
             f"FROM {SCH}.detections "
             f"WHERE type = 'vegetation_encroachment' "
             f"AND detected_at >= add_months(current_date(), -12) "
             f"GROUP BY 1 ORDER BY 1"),
        description="Monthly vegetation encroachment trend to reveal seasonality.",
    )

    # Seeded starter questions (clickable in the Genie UI)
    questions = [
        "Which corridors have unresolved encroachments this quarter, and which are near critical assets?",
        "Which client has the slowest average time from detection to work-order close?",
        "Show the trend of vegetation encroachments by month over the last year.",
    ]
    s._set_list(("config", "sample_questions"),
                [{"id": s._resolve_id(None), "question": [q]} for q in questions])

    s.validate()
    return s


def api(method, path, body_file=None):
    cmd = ["databricks", "api", method, path, "--profile", PROFILE]
    if body_file:
        cmd += ["--json", f"@{body_file}"]
    return subprocess.run(cmd, capture_output=True, text=True)


def _sort_id_lists(d):
    """Genie requires every id-keyed list sorted by id."""
    def walk(node):
        if isinstance(node, dict):
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            if node and all(isinstance(x, dict) and "id" in x for x in node):
                node.sort(key=lambda x: x["id"])
            for x in node:
                walk(x)
    walk(d)
    return d


def main():
    s = build()
    serialized = json.dumps(_sort_id_lists(s.to_dict()))
    payload = {"title": s.title, "description": s.description,
               "parent_path": PARENT, "warehouse_id": s.warehouse_id,
               "serialized_space": serialized}

    tmp = "/tmp/grid_genie_payload.json"
    with open(tmp, "w") as f:
        json.dump(payload, f)

    if os.path.exists(ID_FILE):
        sid = open(ID_FILE).read().strip()
        print(f"Patching existing space {sid}...")
        r = api("patch", f"/api/2.0/genie/spaces/{sid}", tmp)
    else:
        print("Creating new space...")
        r = api("post", "/api/2.0/genie/spaces", tmp)

    if r.returncode != 0:
        print("API ERROR:", r.stderr[:1500], file=sys.stderr)
        sys.exit(1)
    resp = json.loads(r.stdout)
    sid = resp.get("space_id") or resp.get("id")
    if sid:
        with open(ID_FILE, "w") as f:
            f.write(sid)
    print("space_id:", sid)
    print("title:", resp.get("title"))


if __name__ == "__main__":
    main()
