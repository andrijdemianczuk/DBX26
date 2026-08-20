#!/usr/bin/env python3
"""Create + load the four Delta tables for the Grid Corridor demo.

Idempotent: drop-and-recreate within ademianczuk_uc_1_catalog.stantec_grid_ops only.
Reproducible: data comes from generate_data.py (fixed seed).
Loads via the Databricks SQL Statement API using batched multi-row INSERTs.
"""
import sys
from generate_data import build
from dbsql import run_stmt

CAT = "ademianczuk_uc_1_catalog"
SCH = "stantec_grid_ops"
FQ = f"{CAT}.{SCH}"
BATCH = 250


def q(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    # datetime
    if hasattr(v, "strftime"):
        return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'"
    return "'" + str(v).replace("'", "''") + "'"


def load(table, cols, rows):
    fq = f"{FQ}.{table}"
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        values = ",\n".join(
            "(" + ",".join(q(r[c]) for c in cols) + ")" for r in chunk
        )
        stmt = f"INSERT INTO {fq} ({','.join(cols)}) VALUES\n{values}"
        if not run_stmt(stmt):
            print(f"FAILED loading {table} batch {i}", file=sys.stderr)
            sys.exit(1)
    print(f"  loaded {len(rows)} rows into {table}")


DDL = [
    f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT "
    "'Grid Corridor Intelligence — governed satellite monitoring of transmission "
    "corridors (encroachment detections, work orders, inspections). Synthetic demo data.'",

    f"""CREATE OR REPLACE TABLE {FQ}.corridors (
        corridor_id   STRING NOT NULL COMMENT 'Unique corridor identifier (COR-###)',
        name          STRING  COMMENT 'Human-readable corridor name',
        region        STRING  COMMENT 'Operating region',
        client_name   STRING  COMMENT 'Owning utility client (Fictional Utility A/B/C)',
        voltage_class STRING  COMMENT 'Transmission voltage class (69kV-500kV)',
        criticality   STRING  COMMENT 'Asset criticality tier: critical | high | standard',
        centroid_lat  DOUBLE  COMMENT 'Corridor centroid latitude',
        centroid_lon  DOUBLE  COMMENT 'Corridor centroid longitude',
        CONSTRAINT corridors_pk PRIMARY KEY (corridor_id)
     ) COMMENT 'Master list of monitored transmission corridors and the utility client that owns each. One row per corridor.'""",

    f"""CREATE OR REPLACE TABLE {FQ}.detections (
        detection_id      STRING NOT NULL COMMENT 'Unique satellite detection id (DET-#####)',
        corridor_id       STRING    COMMENT 'FK -> corridors.corridor_id',
        client_name       STRING    COMMENT 'Owning utility client (denormalized). Governed by client row-filter policy.',
        detected_at       TIMESTAMP COMMENT 'When the change was detected from satellite imagery',
        type              STRING    COMMENT 'Encroachment type: vegetation_encroachment | construction | excavation | farming | equipment',
        severity          STRING    COMMENT 'Severity: low | medium | high | critical',
        confidence        DOUBLE    COMMENT 'Model confidence 0-1 for the detection',
        source            STRING    COMMENT 'Satellite pass identifier',
        status            STRING    COMMENT 'Lifecycle: open | dispatched | resolved. "Unresolved" = open or dispatched.',
        landowner_contact STRING    COMMENT 'Landowner point-of-contact (PII: name + phone). Governed by column mask.',
        CONSTRAINT detections_pk PRIMARY KEY (detection_id),
        CONSTRAINT detections_corridor_fk FOREIGN KEY (corridor_id) REFERENCES {FQ}.corridors(corridor_id)
     ) COMMENT 'Satellite-derived change detections along corridors (encroachments). ~2k rows over 18 months. landowner_contact is PII and is masked by governance policy.'""",

    f"""CREATE OR REPLACE TABLE {FQ}.work_orders (
        wo_id         STRING NOT NULL COMMENT 'Unique work order id (WO-#####)',
        detection_id  STRING    COMMENT 'FK -> detections.detection_id that triggered this work order',
        client_name   STRING    COMMENT 'Owning utility client (denormalized). Governed by client row-filter policy.',
        opened_at     TIMESTAMP COMMENT 'When the work order was opened',
        closed_at     TIMESTAMP COMMENT 'When the work order was closed (NULL if still open)',
        crew          STRING    COMMENT 'Assigned field crew',
        cost_estimate DOUBLE    COMMENT 'Estimated remediation cost in CAD',
        status        STRING    COMMENT 'Work order status: open | in_progress | closed',
        CONSTRAINT work_orders_pk PRIMARY KEY (wo_id),
        CONSTRAINT work_orders_detection_fk FOREIGN KEY (detection_id) REFERENCES {FQ}.detections(detection_id)
     ) COMMENT 'Field remediation work orders generated from detections. Costs are in CAD. Time from detection to closed_at measures resolution speed.'""",

    f"""CREATE OR REPLACE TABLE {FQ}.inspections (
        inspection_id STRING NOT NULL COMMENT 'Unique manual inspection id (INS-####)',
        corridor_id   STRING    COMMENT 'FK -> corridors.corridor_id',
        client_name   STRING    COMMENT 'Owning utility client (denormalized). Governed by client row-filter policy.',
        inspected_at  TIMESTAMP COMMENT 'When the manual patrol occurred',
        inspector     STRING    COMMENT 'Inspector name',
        method        STRING    COMMENT 'Patrol method: foot_patrol | vehicle_patrol | helicopter',
        findings      STRING    COMMENT 'Free-text inspection finding',
        CONSTRAINT inspections_pk PRIMARY KEY (inspection_id),
        CONSTRAINT inspections_corridor_fk FOREIGN KEY (corridor_id) REFERENCES {FQ}.corridors(corridor_id)
     ) COMMENT 'Manual patrol inspection records. Sparser than satellite coverage — used to contrast manual vs satellite monitoring.'""",
]


def main():
    print("Creating schema + tables...")
    for d in DDL:
        if not run_stmt(d):
            print("DDL failed", file=sys.stderr)
            sys.exit(1)

    corridors, detections, work_orders, inspections = build()
    print("Loading data...")
    load("corridors", ["corridor_id", "name", "region", "client_name", "voltage_class",
                        "criticality", "centroid_lat", "centroid_lon"], corridors)
    load("detections", ["detection_id", "corridor_id", "client_name", "detected_at", "type",
                         "severity", "confidence", "source", "status", "landowner_contact"], detections)
    load("work_orders", ["wo_id", "detection_id", "client_name", "opened_at", "closed_at", "crew",
                          "cost_estimate", "status"], work_orders)
    load("inspections", ["inspection_id", "corridor_id", "client_name", "inspected_at", "inspector",
                          "method", "findings"], inspections)
    print("Done.")


if __name__ == "__main__":
    main()
