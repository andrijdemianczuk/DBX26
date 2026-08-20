#!/usr/bin/env python3
"""Verify the before/after governance flip on the real tables and measure the
propagation latency between applying the policy and the query reflecting it."""
import subprocess, sys, time, json

sys.path.insert(0, ".")
from dbsql import PROFILE, WAREHOUSE
import json as _json

CAT = "ademianczuk_uc_1_catalog.stantec_grid_ops"


def sql(stmt):
    payload = {"warehouse_id": WAREHOUSE, "statement": stmt,
               "wait_timeout": "50s", "format": "JSON_ARRAY", "disposition": "INLINE"}
    p = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements",
                        "--profile", PROFILE, "--json", _json.dumps(payload)],
                       capture_output=True, text=True)
    r = _json.loads(p.stdout)
    if r.get("status", {}).get("state") != "SUCCEEDED":
        return None, r.get("status", {}).get("error", {}).get("message", "")
    rows = r.get("result", {}).get("data_array", []) or []
    return rows, None


HERO = f"""
SELECT client_name, count(*) AS detections,
       count(DISTINCT landowner_contact) AS distinct_contacts,
       max(landowner_contact) AS sample_contact
FROM {CAT}.detections GROUP BY client_name ORDER BY client_name
"""

print("=== BEFORE (expect 3 clients, real PII in sample_contact) ===")
rows, err = sql(HERO)
for r in rows:
    print("  ", r)

print("\n=== Applying policy + polling for propagation latency ===")
apply = open("sql/demo_apply_policy.sql").read()
# split into 2 statements
stmts = [s.strip() for s in apply.split(";") if s.strip() and not s.strip().startswith("--")
         or ("POLICY" in s)]
# simpler: run each CREATE POLICY block
blocks = []
cur = []
for line in apply.splitlines():
    if line.strip().startswith("--") or not line.strip():
        continue
    cur.append(line)
    if line.strip().endswith(";"):
        blocks.append("\n".join(cur).rstrip().rstrip(";"))
        cur = []
for b in blocks:
    _, e = sql(b)
    if e:
        print("  apply error:", e)
        sys.exit(1)
t0 = time.time()

# poll until the flip is visible
deadline = t0 + 60
latency = None
while time.time() < deadline:
    rows, err = sql(HERO)
    if rows and len(rows) == 1 and rows[0][0] == "Fictional Utility A" \
       and "MASKED" in str(rows[0][3]):
        latency = time.time() - t0
        break
    time.sleep(1)

print(f"\n=== AFTER (expect 1 client, MASKED sample_contact) ===")
rows, err = sql(HERO)
for r in rows:
    print("  ", r)

if latency is not None:
    print(f"\n>>> PROPAGATION LATENCY: {latency:.1f}s")
else:
    print("\n>>> policy did NOT propagate within 60s — investigate")
