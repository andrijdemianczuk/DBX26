#!/usr/bin/env bash
# ============================================================================
# Rebuild the ENTIRE Grid Corridor demo data + governance scaffold from zero.
# Idempotent: drops & recreates only within
#   ademianczuk_uc_1_catalog.stantec_grid_ops
# Does NOT apply the live policy (that happens on screen during the demo).
# Prereqs: databricks CLI authed as profile fe-vm-ademianczuk-uc-1.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"

echo "==> [1/3] Ensuring demo governed tags exist (account-level, idempotent)..."
python3 ensure_tags.py

echo "==> [2/3] Building + loading the four Delta tables..."
python3 build_dataset.py

echo "==> [3/3] Creating governance functions + applying governed tags..."
python3 dbsql.py -f 01_governance_setup.sql >/dev/null

echo ""
echo "Rebuild complete. Demo is in the BEFORE state (no policy applied)."
echo "To arm the flip live:   dbsql.py -f ../sql/demo_apply_policy.sql"
echo "To reset after a run:   dbsql.py -f ../sql/demo_remove_policy.sql"
