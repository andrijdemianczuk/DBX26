#!/usr/bin/env python3
"""Deterministic synthetic data generator for the Stantec Grid Corridor demo.

Fixed seed => reproducible rebuilds. Produces four related datasets shaped like
Stantec's PipeWATCH/WireWatch satellite corridor-monitoring products.

Deliberate skews (so demo questions have interesting answers):
  1. COR-007 (Fictional Utility A, critical 500kV): recent cluster of UNRESOLVED
     high/critical CONSTRUCTION encroachments near critical assets.
  2. Fictional Utility C: clearly worse detection -> work-order-close times.
  3. Vegetation encroachments peak in summer months (seasonal pattern).

All PII is obviously fake: made-up names + 555-01xx phone numbers.
"""
import random
from datetime import datetime, timedelta

SEED = 42
NOW = datetime(2026, 8, 18)              # demo "today" (matches CONFIG date)
MONTHS_BACK = 18
START = NOW - timedelta(days=MONTHS_BACK * 30)

CLIENTS = ["Fictional Utility A", "Fictional Utility B", "Fictional Utility C"]
SCOPED_CLIENT = "Fictional Utility A"    # the client the row-filter keeps

REGIONS = ["Pacific Northwest", "Prairies", "Great Lakes", "Gulf Coast", "Desert Southwest"]
VOLTAGES = ["69kV", "138kV", "230kV", "500kV"]
DET_TYPES = ["vegetation_encroachment", "construction", "excavation", "farming", "equipment"]
SEVERITIES = ["low", "medium", "high", "critical"]
CREWS = ["Crew Alpha", "Crew Bravo", "Crew Charlie", "Crew Delta", "Crew Echo", "Crew Foxtrot"]
INSPECT_METHODS = ["foot_patrol", "vehicle_patrol", "helicopter"]

FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery",
               "Quinn", "Skyler", "Dakota", "Reese", "Rowan", "Sage", "Emerson", "Finley",
               "Harper", "Kendall", "Logan", "Parker"]
LAST_NAMES = ["Rivers", "Stone", "Fields", "Brooks", "Vale", "Marsh", "Grove", "Hollow",
              "Ridge", "Banks", "Cross", "Frost", "Lane", "Pike", "Reed", "Snow",
              "Vance", "Webb", "York", "Ash"]

CORRIDOR_NAMES = [
    "North Ridge Intertie", "Fraser Delta Line", "Cascade Summit Corridor", "Prairie Crossing",
    "Great Lakes Spur", "Gulf Shore Tieline", "Desert Mesa Line", "Cedar Valley Corridor",
    "Copper Basin Line", "Silver Creek Intertie", "Highland Loop", "Coastal Reach",
    "Boreal Transit", "Canyon Rim Line", "Maple Bend Corridor", "Iron Range Spur",
    "Sunset Mesa Line", "River Bluff Corridor", "Windward Pass", "Granite Ridge Line",
    "Timberline Intertie", "Harbor Point Spur", "Sagebrush Corridor", "Elk Ridge Line",
    "Delta Junction", "Aspen Grove Line", "Blue Mountain Corridor", "Redstone Intertie",
    "Willow Flats Line", "Cypress Bayou Spur", "Foothills Loop", "Marsh Landing Line",
    "Twin Peaks Corridor", "Cactus Flat Line", "Lakeshore Tieline", "Pine Barrens Spur",
    "Mesa Verde Line", "Cold Harbor Corridor", "Rolling Plains Line", "Sierra Vista Intertie",
]


def fake_contact(rnd):
    name = f"{rnd.choice(FIRST_NAMES)} {rnd.choice(LAST_NAMES)}"
    phone = f"(555) 01{rnd.randint(0, 9)}-{rnd.randint(1000, 9999)}"
    return f"{name} {phone}"


def build():
    rnd = random.Random(SEED)

    # ---- corridors --------------------------------------------------------
    corridors = []
    # weight client assignment so A keeps meaningful content after filtering
    client_cycle = ([CLIENTS[0]] * 16) + ([CLIENTS[1]] * 13) + ([CLIENTS[2]] * 11)
    rnd.shuffle(client_cycle)
    for i in range(40):
        cid = f"COR-{i+1:03d}"
        # COR-007 is our hero: Utility A, critical, 500kV
        if cid == "COR-007":
            client, crit, volt = SCOPED_CLIENT, "critical", "500kV"
        else:
            client = client_cycle[i]
            crit = rnd.choices(["critical", "high", "standard"], weights=[1, 2, 4])[0]
            volt = rnd.choice(VOLTAGES)
        lat = round(rnd.uniform(29.0, 49.0), 5)
        lon = round(rnd.uniform(-123.0, -80.0), 5)
        corridors.append({
            "corridor_id": cid,
            "name": CORRIDOR_NAMES[i],
            "region": rnd.choice(REGIONS),
            "client_name": client,
            "voltage_class": volt,
            "criticality": crit,
            "centroid_lat": lat,
            "centroid_lon": lon,
        })
    corridor_by_id = {c["corridor_id"]: c for c in corridors}

    # ---- detections -------------------------------------------------------
    detections = []
    det_seq = 0

    def month_weighted_date():
        """Pick a date in window; used generally (uniform)."""
        days = rnd.randint(0, MONTHS_BACK * 30)
        return START + timedelta(days=days, hours=rnd.randint(0, 23), minutes=rnd.randint(0, 59))

    def summer_weighted_date():
        """Vegetation: bias toward summer months (Jun-Aug)."""
        for _ in range(6):
            d = month_weighted_date()
            if d.month in (6, 7, 8):
                return d
            if rnd.random() < 0.35:      # sometimes accept off-season
                return d
        return d

    # baseline detections across all corridors
    for _ in range(1950):
        c = rnd.choice(corridors)
        dtype = rnd.choices(DET_TYPES, weights=[5, 3, 2, 2, 2])[0]
        detected = summer_weighted_date() if dtype == "vegetation_encroachment" else month_weighted_date()
        sev = rnd.choices(SEVERITIES, weights=[4, 4, 2, 1])[0]
        status = rnd.choices(["open", "dispatched", "resolved"], weights=[2, 2, 6])[0]
        det_seq += 1
        detections.append({
            "detection_id": f"DET-{det_seq:05d}",
            "corridor_id": c["corridor_id"],
            "client_name": c["client_name"],
            "detected_at": detected,
            "type": dtype,
            "severity": sev,
            "confidence": round(rnd.uniform(0.55, 0.99), 2),
            "source": f"PASS-{detected.year}-{rnd.randint(100, 999)}",
            "status": status,
            "landowner_contact": fake_contact(rnd),
        })

    # SKEW 1: hero cluster on COR-007 — recent, unresolved, high/critical construction
    for _ in range(22):
        detected = NOW - timedelta(days=rnd.randint(3, 70), hours=rnd.randint(0, 23))
        sev = rnd.choices(["high", "critical"], weights=[3, 2])[0]
        status = rnd.choices(["open", "dispatched"], weights=[3, 2])[0]
        det_seq += 1
        detections.append({
            "detection_id": f"DET-{det_seq:05d}",
            "corridor_id": "COR-007",
            "client_name": SCOPED_CLIENT,
            "detected_at": detected,
            "type": "construction",
            "severity": sev,
            "confidence": round(rnd.uniform(0.80, 0.99), 2),
            "source": f"PASS-{detected.year}-{rnd.randint(100, 999)}",
            "status": status,
            "landowner_contact": fake_contact(rnd),
        })

    det_by_id = {d["detection_id"]: d for d in detections}

    # ---- work_orders ------------------------------------------------------
    # WOs come from detections that were dispatched or resolved.
    work_orders = []
    wo_seq = 0
    candidates = [d for d in detections if d["status"] in ("dispatched", "resolved")]
    rnd.shuffle(candidates)
    candidates = candidates[:600]
    for d in candidates:
        client = corridor_by_id[d["corridor_id"]]["client_name"]
        opened = d["detected_at"] + timedelta(days=rnd.randint(0, 4), hours=rnd.randint(0, 12))
        # SKEW 2: Utility C resolves much more slowly
        if client == "Fictional Utility C":
            dur = rnd.randint(35, 80)
        else:
            dur = rnd.randint(3, 25)
        resolved = d["status"] == "resolved"
        closed = opened + timedelta(days=dur) if resolved else None
        # cost by type/severity (CAD)
        base = {"vegetation_encroachment": 4000, "construction": 12000, "excavation": 9000,
                "farming": 3000, "equipment": 6000}[d["type"]]
        sev_mult = {"low": 0.7, "medium": 1.0, "high": 1.5, "critical": 2.2}[d["severity"]]
        cost = round(base * sev_mult * rnd.uniform(0.85, 1.2), 2)
        wo_seq += 1
        work_orders.append({
            "wo_id": f"WO-{wo_seq:05d}",
            "detection_id": d["detection_id"],
            "client_name": client,
            "opened_at": opened,
            "closed_at": closed,
            "crew": rnd.choice(CREWS),
            "cost_estimate": cost,
            "status": "closed" if resolved else rnd.choice(["open", "in_progress"]),
        })

    # ---- inspections ------------------------------------------------------
    # Manual patrols: cover only a subset of corridors, less frequently than sat.
    inspections = []
    insp_seq = 0
    inspected_corridors = [c["corridor_id"] for c in corridors if rnd.random() < 0.55]
    for _ in range(300):
        cid = rnd.choice(inspected_corridors)
        when = START + timedelta(days=rnd.randint(0, MONTHS_BACK * 30))
        insp_seq += 1
        client = corridor_by_id[cid]["client_name"]
        findings = rnd.choice([
            "No issues observed", "Minor vegetation noted", "Access road washout",
            "Signage damage", "Third-party activity nearby", "Structure corrosion flagged",
            "Clear span verified", "Encroachment confirmed on foot",
        ])
        inspections.append({
            "inspection_id": f"INS-{insp_seq:04d}",
            "corridor_id": cid,
            "client_name": client,
            "inspected_at": when,
            "inspector": f"{rnd.choice(FIRST_NAMES)} {rnd.choice(LAST_NAMES)}",
            "method": rnd.choice(INSPECT_METHODS),
            "findings": findings,
        })

    return corridors, detections, work_orders, inspections


if __name__ == "__main__":
    c, d, w, i = build()
    print(f"corridors={len(c)} detections={len(d)} work_orders={len(w)} inspections={len(i)}")
    # quick skew sanity
    hero = [x for x in d if x["corridor_id"] == "COR-007"]
    print(f"COR-007 detections={len(hero)}")
