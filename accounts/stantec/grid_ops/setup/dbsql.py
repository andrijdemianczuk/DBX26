#!/usr/bin/env python3
"""Run SQL against the FEVM serverless warehouse via the Databricks CLI.

Usage:
  dbsql.py "SELECT 1"
  dbsql.py -f path/to/file.sql        # runs statements split on ';' at line ends
  dbsql.py --json "SELECT 1"          # dump raw result rows as JSON
"""
import json, subprocess, sys, textwrap

PROFILE = "fe-vm-ademianczuk-uc-1"
WAREHOUSE = "c6250844810982c2"


def run_stmt(stmt: str, as_json: bool = False):
    payload = {
        "warehouse_id": WAREHOUSE,
        "statement": stmt,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
    }
    p = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )
    if p.returncode != 0:
        print("CLI ERROR:", p.stderr[:2000], file=sys.stderr)
        return False
    try:
        r = json.loads(p.stdout)
    except json.JSONDecodeError:
        print("BAD JSON:", p.stdout[:2000], file=sys.stderr)
        return False
    state = r.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = r.get("status", {}).get("error", {})
        print(f"STATE={state}: {err.get('message','')[:2000]}", file=sys.stderr)
        return False
    cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = r.get("result", {}).get("data_array", []) or []
    if as_json:
        print(json.dumps({"cols": cols, "rows": rows}))
        return True
    if not cols:
        print("OK (no result set)")
        return True
    # pretty print
    widths = [len(c) for c in cols]
    for row in rows[:100]:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(str(v)))
    print(" | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    print("-+-".join("-" * widths[i] for i in range(len(cols))))
    for row in rows[:100]:
        print(" | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    print(f"({len(rows)} row(s))")
    return True


def split_sql(text: str):
    # naive splitter: statements terminated by ';' at end of a line
    stmts, buf = [], []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("--") or not s:
            continue
        buf.append(line)
        if s.endswith(";"):
            stmts.append("\n".join(buf).rstrip().rstrip(";"))
            buf = []
    if buf and "\n".join(buf).strip():
        stmts.append("\n".join(buf).strip())
    return [s for s in stmts if s.strip()]


if __name__ == "__main__":
    args = sys.argv[1:]
    as_json = "--json" in args
    args = [a for a in args if a != "--json"]
    if args and args[0] == "-f":
        text = open(args[1]).read()
        ok = True
        for stmt in split_sql(text):
            print(f"\n>>> {textwrap.shorten(stmt, 90)}")
            ok = run_stmt(stmt, as_json) and ok
        sys.exit(0 if ok else 1)
    else:
        sys.exit(0 if run_stmt(args[0], as_json) else 1)
