#!/usr/bin/env python3
"""Ensure the two demo-scoped governed tag policies exist (account-level).
Idempotent: creates only if missing. Safe to run on every rebuild.

  grid_client = ['scoped']   -> row-filter key (client scoping column)
  grid_pii    = ['contact']  -> column-mask key (landowner PII column)
"""
import json, subprocess, sys

PROFILE = "fe-vm-ademianczuk-uc-1"
WANT = {
    "grid_client": ("Stantec grid demo — marks the client-scoping column for ABAC row filtering", ["scoped"]),
    "grid_pii": ("Stantec grid demo — marks landowner contact PII for ABAC column masking", ["contact"]),
}


def api(method, path, body=None):
    cmd = ["databricks", "api", method, path, "--profile", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def existing_keys():
    keys, token = set(), None
    for _ in range(20):
        path = "/api/2.1/tag-policies?page_size=100" + (f"&page_token={token}" if token else "")
        rc, out, err = api("get", path)
        if rc != 0:
            print("WARN: could not list tag policies:", err[:200], file=sys.stderr)
            return keys
        d = json.loads(out)
        for tp in d.get("tag_policies", []):
            keys.add(tp.get("tag_key"))
        token = d.get("next_page_token")
        if not token:
            break
    return keys


def main():
    have = existing_keys()
    for key, (desc, values) in WANT.items():
        if key in have:
            print(f"  tag policy '{key}' already exists")
            continue
        rc, out, err = api("post", "/api/2.1/tag-policies",
                           {"tag_key": key, "description": desc,
                            "values": [{"name": v} for v in values]})
        if rc == 0:
            print(f"  created tag policy '{key}'")
        else:
            print(f"  FAILED creating '{key}': {err[:300]}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
