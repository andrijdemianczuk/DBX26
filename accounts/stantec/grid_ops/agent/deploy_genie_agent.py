#!/usr/bin/env python3
"""Deploy the Corridor Genie Agent as a traced serving endpoint.

Path: author (corridor_genie_agent.py) -> log (models-from-code) -> validate ->
register to UC -> agents.deploy(). Deployment turns on trace logging to an
inference table + the MLflow experiment, so every conversation the app sends is
captured as a trace.

Usage:
    python3 agent/deploy_genie_agent.py                 # full: log -> deploy
    python3 agent/deploy_genie_agent.py --validate-only # log + local predict, no deploy
    python3 agent/deploy_genie_agent.py --grants-only    # (re)apply SP grants only

Prereq: databricks CLI authenticated as the profile below, with CREATE MODEL on
the schema and serving-deploy permission (you are the workspace admin/owner).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksGenieSpace,
    DatabricksSQLWarehouse,
    DatabricksTable,
)

PROFILE = os.environ.get("GRID_PROFILE", "fe-vm-ademianczuk-uc-1")
CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "stantec_grid_ops"
UC_MODEL = f"{CATALOG}.{SCHEMA}.corridor_genie_agent"
EXPERIMENT = "/Users/andrij.demianczuk@databricks.com/grid_corridor_agent"

GENIE_SPACE_ID = "01f19b488b6e1d4ea44cb81e09ec4a0e"
WAREHOUSE_ID = "c6250844810982c2"

AGENT_FILE = str(Path(__file__).with_name("corridor_genie_agent.py"))
SEEDED_Q1 = ("Which corridors have unresolved encroachments this quarter, "
             "and which are near critical assets?")

# Tables the endpoint's service principal must be able to SELECT for Genie's SQL.
_TABLES = ["corridors", "detections", "work_orders", "inspections"]


def _client() -> WorkspaceClient:
    return WorkspaceClient(profile=PROFILE)


def log_and_register():
    mlflow.set_tracking_uri(f"databricks://{PROFILE}")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run(run_name="corridor_genie_agent"):
        logged = mlflow.pyfunc.log_model(
            name="agent",
            python_model=AGENT_FILE,                       # models-from-code
            model_config={
                "genie_space_id": GENIE_SPACE_ID,
                "warehouse_id": WAREHOUSE_ID,
                "max_result_rows": 25,
            },
            input_example={"input": [{"role": "user", "content": SEEDED_Q1}]},
            pip_requirements=["mlflow>=3.0", "databricks-sdk"],
            resources=[                                    # auth passthrough targets
                DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
                DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID),
                # Genie's generated SQL reads these tables — declare them or the
                # endpoint's passthrough identity can't read them (MessageStatus.FAILED).
                *[DatabricksTable(table_name=f"{CATALOG}.{SCHEMA}.{t}") for t in _TABLES],
                # The ABAC row-filter / column-mask functions invoked by the policy.
                DatabricksFunction(function_name=f"{CATALOG}.{SCHEMA}.rf_client_scope"),
                DatabricksFunction(function_name=f"{CATALOG}.{SCHEMA}.mask_contact"),
            ],
        )
    print(f"logged: {logged.model_uri}")
    return logged


def validate(logged):
    print("validating locally (this calls Genie once)…")
    out = mlflow.models.predict(
        model_uri=logged.model_uri,
        input_data={"input": [{"role": "user", "content": SEEDED_Q1}]},
        env_manager="local",
    )
    print("local predict OK:\n", str(out)[:600])


def register(logged):
    reg = mlflow.register_model(model_uri=logged.model_uri, name=UC_MODEL)
    print(f"registered: {UC_MODEL} v{reg.version}")
    return reg


def deploy(version):
    from databricks import agents
    dep = agents.deploy(model_name=UC_MODEL, model_version=version)
    print("\n=== DEPLOYED ===")
    print("endpoint :", getattr(dep, "endpoint_name", "?"))
    print("query    :", getattr(dep, "query_endpoint", "?"))
    print("review   :", getattr(dep, "review_app_url", "?"))
    return dep


def _endpoint_service_principal(w, endpoint_name):
    """Best-effort: find the app service principal the agent endpoint runs as."""
    try:
        ep = w.serving_endpoints.get(endpoint_name)
        cfg = ep.config or ep.pending_config
        served = (cfg.served_entities or cfg.served_models) if cfg else None
        for s in (served or []):
            sp = getattr(s, "service_principal_id", None) or getattr(s, "creator", None)
            if sp:
                return sp
    except Exception as exc:
        print(f"(could not auto-detect endpoint SP: {exc})")
    return None


def grant_service_principal(sp: str):
    """Grant the endpoint's SP everything Genie needs to run under governance.

    `sp` is the service principal's application-id (or the principal name UC
    accepts in GRANT ... TO `<principal>`).
    """
    if not sp:
        print("\n[grants] No service principal supplied. After deploy, run:")
        print("    python3 agent/deploy_genie_agent.py --grants-only  (with SP)")
        _print_manual_grants("<ENDPOINT_SERVICE_PRINCIPAL>")
        return

    w = _client()
    stmts = [
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp}`",
        *[f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.{t} TO `{sp}`" for t in _TABLES],
        # EXECUTE on the row-filter / column-mask functions used by the policy
        f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.rf_client_scope TO `{sp}`",
        f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.mask_contact TO `{sp}`",
    ]
    for s in stmts:
        w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=s, wait_timeout="30s")
        print("  ✓", s)
    print("\n[grants] UC grants applied. Genie space CAN RUN + warehouse CAN USE "
          "for this SP must be granted in the UI or via the permissions API:")
    _print_manual_grants(sp)


def _print_manual_grants(sp):
    print(f"""
    Genie space  -> Share -> add `{sp}` as CAN RUN
      (space id {GENIE_SPACE_ID})
    SQL warehouse-> Permissions -> add `{sp}` as CAN USE
      (warehouse {WAREHOUSE_ID})
    """)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate-only", action="store_true")
    ap.add_argument("--grants-only", metavar="SP",
                    help="apply grants for the given endpoint service principal")
    args = ap.parse_args()

    if args.grants_only:
        grant_service_principal(args.grants_only)
        return

    logged = log_and_register()
    validate(logged)
    if args.validate_only:
        print("\n--validate-only: stopping before register/deploy.")
        return

    reg = register(logged)
    dep = deploy(reg.version)

    w = _client()
    sp = _endpoint_service_principal(w, getattr(dep, "endpoint_name", ""))
    grant_service_principal(sp)


if __name__ == "__main__":
    sys.exit(main())
