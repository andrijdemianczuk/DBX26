# Grid Corridor Intelligence — Stantec Demo

A 15-minute live demo of **governed, ship-able AI**: an agent grounded in governed
data whose answers are permission-aware, traceable, and cost-attributed. Shaped
like Stantec's PipeWATCH / WireWatch products — satellite monitoring of transmission
corridors, encroachment detections, and inspection work orders across fictional
utility clients.

**The single thesis:** run a query with full visibility → apply one policy on
screen → rerun the same query and watch rows filter + PII mask → then show Genie and
the agent inheriting that same policy with **zero changes**. Everything runs as one
identity; the drama is the before/after policy flip.

> All data is synthetic. Clients are "Fictional Utility A/B/C". PII is obviously
> fake (555-01xx phone numbers).

---

## The two non-negotiable moments

1. **The live policy flip** — SQL editor and Genie, before → after, same identity.
2. **The lineage walk-back** — from a Genie answer back to `detections` / `corridors`.

Everything else is cuttable under time pressure.

---

## Environment / coordinates

| Thing | Value |
|---|---|
| Workspace | `https://fevm-ademianczuk-uc-1.cloud.databricks.com` |
| CLI profile | `fe-vm-ademianczuk-uc-1` |
| Catalog / schema | `ademianczuk_uc_1_catalog.stantec_grid_ops` |
| SQL warehouse | `Serverless Starter Warehouse` (`c6250844810982c2`), tagged `project=corridor-demo` |
| Identity (single) | `andrij.demianczuk@databricks.com` (table owner **and** workspace admin) |
| Governed tags | `grid_client=[scoped]` (row filter), `grid_pii=[contact]` (column mask) |
| Genie space | **Grid Corridor Intelligence** — [open](https://fevm-ademianczuk-uc-1.cloud.databricks.com/genie/rooms/01f19b488b6e1d4ea44cb81e09ec4a0e) (`01f19b488b6e1d4ea44cb81e09ec4a0e`) |
| MLflow experiment | [grid_corridor_agent](https://fevm-ademianczuk-uc-1.cloud.databricks.com/ml/experiments/2874905133974692) (`2874905133974692`) |
| Agent notebook | `/Users/andrij.demianczuk@databricks.com/grid_corridor_demo/corridor_briefing_agent` |
| Foundation model | `databricks-claude-sonnet-5` |

**Measured policy propagation latency: ~1.1 s (effectively immediate).** No filler
needed between apply and rerun.

---

## Setup from zero

Prereq: `databricks` CLI authenticated as `fe-vm-ademianczuk-uc-1`
(`databricks auth login https://fevm-ademianczuk-uc-1.cloud.databricks.com --profile=fe-vm-ademianczuk-uc-1`).

```bash
# 1. Data + governance scaffold (idempotent; drops & recreates only within the schema)
bash setup/build_all.sh

# 2. (Re)create the Genie space — only needed if it doesn't exist yet
python3 setup/build_genie.py

# 3. Agent notebook is already in the workspace; re-import after edits:
databricks workspace import \
  /Users/andrij.demianczuk@databricks.com/grid_corridor_demo/corridor_briefing_agent \
  --file agent/corridor_briefing_notebook.py --language PYTHON --format SOURCE \
  --overwrite --profile=fe-vm-ademianczuk-uc-1
```

`build_all.sh` leaves the demo in the **BEFORE** state (no policy applied).

---

## The dataset (what's in it, and the planted stories)

| Table | Rows | Notes |
|---|---|---|
| `corridors` | 40 | transmission corridors; `client_name`, `criticality`, `voltage_class`, centroid lat/lon |
| `detections` | 1,972 | 18 months of satellite encroachment detections; `landowner_contact` = **fake PII** |
| `work_orders` | 600 | remediation, `cost_estimate` in **CAD**, detection→close time |
| `inspections` | 300 | manual patrols (sparser than satellite, for contrast) |

Three planted skews so questions have crisp answers:
- **COR-007 "Desert Mesa Line"** (Fictional Utility A, *critical* 500 kV): a recent
  cluster of unresolved high/critical **construction** encroachments — the hero corridor.
- **Fictional Utility C**: clearly worst resolution time (~62 days vs ~17 for A/B).
- **Vegetation** encroachments **peak every summer** (Jun–Aug).

---

## Seeded Genie questions (verified good answers)

1. *Which corridors have unresolved encroachments this quarter, and which are near critical assets?*
2. *Which client has the slowest average time from detection to work-order close?*
3. *Show the trend of vegetation encroachments by month over the last year.*

Question 1 is the flip question: **before** it returns all clients; **after** the
policy it returns only Fictional Utility A with contacts masked — same space, no edits.

---

## 15-minute runbook (minute-by-minute)

Have open in tabs beforehand: **Catalog Explorer** on the schema, a **SQL editor** tab
with `sql/demo_query.sql` loaded, the **Genie space**, the **agent notebook**, and the
**MLflow experiment**. Confirm the demo is in the BEFORE state (`sql/demo_remove_policy.sql`).

### ~2 min — Discover (data access & discoverability)
- Catalog Explorer → `ademianczuk_uc_1_catalog.stantec_grid_ops`.
- Show the four tables, the **AI-readable descriptions/comments**, the `grid_client` /
  `grid_pii` **governed tags** on columns, and the PK/FK **relationships**.
- Line: *"Curated, certified, discoverable — and already classified for governance."*

### ~4 min — The governance flip (governance as code, live)
1. **SQL editor** — run `sql/demo_query.sql`. Full results: all three clients,
   landowner **phone numbers in the clear**, Desert Mesa construction cluster visible.
2. Run **`sql/demo_apply_policy.sql`** on screen — two short statements, keyed off tags.
3. **Rerun the identical query.** Rows collapse to Fictional Utility A; `landowner_contact`
   shows `*** MASKED (grid_pii) ***`. (~1 s — no waiting.)
4. **Genie** — ask question 1 again. The answer is now scoped to Utility A, contacts
   masked. *Same Genie space, agent untouched — it inherited the policy.*
5. *(optional 15 s)* Run **`sql/audit_closer.sql`** — who queried what, when, including
   the policy apply. Governance evidence in one screen.

### ~3 min — Lineage + trace
- From the Genie answer, open the generated SQL, then in Catalog Explorer open
  `detections` → **Lineage** tab: walk back to `corridors` / `work_orders`.
- Open the **agent notebook**'s last run (or the MLflow experiment) → **Traces** →
  open one trace: retriever spans (governed SQL) → LLM span (summary).

### ~3 min — Agent briefing (scoped) + cost
- In the agent notebook, run `briefing("COR-007")` → structured summary: open detections
  by severity, recommended work orders costed from history (~$659k CAD), affected
  landowners **masked** (policy still on), executive paragraph.
- *(optional)* run `briefing("Fictional Utility C")` → **empty** — scoped out entirely.
- **Cost**: run `sql/cost_attribution.sql` → this demo's serverless spend under
  `project=corridor-demo`. (See cost note below.)

### ~2 min — Close
- Databricks Genie surfaces in **Teams / Microsoft 365 Copilot** via the Genie/M365
  connector — narrate over the integration slide (connector not configured in this
  workspace; see "Needs admin").
- Recap the thesis: *governed data → permission-aware answers → traceable → cost-attributed.*

---

## Where to click

- **Discover / tags / lineage**: Catalog Explorer → the schema → a table → *Overview*,
  *Sample Data*, *Lineage* tabs.
- **Trace**: MLflow experiment `2874905133974692` → **Traces** tab, or the notebook
  cell's inline trace UI.
- **Cost**: `sql/cost_attribution.sql`; the warehouse tag lives at SQL Warehouses →
  *Serverless Starter Warehouse* → Edit → Tags.

---

## Reset between rehearsals

```bash
# Remove the policy (back to the BEFORE state). PII visible, all 3 clients.
python3 setup/dbsql.py -f sql/demo_remove_policy.sql
```

That's the only state that changes during the demo. Tables, tags, functions, Genie
space, and the agent all stay in place, so you re-arm instantly with
`sql/demo_apply_policy.sql`. For a full rebuild, re-run `bash setup/build_all.sh`.

---

## Cost note (read before the cost segment)

Billing/usage system tables lag a few hours. The `project=corridor-demo` tag on the
warehouse attributes all demo SQL, but **run `sql/cost_attribution.sql` the morning of
the demo** (usage tagged the day before will have landed), or use the by-warehouse-id
fallback (commented in the file). Foundation-model (agent) spend is pay-per-token on a
shared system endpoint and cannot carry a per-demo budget — see "Needs admin" for the
productionization path.

---

## Needs admin / not available in this workspace (flagged, not blocking)

- **Teams / M365 Copilot connector** — not configured here; the close is narrated over
  a slide. Enabling it is an admin + M365 tenant task.
- **Per-endpoint AI budget / rate limit** — the agent uses a shared pay-per-token FM
  endpoint, which can't carry a per-demo budget. The production pattern is a *custom*
  serving endpoint with **AI Gateway** rate limits plus a **serverless budget policy**
  (the budget-policies API was not exposed to this workspace/CLI). Cost is instead
  shown via the tag-based system-tables query.

---

## Deviations from the original spec

1. **No user personas (USER_A/USER_B).** The spec's own narrative commits to a single
   identity and a before/after policy flip, so the persona placeholders were dropped.
2. **Self-managed governed tags** `grid_client` / `grid_pii` (created at account level,
   idempotently, by `setup/ensure_tags.py`) instead of the literal `client` / `pii:contact`.
   The existing account `pii` governed tag only allows values `ssn`/`address`, and its
   catalog-wide policy would have collided with this demo. These two demo-scoped tags are
   reversible (delete via the tag-policies API) and namespaced to avoid collisions.
3. **`client_name` denormalized onto all fact tables** so one schema-level row-filter
   policy scopes every table by tag and any single-table query visibly filters.
4. **ABAC confirmed to enforce on the owner/admin identity** — no service-principal
   restructuring was needed (verified empirically; see below).
5. **Cost via tag-based system-tables query** rather than an endpoint budget (see above).
6. **Agent runs under your identity** (local script or workspace notebook) rather than a
   deployed service-principal endpoint — required so its briefings inherit *your*
   scoped/masked view for the governance story.

---

## Files

```
setup/
  build_all.sh              one-command rebuild (data + governance scaffold)
  generate_data.py          deterministic synthetic data (seed=42)
  build_dataset.py          create tables (comments, PK/FK) + load
  ensure_tags.py            idempotent account-level governed tags
  01_governance_setup.sql   row-filter/mask functions + column tags
  build_genie.py            create/patch the Genie space
  dbsql.py                  tiny SQL runner used by the scripts
  verify_governance.py      before/after check + latency measurement
sql/
  demo_query.sql            the hero query (run before AND after)
  demo_apply_policy.sql     THE live flip — two statements
  demo_remove_policy.sql    reset to BEFORE state
  audit_closer.sql          who/what/when governance evidence
  cost_attribution.sql      demo spend by project tag
agent/
  corridor_agent.py             the agent (verified local runner)
  corridor_briefing_notebook.py workspace notebook (verified on serverless)
```

---

## Verification status (all checked live)

- ABAC row filter + mask **bite on the owner/admin identity**; propagation ~1.1 s.
- Hero query: before = 3 clients + real PII; after = Utility A only + masked.
- Genie Q1 before = all clients; after = Utility A scoped + contacts masked. Q2/Q3 accurate.
- Agent: real PII before; masked for COR-007 and scoped-out (empty) for Utility C after.
- MLflow traces logged; notebook runs green on serverless.
- Lineage populated (`detections`↔`corridors`↔`work_orders`); FK relationships in place.
- Cost query valid; warehouse tagged `project=corridor-demo`.
