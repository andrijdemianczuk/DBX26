# Corridor Intelligence — chat front door

A branded chat UI that replaces the Genie UI for the live demo. It sends questions
to the **Corridor Genie serving endpoint** (which wraps the Genie space and emits
MLflow traces) and renders the markdown answer — SQL block and result table included.

The app runs as its own service principal and calls the endpoint **server-side**;
no token ever reaches the browser.

```
app/
  app.yaml          Databricks App config + serving-endpoint resource (CAN_QUERY)
  server.js         Express proxy: auth + POST /api/chat -> endpoint /invocations
  package.json      one package: express + react + vite (build tools included)
  vite.config.ts    frontend build (-> dist/) and dev proxy
  tsconfig.json
  index.html
  src/
    main.tsx
    App.tsx         chat transcript, seeded chips, multi-turn conversation id
    styles.css
```

## How auth works

`server.js` gets a bearer token in priority order:

1. `DATABRICKS_TOKEN` — for local dev (a PAT, or `databricks auth token`).
2. **client-credentials OAuth** — `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`
   against `${DATABRICKS_HOST}/oidc/v1/token`. The Databricks Apps runtime injects
   these for the app's service principal, so no configuration is needed in-cloud.

It then POSTs to `${DATABRICKS_HOST}/serving-endpoints/${SERVING_ENDPOINT}/invocations`
with the Responses schema:

```json
{ "input": <messages>, "custom_inputs": { "genie_conversation_id": "<id or omitted>" } }
```

and returns the assistant text plus `custom_outputs` (`genie_conversation_id`,
`generated_sql`). The frontend persists `genie_conversation_id` and sends it on the
next turn for multi-turn Genie context; **New conversation** clears it.

`SERVING_ENDPOINT` defaults to
`agents_ademianczuk_uc_1_catalog-stantec_grid_ops-corridor_genie_agent`.

## Run locally

```bash
cd app
npm install

# point at the workspace + a token (this profile owns the endpoint)
export DATABRICKS_HOST=https://fevm-ademianczuk-uc-1.cloud.databricks.com
export DATABRICKS_TOKEN=$(databricks auth token -p fe-vm-ademianczuk-uc-1 | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

# two terminals: API on :8000, Vite dev server on :5173 (proxies /api)
npm run dev:server
npm run dev:client
# open http://localhost:5173
```

Single-process check (build once, serve from Express on :8000):

```bash
npm run build && PORT=8000 npm run start   # note: start rebuilds, that's fine
```

## Deploy

The app's service principal needs **CAN_QUERY** on the serving endpoint (declared in
`app.yaml`) and, transitively, whatever Genie/warehouse/table grants the endpoint's
own SP already carries (see `agent/deploy_genie_agent.py`).

```bash
# from repo root, profile = fe-vm-ademianczuk-uc-1
DST=/Workspace/Users/andrij.demianczuk@databricks.com/corridor_intelligence_app

databricks apps create corridor-intelligence -p fe-vm-ademianczuk-uc-1   # first time only
databricks sync app "$DST" -p fe-vm-ademianczuk-uc-1
databricks apps deploy corridor-intelligence \
  --source-code-path "$DST" -p fe-vm-ademianczuk-uc-1
```

`npm run start` builds the frontend on boot, so nothing built needs to be synced.
After the first deploy, confirm the `serving-endpoint` resource is bound to the app
in the Apps UI (Edit → resources) if it wasn't picked up from `app.yaml`.
