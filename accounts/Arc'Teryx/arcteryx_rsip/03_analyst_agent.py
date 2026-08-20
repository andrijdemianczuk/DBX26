# Databricks notebook source
# MAGIC %md
# MAGIC # Arc'Teryx RSIP - Analyst / Knowledge Assistant Agent
# MAGIC
# MAGIC This is the **Phase 4 (agentic)** notebook. It builds a tool-using
# MAGIC `ResponsesAgent` over the RSIP Delta tables and narrative text, then logs,
# MAGIC registers, and deploys it as a Model Serving endpoint.
# MAGIC
# MAGIC **Tools the agent can call:**
# MAGIC - `get_store_sentiment_trend(store)` — week-by-week sentiment for a store
# MAGIC - `top_negative_topics_by_region(region)` — most-raised challenge topics in a region
# MAGIC - `get_store_metrics(store, week)` — KPIs for a store/week
# MAGIC - `compare_regions_sentiment()` — company-wide regional comparison
# MAGIC - `search_narratives(query)` — semantic Vector Search over raw narrative text
# MAGIC
# MAGIC The agent definition lives in `agent.py` (logged "as code").

# COMMAND ----------

# MAGIC %pip install -U -qqqq "mlflow[databricks]>=3.1" databricks-openai databricks-vectorsearch backoff unitycatalog-ai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick local smoke test
# MAGIC Import the agent and ask a question before we log/deploy it.

# COMMAND ----------

from agent import AGENT
from mlflow.types.responses import ResponsesAgentRequest

req = ResponsesAgentRequest(
    input=[{"role": "user", "content": "Which region has the weakest overall sentiment, and what are the top challenges driving it?"}]
)
resp = AGENT.predict(req)
for item in resp.output:
    for c in item.content:
        print(c.get("text", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent to MLflow
# MAGIC We declare `resources` so the deployed endpoint gets automatic credential
# MAGIC passthrough to the LLM endpoint, the UC functions, and the Vector Search index.

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksFunction,
    DatabricksVectorSearchIndex,
)
from agent import (
    LLM_ENDPOINT,
    UC_TOOL_NAMES,
    VS_INDEX,
    CATALOG,
    SCHEMA,
)

mlflow.set_experiment("/Users/andrij.demianczuk@databricks.com/arcteryx_rsip/rsip_analyst_agent")

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
    DatabricksServingEndpoint(endpoint_name="databricks-bge-large-en"),
]
for fn in UC_TOOL_NAMES:
    resources.append(DatabricksFunction(function_name=fn))

with mlflow.start_run(run_name="rsip_analyst_agent"):
    logged_agent = mlflow.pyfunc.log_model(
        name="rsip_analyst_agent",
        python_model="agent.py",
        resources=resources,
        pip_requirements=[
            "mlflow[databricks]>=3.1",
            "databricks-openai",
            "databricks-vectorsearch",
            "backoff",
            "unitycatalog-ai",
        ],
    )

print("Logged:", logged_agent.model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate against the logged model
# MAGIC Validate the model loads and serves predictions before registering.

# COMMAND ----------

loaded = mlflow.pyfunc.load_model(logged_agent.model_uri)
result = loaded.predict({
    "input": [{"role": "user", "content": "How has sentiment trended for Banff over the weeks?"}]
})
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register to Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.rsip_analyst_agent"

registered = mlflow.register_model(
    model_uri=logged_agent.model_uri,
    name=UC_MODEL_NAME,
)
print(f"Registered {UC_MODEL_NAME} version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy to a Model Serving endpoint

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=registered.version,
    scale_to_zero=True,
)
print("Endpoint:", deployment.endpoint_name)
print("Review/query URL:", deployment.query_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC The agent is now a UC-registered, served model. Open the serving endpoint to
# MAGIC chat with it, or call it via the REST API. All invocations are traced in MLflow.
