# Databricks notebook source
# MAGIC %md
# MAGIC # Arc'Teryx RSIP — Agent Evaluation with `mlflow.genai.evaluate()`
# MAGIC
# MAGIC This notebook is **fully standalone**. It does NOT modify or import `agent.py`,
# MAGIC the served endpoint, or notebooks 01–03. It treats the **registered UC model
# MAGIC version** as a black box, runs it over a curated evaluation dataset, and scores
# MAGIC the outputs with LLM judges. All results go to a **dedicated experiment** so
# MAGIC nothing here conflates with the extraction or agent-authoring work.
# MAGIC
# MAGIC ### Why `mlflow.genai.evaluate()` (not classic `mlflow.evaluate()`)
# MAGIC The `genai` evaluation harness is purpose-built for tool-using agents: it runs the
# MAGIC agent, captures the **traces** (including tool calls and retrieved context), and
# MAGIC applies **LLM-judge scorers**. Classic `mlflow.evaluate()` targets traditional ML
# MAGIC and static prediction tables and would not capture the agent's tool behavior.
# MAGIC
# MAGIC ### What we evaluate against
# MAGIC The **registered model version** `ademianczuk_uc_1_catalog.arcteryx_rsip.rsip_analyst_agent`
# MAGIC (loaded locally), so the eval is reproducible, version-pinned, and puts no eval
# MAGIC traffic on the production serving endpoint.
# MAGIC
# MAGIC ### How we judge
# MAGIC - **Curated ground truth** — hand-authored questions with `expected_facts`, scored
# MAGIC   by the built-in **Correctness** judge.
# MAGIC - **Reference-free judges** — **RelevanceToQuery** and **RetrievalGroundedness**
# MAGIC   (groundedness uses the retrieved context captured in the trace).
# MAGIC - **Custom Guidelines judges** — plain-English pass/fail rules specific to this
# MAGIC   demo (cite stores/weeks, ground claims in tool results).

# COMMAND ----------

# MAGIC %pip install -U -qqqq "mlflow[databricks]>=3.1" databricks-openai databricks-vectorsearch backoff unitycatalog-ai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC A dedicated experiment path keeps evaluation runs separate from the extraction
# MAGIC experiment (`02`) and the agent-authoring experiment (`03`).

# COMMAND ----------

import mlflow

CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "arcteryx_rsip"

UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.rsip_analyst_agent"
MODEL_VERSION = 1  # version-pin the eval; bump when comparing a new agent version
MODEL_URI = f"models:/{UC_MODEL_NAME}/{MODEL_VERSION}"

EVAL_EXPERIMENT = "/Users/andrij.demianczuk@databricks.com/arcteryx_rsip/rsip_agent_eval"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EVAL_EXPERIMENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the registered model and wrap it in a `predict_fn`
# MAGIC
# MAGIC `mlflow.genai.evaluate()` calls `predict_fn(**inputs)` for each row. Our eval rows
# MAGIC use `inputs={"question": "..."}`, so the function takes a `question` string,
# MAGIC invokes the agent in the ResponsesAgent request shape, and returns the final
# MAGIC assistant text. Because the loaded model is traced, the judges can see tool calls
# MAGIC and retrieved context.

# COMMAND ----------

# Load the registered agent model from Unity Catalog using the specified URI.
loaded_agent = mlflow.pyfunc.load_model(MODEL_URI)

def predict_fn(question: str) -> str:
    """
    Invoke the registered agent and return its final text answer.

    Args:
        question (str): The input question to be answered by the agent.

    Returns:
        str: The concatenated text response from the agent.

    Behavior:
        - Calls the loaded agent model's predict method with a formatted input.
        - The input is a dictionary with a key "input" containing a list of messages,
          where each message has a role ("user") and the question content.
        - The agent returns a response dictionary, expected to have an "output" key.
        - Iterates through each item in the "output" list:
            - If the item type is "message", iterates through its "content" list.
            - If a content dictionary has a "text" key, appends the text to the result list.
        - Joins all collected text segments with newline characters and returns the result.
    """
    response = loaded_agent.predict(
        {"input": [{"role": "user", "content": question}]}
    )
    # ResponsesAgent output -> concatenate assistant text items.
    texts = []
    for item in response.get("output", []):
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("text"):
                    texts.append(c["text"])
    return "\n".join(texts)

# Quick sanity check before the full eval run.
print(predict_fn("Which region has the weakest overall sentiment?")[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation dataset
# MAGIC
# MAGIC ~12 questions deliberately spanning every tool path. Each row carries
# MAGIC `expected_facts` — the atomic, verifiable claims a correct answer must contain.
# MAGIC The Correctness judge checks the answer against these facts (it does not require
# MAGIC verbatim wording).
# MAGIC
# MAGIC > **Note:** `expected_facts` were derived from the actual table values queried
# MAGIC > during development. If the underlying tables are re-extracted with different
# MAGIC > random data, refresh these facts so Correctness stays meaningful.

# COMMAND ----------

eval_dataset = [
    # --- Structured: single-store sentiment trend ---
    {
        "inputs": {"question": "How has overall sentiment trended for Banff across the weeks?"},
        "expectations": {
            "expected_facts": [
                "Banff's sentiment is generally positive across the weeks.",
                "Week 12 and Week 16 are the lowest-scoring / weakest weeks for Banff.",
                "The dips are associated with missing or being below the sales plan.",
            ]
        },
    },
    # --- Aggregation: regional comparison ---
    {
        "inputs": {"question": "Which region has the weakest overall sentiment?"},
        "expectations": {
            "expected_facts": [
                "Ontario has the weakest overall average sentiment.",
                "Alberta has the highest average overall sentiment.",
                "The three regions are Alberta, British Columbia, and Ontario.",
            ]
        },
    },
    # --- Structured: specific store + week metrics ---
    {
        "inputs": {"question": "What were the sales plan, sales actual, and attainment for Banff in week 14?"},
        "expectations": {
            "expected_facts": [
                "Banff in week 14 exceeded its sales plan (attainment above 100%).",
            ]
        },
    },
    # --- Aggregation: top challenges in a region ---
    {
        "inputs": {"question": "What are the most common challenge topics raised in Ontario?"},
        "expectations": {
            "expected_facts": [
                "Customer Traffic is among the most frequently raised challenge topics in Ontario.",
                "Staffing and inventory-related concerns also appear among Ontario challenges.",
            ]
        },
    },
    # --- Vector search: qualitative narrative theme ---
    {
        "inputs": {"question": "What are stores saying about staffing and employee burnout?"},
        "expectations": {
            "expected_facts": [
                "Multiple stores report team fatigue from consecutive high-volume weekends.",
                "The burnout / fatigue theme appears across more than one region.",
            ]
        },
    },
    # --- Vector search: product-specific narratives ---
    {
        "inputs": {"question": "Are there any recurring inventory or stockout problems mentioned in the reports?"},
        "expectations": {
            "expected_facts": [
                "Reports mention stockouts of popular products or core sizes.",
                "Some inventory shortages persist across multiple weeks.",
            ]
        },
    },
    # --- Multi-tool reasoning ---
    {
        "inputs": {"question": "Find the lowest-scoring store and explain why using what the team reported."},
        "expectations": {
            "expected_facts": [
                "The answer identifies a specific store and week with the lowest sentiment score.",
                "The explanation references being below sales plan.",
                "The explanation cites qualitative challenges such as staffing, traffic, or inventory.",
            ]
        },
    },
    # --- Multi-tool: regional + narrative ---
    {
        "inputs": {"question": "Ontario seems to be struggling — what's driving its lower sentiment?"},
        "expectations": {
            "expected_facts": [
                "Ontario has the lowest regional average sentiment.",
                "Challenges such as customer traffic, staffing, or inventory contribute to the lower sentiment.",
            ]
        },
    },
    # --- Scope / grounding guardrail: out-of-data question ---
    {
        "inputs": {"question": "What were the sales for the Tokyo store in week 14?"},
        "expectations": {
            "expected_facts": [
                "There is no Tokyo store in the data.",
                "The answer does not fabricate sales figures for a Tokyo store.",
            ]
        },
    },
    # --- Comparison across stores ---
    {
        "inputs": {"question": "Compare overall sentiment between Whistler and Victoria."},
        "expectations": {
            "expected_facts": [
                "The answer reports sentiment for both Whistler and Victoria.",
                "Both stores are in British Columbia.",
            ]
        },
    },
    # --- Topic-level question ---
    {
        "inputs": {"question": "Which topics tend to have the most positive sentiment across stores?"},
        "expectations": {
            "expected_facts": [
                "The topics evaluated include Product, People, Operations, and Experience.",
            ]
        },
    },
    # --- Trend / time question ---
    {
        "inputs": {"question": "Did any store show a clear decline in sentiment over consecutive weeks?"},
        "expectations": {
            "expected_facts": [
                "The answer identifies at least one store with a week-over-week sentiment dip.",
            ]
        },
    },
]

print(f"Evaluation dataset: {len(eval_dataset)} questions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scorers
# MAGIC
# MAGIC - **Correctness** — compares the answer to `expected_facts` (LLM judge).
# MAGIC - **RelevanceToQuery** — does the answer actually address the question? (reference-free)
# MAGIC - **RetrievalGroundedness** — are claims supported by the context retrieved in the
# MAGIC   trace? (catches hallucination; reference-free)
# MAGIC - **Guidelines (custom)** — plain-English pass/fail rules tailored to this demo.

# COMMAND ----------

from mlflow.genai.scorers import (
    Correctness,
    RelevanceToQuery,
    RetrievalGroundedness,
    Guidelines,
)

cites_specifics = Guidelines(
    name="cites_specifics",
    guidelines=(
        "When the question is about specific stores, regions, or weeks, the response "
        "must cite the specific store names, region names, and/or week numbers it used. "
        "General hand-waving without specifics should fail."
    ),
)

grounded_no_fabrication = Guidelines(
    name="grounded_no_fabrication",
    guidelines=(
        "The response must not fabricate data. If the question asks about an entity "
        "that does not exist in the Arc'Teryx store data (e.g. a store or region not "
        "covered), the response must say so rather than inventing numbers."
    ),
)

scorers = [
    Correctness(),
    RelevanceToQuery(),
    RetrievalGroundedness(),
    cites_specifics,
    grounded_no_fabrication,
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the evaluation
# MAGIC
# MAGIC This executes `predict_fn` over every row, captures traces, and applies all
# MAGIC scorers. Results (per-row scores, aggregate metrics, and traces) land in the
# MAGIC dedicated experiment and in the returned object.

# COMMAND ----------

with mlflow.start_run(run_name=f"agent_eval_v{MODEL_VERSION}"):
    mlflow.log_params({
        "model_uri": MODEL_URI,
        "model_version": MODEL_VERSION,
        "num_eval_questions": len(eval_dataset),
        "num_scorers": len(scorers),
    })

    results = mlflow.genai.evaluate(
        data=eval_dataset,
        predict_fn=predict_fn,
        scorers=scorers,
    )

print("Evaluation complete.")
print("Aggregate metrics:")
for k, v in results.metrics.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review results
# MAGIC
# MAGIC Open the **Experiments** tab → this run → **Evaluations** to see per-question
# MAGIC judge scores, rationales, and the captured traces. The per-row table is also
# MAGIC available on the result object below.

# COMMAND ----------

display(results.tables["eval_results"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use this for a before/after comparison
# MAGIC
# MAGIC 1. Make a change to the agent (e.g. a new prompt or tool) and register a new
# MAGIC    model version via notebook `03`.
# MAGIC 2. Bump `MODEL_VERSION` at the top of this notebook and re-run.
# MAGIC 3. In the Experiments UI, compare the two `agent_eval_v*` runs side by side — the
# MAGIC    judge metrics show whether quality improved, held, or regressed.
# MAGIC
# MAGIC This closes the productionization loop for the **agent**, mirroring the tracking /
# MAGIC tracing / prompt-registry story we built for the **extraction pipeline** in `02`.
