# Databricks notebook source
# MAGIC %md
# MAGIC # Arc'Teryx RSIP — Store Report Extraction
# MAGIC ## From a Working Prototype to a Production-Ready Pipeline
# MAGIC
# MAGIC ### Why this notebook exists
# MAGIC
# MAGIC Notebook `01_extract_store_reports` (the **baseline**) already *works*: it reads
# MAGIC store report PDFs, calls an LLM to extract structured data and derive sentiment,
# MAGIC and writes five Delta tables. For a prototype, that is exactly the right amount
# MAGIC of engineering.
# MAGIC
# MAGIC But "it runs" and "it is production-ready" are different bars. The moment this
# MAGIC pipeline matters to the business, a new set of questions appears that the baseline
# MAGIC simply cannot answer:
# MAGIC
# MAGIC - *"Sentiment for the Banff store looks off this week — what exactly did the model
# MAGIC   see, and what did it return?"* The baseline has **no record of any LLM call**.
# MAGIC - *"We tweaked the prompt last month and quality dropped. What was the old prompt?"*
# MAGIC   The baseline prompt is a **string literal**; the previous version is gone.
# MAGIC - *"How long does a full run take, how many tokens does it cost, and is that getting
# MAGIC   worse over time?"* The baseline **measures nothing**.
# MAGIC - *"Run #14 produced weird output. What model, prompt, and settings were used?"*
# MAGIC   The baseline **keeps no run history**.
# MAGIC
# MAGIC This notebook — the **"after"** — answers all of those questions by wrapping the
# MAGIC *exact same pipeline* in **MLflow**. The extraction logic is unchanged. What we add
# MAGIC is **observability, reproducibility, and governance**.
# MAGIC
# MAGIC ### The three capabilities we layer on
# MAGIC
# MAGIC | Capability | The question it answers | The baseline gap it closes |
# MAGIC |---|---|---|
# MAGIC | **1. Experiment Tracking** | *"What happened in each run, and how do runs compare?"* | The baseline is fire-and-forget. Here, every run records its **params** (model, prompt version, temperature) and **metrics** (success rate, latency, token usage) to a named Experiment you can sort, filter, and compare. |
# MAGIC | **2. GenAI Tracing** | *"What did the model actually see and return for this one report?"* | The baseline LLM call is a black box. Here, **every call is traced** — inputs, outputs, latency, token counts — and rendered as an inspectable trace tree in the MLflow UI. |
# MAGIC | **3. Prompt Registry** | *"What prompt produced these results, and can I roll back?"* | The baseline prompt is a hard-coded string. Here it is a **versioned, governed asset** in Unity Catalog, with diffable history and rollback. |
# MAGIC
# MAGIC ### What did NOT change (and why that matters)
# MAGIC
# MAGIC The PDF parsing, the prompt *content*, the extraction function's behavior, and all
# MAGIC five output tables (`store_metrics`, `topic_sentiments`, `challenges`,
# MAGIC `topic_details`, `overall_sentiment`) are **identical** to the baseline. This is
# MAGIC deliberate: it demonstrates that productionizing a GenAI pipeline is **additive**.
# MAGIC You are not rewriting working logic — you are wrapping it in a layer that makes it
# MAGIC observable and trustworthy. That is the whole story of this notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Input:** `/Volumes/ademianczuk_uc_1_catalog/arcteryx_rsip/store_reports/*.pdf`
# MAGIC
# MAGIC **Output Tables** (byte-for-byte the same as baseline):
# MAGIC `store_metrics`, `topic_sentiments`, `challenges`, `topic_details`, `overall_sentiment`

# COMMAND ----------

# MAGIC %md
# MAGIC ## A side-by-side map: baseline → production
# MAGIC
# MAGIC Read this table top to bottom and you have the entire diff between the two
# MAGIC notebooks. Every other markdown cell below zooms into one of these rows.
# MAGIC
# MAGIC | Pipeline stage | Baseline (`01`) | This notebook (`02`) | What the change buys you |
# MAGIC |---|---|---|---|
# MAGIC | **Dependencies** | `pypdf`, `openai` | `+ mlflow[databricks]>=3.1` | Access to tracking, tracing, and the prompt registry. |
# MAGIC | **Setup** | none | `mlflow.set_experiment(...)` + `mlflow.openai.autolog()` | One line turns on automatic tracing for *every* LLM call. |
# MAGIC | **Prompt** | Python string literal `EXTRACTION_PROMPT` | Registered template in the **Prompt Registry** (UC) | Versioning, diffing, rollback, and reuse across notebooks/jobs. |
# MAGIC | **Extraction fn** | plain function, returns parsed JSON | `@mlflow.trace`-decorated, returns JSON **+ token usage** | A span per report; tokens roll up into run metrics. |
# MAGIC | **Batch loop** | bare `for` loop | same loop inside `with mlflow.start_run()` | Params + metrics logged once per run, comparable over time. |
# MAGIC | **Table writes** | 5 Delta tables | **the same 5 Delta tables** | Proof the productionization is additive, not a rewrite. |
# MAGIC
# MAGIC > **Demo note:** the highest-risk change is the prompt move (string → template).
# MAGIC > Because we now render the prompt with `str.format()`, every literal `{`/`}` in
# MAGIC > the JSON schema example had to be escaped as `{{`/`}}`. That detail is called out
# MAGIC > again at the Prompt Registry cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0 — Dependencies
# MAGIC
# MAGIC The only new dependency versus the baseline is **`mlflow[databricks]>=3.1`**. The
# MAGIC `[databricks]` extra pulls in the integrations that make tracing and the prompt
# MAGIC registry work against the workspace, and `>=3.1` is the line where MLflow's GenAI
# MAGIC features (`mlflow.genai.*`, OpenAI autologging) are stable.
# MAGIC
# MAGIC `dbutils.library.restartPython()` restarts the Python interpreter so the freshly
# MAGIC installed version is the one that gets imported below.

# COMMAND ----------

# MAGIC %pip install pypdf openai "mlflow[databricks]>=3.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import re
import time
from pathlib import Path

import mlflow
from openai import OpenAI
from pypdf import PdfReader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "arcteryx_rsip"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/store_reports"
MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"

# MLflow configuration
EXPERIMENT_PATH = "/Users/andrij.demianczuk@databricks.com/arcteryx_rsip/rsip_extraction"
PROMPT_NAME = f"{CATALOG}.{SCHEMA}.rsip_extraction_prompt"
TEMPERATURE = 0.0
MAX_TOKENS = 4096

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capability 2 — GenAI Tracing (setup)
# MAGIC ### *Baseline gap: the LLM call was a black box*
# MAGIC
# MAGIC In the baseline, when the model returned something surprising, you had no way to
# MAGIC see what it was actually sent or what it replied — the call happened and vanished.
# MAGIC These **two lines** fix that for the entire notebook:
# MAGIC
# MAGIC - **`mlflow.set_experiment(...)`** — names a destination so every run and every
# MAGIC   trace from this notebook lands in one place in the **Experiments** UI. Without
# MAGIC   this, artifacts scatter to a default location and are hard to find.
# MAGIC - **`mlflow.openai.autolog()`** — the key line. It monkey-patches the OpenAI client
# MAGIC   so that **every** `chat.completions.create(...)` call is automatically captured as
# MAGIC   a **trace**: the exact messages in, the full response out, latency, and token
# MAGIC   counts. We point that client at the Databricks Foundation Model API, so this
# MAGIC   works for Claude with zero changes to the call sites.
# MAGIC
# MAGIC > **Why "autolog"?** Notice we add *no* logging code inside the extraction function
# MAGIC > for the LLM call itself. Tracing is enabled once, here, and applies everywhere.
# MAGIC > After a run, open the **Traces** tab to replay any single report's call.

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.openai.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Foundation Model API Client

# COMMAND ----------

client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .get(),
    base_url=f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/serving-endpoints",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capability 3 — Prompt Registry
# MAGIC ### *Baseline gap: the prompt was a buried string literal*
# MAGIC
# MAGIC In the baseline, the prompt lived as a Python string in the notebook. That has
# MAGIC three problems in production:
# MAGIC 1. **No history** — change it and the previous version is gone forever.
# MAGIC 2. **No governance** — it's invisible to anyone not reading the source.
# MAGIC 3. **No reuse** — another notebook or job can't reference "the current prompt."
# MAGIC
# MAGIC The **MLflow Prompt Registry** (backed by Unity Catalog) turns the prompt into a
# MAGIC **first-class, versioned asset**. `register_prompt(...)` commits the template; if
# MAGIC the text is byte-identical to the latest version, MLflow reuses it, otherwise it
# MAGIC creates a new version. You get diffable history, rollback, and a stable name
# MAGIC (`rsip_extraction_prompt`) that jobs and other notebooks can load by version or
# MAGIC alias (e.g. `@production`).
# MAGIC
# MAGIC ### ⚠️ The one tricky detail: brace escaping
# MAGIC
# MAGIC A registered prompt is a **template** with `{placeholder}` slots, rendered via
# MAGIC Python's `str.format()`. Our prompt contains a large JSON **schema example** full
# MAGIC of literal `{` and `}`. To `str.format()`, those look like (malformed) placeholders
# MAGIC and would raise a `KeyError`. So every literal brace in the schema is **doubled**
# MAGIC (`{{` and `}}`), which `.format()` renders back to a single brace. The only *real*
# MAGIC placeholder is `{report_text}` at the end. This is the single most error-prone
# MAGIC difference from the baseline prompt — if a run fails on the first report with a
# MAGIC `KeyError`, an unescaped brace is almost always the cause.

# COMMAND ----------

EXTRACTION_PROMPT_TEMPLATE = """You are a structured data extraction and sentiment analysis agent. Given the text of an Arc'Teryx weekly store report, you must:

1. EXTRACT factual data (store info, metrics, topics with wins/actions, challenges)
2. DERIVE sentiment scores by analyzing the tone and content of each section

Return ONLY valid JSON with this exact schema:

{{
  "store_name": "string",
  "region": "string",
  "week_number": integer,
  "metrics": {{
    "sales_plan": float,
    "sales_actual": float,
    "attainment_pct": float,
    "weekly_conversion_pct": float
  }},
  "store_summary": "string",
  "topics": [
    {{
      "topic_name": "string",
      "wins": ["string", ...],
      "actions": ["string", ...],
      "sentiment": {{
        "category": "string (positive|negative|neutral)",
        "score": float (0.0-1.0, where 1.0 is most positive),
        "confidence": float (0.0-1.0, how confident you are in the assessment),
        "reasoning": "string (brief explanation of why this score was assigned)"
      }}
    }}
  ],
  "challenges": [
    {{
      "topic": "string",
      "narrative": "string",
      "sentiment": {{
        "category": "negative",
        "score": float (0.0-1.0, where 0.0 is most negative),
        "confidence": float (0.0-1.0),
        "reasoning": "string"
      }}
    }}
  ],
  "overall_sentiment": {{
    "category": "string (positive|negative|neutral)",
    "score": float (0.0-1.0),
    "confidence": float (0.0-1.0),
    "reasoning": "string (holistic assessment considering metrics, wins, actions, and challenges)"
  }}
}}

Rules:
- Extract numbers exactly as they appear (dollar amounts without $ or commas)
- If a field is not present, use null
- Do not add any text outside the JSON object
- DERIVE sentiment by analyzing:
  - The language used (strong/positive words vs concern/negative words)
  - Metrics performance (attainment above/below 100%)
  - Ratio of wins to actions (more wins = more positive)
  - Severity of challenges described
- Score guidelines:
  - 0.8-1.0: Strongly positive (exceeding targets, enthusiastic language, many wins)
  - 0.6-0.8: Moderately positive (meeting targets, generally good with minor concerns)
  - 0.4-0.6: Neutral/mixed (mixed results, some good some bad)
  - 0.2-0.4: Moderately negative (below targets, significant concerns)
  - 0.0-0.2: Strongly negative (major issues, urgent language)

Report text:
{report_text}"""

# Register this prompt template. MLflow creates a new version only if the
# template differs from the latest registered version.
registered_prompt = mlflow.genai.register_prompt(
    name=PROMPT_NAME,
    template=EXTRACTION_PROMPT_TEMPLATE,
    commit_message="RSIP extraction + sentiment derivation prompt",
)
print(f"Using prompt '{registered_prompt.name}' version {registered_prompt.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDF Text Extraction

# COMMAND ----------

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text content from a PDF file."""
    reader = PdfReader(pdf_path)
    pages = [page.extract_text() for page in reader.pages]
    return "\n\n".join(pages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Function — now traced and token-aware
# MAGIC
# MAGIC Compared to the baseline's `extract_report_data`, three things change here, and
# MAGIC nothing about the extraction *behavior* does:
# MAGIC
# MAGIC 1. **`@mlflow.trace(span_type="CHAIN")`** wraps the function in its own span. The
# MAGIC    autologged LLM call (from Capability 2) nests *underneath* it automatically, so
# MAGIC    each report yields a clean trace tree: `extract_report` → `chat.completions`.
# MAGIC    You can open any report and see PDF-in, prompt-sent, JSON-out, latency, tokens.
# MAGIC 2. **It renders the registered prompt template** with this report's text
# MAGIC    (`prompt_template.format(report_text=text)`) instead of referencing a hard-coded
# MAGIC    string. Note this also collapses the baseline's separate system+user messages
# MAGIC    into a single rendered prompt, since the template now contains everything.
# MAGIC 3. **It returns token usage** alongside the parsed JSON, so the batch loop can sum
# MAGIC    tokens across all reports and log them as run metrics.

# COMMAND ----------

@mlflow.trace(name="extract_report", span_type="CHAIN")
def extract_report_data(pdf_path: str, prompt_template: str) -> dict:
    """Extract structured data from a single store report PDF."""
    text = extract_text_from_pdf(pdf_path)

    # Render the registered prompt template with this report's text.
    rendered_prompt = prompt_template.format(report_text=text)

    response = client.chat.completions.create(
        model=MODEL_ENDPOINT,
        messages=[{"role": "user", "content": rendered_prompt}],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )

    content = response.choices[0].message.content.strip()
    # Strip optional markdown code fences (```json ... ``` or ``` ... ```).
    # Regex-based so it doesn't depend on fragile literal-backtick handling.
    fence = chr(96) * 3  # ``` without typing literal backticks
    if content.startswith(fence):
        content = re.sub(rf"^{fence}[a-zA-Z]*\n", "", content)
        content = re.sub(rf"\n?{fence}$", "", content)
        content = content.strip()

    usage = response.usage
    return {
        "data": json.loads(content),
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capability 1 — Experiment Tracking
# MAGIC ### *Baseline gap: runs left no record*
# MAGIC
# MAGIC The baseline ran the batch in a bare `for` loop and kept nothing. Here the
# MAGIC identical loop runs inside **`with mlflow.start_run()`**, which records two kinds
# MAGIC of facts about the run:
# MAGIC
# MAGIC - **Params — the inputs / configuration** (logged once, immutable): model endpoint,
# MAGIC   temperature, max tokens, **which prompt version** was used, and report count.
# MAGIC   These are how you answer *"what settings produced run #14?"*
# MAGIC - **Metrics — the measured outcomes**: reports succeeded/failed, success rate,
# MAGIC   elapsed seconds, average seconds per report, and total prompt/completion tokens.
# MAGIC   These are how you answer *"is it getting slower or more expensive over time?"*
# MAGIC
# MAGIC Because params include the prompt version, **Tracking and the Prompt Registry work
# MAGIC together**: when quality changes between runs, you can see at a glance whether the
# MAGIC prompt version changed too. Every re-run is a new, comparable row in the
# MAGIC Experiments UI — sortable, filterable, and diffable. That is the core of
# MAGIC reproducibility.

# COMMAND ----------

pdf_files = sorted(Path(VOLUME_PATH).glob("*.pdf"))
print(f"Found {len(pdf_files)} PDF reports to process")

# COMMAND ----------

results = []
errors = []

with mlflow.start_run(run_name="rsip_batch_extraction") as run:
    # --- Log configuration as params ---
    mlflow.log_params({
        "model_endpoint": MODEL_ENDPOINT,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "prompt_name": PROMPT_NAME,
        "prompt_version": registered_prompt.version,
        "num_reports": len(pdf_files),
    })

    start_time = time.time()
    total_prompt_tokens = 0
    total_completion_tokens = 0

    for i, pdf_path in enumerate(pdf_files):
        try:
            out = extract_report_data(str(pdf_path), EXTRACTION_PROMPT_TEMPLATE)
            data = out["data"]
            data["source_file"] = pdf_path.name
            results.append(data)
            total_prompt_tokens += out["prompt_tokens"]
            total_completion_tokens += out["completion_tokens"]
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(pdf_files)} reports")
        except Exception as e:
            errors.append({"file": pdf_path.name, "error": str(e)})
            print(f"Error processing {pdf_path.name}: {e}")

    elapsed = time.time() - start_time

    # --- Log outcome as metrics ---
    mlflow.log_metrics({
        "reports_succeeded": len(results),
        "reports_failed": len(errors),
        "success_rate": len(results) / len(pdf_files) if pdf_files else 0,
        "elapsed_seconds": elapsed,
        "avg_seconds_per_report": elapsed / len(pdf_files) if pdf_files else 0,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
    })

    run_id = run.info.run_id

print(f"\nCompleted: {len(results)} successful, {len(errors)} errors")
print(f"MLflow run_id: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Delta Tables — deliberately unchanged
# MAGIC
# MAGIC Everything below this point is **copied verbatim from the baseline notebook**.
# MAGIC Same five tables, same schemas, same write logic. We include it unchanged on
# MAGIC purpose: it is the proof point of the entire exercise.
# MAGIC
# MAGIC > **The takeaway for the room:** productionizing a GenAI pipeline did **not** mean
# MAGIC > rewriting the part that does the work. The extraction logic and its outputs are
# MAGIC > identical. Everything we added — tracking, tracing, prompt versioning — wraps
# MAGIC > *around* the working code. That is what makes this a safe, incremental upgrade
# MAGIC > rather than a risky rebuild.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Metrics Table

# COMMAND ----------

metrics_rows = []
for r in results:
    m = r.get("metrics", {})
    metrics_rows.append({
        "store_name": r["store_name"],
        "region": r["region"],
        "week_number": r["week_number"],
        "sales_plan": m.get("sales_plan"),
        "sales_actual": m.get("sales_actual"),
        "attainment_pct": m.get("attainment_pct"),
        "weekly_conversion_pct": m.get("weekly_conversion_pct"),
        "store_summary": r.get("store_summary"),
        "source_file": r["source_file"],
    })

df_metrics = spark.createDataFrame(metrics_rows)
df_metrics.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.store_metrics")
print(f"Wrote {df_metrics.count()} rows to store_metrics")
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topic Sentiments Table (Derived)

# COMMAND ----------

sentiment_rows = []
for r in results:
    for topic in r.get("topics", []):
        s = topic.get("sentiment", {})
        sentiment_rows.append({
            "store_name": r["store_name"],
            "region": r["region"],
            "week_number": r["week_number"],
            "topic": topic["topic_name"],
            "category": s.get("category"),
            "score": s.get("score"),
            "confidence": s.get("confidence"),
            "reasoning": s.get("reasoning"),
            "source_file": r["source_file"],
        })

df_sentiments = spark.createDataFrame(sentiment_rows)
df_sentiments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.topic_sentiments")
print(f"Wrote {df_sentiments.count()} rows to topic_sentiments")
display(df_sentiments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenges Table (with Derived Sentiment)

# COMMAND ----------

challenge_rows = []
for r in results:
    for ch in r.get("challenges", []):
        s = ch.get("sentiment", {})
        challenge_rows.append({
            "store_name": r["store_name"],
            "region": r["region"],
            "week_number": r["week_number"],
            "topic": ch["topic"],
            "narrative": ch["narrative"],
            "severity_score": s.get("score"),
            "confidence": s.get("confidence"),
            "reasoning": s.get("reasoning"),
            "source_file": r["source_file"],
        })

df_challenges = spark.createDataFrame(challenge_rows)
df_challenges.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.challenges")
print(f"Wrote {df_challenges.count()} rows to challenges")
display(df_challenges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topic Details Table (Wins & Actions)

# COMMAND ----------

detail_rows = []
for r in results:
    for topic in r.get("topics", []):
        for win in topic.get("wins", []):
            detail_rows.append({
                "store_name": r["store_name"],
                "region": r["region"],
                "week_number": r["week_number"],
                "topic": topic["topic_name"],
                "type": "win",
                "detail": win,
                "source_file": r["source_file"],
            })
        for action in topic.get("actions", []):
            detail_rows.append({
                "store_name": r["store_name"],
                "region": r["region"],
                "week_number": r["week_number"],
                "topic": topic["topic_name"],
                "type": "action",
                "detail": action,
                "source_file": r["source_file"],
            })

df_details = spark.createDataFrame(detail_rows)
df_details.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.topic_details")
print(f"Wrote {df_details.count()} rows to topic_details")
display(df_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Store Sentiment Table

# COMMAND ----------

overall_rows = []
for r in results:
    o = r.get("overall_sentiment", {})
    overall_rows.append({
        "store_name": r["store_name"],
        "region": r["region"],
        "week_number": r["week_number"],
        "category": o.get("category"),
        "score": o.get("score"),
        "confidence": o.get("confidence"),
        "reasoning": o.get("reasoning"),
        "source_file": r["source_file"],
    })

df_overall = spark.createDataFrame(overall_rows)
df_overall.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.overall_sentiment")
print(f"Wrote {df_overall.count()} rows to overall_sentiment")
display(df_overall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — what to show after running this
# MAGIC
# MAGIC The pipeline produced the same five tables as the baseline. The difference is
# MAGIC everything you can now *see and trust* about how they were produced. Walk through
# MAGIC these three places in the UI to close the before/after story:
# MAGIC
# MAGIC | Where to look | What you'll see | Which baseline gap it closes |
# MAGIC |---|---|---|
# MAGIC | **Experiments** tab → this run | Params (model, prompt version, temperature) and metrics (success rate, latency, tokens). Re-run and a second comparable row appears. | *"Runs left no record."* |
# MAGIC | **Traces** tab → any report | The full trace tree for one report: prompt sent → Claude's response → latency → token counts. | *"The LLM call was a black box."* |
# MAGIC | **Prompt Registry** → `rsip_extraction_prompt` | The versioned prompt, its commit history, and the version this run used. | *"The prompt was a buried string."* |
# MAGIC
# MAGIC ### The one-sentence takeaway for Arc'Teryx
# MAGIC > We did not change what the pipeline does — we made it **observable, reproducible,
# MAGIC > and governed**, which is precisely the gap between a working prototype and a
# MAGIC > production system. And it cost only a thin wrapper of MLflow around code that
# MAGIC > already worked.
# MAGIC
# MAGIC ### Where this goes next (Phase 4)
# MAGIC With the data structured and the pipeline productionized, notebook `03` builds an
# MAGIC **agentic Knowledge Assistant** on top of these tables — a tool-using agent that
# MAGIC answers natural-language questions about store performance and sentiment.

# COMMAND ----------

print("=" * 60)
print("EXTRACTION & SENTIMENT ANALYSIS COMPLETE (MLflow-tracked)")
print("=" * 60)
print(f"\nReports processed: {len(results)}")
print(f"Errors: {len(errors)}")
print(f"MLflow run_id: {run_id}")
print(f"Prompt: {PROMPT_NAME} v{registered_prompt.version}")
print(f"\nTables created in {CATALOG}.{SCHEMA}:")
print(f"  - store_metrics:      {df_metrics.count()} rows")
print(f"  - topic_sentiments:   {df_sentiments.count()} rows (DERIVED)")
print(f"  - challenges:         {df_challenges.count()} rows (DERIVED)")
print(f"  - topic_details:      {df_details.count()} rows")
print(f"  - overall_sentiment:  {df_overall.count()} rows (DERIVED)")

if errors:
    print(f"\nFailed files:")
    for err in errors:
        print(f"  - {err['file']}: {err['error']}")
