"""Corridor Genie Agent — a served, traced front door to the Genie space.

Models-from-code (MLflow 3 ResponsesAgent). This file IS the model: the deploy
driver logs it with `mlflow.pyfunc.log_model(python_model=".../corridor_genie_agent.py")`
and the final `set_model(AGENT)` tells MLflow what to serve.

Why this exists
---------------
The demo's seeded questions used to run in the Genie *UI*, which does not emit
MLflow traces. This agent wraps the SAME Genie space as a tool and is deployed as
a serving endpoint, so a Databricks App can front it and EVERY conversation is
captured as an MLflow trace (auto-logged to an inference table + the experiment).

Governance is unchanged: the row filter is static (`client = 'Fictional Utility A'`),
so whether the caller is you or the endpoint's service principal, Genie's generated
SQL sees the same before/after view when the ABAC policy is flipped.

All config is read from `model_config` at log time — nothing workspace-specific is
hardcoded here.
"""
from __future__ import annotations

from typing import Generator, Optional
from uuid import uuid4

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.models import set_model
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

# Genie is not an LLM-SDK call, so autolog won't capture it; the @mlflow.trace on
# the tool below is what puts the question + SQL + answer into the trace. The
# serving runtime traces `predict`/`predict_stream` as the top-level AGENT span.

_DEFAULTS = {
    # Grid Corridor Intelligence Genie space
    "genie_space_id": "01f19b488b6e1d4ea44cb81e09ec4a0e",
    # Serverless Starter Warehouse (Genie runs its generated SQL here)
    "warehouse_id": "c6250844810982c2",
    "max_result_rows": 25,
}


class CorridorGenieAgent(ResponsesAgent):
    def __init__(self) -> None:
        cfg = mlflow.models.ModelConfig(development_config=_DEFAULTS)
        self.space_id: str = cfg.get("genie_space_id")
        self.max_rows: int = int(cfg.get("max_result_rows") or 25)
        self.w = WorkspaceClient()

    # ---- the Genie tool (its args + return value land in the trace) -----------
    @mlflow.trace(span_type="TOOL", name="genie_conversation")
    def _ask_genie(self, question: str, conversation_id: Optional[str]):
        if conversation_id:
            msg = self.w.genie.create_message_and_wait(
                self.space_id, conversation_id, question)
        else:
            msg = self.w.genie.start_conversation_and_wait(self.space_id, question)

        conversation_id = msg.conversation_id
        message_id = msg.message_id or msg.id
        if getattr(msg, "error", None):
            return {"answer": f"Genie error: {msg.error}", "sql": None,
                    "table": None, "conversation_id": conversation_id}

        texts: list[str] = []
        sql: Optional[str] = None
        data: Optional[dict] = None
        for att in (msg.attachments or []):
            if att.text and getattr(att.text, "content", None):
                texts.append(att.text.content)
            if att.query:
                sql = att.query.query
                if getattr(att.query, "description", None):
                    texts.append(att.query.description)
                data = self._fetch_result(
                    conversation_id, message_id, att.attachment_id)

        return {
            "answer": "\n\n".join(t for t in texts if t).strip(),
            "sql": sql,
            "data": data,                          # structured {columns, rows} for charting
            "table": self._result_markdown(data),  # markdown for the text answer / table view
            "conversation_id": conversation_id,
        }

    @mlflow.trace(span_type="PARSER", name="fetch_result")
    def _fetch_result(self, conversation_id, message_id, attachment_id):
        """Return the query result as structured {columns, rows, row_count, truncated}."""
        try:
            res = self.w.genie.get_message_attachment_query_result(
                self.space_id, conversation_id, message_id, attachment_id)
        except Exception as exc:  # results are a nicety, never fail the answer on them
            return {"error": f"could not fetch result rows: {exc}"}
        sr = getattr(res, "statement_response", None)
        if not sr or not sr.result or not sr.result.data_array:
            return None
        cols = [c.name for c in sr.manifest.schema.columns] if sr.manifest and sr.manifest.schema else []
        all_rows = sr.result.data_array
        rows = all_rows[: self.max_rows]
        return {"columns": cols, "rows": rows, "row_count": len(all_rows),
                "truncated": len(all_rows) > self.max_rows}

    @staticmethod
    def _result_markdown(data):
        if not data or not data.get("columns"):
            return None
        cols, rows = data["columns"], data.get("rows", [])
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join("---" for _ in cols) + " |"
        body = "\n".join(
            "| " + " | ".join("" if v is None else str(v) for v in row) + " |"
            for row in rows)
        more = "" if not data.get("truncated") else \
            f"\n\n_…{data['row_count'] - len(rows)} more row(s) not shown._"
        return f"{header}\n{sep}\n{body}{more}"

    # ---- ResponsesAgent contract ---------------------------------------------
    @staticmethod
    def _latest_question(request: ResponsesAgentRequest) -> str:
        users = [m for m in request.input if m.role == "user"]
        return users[-1].content if users else ""

    @staticmethod
    def _conversation_id(request: ResponsesAgentRequest) -> Optional[str]:
        ci = getattr(request, "custom_inputs", None) or {}
        return ci.get("genie_conversation_id")

    @staticmethod
    def _compose(result: dict) -> str:
        parts = []
        if result["answer"]:
            parts.append(result["answer"])
        if result["table"]:
            parts.append(result["table"])
        if result["sql"]:
            parts.append(f"**Generated SQL**\n```sql\n{result['sql']}\n```")
        return "\n\n".join(parts) or "Genie returned no answer for that question."

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        result = self._ask_genie(
            self._latest_question(request), self._conversation_id(request))
        item = {
            "id": str(uuid4()),
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": self._compose(result)}],
        }
        return ResponsesAgentResponse(
            output=[item],
            custom_outputs={
                "genie_conversation_id": result["conversation_id"],
                "generated_sql": result["sql"],
                "result_table": result["data"],
            },
        )

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Genie replies in one shot; we stream the composed text in chunks for a
        # typewriter feel, then emit the final item so the client gets metadata.
        result = self._ask_genie(
            self._latest_question(request), self._conversation_id(request))
        text = self._compose(result)
        item_id = str(uuid4())
        chunk = 48
        for i in range(0, len(text), chunk):
            yield ResponsesAgentStreamEvent(
                type="response.output_text.delta",
                item_id=item_id,
                delta=text[i : i + chunk],
            )
        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item={
                "id": item_id,
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            },
            custom_outputs={
                "genie_conversation_id": result["conversation_id"],
                "generated_sql": result["sql"],
                "result_table": result["data"],
            },
        )


AGENT = CorridorGenieAgent()
set_model(AGENT)
