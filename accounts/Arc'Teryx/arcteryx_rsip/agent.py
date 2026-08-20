"""
Arc'Teryx RSIP Analyst Agent.

A tool-using MLflow ResponsesAgent that answers questions about store
performance and sentiment by combining:
  - UC function tools  -> structured SQL over the Delta tables
  - Vector Search tool -> semantic retrieval over raw narrative text

This module is logged "as code" by the driver notebook (03_analyst_agent),
so everything the agent needs at serving time must be importable from here.
"""

import json
from dataclasses import dataclass
from typing import Any, Callable, Generator

import backoff
import mlflow
from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from openai import OpenAI
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

# --- Configuration ---

CATALOG = "ademianczuk_uc_1_catalog"
SCHEMA = "arcteryx_rsip"
LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

VS_INDEX = f"{CATALOG}.{SCHEMA}.narratives_index"

# UC functions exposed as tools (created in setup; see driver notebook).
UC_TOOL_NAMES = [
    f"{CATALOG}.{SCHEMA}.get_store_sentiment_trend",
    f"{CATALOG}.{SCHEMA}.top_negative_topics_by_region",
    f"{CATALOG}.{SCHEMA}.get_store_metrics",
    f"{CATALOG}.{SCHEMA}.compare_regions_sentiment",
]

SYSTEM_PROMPT = """You are the Arc'Teryx Retail Store Intelligence analyst. You help \
regional managers and executives understand store performance and sentiment derived \
from weekly store reports.

You have two kinds of tools:
1. STRUCTURED tools (UC functions) - use these for precise, numeric questions about \
metrics, sentiment scores, trends, and regional comparisons. Prefer these whenever the \
question maps to specific stores, weeks, regions, or aggregates.
2. A SEARCH tool (search_narratives) - use this for open-ended, qualitative questions \
about WHAT store teams actually said (wins, actions, challenges). Use it when the user \
asks about themes, concerns, or specifics that aren't captured in the numeric tables.

Guidance:
- Sentiment is scored 0.0-1.0 (higher = more positive). A report can be categorized \
"neutral" while still trending down in score, so reason about scores, not just categories.
- Combine tools when helpful: e.g. find a low-scoring store with a structured tool, then \
search narratives to explain WHY.
- Always ground your answer in tool results. Cite the stores, weeks, and regions you used.
- Be concise and lead with the answer, then the supporting detail."""

MAX_ITERATIONS = 6

mlflow.openai.autolog()


@dataclass
class ToolInfo:
    """Unifies a UC function tool and the Vector Search tool behind one interface.

    `spec` is the OpenAI tool spec passed to the LLM; `exec_fn` takes the parsed
    tool-call arguments dict and returns the tool result.
    """

    name: str
    spec: dict
    exec_fn: Callable[[dict], Any]


class RSIPAnalystAgent(ResponsesAgent):
    def __init__(self):
        host_creds = mlflow.utils.databricks_utils.get_databricks_host_creds()
        self.client = OpenAI(
            base_url=host_creds.host + "/serving-endpoints",
            api_key=host_creds.token,
        )

        self._tools: dict[str, ToolInfo] = {}

        # --- UC function tools (structured SQL) ---
        # UCFunctionToolkit emits OpenAI tool specs whose name is the UC name with
        # dots replaced by "__". For a fully-qualified name like
        # `catalog.schema.func` that easily exceeds OpenAI's 64-char tool-name
        # limit, and the toolkit truncates from the FRONT (mangling the catalog).
        # We avoid the whole problem by RENAMING each spec to the function's short
        # name (its last segment): unique, well under 64 chars, and what the model
        # sees. We keep our own map from short name -> fully-qualified UC name for
        # execution, so nothing depends on the toolkit's truncated name.
        uc_function_client = DatabricksFunctionClient()
        uc_toolkit = UCFunctionToolkit(
            function_names=UC_TOOL_NAMES, client=uc_function_client
        )
        short_to_uc = {n.split(".")[-1]: n for n in UC_TOOL_NAMES}
        if len(short_to_uc) != len(UC_TOOL_NAMES):
            raise ValueError(
                "UC function short names are not unique; renaming would collide: "
                f"{UC_TOOL_NAMES}"
            )
        for spec in uc_toolkit.tools:
            # The toolkit's (possibly truncated) name still ends with the short
            # name, so match on suffix rather than trusting list order.
            mangled = spec["function"]["name"]
            short_name = next(s for s in short_to_uc if mangled.endswith(s))
            udf_name = short_to_uc[short_name]
            spec["function"]["name"] = short_name  # override truncated/mangled name
            self._tools[short_name] = ToolInfo(
                name=short_name,
                spec=spec,
                exec_fn=self._make_uc_exec(uc_function_client, udf_name),
            )

        # --- Vector Search tool (semantic retrieval) ---
        # VectorSearchRetrieverTool exposes `.tool` (singular spec) and `.execute()`.
        vs_tool = VectorSearchRetrieverTool(
            index_name=VS_INDEX,
            tool_name="search_narratives",
            tool_description=(
                "Semantic search over the raw text of store report narratives "
                "(wins, actions, and challenges). Use for qualitative, open-ended "
                "questions about what store teams reported."
            ),
            num_results=8,
            columns=["store_name", "region", "week_number", "topic",
                     "narrative_type", "narrative_text"],
        )
        vs_name = vs_tool.tool["function"]["name"]
        self._tools[vs_name] = ToolInfo(
            name=vs_name,
            spec=vs_tool.tool,
            exec_fn=lambda args: vs_tool.execute(**args),
        )

        self._tool_specs = [t.spec for t in self._tools.values()]

    @staticmethod
    def _make_uc_exec(client: DatabricksFunctionClient, udf_name: str):
        def _exec(args: dict) -> Any:
            return client.execute_function(udf_name, args).value
        return _exec

    # --- Tool execution ---

    @mlflow.trace(span_type=SpanType.TOOL)
    def _exec_tool(self, name: str, args: dict) -> Any:
        return self._tools[name].exec_fn(args)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def _call_llm(self, messages: list[dict]) -> Any:
        return self.client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=messages,
            tools=self._tool_specs,
            temperature=0.0,
        )

    # --- ResponsesAgent interface ---
    # Note: predict / predict_stream are auto-traced by the ResponsesAgent base
    # class, so we do NOT decorate them with @mlflow.trace (it would double-trace).

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        messages += self._to_chat_messages(request.input)

        output_items = []

        for _ in range(MAX_ITERATIONS):
            response = self._call_llm(messages)
            choice = response.choices[0].message

            if choice.tool_calls:
                # Build a CLEAN assistant message (not choice.model_dump(), which
                # carries null fields that break the Anthropic tool_use/tool_result
                # pairing). content must be present (Claude tolerates empty string).
                messages.append({
                    "role": "assistant",
                    "content": choice.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in choice.tool_calls
                    ],
                })
                # Append every tool result IMMEDIATELY after, in call order, so each
                # tool_use id has its matching tool_result.
                for tc in choice.tool_calls:
                    args = json.loads(tc.function.arguments)
                    result = self._exec_tool(tc.function.name, args)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": str(result),
                    })
                continue

            # No more tool calls -> final answer.
            output_items.append(
                self.create_text_output_item(text=choice.content, id=response.id)
            )
            break

        return ResponsesAgentResponse(output=output_items)

    @staticmethod
    def _to_chat_messages(input_items: list) -> list[dict]:
        """Convert ResponsesAgentRequest input items into OpenAI chat messages.

        Request input arrives as Responses API items. For the agent's purposes we
        only need to carry user/assistant text turns into the chat-completions
        message list; tool-call plumbing is regenerated fresh inside the loop.
        """
        messages = []
        for item in input_items:
            data = item.model_dump() if hasattr(item, "model_dump") else dict(item)
            role = data.get("role")
            content = data.get("content")
            if role and content is not None:
                # Normalize content to a plain string for the chat API.
                if isinstance(content, list):
                    content = "".join(
                        part.get("text", "")
                        for part in content
                        if isinstance(part, dict)
                    )
                messages.append({"role": role, "content": content})
        return messages

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Non-streaming fallback: emit the final response as a single event.
        response = self.predict(request)
        for item in response.output:
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done", item=item
            )


from mlflow.models import set_model

AGENT = RSIPAnalystAgent()
set_model(AGENT)
