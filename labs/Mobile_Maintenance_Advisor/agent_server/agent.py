from typing import AsyncGenerator

import mlflow
from agents import Agent, Runner, set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)

from agent_server.tools import TOOL_REGISTRY
from agent_server.utils import (
    get_databricks_host_from_env,
    get_user_workspace_client,
    process_agent_stream_events,
)

#Databricks MLFlow flavour of LangChain for Databricks interop
# from databricks_langchain import (
#     ChatDatabricks,
#     UCFunctionToolkit,
#     VectorSearchRetrieverTool,
# )

#Standard Langchain libs
# from langchain_core.messages import AIMessage, AIMessageChunk, AnyMessage
# from langchain_core.runnables import RunnableConfig, RunnableLambda
# from langchain_core.tools import BaseTool
# from langchain.tools import tool
# from langchain_openai import ChatOpenAI


def _normalize_input_messages(messages: list[dict]) -> list[dict]:
    """Normalize message content so it's compatible with the OpenAI-Agents ChatCompletions converter.

    The converter expects message['content'] to be a list of *typed parts*.
    - For user/system input:  [{"type": "input_text", "text": "..."}]
    - For assistant output:   [{"type": "output_text", "text": "..."}]

    Clients sometimes send plain strings or mix types. We coerce to the expected
    shape based on role and fix obvious mismatches (e.g., assistant parts marked
    as input_text).
    """
    out: list[dict] = []

    for msg in messages:
        if not isinstance(msg, dict):
            out.append({"role": "user", "content": [{"type": "input_text", "text": str(msg)}]})
            continue

        role = (msg.get("role") or "user").lower()
        content = msg.get("content", "")

        # Choose the default part type by role
        default_type = "output_text" if role == "assistant" else "input_text"

        def coerce_part(part) -> dict:
            # Turn any part-ish value into a {"type": ..., "text": ...} dict
            if isinstance(part, str):
                return {"type": default_type, "text": part}
            if isinstance(part, dict):
                p = dict(part)
                p_type = p.get("type") or default_type
                if isinstance(p_type, str) and p_type.startswith("input_") and p_type != "input_text":
                    p["type"] = p_type
                    return p
                # Normalize text field
                if "text" not in p and "content" in p and isinstance(p["content"], str):
                    p["text"] = p["content"]
                if "text" not in p:
                    # Best-effort stringify
                    p["text"] = str(p.get("value", p))
                # Normalize type field
                p_type = p.get("type") or default_type
                # Fix obvious mismatch: assistant content must be output_text
                if role == "assistant" and p_type == "input_text":
                    p_type = "output_text"
                # Fix opposite mismatch for user/system
                if role != "assistant" and p_type == "output_text":
                    p_type = "input_text"
                p["type"] = p_type
                return p
            return {"type": default_type, "text": str(part)}

        # Convert content to list of parts
        if isinstance(content, list):
            parts = [coerce_part(p) for p in content]
        elif isinstance(content, dict):
            parts = [coerce_part(content)]
        elif isinstance(content, str):
            parts = [{"type": default_type, "text": content}]
        else:
            parts = [{"type": default_type, "text": str(content)}]

        out.append({**msg, "role": role, "content": parts})

    return out
# NOTE: this will work for all databricks models OTHER than GPT-OSS, which uses a slightly different API
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])  # only use mlflow for trace processing
mlflow.openai.autolog()


async def init_mcp_server():
    return McpServer(
        url=f"{get_databricks_host_from_env()}/api/2.0/mcp/functions/system/ai",
        name="system.ai uc function mcp server",
    )


def create_coding_agent(mcp_server: McpServer) -> Agent:
    return Agent(
        name="code execution agent",
        instructions=(
            "You are a code execution agent. You can execute code and return the results. "
            "When a user asks to test connectivity, call the connectivity_check tool."
        ),
        model="databricks-gpt-5-2",
        mcp_servers=[mcp_server],
        tools=TOOL_REGISTRY,
    )


@invoke()
async def invoke(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    # Optionally use the user's workspace client for on-behalf-of authentication
    # user_workspace_client = get_user_workspace_client()
    async with await init_mcp_server() as mcp_server:
        agent = create_coding_agent(mcp_server)
        messages = [i.model_dump() for i in request.input]
        messages = _normalize_input_messages(messages)
        result = await Runner.run(agent, messages)
        return ResponsesAgentResponse(output=[item.to_input_item() for item in result.new_items])


@stream()
async def stream(request: ResponsesAgentRequest) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    # Optionally use the user's workspace client for on-behalf-of authentication
    # user_workspace_client = get_user_workspace_client()
    async with await init_mcp_server() as mcp_server:
        agent = create_coding_agent(mcp_server)
        messages = [i.model_dump() for i in request.input]
        messages = _normalize_input_messages(messages)
        result = Runner.run_streamed(agent, input=messages)

        async for event in process_agent_stream_events(result.stream_events()):
            yield event
