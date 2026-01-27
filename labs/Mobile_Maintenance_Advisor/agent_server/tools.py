from __future__ import annotations

from datetime import datetime, timezone

from agents import function_tool


@function_tool
def connectivity_check(note: str | None = None) -> str:
    """Confirm the agent can invoke tools and return a short response."""
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if note:
        return f"connectivity ok at {timestamp}; note={note}"
    return f"connectivity ok at {timestamp}"


@function_tool
def echo(text: str) -> str:
    """Simple echo tool; use as a template for new tools."""
    return text


TOOL_REGISTRY = [
    connectivity_check,
    echo,
]
