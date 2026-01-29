from __future__ import annotations

import base64
import io
import os
from datetime import datetime, timezone

from agents import function_tool
from openai import OpenAI


@function_tool
def connectivity_check(note: str | None = None) -> str:
    """Confirm the agent can invoke tools and return a short response."""
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if note:
        return f"connectivity ok at {timestamp}; note={note}"
    return f"connectivity ok at {timestamp}"


@function_tool
def echo(text: str | None = None) -> str:
    """Simple echo tool; use as a template for new tools."""
    return text


def _transcribe_audio_impl(audio_b64: str, audio_format: str) -> str:
    """Transcribe base64-encoded audio using gpt-4o-transcribe."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENAI_API_KEY in environment.")

    audio_bytes = base64.b64decode(audio_b64)
    audio_file = io.BytesIO(audio_bytes)
    audio_file.name = f"audio.{audio_format}"

    client = OpenAI(api_key=api_key)
    transcript = client.audio.transcriptions.create(
        model="gpt-4o-transcribe",
        file=audio_file,
    )
    return transcript.text


@function_tool
def transcribe_audio(audio_b64: str, audio_format: str) -> str:
    """Transcribe base64-encoded audio using gpt-4o-transcribe."""
    return _transcribe_audio_impl(audio_b64, audio_format)


TOOL_REGISTRY = [
    connectivity_check,
    echo,
    transcribe_audio,
]
