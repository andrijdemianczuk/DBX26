from dotenv import load_dotenv
from fastapi import HTTPException
from pydantic import BaseModel
from mlflow.genai.agent_server import AgentServer, setup_mlflow_git_based_version_tracking

# Load env vars from .env.local before importing the agent for proper auth
load_dotenv(dotenv_path=".env.local", override=True)

# Need to import the agent to register the functions with the server
import agent_server.agent  # noqa: E402
from agent_server.tools import _transcribe_audio_impl  # noqa: E402

agent_server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
# Define the app as a module level variable to enable multiple workers
app = agent_server.app  # noqa: F841
setup_mlflow_git_based_version_tracking()


class TranscribeRequest(BaseModel):
    audio_b64: str
    audio_format: str


class TranscribeResponse(BaseModel):
    text: str


@app.post("/transcribe", response_model=TranscribeResponse)
async def transcribe(request: TranscribeRequest) -> TranscribeResponse:
    try:
        text = _transcribe_audio_impl(request.audio_b64, request.audio_format)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return TranscribeResponse(text=text)


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
