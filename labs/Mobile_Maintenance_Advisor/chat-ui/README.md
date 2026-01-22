# Mobile Maintenance Advisor - Custom Chat UI

This folder contains a self-contained Node + React chat UI:

- **React** UI (Vite) in `src/`
- **Node/Express** server in `server/`
- Backend proxy route: `POST /api/chat` -> `${API_PROXY}` (defaults to `http://localhost:8000/invocations`)

## Local dev

From repo root:

```bash
cd chat-ui
npm install
# in another terminal: uv run start-server --reload --port 8000
npm run dev
```

In Databricks Apps, `uv run start-app` will run the agent server on port 8000
and the UI on `CHAT_APP_PORT` (default 3000).
