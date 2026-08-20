import { useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Chart, { ResultTable } from './Chart';

type Role = 'user' | 'assistant';
interface Message {
  role: Role;
  content: string;
  chart?: ResultTable | null;
}

const SEEDED = [
  'Which corridors have unresolved encroachments this quarter, and which are near critical assets?',
  'Which client has the slowest average time from detection to work-order close?',
  'Show the trend of vegetation encroachments by month over the last year.',
];

export default function App() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [messages, busy]);

  async function send(text: string) {
    const q = text.trim();
    if (!q || busy) return;
    setError(null);
    setInput('');

    const next = [...messages, { role: 'user' as Role, content: q }];
    setMessages(next);
    setBusy(true);

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: next,
          genie_conversation_id: conversationId || undefined,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);

      if (data.genie_conversation_id) setConversationId(data.genie_conversation_id);
      const chart: ResultTable | null = data.custom_outputs?.result_table ?? null;
      setMessages((m) => [...m, { role: 'assistant', content: data.content, chart }]);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }

  function newConversation() {
    setMessages([]);
    setConversationId(null);
    setError(null);
    setInput('');
  }

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <h1>Grid Corridor Intelligence</h1>
          <p className="sub">Governed corridor Q&amp;A, grounded in Unity Catalog.</p>
        </div>
        <button className="ghost" onClick={newConversation} disabled={busy}>
          New conversation
        </button>
      </header>

      <div className="transcript" ref={scrollRef}>
        {messages.length === 0 && (
          <div className="empty">
            <p>Ask about corridors, encroachments, work orders, or inspections.</p>
            <div className="chips">
              {SEEDED.map((q) => (
                <button key={q} className="chip" onClick={() => send(q)} disabled={busy}>
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`bubble ${m.role}`}>
            <div className="who">{m.role === 'user' ? 'You' : 'Corridor Genie'}</div>
            {m.role === 'assistant' && m.chart && <Chart data={m.chart} />}
            <div className="md">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.content}</ReactMarkdown>
            </div>
          </div>
        ))}

        {busy && (
          <div className="bubble assistant">
            <div className="who">Corridor Genie</div>
            <div className="typing">
              <span></span>
              <span></span>
              <span></span>
            </div>
          </div>
        )}

        {error && <div className="error">{error}</div>}
      </div>

      <form
        className="composer"
        onSubmit={(e) => {
          e.preventDefault();
          send(input);
        }}
      >
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask a question…"
          disabled={busy}
          autoFocus
        />
        <button type="submit" disabled={busy || !input.trim()}>
          Send
        </button>
      </form>

      <footer className="foot">
        Every question is traced — see the MLflow experiment / inference table.
      </footer>
    </div>
  );
}
