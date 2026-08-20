// Auto-visualization for a Genie result table. Follows the dataviz form heuristic:
//   temporal + numeric  -> line (trend over time)
//   category + numeric   -> horizontal bar (magnitude, low->high; horizontal for long names)
//   single value         -> stat number
//   otherwise            -> nothing (the markdown table already shows the data)
// Single series throughout, so one sequential-blue hue and no legend. Hand-rolled
// SVG (no chart lib) so the app keeps zero runtime deps.

export interface ResultTable {
  columns: string[];
  rows: (string | number | null)[][];
  row_count?: number;
  truncated?: boolean;
}

const SERIES = '#3987e5'; // dataviz sequential-blue, dark-surface step
const MAX_BARS = 15;

// --- column typing -----------------------------------------------------------
function asNumber(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : Number(String(v).replace(/,/g, ''));
  return Number.isFinite(n) ? n : null;
}
const TEMPORAL_NAME = /(^|_)(month|date|day|year|quarter|week|period|time)s?($|_)/i;
const MONTH_WORD = /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i;

function isTemporal(name: string, vals: (string | number | null)[]): boolean {
  if (TEMPORAL_NAME.test(name)) return true;
  const nonNull = vals.filter((v) => v !== null && v !== '');
  if (!nonNull.length) return false;
  return nonNull.every(
    (v) => /^\d{4}(-\d{1,2}){0,2}$/.test(String(v)) || MONTH_WORD.test(String(v))
  );
}
function isNumeric(vals: (string | number | null)[]): boolean {
  const nonNull = vals.filter((v) => v !== null && v !== '');
  return nonNull.length > 0 && nonNull.every((v) => asNumber(v) !== null);
}

interface Plan {
  kind: 'line' | 'bar' | 'stat' | 'none';
  labelCol?: number;
  valueCol?: number;
  title?: string;
}

function plan(t: ResultTable): Plan {
  if (!t || !t.columns || !t.rows || t.rows.length === 0) return { kind: 'none' };
  const cols = t.columns;
  const colVals = cols.map((_, i) => t.rows.map((r) => r[i]));
  const numeric = cols.map((_, i) => isNumeric(colVals[i]));
  const temporal = cols.map((c, i) => !numeric[i] && isTemporal(c, colVals[i]));

  // measure = first numeric column that isn't obviously an id/year
  const valueCol = cols.findIndex(
    (c, i) => numeric[i] && !/(^|_)(id|year)($|_)/i.test(c)
  );
  const valueColFinal = valueCol >= 0 ? valueCol : numeric.findIndex(Boolean);

  const tCol = temporal.findIndex(Boolean);
  // dimension: prefer a name-ish column, else first non-numeric, non-temporal
  const named = cols.findIndex(
    (c, i) => !numeric[i] && !temporal[i] && /name|title|label/i.test(c)
  );
  const catCol =
    named >= 0 ? named : cols.findIndex((_, i) => !numeric[i] && !temporal[i]);

  if (valueColFinal < 0) return { kind: 'none' };
  const h = (s: string) => s.replace(/_/g, ' ');
  const measure = h(cols[valueColFinal]);
  const generic = (c: number) => /^(name|label|title)$/i.test(cols[c] || '');

  // A single row is a headline number, not a one-bar chart.
  if (t.rows.length === 1) {
    return { kind: 'stat', valueCol: valueColFinal, labelCol: catCol >= 0 ? catCol : undefined, title: measure };
  }
  if (tCol >= 0) {
    return { kind: 'line', labelCol: tCol, valueCol: valueColFinal, title: `${measure} over ${h(cols[tCol])}` };
  }
  if (catCol >= 0) {
    return {
      kind: 'bar', labelCol: catCol, valueCol: valueColFinal,
      title: generic(catCol) ? measure : `${measure} by ${h(cols[catCol])}`,
    };
  }
  return { kind: 'none' };
}

// ISO timestamp / date -> compact YYYY-MM for axis labels; other labels pass through.
function fmtLabel(s: string): string {
  const m = /^(\d{4})-(\d{2})/.exec(s);
  return m ? `${m[1]}-${m[2]}` : s;
}

// --- formatting --------------------------------------------------------------
function fmt(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString();
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
function truncate(s: string, max = 22): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

// --- charts ------------------------------------------------------------------
function BarChart({ t, p }: { t: ResultTable; p: Plan }) {
  const rows = t.rows
    .map((r) => ({ label: String(r[p.labelCol!] ?? ''), value: asNumber(r[p.valueCol!]) ?? 0 }))
    .sort((a, b) => b.value - a.value)
    .slice(0, MAX_BARS);
  const hidden = t.rows.length - rows.length;

  const W = 680, rowH = 26, padTop = 8, padBottom = 8, labelW = 168, valueW = 64;
  const H = padTop + padBottom + rows.length * rowH;
  const plotW = W - labelW - valueW;
  const max = Math.max(1, ...rows.map((r) => r.value));

  return (
    <svg className="viz-svg" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={p.title}>
      {rows.map((r, i) => {
        const y = padTop + i * rowH;
        const bh = rowH - 8;
        const w = Math.max(2, (r.value / max) * plotW);
        return (
          <g key={i}>
            <text className="viz-cat" x={labelW - 8} y={y + bh / 2} dominantBaseline="middle" textAnchor="end">
              {truncate(r.label)}
              <title>{r.label}</title>
            </text>
            <rect x={labelW} y={y} width={w} height={bh} rx={4} fill={SERIES}>
              <title>{`${r.label}: ${fmt(r.value)}`}</title>
            </rect>
            <text className="viz-val" x={labelW + w + 6} y={y + bh / 2} dominantBaseline="middle">
              {fmt(r.value)}
            </text>
          </g>
        );
      })}
      {hidden > 0 && (
        <text className="viz-note" x={labelW} y={H - 0} dominantBaseline="ideographic">
          +{hidden} more (see table)
        </text>
      )}
    </svg>
  );
}

function LineChart({ t, p }: { t: ResultTable; p: Plan }) {
  const pts = t.rows.map((r) => ({ label: String(r[p.labelCol!] ?? ''), value: asNumber(r[p.valueCol!]) ?? 0 }));
  const W = 680, H = 300, mL = 44, mR = 16, mT = 12, mB = 40;
  const plotW = W - mL - mR, plotH = H - mT - mB;
  const max = Math.max(1, ...pts.map((p) => p.value));
  const n = pts.length;
  const x = (i: number) => mL + (n <= 1 ? plotW / 2 : (i / (n - 1)) * plotW);
  const y = (v: number) => mT + plotH - (v / max) * plotH;

  const line = pts.map((pt, i) => `${i === 0 ? 'M' : 'L'} ${x(i).toFixed(1)} ${y(pt.value).toFixed(1)}`).join(' ');
  const area = `${line} L ${x(n - 1).toFixed(1)} ${mT + plotH} L ${x(0).toFixed(1)} ${mT + plotH} Z`;
  const ticks = 4;
  const everyLabel = Math.ceil(n / 12); // avoid crowding the x-axis

  return (
    <svg className="viz-svg" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={p.title}>
      {Array.from({ length: ticks + 1 }, (_, i) => {
        const gv = (max / ticks) * i;
        const gy = y(gv);
        return (
          <g key={i}>
            <line className="viz-grid" x1={mL} y1={gy} x2={W - mR} y2={gy} />
            <text className="viz-val" x={mL - 6} y={gy} dominantBaseline="middle" textAnchor="end">{fmt(gv)}</text>
          </g>
        );
      })}
      <path d={area} fill={SERIES} fillOpacity={0.14} />
      <path d={line} fill="none" stroke={SERIES} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
      {pts.map((pt, i) => (
        <g key={i}>
          <circle cx={x(i)} cy={y(pt.value)} r={4} fill={SERIES}>
            <title>{`${fmtLabel(pt.label)}: ${fmt(pt.value)}`}</title>
          </circle>
          {i % everyLabel === 0 && (
            <text className="viz-cat" x={x(i)} y={H - mB + 16} textAnchor="middle">{fmtLabel(pt.label)}</text>
          )}
        </g>
      ))}
    </svg>
  );
}

function StatTile({ t, p }: { t: ResultTable; p: Plan }) {
  const v = asNumber(t.rows[0][p.valueCol!]);
  const ctx = p.labelCol !== undefined ? String(t.rows[0][p.labelCol] ?? '') : '';
  return (
    <div className="viz-stat">
      <div className="viz-stat-num">{v === null ? '—' : fmt(v)}</div>
      <div className="viz-stat-label">{p.title}{ctx ? ` · ${ctx}` : ''}</div>
    </div>
  );
}

export default function Chart({ data }: { data?: ResultTable | null }) {
  if (!data || (data as any).error) return null;
  const p = plan(data);
  if (p.kind === 'none') return null;
  return (
    <figure className="viz">
      <figcaption className="viz-title">{p.title}</figcaption>
      {p.kind === 'bar' && <BarChart t={data} p={p} />}
      {p.kind === 'line' && <LineChart t={data} p={p} />}
      {p.kind === 'stat' && <StatTile t={data} p={p} />}
    </figure>
  );
}
