"use client";
import { useEffect, useMemo, useState } from "react";
import { sdk } from "@farcaster/miniapp-sdk";

type Item = {
  cast_hash: string;
  fid: number | null;
  text: string;
  channel: string | null;
  timestamp: string;
  likes: number;
  recasts: number;
  replies: number;
  score: number;
};
type Metric = "likes" | "replies" | "recasts";
type Range = "24h" | "today" | "yesterday" | "7d";

const METRIC_LABELS: Record<Metric, string> = {
  likes: "По лайкам",
  replies: "По реплаям",
  recasts: "По рекастам",
};
const RANGE_LABELS: Record<Range, string> = {
  "24h": "24 часа",
  "today": "Сегодня",
  "yesterday": "Вчера",
  "7d": "7 дней",
};

export default function Page() {
  const [metric, setMetric] = useState<Metric>("likes");
  const [range, setRange] = useState<Range>("24h");
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const perPage = 12;

  // Сообщаем Warpcast Mini App-контейнеру, что UI готов
  useEffect(() => {
    (async () => { try { await sdk.actions.ready(); } catch {} })();
  }, []);

  async function load(m: Metric, r: Range) {
    setLoading(true);
    try {
      const res = await fetch(`/api/top?metric=${m}&range=${r}`, { cache: "no-store" });
      const data = await res.json();
      setItems(Array.isArray(data.items) ? data.items : []);
      setPage(1);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(metric, range); }, [metric, range]);

  const groupedEntries = useMemo(() => {
    const map = new Map<string, Item[]>();
    for (const it of items) {
      const key = it.channel ?? "Без канала";
      const arr = map.get(key) ?? [];
      if (arr.length < 10) arr.push(it); // сервер уже отсортировал
      map.set(key, arr);
    }
    const entries = Array.from(map.entries()).sort((a, b) => {
      const [A] = a[1]; const [B] = b[1];
      const get = (x: Item) => metric === "likes" ? x.likes : metric === "recasts" ? x.recasts : x.replies;
      return (get(B) || 0) - (get(A) || 0);
    });
    return entries;
  }, [items, metric]);

  const totalTiles = groupedEntries.length;
  const totalPages = Math.max(1, Math.ceil(totalTiles / perPage));
  const pageEntries = groupedEntries.slice((page - 1) * perPage, page * perPage);

  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <h1 style={{ fontSize: 28, margin: 0 }}>Топ кастов</h1>
        <div style={{ display: "flex", gap: 8 }}>
          {(["likes","replies","recasts"] as Metric[]).map((m) => (
            <button key={m} onClick={() => setMetric(m)} style={chip(metric === m)}>
              {METRIC_LABELS[m]}
            </button>
          ))}
        </div>
      </header>

      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        {(["24h","today","yesterday","7d"] as Range[]).map(r => (
          <button key={r} onClick={() => setRange(r)} style={chip(range === r)}>
            {RANGE_LABELS[r]}
          </button>
        ))}
      </div>

      {loading && <div style={{ margin: "16px 0", color: "#6b7280" }}>Загружаю…</div>}

      {!loading && pageEntries.length === 0 && (
        <div style={{ marginTop: 24, color: "#6b7280" }}>
          Пусто для выбранного диапазона. Открой <code>/api/ingest</code> или дождись крона.
        </div>
      )}

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", gap: 16 }}>
        {pageEntries.map(([channel, list]) => (
          <article key={channel} style={{
            border: "1px solid #e5e7eb", borderRadius: 16, padding: 16, background: "#fff",
            boxShadow: "0 1px 2px rgba(0,0,0,0.04)"
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 8 }}>
              <h2 style={{ fontSize: 18, margin: 0, lineHeight: 1.2 }}>
                {channel === "Без канала" ? channel : `#${channel}`}
              </h2>
              <small style={{ color: "#6b7280" }}>
                ТОП-10 · {METRIC_LABELS[metric].toLowerCase()} · {RANGE_LABELS[range]}
              </small>
            </div>

            <ol style={{ margin: 0, paddingLeft: 20 }}>
              {list.map((it, idx) => (
                <li key={it.cast_hash} style={{ marginBottom: 10 }}>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                    <span style={{ color: "#6b7280", minWidth: 18 }}>{idx + 1}.</span>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 14, whiteSpace: "pre-wrap" }}>
                        {truncate(it.text || "(без текста)", 140)}
                      </div>
                      <div style={{ display: "flex", gap: 10, fontSize: 12, color: "#6b7280", marginTop: 4 }}>
                        <span>fid:{it.fid ?? "?"}</span>
                        <span>💬 {it.replies}</span>
                        <span>🔁 {it.recasts}</span>
                        <span>❤️ {it.likes}</span>
                        <a href={`https://warpcast.com/~/conversations/${it.cast_hash}`} target="_blank" rel="noreferrer" style={{ textDecoration: "none" }}>
                          Открыть ↗
                        </a>
                      </div>
                    </div>
                  </div>
                </li>
              ))}
            </ol>
          </article>
        ))}
      </section>

      {totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", gap: 8, marginTop: 16 }}>
          <button disabled={page <= 1} onClick={() => setPage(p => Math.max(1, p - 1))} style={pagerBtn(page <= 1)}>← Назад</button>
          <span style={{ alignSelf: "center", color: "#6b7280" }}>{page} / {totalPages}</span>
          <button disabled={page >= totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))} style={pagerBtn(page >= totalPages)}>Вперёд →</button>
        </div>
      )}
    </main>
  );
}

function truncate(s: string, n: number) { return s.length <= n ? s : s.slice(0, n - 1) + "…"; }
function chip(active: boolean): React.CSSProperties {
  return {
    padding: "8px 12px", borderRadius: 999, border: "1px solid #e5e7eb",
    background: active ? "#111827" : "#fff", color: active ? "#fff" : "#111827",
    cursor: "pointer", fontWeight: 600
  };
}
function pagerBtn(disabled: boolean): React.CSSProperties {
  return {
    padding: "8px 12px", borderRadius: 8, border: "1px solid #e5e7eb",
    background: disabled ? "#f3f4f6" : "#fff", color: "#111827", cursor: disabled ? "not-allowed" : "pointer"
  };
}

