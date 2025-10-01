"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { sdk } from "@farcaster/miniapp-sdk";

type Cast = {
  cast_hash: string;
  fid: number;
  text: string;
  channel: string | null;
  timestamp: string;
  likes: number;
  recasts: number;
  replies: number;
  score: number;
  username?: string | null;
  display_name?: string | null;
  pfp_url?: string | null;
};

const METRICS = [
  { key: "likes", label: "By likes" },
  { key: "replies", label: "By replies" },
  { key: "recasts", label: "By recasts" },
] as const;

const RANGES = [
  { key: "24h", label: "24 hours" },
  { key: "today", label: "Today" },
  { key: "yesterday", label: "Yesterday" },
  { key: "7d", label: "7 days" },
] as const;

// Correct Warpcast cast URL
function makeCastUrl(hash: string, username?: string | null) {
  if (username && username.trim()) return `https://warpcast.com/${username}/${hash}`;
  return `https://warpcast.com/~/cast/${hash}`; // NOTE: /~/cast/, not /~/casts/
}

export default function Page() {
  // filters
  const [metric, setMetric] =
    useState<(typeof METRICS)[number]["key"]>("replies");
  const [range, setRange] =
    useState<(typeof RANGES)[number]["key"]>("24h");

  // data
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<Cast[]>([]);
  const [error, setError] = useState<string | null>(null);

  // mini app state
  const [isMiniApp, setIsMiniApp] = useState(false);
  const [isAdded, setIsAdded] = useState<boolean>(true); // assume added on web
  const [debugInfo, setDebugInfo] = useState<Record<string, any> | null>(null);
  const lastCtxRef = useRef<any>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const url = `/api/top?metric=${metric}&range=${range}&limit=15`;
      const r = await fetch(url, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setItems(Array.isArray(data.items) ? data.items : []);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  // Init Mini App SDK and robust “added” detection — БЕЗ cleanup()
  useEffect(() => {
    let cancelled = false;
    let timers: Array<ReturnType<typeof setTimeout>> = [];
    let off: undefined | (() => void);

    (async () => {
      try {
        const url =
          typeof window !== "undefined"
            ? new URL(window.location.href)
            : new URL("https://example.com");
        const forcePanel = url.searchParams.get("debugAdd") === "1";
        const wantDebug = url.searchParams.get("debug") === "1";

        const inMini = await sdk.isInMiniApp();
        if (cancelled) return;
        setIsMiniApp(inMini);

        if (!inMini) {
          setIsAdded(!forcePanel);
          if (wantDebug) setDebugInfo({ inMini, reason: "not in mini app" });
          return;
        }

        await sdk.actions.ready();

        const computeAdded = (ctx: any) => {
          if (!ctx) return undefined;
          const v =
            ctx?.client?.added ??
            ctx?.miniApp?.added ??
            (Array.isArray(ctx?.client?.apps)
              ? ctx.client.apps.some((a: any) => a?.id && a?.added)
              : undefined);
          return typeof v === "boolean" ? v : undefined;
        };

        let ctx: any;
        if (typeof (sdk as any).getContext === "function") {
          ctx = await (sdk as any).getContext();
        } else {
          ctx = (sdk as any).context;
        }
        lastCtxRef.current = ctx;

        let added = computeAdded(ctx);
        if (forcePanel) added = false;
        if (added === undefined) setIsAdded(false);
        else setIsAdded(!!added);

        if (wantDebug) {
          setDebugInfo({
            inMini,
            initialAdded: added,
            initialCtx: ctx,
          });
        }

        const onContextUpdate = async () => {
          try {
            const c =
              typeof (sdk as any).getContext === "function"
                ? await (sdk as any).getContext()
                : (sdk as any).context;
            lastCtxRef.current = c;
            const a = computeAdded(c);
            if (a !== undefined) setIsAdded(!!a);
            if (wantDebug) {
              setDebugInfo((d) => ({
                ...(d || {}),
                updatedAdded: a,
                updatedCtx: c,
              }));
            }
          } catch {}
        };

        // подписка на события, если SDK их даёт
        off = (sdk as any)?.events?.on?.("context", onContextUpdate);

        // страховочный поллинг
        [500, 1200, 2500].forEach((ms) => {
          const t = setTimeout(onContextUpdate, ms);
          timers.push(t);
        });
      } catch (e) {
        setIsMiniApp(false);
        setIsAdded(true);
        const wantDebug =
          typeof window !== "undefined" &&
          new URL(window.location.href).searchParams.get("debug") === "1";
        if (wantDebug) setDebugInfo({ error: String(e) });
      }
    })();

    return () => {
      cancelled = true;
      timers.forEach(clearTimeout);
      if (typeof off === "function") off();
    };
  }, []);

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metric, range]);

  const title = useMemo(() => {
    const m = METRICS.find((m) => m.key === metric)?.label ?? "";
    const r = RANGES.find((r) => r.key === range)?.label ?? "";
    return `Top casts · ${m.toLowerCase()} · ${r}`;
  }, [metric, range]);

  const handleAddMiniApp = async () => {
    try {
      await sdk.actions.addMiniApp();
      setIsAdded(true);
    } catch (e) {
      console.warn("addMiniApp failed:", e);
    }
  };

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 pb-24">
      <h1 className="text-2xl font-semibold mb-4">Top casts</h1>

      {/* range */}
      <div className="flex gap-2 mb-3 flex-wrap">
        {RANGES.map((r) => (
          <button
            key={r.key}
            onClick={() => setRange(r.key)}
            className={`px-3 py-1.5 rounded-full text-sm border ${
              range === r.key ? "bg-black text-white border-black" : "bg-white hover:bg-gray-50"
            }`}
          >
            {r.label}
          </button>
        ))}
      </div>

      {/* metric */}
      <div className="flex gap-2 mb-6 flex-wrap">
        {METRICS.map((m) => (
          <button
            key={m.key}
            onClick={() => setMetric(m.key)}
            className={`px-3 py-1.5 rounded-full text-sm border ${
              metric === m.key ? "bg-black text-white border-black" : "bg-white hover:bg-gray-50"
            }`}
          >
            {m.label}
          </button>
        ))}
      </div>

      <div className="text-sm text-gray-500 mb-4">{title}</div>

      {error && <div className="text-red-600 mb-4">Error: {error}</div>}
      {loading && <div className="mb-4">Loading…</div>}

      {/* equal-height cards grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {items.map((c, idx) => (
          <article
            key={c.cast_hash}
            className="h-full rounded-2xl border bg-white shadow-sm p-4 flex flex-col"
          >
            {/* top */}
            <header className="mb-3 flex items-center justify-between">
              <div className="text-xs text-gray-500">#{idx + 1}</div>

              <div className="flex items-center gap-2 min-w-0">
                {/* avatar */}
                {c.pfp_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img
                    src={c.pfp_url}
                    alt=""
                    className="w-6 h-6 rounded-full object-cover"
                    referrerPolicy="no-referrer"
                  />
                ) : (
                  <div className="w-6 h-6 rounded-full bg-gray-200" />
                )}

                {/* name/handle */}
                <a
                  className="text-sm font-medium hover:underline truncate"
                  href={`https://warpcast.com/~/profiles/${c.fid}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  title={`fid:${c.fid}`}
                >
                  {c.display_name || (c.username ? `@${c.username}` : `fid:${c.fid}`)}
                </a>

                {/* channel */}
                {c.channel ? (
                  <span className="ml-1 shrink-0 text-xs bg-gray-100 px-2 py-0.5 rounded-full">#{c.channel}</span>
                ) : (
                  <span className="ml-1 shrink-0 text-xs text-gray-400">no channel</span>
                )}
              </div>
            </header>

            {/* text */}
            <p className="whitespace-pre-wrap text-sm leading-5 line-clamp-6 text-gray-900">
              {c.text}
            </p>

            {/* time */}
            <div className="mt-3 text-xs text-gray-600">
              <time title={new Date(c.timestamp).toLocaleString()}>
                {new Date(c.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
              </time>
            </div>

            {/* spacer to push footer to bottom */}
            <div className="mt-auto" />

            {/* bottom: stats + link */}
            <div className="pt-3 flex items-center justify-between">
              <div className="flex items-center gap-3 text-sm">
                <Badge label="💬" value={c.replies} active={metric === "replies"} />
                <Badge label="❤️" value={c.likes} active={metric === "likes"} />
                <Badge label="🔁" value={c.recasts} active={metric === "recasts"} />
              </div>

              <a
                className="text-blue-600 hover:underline text-sm shrink-0"
                href={makeCastUrl(c.cast_hash, c.username)}
                target="_blank"
                rel="noopener noreferrer"
              >
                Open ↗
              </a>
            </div>
          </article>
        ))}
      </div>

      {!loading && items.length === 0 && !error && (
        <div className="text-gray-500 mt-6">Nothing here yet. Try again later.</div>
      )}

      {/* Bottom prompt — only in Mini App and when not yet added */}
      {isMiniApp && !isAdded && (
        <div className="fixed inset-x-0 bottom-0 z-50">
          <div className="mx-auto max-w-6xl px-4 pb-[max(env(safe-area-inset-bottom),1rem)] pt-3">
            <div className="bg-white border shadow-lg rounded-xl p-3 flex items-center justify-between">
              <div className="text-sm">
                <div className="font-medium">Add this Mini App</div>
                <div className="text-gray-500">Save to your Apps for quick access.</div>
              </div>
              <button
                onClick={handleAddMiniApp}
                className="px-3 py-2 rounded-lg bg-black text-white text-sm"
              >
                Add to my apps
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Optional: debug overlay (?debug=1) */}
      {debugInfo && (
        <pre className="fixed bottom-2 left-2 right-2 max-h-[40vh] overflow-auto bg-black/80 text-green-200 text-[11px] p-2 rounded">
          {JSON.stringify(debugInfo, null, 2)}
        </pre>
      )}
    </div>
  );
}

function Badge({ label, value, active }: { label: string; value: number; active?: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-1 rounded-full border text-xs ${
        active ? "bg-black text-white border-black" : "bg-gray-50"
      }`}
    >
      <span>{label}</span>
      <span className="tabular-nums">{value ?? 0}</span>
    </span>
  );
}
