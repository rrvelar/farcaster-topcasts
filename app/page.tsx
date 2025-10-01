"use client";

import { useCallback, useEffect, useMemo, useRef, useState, ReactNode } from "react";
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

// URLs
function makeCastUrl(hash: string, username?: string | null) {
  if (username && username.trim()) return `https://warpcast.com/${username}/${hash}`;
  return `https://warpcast.com/~/cast/${hash}`;
}
function makeProfileUrl(fid: number, handle?: string) {
  if (handle && handle.trim()) return `https://warpcast.com/${handle}`;
  return `https://warpcast.com/~/profiles/${fid}`;
}

// Promo account
const PROMO_FID = Number(process.env.NEXT_PUBLIC_PROMO_FID || "0");
const PROMO_HANDLE = (process.env.NEXT_PUBLIC_PROMO_HANDLE || "").trim();

// ----- localStorage helpers (per viewer fid) -----
const addedKey = (fid: number | null) => `fc_added_${fid ?? "anon"}`;
const followKey = (viewer: number, promo: number) => `fc_follow_${viewer}_${promo}`;

const readAddedCache = (fid: number | null) => {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(addedKey(fid)) === "1";
  } catch { return false; }
};
const writeAddedCache = (fid: number | null, val = true) => {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(addedKey(fid), val ? "1" : "0");
  } catch {}
};

/* ===========================
   Smart hover (auto-flip)
   =========================== */

type TooltipPos = { left: number; top: number; side: "right" | "left" };

function useSmartTooltip() {
  const compute = useCallback((anchor: HTMLElement, tooltipWidth = 300): TooltipPos => {
    const r = anchor.getBoundingClientRect();
    const gap = 10;
    const preferredLeft = r.right + gap;
    const fallbackLeft  = r.left - tooltipWidth - gap;
    const fitsRight = preferredLeft + tooltipWidth <= window.innerWidth - 8;
    const left = fitsRight ? preferredLeft : Math.max(8, fallbackLeft);
    const midY = r.top + r.height / 2;
    const top = Math.max(8, Math.min(midY - 72, window.innerHeight - 168));
    return { left, top, side: fitsRight ? "right" : "left" };
  }, []);
  return { compute };
}

function AuthorHoverWrap({
  c,
  children,
}: {
  c: { fid: number; username?: string | null; display_name?: string | null; pfp_url?: string | null };
  children: ReactNode;
}) {
  const anchorRef = useRef<HTMLDivElement | null>(null);
  const { compute } = useSmartTooltip();
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<TooltipPos | null>(null);
  const hideTimer = useRef<any>(null);

  const show = () => {
    const el = anchorRef.current;
    if (!el) return;
    if (hideTimer.current) clearTimeout(hideTimer.current);
    setPos(compute(el, 300));
    setOpen(true);
  };
  const hide = (delay = 0) => {
    if (hideTimer.current) clearTimeout(hideTimer.current);
    hideTimer.current = setTimeout(() => setOpen(false), delay);
  };

  const onClick = (e: React.MouseEvent) => {
    // не трогаем клики по ссылкам внутри
    const target = e.target as HTMLElement;
    if (target.closest("a")) return;
    setOpen((v) => {
      if (v) return false;
      show();
      return true;
    });
  };

  return (
    <>
      <div
        ref={anchorRef}
        className="flex items-center gap-2 min-w-0"
        onMouseEnter={show}
        onMouseLeave={() => hide(120)}
        onClick={onClick}
        onTouchStart={show}
        onTouchEnd={() => hide(200)}
      >
        {children}
      </div>

      {open && pos && (
        <div
          style={{ left: pos.left, top: pos.top, position: "fixed" }}
          className="z-50 w-[300px] rounded-2xl border border-zinc-200 bg-white/95 shadow-lg ring-1 ring-black/5 p-4 pointer-events-none backdrop-blur-sm"
          aria-hidden
        >
          <div className="flex items-center gap-3">
            {c.pfp_url ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={c.pfp_url}
                alt=""
                className="w-14 h-14 rounded-xl object-cover ring-1 ring-zinc-200"
                referrerPolicy="no-referrer"
              />
            ) : (
              <div className="w-14 h-14 rounded-xl bg-zinc-200" />
            )}
            <div className="min-w-0">
              <div className="font-medium truncate text-zinc-900">
                {c.display_name || "User"}
              </div>
              {c.username && (
                <div className="text-sm text-zinc-500 truncate">@{c.username}</div>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}

/* ===========================
   Page
   =========================== */

export default function Page() {
  // стартуем By likes
  const [metric, setMetric] = useState<(typeof METRICS)[number]["key"]>("likes");
  const [range, setRange]   = useState<(typeof RANGES)[number]["key"]>("24h");

  // data
  const [loading, setLoading] = useState(false);
  const [items, setItems]     = useState<Cast[]>([]);
  const [error, setError]     = useState<string | null>(null);

  // mini app state
  const [isMiniApp, setIsMiniApp]   = useState(false);
  const [isAdded, setIsAdded]       = useState<boolean>(true); // web => true
  const [viewerFid, setViewerFid]   = useState<number | null>(null);

  // follow gate
  const [followConfirmed, setFollowConfirmed] = useState<boolean>(false);
  const [followChecking, setFollowChecking]   = useState<boolean>(false);

  const lastCtxRef = useRef<any>(null);
  const [debugInfo, setDebugInfo] = useState<Record<string, any> | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`/api/top?metric=${metric}&range=${range}&limit=15`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setItems(Array.isArray(data.items) ? data.items : []);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  // Init & context
  useEffect(() => {
    let timers: Array<ReturnType<typeof setTimeout>> = [];
    let off: undefined | (() => void);

    (async () => {
      try {
        const url =
          typeof window !== "undefined"
            ? new URL(window.location.href)
            : new URL("https://example.com");
        const wantDebug = url.searchParams.get("debug") === "1";

        const inMini = await sdk.isInMiniApp();
        setIsMiniApp(inMini);

        if (!inMini) {
          setIsAdded(true);
          setViewerFid(null);
          setFollowConfirmed(true);
          if (wantDebug) setDebugInfo({ inMini, reason: "not in mini app" });
          return;
        }

        await sdk.actions.ready();

        const compute = (ctx: any) => {
          const added =
            ctx?.client?.added ??
            ctx?.miniApp?.added ??
            (Array.isArray(ctx?.client?.apps)
              ? ctx.client.apps.some((a: any) => a?.id && a?.added)
              : undefined);
          const fid =
            ctx?.viewer?.fid ??
            ctx?.user?.fid ??
            ctx?.client?.viewer?.fid ??
            null;
          return { added: typeof added === "boolean" ? added : undefined, fid };
        };

        const getCtx = async () =>
          typeof (sdk as any).getContext === "function"
            ? await (sdk as any).getContext()
            : (sdk as any).context;

        // initial context
        let ctx = await getCtx();
        lastCtxRef.current = ctx;
        let { added, fid } = compute(ctx);
        setViewerFid(typeof fid === "number" ? fid : null);

        // fallback к локальному кэшу
        const cachedAdded = readAddedCache(typeof fid === "number" ? fid : null);
        if (added === undefined || added === false) {
          if (cachedAdded) {
            added = true;
          }
        }
        if (added !== undefined) setIsAdded(!!added);

        // follow cache
        if (typeof fid === "number") {
          if (fid === PROMO_FID) {
            setFollowConfirmed(true);
          } else {
            const cachedFollow =
              typeof window !== "undefined" &&
              window.localStorage.getItem(followKey(fid, PROMO_FID)) === "1";
            setFollowConfirmed(PROMO_FID ? cachedFollow : true);
          }
        } else {
          setFollowConfirmed(true);
        }

        if (wantDebug) {
          setDebugInfo({
            inMini,
            addedInitial: added,
            cachedAdded,
            viewerFidInitial: fid ?? null,
            promoFid: PROMO_FID,
            promoHandle: PROMO_HANDLE || null,
          });
        }

        // subscribe + rechecks
        const recheck = async () => {
          try {
            const c = await getCtx();
            lastCtxRef.current = c;
            const { added: a, fid: f } = compute(c);

            if (typeof f === "number" && f !== viewerFid) {
              setViewerFid(f);
              const addCache = readAddedCache(f);
              if (a === undefined || a === false) {
                setIsAdded(addCache);
              } else {
                setIsAdded(!!a);
              }
              if (f === PROMO_FID) {
                setFollowConfirmed(true);
              } else {
                const fl = typeof window !== "undefined" &&
                  window.localStorage.getItem(followKey(f, PROMO_FID)) === "1";
                setFollowConfirmed(PROMO_FID ? fl : true);
              }
            } else {
              const addCache = readAddedCache(viewerFid);
              if (a === undefined || a === false) {
                if (addCache) setIsAdded(true);
              } else {
                setIsAdded(!!a);
              }
            }
          } catch {}
        };

        off = (sdk as any)?.events?.on?.("context", recheck);
        [600, 1500, 3000].forEach((ms) => {
          const t = setTimeout(recheck, ms);
          timers.push(t);
        });
      } catch {
        setIsMiniApp(false);
        setIsAdded(true);
        setViewerFid(null);
        setFollowConfirmed(true);
      }
    })();

    return () => {
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

  // actions
  const handleAddMiniApp = async () => {
    try {
      await sdk.actions.addMiniApp();
      writeAddedCache(viewerFid, true);
      setIsAdded(true);
      setTimeout(async () => {
        try {
          const ctx =
            typeof (sdk as any).getContext === "function"
              ? await (sdk as any).getContext()
              : (sdk as any).context;
          const added =
            ctx?.client?.added ??
            ctx?.miniApp?.added ??
            (Array.isArray(ctx?.client?.apps)
              ? ctx.client.apps.some((a: any) => a?.id и a?.added)
              : undefined);
          if (added === true) setIsAdded(true);
        } catch {}
      }, 1200);
    } catch (e) {
      console.warn("addMiniApp failed:", e);
    }
  };

  const openUrl = async (url: string) => {
    try {
      if ((sdk as any)?.actions?.openURL) {
        await (sdk as any).actions.openURL(url);
      } else {
        window.open(url, "_blank", "noopener,noreferrer");
      }
    } catch {
      window.open(url, "_blank", "noopener,noreferrer");
    }
  };

  const handleFollow = async () => {
    if (!PROMO_FID) return;
    setFollowChecking(true);
    try {
      await openUrl(makeProfileUrl(PROMO_FID, PROMO_HANDLE));
      if (viewerFid && typeof window !== "undefined") {
        window.localStorage.setItem(followKey(viewerFid, PROMO_FID), "1");
      }
      setFollowConfirmed(true);
    } finally {
      setFollowChecking(false);
    }
  };

  // UI
  return (
    <div
      className={`
        mx-auto max-w-6xl px-4 py-6 pb-28
        bg-zinc-50
        bg-[radial-gradient(1200px_500px_at_50%_-200px,rgba(0,0,0,0.05),transparent)]
      `}
    >
      {/* Gate #1: Add app */}
      {isMiniApp && !isAdded && (
        <div className="fixed inset-0 z-[60] bg-white/80 backdrop-blur-sm">
          <div className="absolute inset-x-0 bottom-0 p-4 pb-[max(env(safe-area-inset-bottom),1rem)]">
            <div className="mx-auto max-w-xl rounded-2xl border bg-white shadow-xl p-5">
              <h2 className="text-lg font-semibold mb-1">Add “Top Casts” to My Apps</h2>
              <p className="text-sm text-zinc-600 mb-4">
                To continue, please add this mini app to your Warpcast apps.
              </p>
              <button
                onClick={handleAddMiniApp}
                className="w-full px-4 py-2 rounded-xl bg-black text-white text-sm hover:bg-zinc-900"
              >
                Add to My Apps
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Gate #2: Follow */}
      {isMiniApp && isAdded && PROMO_FID > 0 && viewerFid !== null && viewerFid !== PROMO_FID && !followConfirmed && (
        <div className="fixed inset-0 z-[50] bg-white/60 backdrop-blur-[2px]">
          <div className="absolute inset-x-0 bottom-0 p-4 pb-[max(env(safe-area-inset-bottom),1rem)]">
            <div className="mx-auto max-w-xl rounded-2xl border bg-white shadow-xl p-5">
              <h2 className="text-lg font-semibold mb-1">
                Follow {PROMO_HANDLE ? `@${PROMO_HANDLE}` : "our account"}
              </h2>
              <p className="text-sm text-zinc-600 mb-4">
                Please follow to access the app.
              </p>
              <button
                onClick={handleFollow}
                disabled={followChecking || !viewerFid}
                title={!viewerFid ? "Open inside Warpcast to continue" : undefined}
                className={`w-full px-4 py-2 rounded-xl text-sm transition
                  ${followChecking || !viewerFid
                    ? "bg-zinc-200 text-zinc-700 cursor-not-allowed"
                    : "bg-black text-white hover:bg-zinc-900"}`}
              >
                {followChecking
                  ? "Waiting for follow…"
                  : `Follow ${PROMO_HANDLE ? `@${PROMO_HANDLE}` : "in Warpcast"}`}
              </button>
            </div>
          </div>
        </div>
      )}

      <h1 className="text-4xl font-semibold tracking-tight mb-4 text-zinc-900">Top casts</h1>

      {/* filters */}
      <div className="flex gap-2 mb-3 flex-wrap">
        {RANGES.map((r) => (
          <button
            key={r.key}
            onClick={() => setRange(r.key)}
            className={`px-3 py-1.5 rounded-full text-sm border
              ${range === r.key ? "bg-black text-white border-black" : "bg-white hover:bg-zinc-50"}`}
          >
            {r.label}
          </button>
        ))}
      </div>

      <div className="flex gap-2 mb-6 flex-wrap">
        {METRICS.map((m) => (
          <button
            key={m.key}
            onClick={() => setMetric(m.key)}
            className={`px-3 py-1.5 rounded-full text-sm border
              ${metric === m.key ? "bg-black text-white border-black" : "bg-white hover:bg-zinc-50"}`}
          >
            {m.label}
          </button>
        ))}
      </div>

      <div className="text-sm text-zinc-500 mb-4">{title}</div>

      {error && <div className="text-red-600 mb-4">Error: {error}</div>}

      {/* Skeletons */}
      {loading && items.length === 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div
              key={i}
              className="h-44 rounded-2xl border border-zinc-200 bg-white shadow-sm md:shadow-md p-4 animate-pulse"
            >
              <div className="h-4 w-16 bg-zinc-200 rounded mb-3" />
              <div className="h-4 w-3/4 bg-zinc-200 rounded mb-2" />
              <div className="h-4 w-2/3 bg-zinc-200 rounded mb-6" />
              <div className="h-5 w-24 bg-zinc-200 rounded" />
            </div>
          ))}
        </div>
      )}

      {/* grid */}
      {!loading && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {items.map((c, idx) => (
            <article
              key={c.cast_hash}
              className={`
                h-full rounded-2xl border border-zinc-200/80 bg-white
                shadow-sm md:shadow-[0_10px_30px_rgba(0,0,0,0.06)]
                transition hover:shadow-[0_14px_40px_rgba(0,0,0,0.08)] will-change-transform
              `}
            >
              <div className="p-4 flex flex-col">
                <header className="mb-3 flex items-center justify-between">
                  <div className="text-xs text-zinc-500">#{idx + 1}</div>

                  {/* hover wrapper */}
                  <AuthorHoverWrap c={c}>
                    {c.pfp_url ? (
                      // eslint-disable-next-line @next/next/no-img-element
                      <img
                        src={c.pfp_url}
                        alt=""
                        className="w-6 h-6 rounded-full object-cover ring-1 ring-zinc-200"
                        referrerPolicy="no-referrer"
                      />
                    ) : (
                      <div className="w-6 h-6 rounded-full bg-zinc-200" />
                    )}
                    <a
                      className="text-sm font-medium hover:underline truncate"
                      href={`https://warpcast.com/~/profiles/${c.fid}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      title={`fid:${c.fid}`}
                    >
                      {c.display_name || (c.username ? `@${c.username}` : `fid:${c.fid}`)}
                    </a>
                  </AuthorHoverWrap>

                  {c.channel ? (
                    <span className="ml-1 shrink-0 text-xs text-zinc-600 bg-zinc-100 px-2 py-0.5 rounded-full">#{c.channel}</span>
                  ) : (
                    <span className="ml-1 shrink-0 text-xs text-zinc-400">no channel</span>
                  )}
                </header>

                <p className="whitespace-pre-wrap text-sm leading-6 line-clamp-5 text-zinc-900">
                  {c.text}
                </p>

                <div className="mt-3 text-xs text-zinc-600">
                  <time title={new Date(c.timestamp).toLocaleString()}>
                    {new Date(c.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                  </time>
                </div>

                <div className="mt-auto" />

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
              </div>
            </article>
          ))}
        </div>
      )}

      {!loading && items.length === 0 && !error && (
        <div className="text-zinc-500 mt-6">Nothing here yet. Try again later.</div>
      )}

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
      className={`inline-flex items-center gap-1 px-2 py-1 rounded-full border text-xs tabular-nums
        ${active ? "bg-black text-white border-black" : "bg-zinc-50 border-zinc-200 text-zinc-700"}`}
    >
      <span>{label}</span>
      <span className="tabular-nums">{value ?? 0}</span>
    </span>
  );
}
