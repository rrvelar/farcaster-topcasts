"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { sdk } from "@farcaster/miniapp-sdk";

/* -------------------- Types -------------------- */
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

/* -------------------- Utils -------------------- */
// Warpcast URLs
function makeCastUrl(hash: string, username?: string | null) {
  if (username && username.trim()) return `https://warpcast.com/${username}/${hash}`;
  return `https://warpcast.com/~/cast/${hash}`;
}
function makeProfileUrl(fid: number, handle?: string) {
  if (handle && handle.trim()) return `https://warpcast.com/${handle}`;
  return `https://warpcast.com/~/profiles/${fid}`;
}

// Promo account (который юзер должен зафолловить)
const PROMO_FID = Number(process.env.NEXT_PUBLIC_PROMO_FID || "0");
const PROMO_HANDLE = (process.env.NEXT_PUBLIC_PROMO_HANDLE || "").trim();

// Простые ключи для локального кеша
const addedKey = (fid: number | null) => `fc_added_${fid ?? "anon"}`;
const followKey = (viewer: number, promo: number) => `fc_follow_${viewer}_${promo}`;

const readLS = (k: string) => {
  if (typeof window === "undefined") return null;
  try { return window.localStorage.getItem(k); } catch { return null; }
};
const writeLS = (k: string, v: string) => {
  if (typeof window === "undefined") return;
  try { window.localStorage.setItem(k, v); } catch {}
};

/* -------------------- Page -------------------- */
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
  const [viewerFid, setViewerFid] = useState<number | null>(null);

  // gates
  const [isAdded, setIsAdded] = useState<boolean>(false); // <— по умолчанию false, чтобы кнопка точно показалась в мини-аппе
  const [followConfirmed, setFollowConfirmed] = useState<boolean>(false);
  const [followChecking, setFollowChecking] = useState<boolean>(false);
  const [followTries, setFollowTries] = useState<number>(0);

  const lastCtxRef = useRef<any>(null);

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

  // Инициализация Mini App + вычисление added/viewerFid c жёсткими фаллбэками
  useEffect(() => {
    let timers: Array<ReturnType<typeof setTimeout>> = [];
    let off: undefined | (() => void);
    let cancelled = false;

    (async () => {
      try {
        const inMini = await sdk.isInMiniApp();
        if (cancelled) return;
        setIsMiniApp(inMini);

        if (!inMini) {
          // В обычном вебе ничего не блокируем
          setViewerFid(null);
          setIsAdded(true);
          setFollowConfirmed(true);
          return;
        }

        await sdk.actions.ready();

        const getCtx = async () =>
          typeof (sdk as any).getContext === "function"
            ? await (sdk as any).getContext()
            : (sdk as any).context;

        const compute = (ctx: any) => {
          const addedRaw =
            ctx?.client?.added ??
            ctx?.miniApp?.added ??
            (Array.isArray(ctx?.client?.apps)
              ? ctx.client.apps.some((a: any) => a?.id && a?.added)
              : undefined);
          const fidRaw =
            ctx?.viewer?.fid ??
            ctx?.user?.fid ??
            ctx?.client?.viewer?.fid ??
            null;
          return {
            added: typeof addedRaw === "boolean" ? addedRaw : undefined,
            fid: typeof fidRaw === "number" ? fidRaw : null,
          };
        };

        // первичный контекст
        const ctx = await getCtx();
        lastCtxRef.current = ctx;
        const { added, fid } = compute(ctx);
        setViewerFid(fid);

        // added: сначала контекст, если он «undefined» — пробуем кеш локалстореджа
        let addedVal = added;
        const addedCache = readLS(addedKey(fid));
        if (addedVal === undefined && addedCache === "1") addedVal = true;
        setIsAdded(!!addedVal);

        // follow: если сам на себя — сразу ок, иначе читаем кеш
        if (!PROMO_FID || fid === null || fid === PROMO_FID) {
          setFollowConfirmed(true);
        } else {
          const fCache = readLS(followKey(fid, PROMO_FID)) === "1";
          setFollowConfirmed(fCache);
        }

        // слушатель обновления контекста
        const onCtx = async () => {
          try {
            const c = await getCtx();
            lastCtxRef.current = c;
            const { added: a, fid: f } = compute(c);

            if (f !== null) setViewerFid(f);
            if (a === true) {
              setIsAdded(true);
              writeLS(addedKey(f), "1");
            }
          } catch {}
        };

        off = (sdk as any)?.events?.on?.("context", onCtx);
        [600, 1500, 3000].forEach((ms) => timers.push(setTimeout(onCtx, ms)));
      } catch {
        // если SDK грохнулся — ведём себя как обычное веб-приложение
        setIsMiniApp(false);
        setViewerFid(null);
        setIsAdded(true);
        setFollowConfirmed(true);
      }
    })();

    return () => {
      cancelled = true;
      timers.forEach(clearTimeout);
      if (typeof off === "function") off();
    };
  }, []);

  // подгрузка данных
  useEffect(() => { load(); /* eslint-disable-next-line */ }, [metric, range]);

  const title = useMemo(() => {
    const m = METRICS.find((m) => m.key === metric)?.label ?? "";
    const r = RANGES.find((r) => r.key === range)?.label ?? "";
    return `Top casts · ${m.toLowerCase()} · ${r}`;
  }, [metric, range]);

  // Добавление приложения
  const handleAddMiniApp = async () => {
    try {
      await sdk.actions.addMiniApp();
      writeLS(addedKey(viewerFid), "1");
      setIsAdded(true);

      // лёгкая повторная проверка через секунду
      setTimeout(async () => {
        try {
          const ctx =
            typeof (sdk as any).getContext === "function"
              ? await (sdk as any).getContext()
              : (sdk as any).context;
          const addedRaw =
            ctx?.client?.added ??
            ctx?.miniApp?.added ??
            (Array.isArray(ctx?.client?.apps)
              ? ctx.client.apps.some((a: any) => a?.id && a?.added)
              : undefined);
          if (addedRaw === true) setIsAdded(true);
        } catch {}
      }, 1200);
    } catch (e) {
      console.warn("addMiniApp failed:", e);
    }
  };

  // Бэкенд-проверка подписки (Neynar)
  const verifyFollowOnce = async (): Promise<boolean> => {
    if (!viewerFid || !PROMO_FID) return false;
    try {
      const r = await fetch(
        `/api/follow/verify?viewer=${viewerFid}&target=${PROMO_FID}`,
        { cache: "no-store" }
      );
      if (!r.ok) return false;
      const j = await r.json();
      return !!j?.following;
    } catch {
      return false;
    }
  };

  // открыть URL через SDK / или в новой вкладке
  const openUrl = async (url: string) => {
    try {
      if ((sdk as any)?.actions?.openURL) await (sdk as any).actions.openURL(url);
      else window.open(url, "_blank", "noopener,noreferrer");
    } catch {
      window.open(url, "_blank", "noopener,noreferrer");
    }
  };

  // Кнопка Follow: открываем профиль и ждём реальной подписки (poll + visibility)
  const handleFollow = async () => {
    if (!PROMO_FID) return;

    // 1) открываем профиль
    await openUrl(makeProfileUrl(PROMO_FID, PROMO_HANDLE));

    // 2) включаем ожидание и пуллинг
    setFollowChecking(true);
    setFollowTries(0);

    let stop = false;
    const onVisible = async () => {
      if (document.visibilityState === "visible" && !stop) {
        const pass = await verifyFollowOnce();
        if (pass) {
          stop = true;
          document.removeEventListener("visibilitychange", onVisible);
          if (viewerFid) writeLS(followKey(viewerFid, PROMO_FID), "1");
          setFollowConfirmed(true);
          setFollowChecking(false);
        }
      }
    };
    document.addEventListener("visibilitychange", onVisible);

    const MAX_TRIES = 10;      // ~20 сек суммарно
    const INTERVAL  = 2000;

    for (let i = 0; i < MAX_TRIES && !stop; i++) {
      await new Promise((res) => setTimeout(res, INTERVAL));
      setFollowTries(i + 1);
      const pass = await verifyFollowOnce();
      if (pass) {
        stop = true;
        document.removeEventListener("visibilitychange", onVisible);
        if (viewerFid) writeLS(followKey(viewerFid, PROMO_FID), "1");
        setFollowConfirmed(true);
        break;
      }
    }

    if (!stop) {
      // не удалось подтвердить — оставляем гейт включённым
      setFollowChecking(false);
    }
  };

  /* -------------------- UI -------------------- */
  return (
    <div className="mx-auto max-w-6xl px-4 py-6 pb-28">
      {/* Gate #1: Add app */}
      {isMiniApp && !isAdded && (
        <div className="fixed inset-0 z-[60] bg-white/80 backdrop-blur-sm">
          <div className="absolute inset-x-0 bottom-0 p-4 pb-[max(env(safe-area-inset-bottom),1rem)]">
            <div className="mx-auto max-w-xl rounded-2xl border bg-white shadow-xl p-5">
              <h2 className="text-lg font-semibold mb-1">Add “Top Casts” to My Apps</h2>
              <p className="text-sm text-gray-600 mb-4">
                To continue, please add this mini app to your Warpcast apps.
              </p>
              <button
                onClick={handleAddMiniApp}
                className="w-full px-4 py-2 rounded-xl bg-black text-white text-sm hover:bg-gray-900"
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
              <p className="text-sm text-gray-600 mb-4">
                Please follow to access the app.
              </p>

              <button
                onClick={handleFollow}
                disabled={!viewerFid || followChecking}
                className={`w-full px-4 py-2 rounded-xl text-sm transition ${
                  !viewerFid || followChecking ? "bg-gray-200 text-gray-700 cursor-not-allowed"
                                               : "bg-black text-white hover:bg-gray-900"
                }`}
                title={!viewerFid ? "Open inside Warpcast to continue" : undefined}
              >
                {followChecking ? `Checking… (${followTries})` : `Follow in Warpcast`}
              </button>
            </div>
          </div>
        </div>
      )}

      <h1 className="text-2xl font-semibold mb-4">Top casts</h1>

      {/* filters */}
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

      {/* grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {items.map((c, idx) => (
          <article key={c.cast_hash} className="h-full rounded-2xl border bg-white shadow-sm p-4 flex flex-col">
            <header className="mb-3 flex items-center justify-between">
              <div className="text-xs text-gray-500">#{idx + 1}</div>
              <div className="flex items-center gap-2 min-w-0">
                {c.pfp_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={c.pfp_url} alt="" className="w-6 h-6 rounded-full object-cover" referrerPolicy="no-referrer" />
                ) : (
                  <div className="w-6 h-6 rounded-full bg-gray-200" />
                )}
                <a className="text-sm font-medium hover:underline truncate" href={`https://warpcast.com/~/profiles/${c.fid}`} target="_blank" rel="noopener noreferrer" title={`fid:${c.fid}`}>
                  {c.display_name || (c.username ? `@${c.username}` : `fid:${c.fid}`)}
                </a>
                {c.channel ? (
                  <span className="ml-1 shrink-0 text-xs bg-gray-100 px-2 py-0.5 rounded-full">#{c.channel}</span>
                ) : (
                  <span className="ml-1 shrink-0 text-xs text-gray-400">no channel</span>
                )}
              </div>
            </header>

            <p className="whitespace-pre-wrap text-sm leading-5 line-clamp-6 text-gray-900">{c.text}</p>

            <div className="mt-3 text-xs text-gray-600">
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
              <a className="text-blue-600 hover:underline text-sm shrink-0" href={makeCastUrl(c.cast_hash, c.username)} target="_blank" rel="noopener noreferrer">
                Open ↗
              </a>
            </div>
          </article>
        ))}
      </div>

      {!loading && items.length === 0 && !error && (
        <div className="text-gray-500 mt-6">Nothing here yet. Try again later.</div>
      )}
    </div>
  );
}

function Badge({ label, value, active }: { label: string; value: number; active?: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-full border text-xs ${
      active ? "bg-black text-white border-black" : "bg-gray-50"
    }`}>
      <span>{label}</span>
      <span className="tabular-nums">{value ?? 0}</span>
    </span>
  );
}
