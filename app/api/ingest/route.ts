import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// языки (пусто => без фильтра). Пример ENV: TOP_LANGS="en,ru"
const TOP_LANGS = (process.env.TOP_LANGS || "en,ru")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// ---- лимиты/тайминги (ENV-переопределяемо) ----
const SLEEP_MS = Number(process.env.SLEEP_MS || 2500);             // базовая пауза между вызовами
const BULK_BATCH = 100;
const MIN_INTERVAL_MIN = 15;                                       // анти-частота запусков
const PAGE_LIMIT = Number(process.env.PAGE_LIMIT || 3);            // инкрементальный режим
const SLICE_MINUTES_DEFAULT = Number(process.env.SLICE_MINUTES || 60);
const SLICE_PAGES_PER_CHUNK = Number(process.env.SLICE_PAGES || 1);// страниц на один слайс (1 = щадяще)
const MAX_REQUESTS_PER_MIN = Number(process.env.RPM || 45);        // держим ниже 60/мин (Starter)
const MAX_SEARCH_REQ = Number(process.env.MAX_SEARCH_REQ || 60);   // общий бюджет запросов cast/search за запуск
const MAX_BULK_REQ   = Number(process.env.MAX_BULK_REQ   || 25);   // общий бюджет /casts за запуск
const EMPTY_SLICE_ABORT = Number(process.env.EMPTY_SLICE_ABORT || 6); // прерываемся, если подряд пустых слайсов >= N

// ------- утилиты -------
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function isoMinus(minutes: number) {
  const d = new Date(); d.setUTCMinutes(d.getUTCMinutes() - minutes, 0, 0); return d.toISOString();
}

// --- счётчики вызовов и бюджет ---
let REQ_SEARCH = 0;
let REQ_BULK = 0;
function budgetLeft() {
  return REQ_SEARCH < MAX_SEARCH_REQ && REQ_BULK < MAX_BULK_REQ;
}
function ensureBudget(kind: "search" | "bulk") {
  if (kind === "search" && REQ_SEARCH >= MAX_SEARCH_REQ) return false;
  if (kind === "bulk"   && REQ_BULK   >= MAX_BULK_REQ)   return false;
  return true;
}

// простейший rate-limiter (скользящее окно по минуте)
const callTimestamps: number[] = [];
async function respectRateLimit() {
  const now = Date.now();
  while (callTimestamps.length && now - callTimestamps[0] > 60_000) callTimestamps.shift();
  if (callTimestamps.length >= MAX_REQUESTS_PER_MIN) {
    const wait = 60_000 - (now - callTimestamps[0]) + 100; // небольшой запас
    await sleep(wait);
  }
  callTimestamps.push(Date.now());
}

async function fetchRL(input: RequestInfo | URL, init?: RequestInit, retries = 2): Promise<Response> {
  await respectRateLimit();
  const r = await fetch(input, init);
  if (r.status === 429 && retries > 0) {
    const ra = r.headers.get("retry-after");
    const wait = ra ? Number(ra) * 1000 : (800 * Math.pow(2, 2 - retries)); // 1.6s -> 0.8s
    await sleep(wait);
    return fetchRL(input, init, retries - 1);
  }
  return r;
}

async function getState() {
  const rows = await sql.unsafe(`select last_ts, updated_at from ingest_state where id='casts' limit 1`);
  return rows?.[0] ?? null;
}
async function upsertState(fields: { last_ts?: string; updated_at?: string }) {
  const last_ts = fields.last_ts ?? null;
  const updated_at = fields.updated_at ?? new Date().toISOString();
  await sql.unsafe(`
    insert into ingest_state (id, last_ts, updated_at)
    values ('casts', $1, $2)
    on conflict (id) do update set
      last_ts = coalesce(excluded.last_ts, ingest_state.last_ts),
      updated_at = excluded.updated_at
  `, [last_ts, updated_at]);
}

// ---- один вызов cast/search ----
async function searchPage(params: {
  q: string;
  timeKey: "since" | "after";
  sinceISO: string;
  cursor?: string;
  sort: "desc_chron" | "chron" | "algorithmic";
}) {
  if (!ensureBudget("search")) return { casts: [], cursor: undefined, aborted: "budget" as const };
  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", params.q);
  u.searchParams.set("limit", "100");
  u.searchParams.set("sort_type", params.sort); // допустимые: desc_chron | chron | algorithmic
  u.searchParams.set(params.timeKey, params.sinceISO);
  if (params.cursor) u.searchParams.set("cursor", params.cursor);

  const r = await fetchRL(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  REQ_SEARCH += 1;

  if (!r.ok) {
    const txt = await r.text();
    // На любой 4xx/5xx прекращаем дальнейшую пагинацию — экономим CU
    return { casts: [], cursor: undefined, error: `Neynar cast/search (${params.timeKey}, q=${params.q}, sort=${params.sort}) ${r.status}: ${txt}` } as any;
  }
  const data = await r.json();
  const result = data?.result ?? data;
  const casts = Array.isArray(result?.casts) ? result.casts :
                Array.isArray(result?.messages) ? result.messages : [];
  const nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;
  return { casts, cursor: nextCursor };
}

// ---- “умный” поиск: в инкрементальном режиме — fan-out; в бэкфилле — только baseQ ----
async function searchSmart(sinceISO: string, cursor: string | undefined, opts: {
  qOverride?: string;
  sort?: "desc_chron" | "chron" | "algorithmic";
  fanOut?: boolean; // true только в инкрементальном режиме
}) {
  const sort = opts.sort ?? "desc_chron";
  const qLang = TOP_LANGS.length > 0 ? TOP_LANGS.map((l) => `lang:${l}`).join(" OR ") : "*";
  const baseQ = (opts.qOverride?.trim() || qLang).replace(/\s+/g, " ").trim();

  const queries = (opts.fanOut ? [
    baseQ,
    `${baseQ} has:replies`,
    `${baseQ} has:likes`,
    `${baseQ} has:recasts`,
    "*",
  ] : [baseQ]).map(q => q.replace(/\s+/g, " ").trim())
               .filter((q, i, arr) => q && arr.indexOf(q) === i);

  const timeKeys: Array<"since" | "after"> = ["since", "after"];

  for (const q of queries) {
    for (const timeKey of timeKeys) {
      const page = await searchPage({ q, timeKey, sinceISO, cursor, sort });
      if ((page as any).aborted === "budget") return { casts: [], cursor: undefined, qUsed: q, timeKeyUsed: timeKey, aborted: "budget" as const };
      if ((page as any).error) return { casts: [], cursor: undefined, qUsed: q, timeKeyUsed: timeKey, error: (page as any).error };

      if (page.casts.length > 0 || cursor) {
        return { ...page, qUsed: q, timeKeyUsed: timeKey };
      }
      if (cursor) return { ...page, qUsed: q, timeKeyUsed: timeKey };
    }
  }
  return { casts: [] as any[], cursor: undefined, qUsed: baseQ, timeKeyUsed: "since" as const };
}

// ---- батч деталей кастов ----
async function fetchBulkCasts(hashes: string[]) {
  const out: any[] = [];
  for (let i = 0; i < hashes.length; i += BULK_BATCH) {
    if (!ensureBudget("bulk")) break;

    const chunk = hashes.slice(i, i + BULK_BATCH);
    if (chunk.length === 0) break;

    const u = new URL(`${API}/casts`);
    u.searchParams.set("casts", chunk.join(","));
    const r = await fetchRL(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    REQ_BULK += 1;

    if (!r.ok) break; // экономим бюджет — на ошибке прекращаем дальнейшие батчи

    const data = await r.json();
    const result = data?.result ?? data;
    const casts = Array.isArray(result?.casts) ? result.casts : [];
    out.push(...casts);

    if (i + BULK_BATCH < hashes.length) await sleep(SLEEP_MS);
  }
  return out;
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    // защита
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);
    const token = url.searchParams.get("token");
    const force = url.searchParams.get("force") === "1";

    // параметры
    const hours = Math.max(0, Math.min(168, Number(url.searchParams.get("hours") || "0"))); // 0 => инкремент
    const includeExisting = url.searchParams.get("includeExisting") === "1";
    const pageLimitOverride = Number(url.searchParams.get("pages") || "0");
    const qOverride = url.searchParams.get("q") || undefined;
    const sliceMinutes = Math.max(15, Math.min(240, Number(url.searchParams.get("slice") || SLICE_MINUTES_DEFAULT)));
    const reset = url.searchParams.get("reset") === "1";

    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    const st = await getState();
    if (!force && st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    await upsertState({ updated_at: new Date().toISOString() });

    const since = reset
      ? isoMinus(24 * 60)
      : (hours
          ? isoMinus(hours * 60)
          : (st?.last_ts
              ? new Date(new Date(st.last_ts).getTime() - 5 * 60 * 1000).toISOString()
              : isoMinus(24 * 60)));

    const found: any[] = [];
    let pagesFetched = 0;
    let aborted: "budget" | "empty_streak" | undefined;
    let errorMsg: string | undefined;

    if (hours > 0) {
      // --- SLICING: q="*" + since + sort=desc_chron; строгие лимиты и ранний выход ---
      const end = new Date();
      const start = new Date(end.getTime() - hours * 60 * 60 * 1000);
      const sliceMs = sliceMinutes * 60 * 1000;
      const pagesPerSlice = SLICE_PAGES_PER_CHUNK;

      let emptyStreak = 0;

      for (let t = start.getTime(); t < end.getTime(); t += sliceMs) {
        if (!budgetLeft()) { aborted = "budget"; break; }

        const sliceStart = new Date(t);
        const sliceEnd = new Date(Math.min(t + sliceMs, end.getTime()));
        let cursor: string | undefined = undefined;
        let pages = 0;
        let addedThisSlice = 0;

        do {
          if (!budgetLeft()) { aborted = "budget"; break; }

          const page = await searchPage({
            q: "*",
            timeKey: "since",
            sinceISO: sliceStart.toISOString(),
            cursor,
            sort: "desc_chron",
          });

          if ((page as any).error) { errorMsg = (page as any).error; break; }
          if ((page as any).aborted === "budget") { aborted = "budget"; break; }

          if (page.casts?.length) {
            const inWindow = page.casts.filter((c: any) => {
              const ts = new Date(c.timestamp).getTime();
              return ts >= sliceStart.getTime() && ts <= sliceEnd.getTime();
            });
            if (inWindow.length) {
              found.push(...inWindow);
              addedThisSlice += inWindow.length;
            }
          }

          cursor = page.cursor;
          pages += 1;
          pagesFetched += 1;

          if (pages >= pagesPerSlice || !cursor) break;
          await sleep(SLEEP_MS);
        } while (true);

        // учёт пустых слайсов подряд — ранний выход
        if (addedThisSlice === 0) {
          emptyStreak += 1;
          if (emptyStreak >= EMPTY_SLICE_ABORT) { aborted = "empty_streak"; break; }
        } else {
          emptyStreak = 0;
        }

        if (aborted || errorMsg) break;
        await sleep(SLEEP_MS);
      }
    } else {
      // --- инкрементальный прогон: допускаем fan-out по q ---
      let cursor: string | undefined;
      const maxPages = pageLimitOverride > 0
        ? Math.min(pageLimitOverride, 15)
        : PAGE_LIMIT;

      do {
        if (!budgetLeft()) { aborted = "budget"; break; }

        const page = await searchSmart(since, cursor, {
          qOverride,
          sort: "desc_chron",
          fanOut: true,
        });

        if ((page as any).error) { errorMsg = (page as any).error; break; }
        if ((page as any).aborted === "budget") { aborted = "budget"; break; }

        if (page.casts?.length) found.push(...page.casts);
        cursor = page.cursor;
        pagesFetched += 1;

        if (pagesFetched >= maxPages || !cursor) break;
        await sleep(SLEEP_MS);
      } while (true);

      // авто-фоллбек: если ничего не нашли — расширим окно до 6ч и q="*"
      if (found.length === 0 && !aborted && !errorMsg) {
        let cursor2: string | undefined = undefined;
        const sinceFallback = isoMinus(6 * 60);
        let pages2 = 0;
        do {
          if (!budgetLeft()) { aborted = "budget"; break; }
          const page2 = await searchSmart(sinceFallback, cursor2, {
            qOverride: "*",
            sort: "desc_chron",
            fanOut: false
          });
          if ((page2 as any).error) { errorMsg = (page2 as any).error; break; }
          if ((page2 as any).aborted === "budget") { aborted = "budget"; break; }

          if (page2.casts?.length) found.push(...page2.casts);
          cursor2 = page2.cursor;
          pagesFetched += 1;
          pages2 += 1;
          if (pages2 >= 5 || !cursor2) break;
          await sleep(SLEEP_MS);
        } while (true);
      }
    }

    if (found.length === 0) {
      return NextResponse.json({
        upserted: 0,
        pagesFetched,
        note: "no casts",
        since,
        aborted,
        error: errorMsg,
        reqSearch: REQ_SEARCH,
        reqBulk: REQ_BULK
      });
    }

    // уникальные хэши
    const uniq = Array.from(new Set(found.map((c: any) => c.hash)));
    let toFetch = uniq;
    if (!includeExisting) {
      const placeholders = uniq.map((_, i) => `$${i + 1}`).join(",");
      const existing = await sql.unsafe(
        `select cast_hash from top_casts where cast_hash in (${placeholders})`,
        uniq
      );
      const existSet = new Set(existing.map((r: any) => r.cast_hash));
      toFetch = uniq.filter((h) => !existSet.has(h));
    }

    if (toFetch.length === 0) {
      return NextResponse.json({
        upserted: 0,
        pagesFetched,
        requestedBulk: 0,
        skippedExisting: uniq.length,
        since,
        aborted,
        reqSearch: REQ_SEARCH,
        reqBulk: REQ_BULK
      });
    }

    // детали и счётчики
    const withCounts = await fetchBulkCasts(toFetch);

    const byHash = new Map(withCounts.map((c: any) => [c.hash, c]));
    const rows = toFetch.map((hash) => {
      const c = byHash.get(hash) || {};
      const likes = c?.reactions?.likes_count ?? 0;
      const recasts = c?.reactions?.recasts_count ?? 0;
      const replies = c?.replies?.count ?? 0;
      return {
        cast_hash: hash,
        fid: c?.author?.fid ?? null,
        text: c?.text ?? "",
        channel: c?.channel?.id ?? null,
        timestamp: c?.timestamp ?? new Date().toISOString(),
        likes, recasts, replies
      };
    }).filter(r => r.fid !== null);

    if (rows.length) {
      const values = rows.map((_, i) =>
        `($${i*8+1}, $${i*8+2}, $${i*8+3}, $${i*8+4}, $${i*8+5}, $${i*8+6}, $${i*8+7}, $${i*8+8})`
      ).join(",");
      const params = rows.flatMap(r => [
        r.cast_hash, r.fid, r.text, r.channel, r.timestamp, r.likes, r.recasts, r.replies
      ]);

      await sql.unsafe(
        `
        insert into top_casts
        (cast_hash, fid, text, channel, timestamp, likes, recasts, replies)
        values ${values}
        on conflict (cast_hash) do update set
          fid = excluded.fid,
          text = excluded.text,
          channel = excluded.channel,
          timestamp = excluded.timestamp,
          likes = greatest(excluded.likes, top_casts.likes),
          recasts = greatest(excluded.recasts, top_casts.recasts),
          replies = greatest(excluded.replies, top_casts.replies)
        `,
        params
      );
    }

    const st2 = await getState();
    const maxTs = rows.reduce<string | null>((acc, r: any) => {
      const t = r.timestamp ? new Date(r.timestamp).toISOString() : null;
      if (!t) return acc;
      if (!acc || new Date(t) > new Date(acc)) return t;
      return acc;
    }, st2?.last_ts ?? null);
    await upsertState({ last_ts: maxTs || st2?.last_ts || new Date().toISOString() });

    return NextResponse.json({
      upserted: rows.length,
      pagesFetched,
      scanned: found.length,
      requestedBulk: toFetch.length,
      skippedExisting: uniq.length - toFetch.length,
      since,
      aborted,
      reqSearch: REQ_SEARCH,
      reqBulk: REQ_BULK
    });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
