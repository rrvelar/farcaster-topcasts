import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

/**
 * ЖИРНЫЕ ЗА СУТКИ:
 * - Берём только engagement-контент за окно hours (по умолчанию 24ч):
 *   "* has:replies", "* has:likes", "* has:recasts"
 * - По каждому запросу снимаем ENG_PAGES страниц (по умолчанию 2).
 * - Дедуп по hash, догружаем детали /casts батчами, upsert с greatest().
 */

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// --- включение "только жирных" (по умолчанию ВКЛ) ---
const ENG_ONLY = (process.env.ENG_ONLY || "1") === "1";

// --- какие engagement-запросы дергать (через запятую) ---
const ENG_QUERIES = (process.env.ENG_QUERIES || "* has:replies,* has:likes,* has:recasts")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// --- сколько страниц на каждый запрос (щадим лимит Neynar) ---
const ENG_PAGES = Number(process.env.ENG_PAGES || 2);

// --- лимиты/тайминги Neynar ---
const RPM = Number(process.env.RPM || 45);            // запросов/минуту (Starter: < 60)
const SLEEP_MS = Number(process.env.SLEEP_MS || 2000); // пауза между запросами
const BULK_BATCH = 100;                                // /casts батч
const MAX_SEARCH_REQ = Number(process.env.MAX_SEARCH_REQ || 60);
const MAX_BULK_REQ = Number(process.env.MAX_BULK_REQ || 30);

// --- анти-частота запусков (для кронов/ручных) ---
const MIN_INTERVAL_MIN = Number(process.env.MIN_INTERVAL_MIN || 10);

// ------- утилиты -------
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function isoMinus(minutes: number) {
  const d = new Date(); d.setUTCMinutes(d.getUTCMinutes() - minutes, 0, 0);
  return d.toISOString();
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

// ---- простейший rate limiter по минуте ----
const callTimestamps: number[] = [];
async function respectRateLimit() {
  const now = Date.now();
  while (callTimestamps.length && now - callTimestamps[0] > 60_000) callTimestamps.shift();
  if (callTimestamps.length >= RPM) {
    const wait = 60_000 - (now - callTimestamps[0]) + 100;
    await sleep(wait);
  }
  callTimestamps.push(Date.now());
}

let REQ_SEARCH = 0;
let REQ_BULK = 0;

async function fetchRL(input: RequestInfo | URL, init?: RequestInit, retries = 2): Promise<Response> {
  await respectRateLimit();
  const r = await fetch(input, init);
  if (r.status === 429 && retries > 0) {
    const ra = r.headers.get("retry-after");
    const wait = ra ? Number(ra) * 1000 : 1200 * Math.pow(2, 2 - retries); // 1.2s -> 2.4s
    await sleep(wait);
    return fetchRL(input, init, retries - 1);
  }
  return r;
}

// ---- одна страница cast/search ----
async function searchPage(q: string, sinceISO: string, cursor?: string, sort: "algorithmic" | "desc_chron" | "chron" = "algorithmic") {
  if (REQ_SEARCH >= MAX_SEARCH_REQ) {
    return { casts: [], cursor: undefined, aborted: "budget" as const };
  }

  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", q);
  u.searchParams.set("limit", "100");
  u.searchParams.set("sort_type", sort); // допустимо: algorithmic | desc_chron | chron
  u.searchParams.set("since", sinceISO);
  if (cursor) u.searchParams.set("cursor", cursor);

  const r = await fetchRL(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  REQ_SEARCH += 1;

  if (!r.ok) {
    const txt = await r.text();
    return { casts: [], cursor: undefined, error: `cast/search (q=${q}) ${r.status}: ${txt}` } as any;
  }
  const data = await r.json();
  const result = data?.result ?? data;
  const casts = Array.isArray(result?.casts) ? result.casts :
                Array.isArray(result?.messages) ? result.messages : [];
  const nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;
  return { casts, cursor: nextCursor };
}

// ---- батч деталей по /casts ----
async function fetchBulkCasts(hashes: string[]) {
  const out: any[] = [];
  for (let i = 0; i < hashes.length; i += BULK_BATCH) {
    if (REQ_BULK >= MAX_BULK_REQ) break;
    const chunk = hashes.slice(i, i + BULK_BATCH);
    if (!chunk.length) break;

    const u = new URL(`${API}/casts`);
    u.searchParams.set("casts", chunk.join(","));

    const r = await fetchRL(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    REQ_BULK += 1;

    if (!r.ok) break; // экономим бюджет

    const data = await r.json();
    const result = data?.result ?? data;
    const casts = Array.isArray(result?.casts) ? result.casts : [];
    out.push(...casts);

    if (i + BULK_BATCH < hashes.length) await sleep(SLEEP_MS);
  }
  return out;
}

// ---- основной режим: ТОЛЬКО ENGAGEMENT ЗА ОКНО ----
async function runEngagementOnly(hours: number, pagesPerQuery: number) {
  const sinceISO = isoMinus(hours * 60);
  const found: any[] = [];

  for (const q of ENG_QUERIES) {
    let cursor: string | undefined = undefined;
    let pages = 0;

    do {
      const page = await searchPage(q, sinceISO, cursor, "algorithmic");
      if ((page as any).aborted === "budget" || (page as any).error) break;

      if (page.casts?.length) {
        // ограничим по окну (на всякий)
        const inWindow = page.casts.filter((c: any) => {
          const ts = new Date(c.timestamp).getTime();
          return ts >= new Date(sinceISO).getTime() && ts <= Date.now();
        });
        if (inWindow.length) found.push(...inWindow);
      }

      cursor = page.cursor;
      pages += 1;
      if (pages >= pagesPerQuery || !cursor || REQ_SEARCH >= MAX_SEARCH_REQ) break;
      await sleep(SLEEP_MS);
    } while (true);

    if (REQ_SEARCH >= MAX_SEARCH_REQ) break;
    await sleep(SLEEP_MS);
  }

  return { sinceISO, items: found };
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);

    const token = url.searchParams.get("token");
    const force = url.searchParams.get("force") === "1";
    const mode = (url.searchParams.get("mode") || "").toLowerCase(); // "eng" = принудительно только жирные
    const hours = Math.max(1, Math.min(168, Number(url.searchParams.get("hours") || "24"))); // окно часов

    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }
    if (!KEY) {
      return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });
    }

    // защита от частых запусков
    const st = await getState();
    if (!force && st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    await upsertState({ updated_at: new Date().toISOString() });

    // --- запускаем режим "только жирные" ---
    if (ENG_ONLY || mode === "eng") {
      const { sinceISO, items } = await runEngagementOnly(hours, ENG_PAGES);

      if (!items.length) {
        return NextResponse.json({
          upserted: 0,
          note: "no_engagement_casts",
          since: sinceISO,
          reqSearch: REQ_SEARCH,
          reqBulk: REQ_BULK
        });
      }

      // дедуп по hash
      const uniq = Array.from(new Set(items.map((c: any) => c.hash)));

      // детали и счётчики одним /casts
      const withCounts = await fetchBulkCasts(uniq);

      // upsert
      const byHash = new Map(withCounts.map((c: any) => [c.hash, c]));
      const rows = uniq.map((hash) => {
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

      // обновим last_ts «для вида» (хотя мы работаем по окну, не по инкременту)
      const maxTs = rows.reduce<string | null>((acc, r: any) => {
        const t = r.timestamp ? new Date(r.timestamp).toISOString() : null;
        if (!t) return acc;
        if (!acc || new Date(t) > new Date(acc)) return t;
        return acc;
      }, st?.last_ts ?? null);
      await upsertState({ last_ts: maxTs || st?.last_ts || new Date().toISOString() });

      return NextResponse.json({
        upserted: rows.length,
        scanned: uniq.length,
        since: sinceISO,
        queries: ENG_QUERIES,
        pagesPerQuery: ENG_PAGES,
        reqSearch: REQ_SEARCH,
        reqBulk: REQ_BULK
      });
    }

    // если ENG_ONLY=0 и mode!=eng — можно добавить альтернативный режим (например, инкрементальный),
    // но по твоей задаче это не требуется. Вернём 400, чтобы случайно не ушли в «тихий ноль».
    return NextResponse.json({ error: "ENG_ONLY disabled and no alternative mode implemented. Pass ?mode=eng or set ENG_ONLY=1." }, { status: 400 });

  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
