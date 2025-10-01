import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

/**
 * «Только жирные за окно (по умолчанию 24ч)»
 * - Поиск: q="*" + since=ISO, sort_type=algorithmic (фоллбек на desc_chron)
 * - Берём ENG_PAGES страниц (по умолчанию 5)
 * - Детали через /casts, upsert c greatest(...)
 */

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// режим «eng-only» включён по умолчанию
const ENG_ONLY = (process.env.ENG_ONLY || "1") === "1";
// сколько страниц «тяжёлой» выборки снять
const ENG_PAGES = Number(process.env.ENG_PAGES || 5);

// лимиты/тайминги Neynar (щадя Starter)
const RPM = Number(process.env.RPM || 45);             // запросов/мин (держим < 60)
const SLEEP_MS = Number(process.env.SLEEP_MS || 1800);  // пауза между запросами
const BULK_BATCH = 100;                                 // батч /casts
const MAX_SEARCH_REQ = Number(process.env.MAX_SEARCH_REQ || 60);
const MAX_BULK_REQ = Number(process.env.MAX_BULK_REQ || 30);

// анти-частота запусков
const MIN_INTERVAL_MIN = Number(process.env.MIN_INTERVAL_MIN || 10);

// ------- утилиты -------
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function toISOFloorMinutes(d: Date) {
  const x = new Date(d);
  x.setUTCSeconds(0, 0);
  return x.toISOString();
}
function isoMinusMinutes(min: number) {
  const d = new Date();
  d.setUTCMinutes(d.getUTCMinutes() - min, 0, 0);
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

// ---- rate limiter (скользящее окно 60с) ----
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

// ---- одна страница cast/search (q="*", since=ISO, sort_type=...) ----
async function searchPageSince(sinceISO: string, cursor?: string, sort: "algorithmic" | "desc_chron" = "algorithmic") {
  if (REQ_SEARCH >= MAX_SEARCH_REQ) return { casts: [], cursor: undefined, aborted: "budget" as const };

  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", "*");            // максимально широкий запрос
  u.searchParams.set("limit", "100");
  u.searchParams.set("sort_type", sort);   // algorithmic приоритетно, дальше фоллбек
  u.searchParams.set("since", sinceISO);
  if (cursor) u.searchParams.set("cursor", cursor);

  const r = await fetchRL(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  REQ_SEARCH += 1;

  if (!r.ok) {
    const txt = await r.text();
    return { casts: [], cursor: undefined, error: `cast/search ${r.status}: ${txt}` } as any;
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

    if (!r.ok) break;

    const data = await r.json();
    const result = data?.result ?? data;
    const casts = Array.isArray(result?.casts) ? result.casts : [];
    out.push(...casts);

    if (i + BULK_BATCH < hashes.length) await sleep(SLEEP_MS);
  }
  return out;
}

// ---- основной режим: ENG-ONLY (algorithmic c фоллбеком) ----
async function runEngagementOnly(hours: number) {
  const sinceISO = toISOFloorMinutes(new Date(Date.now() - hours * 60 * 60 * 1000));

  const collected: any[] = [];
  let cursor: string | undefined = undefined;
  let pages = 0;

  // 1) пробуем algorithmic
  do {
    const page = await searchPageSince(sinceISO, cursor, "algorithmic");
    if ((page as any).aborted === "budget" || (page as any).error) break;

    if (page.casts?.length) {
      const inWindow = page.casts.filter((c: any) => {
        const ts = new Date(c.timestamp).getTime();
        return ts >= new Date(sinceISO).getTime() && ts <= Date.now();
      });
      if (inWindow.length) collected.push(...inWindow);
    }

    cursor = page.cursor;
    pages += 1;
    if (pages >= ENG_PAGES || !cursor) break;
    await sleep(SLEEP_MS);
  } while (true);

  // 2) если собрали мало/пусто — один прогон фоллбеком desc_chron (чуть дешевле)
  if (collected.length === 0 && REQ_SEARCH < MAX_SEARCH_REQ) {
    cursor = undefined;
    pages = 0;
    do {
      const page = await searchPageSince(sinceISO, cursor, "desc_chron");
      if ((page as any).aborted === "budget" || (page as any).error) break;

      if (page.casts?.length) {
        const inWindow = page.casts.filter((c: any) => {
          const ts = new Date(c.timestamp).getTime();
          return ts >= new Date(sinceISO).getTime() && ts <= Date.now();
        });
        if (inWindow.length) collected.push(...inWindow);
      }

      cursor = page.cursor;
      pages += 1;
      if (pages >= Math.max(1, Math.floor(ENG_PAGES / 2)) || !cursor) break;
      await sleep(SLEEP_MS);
    } while (true);
  }

  return { sinceISO, items: collected };
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);

    const token = url.searchParams.get("token");
    const force = url.searchParams.get("force") === "1";
    const mode = (url.searchParams.get("mode") || "").toLowerCase(); // "eng" — принудительно
    const hours = Math.max(1, Math.min(168, Number(url.searchParams.get("hours") || "24"))); // окно часов

    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // анти-частота
    const st = await getState();
    if (!force && st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    await upsertState({ updated_at: new Date().toISOString() });

    if (!(ENG_ONLY || mode === "eng")) {
      return NextResponse.json({ error: "ENG_ONLY disabled. Pass ?mode=eng or set ENG_ONLY=1." }, { status: 400 });
    }

    const { sinceISO, items } = await runEngagementOnly(hours);

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

    // детали и счётчики
    const withCounts = await fetchBulkCasts(uniq);
    // --- NEW: апсертим авторов в таблицу users (без доп. запросов к Neynar) ---
try {
  const authorsRaw = withCounts
    .map((c: any) => c?.author)
    .filter((a: any) => a && typeof a.fid === "number");

  if (authorsRaw.length) {
    // дедуп по fid
    const map = new Map<number, any>();
    for (const a of authorsRaw) {
      if (!map.has(a.fid)) {
        map.set(a.fid, {
          fid: a.fid,
          username: a.username ?? null,
          display_name: a.display_name ?? null,
          pfp_url: a.pfp?.url ?? null,
        });
      }
    }
    const authors = Array.from(map.values());

    const values = authors
      .map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`)
      .join(",");
    const params = authors.flatMap((u: any) => [
      u.fid,
      u.username,
      u.display_name,
      u.pfp_url,
    ]);

    await sql.unsafe(
      `
      insert into users (fid, username, display_name, pfp_url)
      values ${values}
      on conflict (fid) do update set
        username = coalesce(excluded.username, users.username),
        display_name = coalesce(excluded.display_name, users.display_name),
        pfp_url = coalesce(excluded.pfp_url, users.pfp_url),
        updated_at = now()
      `,
      params
    );
  }
} catch (e) {
  console.warn("users upsert skipped:", e);
}


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
      scanned: uniq.length,
      since: sinceISO,
      reqSearch: REQ_SEARCH,
      reqBulk: REQ_BULK
    });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
