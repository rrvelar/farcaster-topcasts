// app/api/ingest/route.ts
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
// import { headers } from "next/headers"; // больше не используем
import { sql } from "../../_db";

/**
 * «Только жирные за окно (по умолчанию 24ч)»
 * ...
 */

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// режим «eng-only» включён по умолчанию
const ENG_ONLY = (process.env.ENG_ONLY || "1") === "1";
// сколько страниц «тяжёлой» выборки снять
const ENG_PAGES = Number(process.env.ENG_PAGES || 5);

// лимиты/тайминги Neynar (щадя Starter)
const RPM = Number(process.env.RPM || 45);
const SLEEP_MS = Number(process.env.SLEEP_MS || 1800);
const BULK_BATCH = 100;
const MAX_SEARCH_REQ = Number(process.env.MAX_SEARCH_REQ || 60);
const MAX_BULK_REQ = Number(process.env.MAX_BULK_REQ || 30);

// анти-частота запусков
const MIN_INTERVAL_MIN = Number(process.env.MIN_INTERVAL_MIN || 10);

// ------- утилиты -------
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function toISOFloorMinutes(d: Date) { const x = new Date(d); x.setUTCSeconds(0, 0); return x.toISOString(); }
function isoMinusMinutes(min: number) { const d = new Date(); d.setUTCMinutes(d.getUTCMinutes() - min, 0, 0); return d.toISOString(); }

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
    const wait = ra ? Number(ra) * 1000 : 1200 * Math.pow(2, 2 - retries);
    await sleep(wait);
    return fetchRL(input, init, retries - 1);
  }
  return r;
}

// ---- helpers для авторов ----
function pickPfpUrl(author: any): string | null {
  if (!author) return null;
  return author.pfp_url ?? author.pfp?.url ?? author.profile?.pfp_url ?? null;
}
function normalizeAuthor(a: any) {
  if (!a || typeof a.fid !== "number") return null;
  return { fid: a.fid, username: a.username ?? null, display_name: a.display_name ?? null, pfp_url: pickPfpUrl(a) };
}
async function upsertAuthors(authorsRaw: any[]) {
  const cleaned = authorsRaw.map(normalizeAuthor).filter(Boolean) as Array<{ fid: number; username: string | null; display_name: string | null; pfp_url: string | null; }>;
  if (cleaned.length === 0) return 0;
  const map = new Map<number, (typeof cleaned)[number]>();
  for (const a of cleaned) if (!map.has(a.fid)) map.set(a.fid, a);
  const authors = Array.from(map.values());

  const values = authors.map((_, i) => `($${i*4+1}, $${i*4+2}, $${i*4+3}, $${i*4+4})`).join(",");
  const params = authors.flatMap(a => [a.fid, a.username, a.display_name, a.pfp_url]);

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
  return authors.length;
}

// ---- одна страница cast/search ----
async function searchPageSince(
  sinceISO: string,
  cursor?: string,
  sort: "algorithmic" | "desc_chron" = "algorithmic"
) {
  if (REQ_SEARCH >= MAX_SEARCH_REQ) return { casts: [], cursor: undefined, aborted: "budget" as const };

  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", "*");
  u.searchParams.set("limit", "100");
  u.searchParams.set("sort_type", sort);
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

// ---- основной режим: ENG-ONLY ----
async function runEngagementOnly(hours: number) {
  const sinceISO = toISOFloorMinutes(new Date(Date.now() - hours * 60 * 60 * 1000));

  const collected: any[] = [];
  let cursor: string | undefined = undefined;
  let pages = 0;

  // 1) algorithmic
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

  // 2) fallback desc_chron
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
    // ---------- АВТОРИЗАЦИЯ КРОНА/ТОКЕНА ----------
    const url = new URL(req.url);
    const hdr = req.headers;
    const ua = hdr.get("user-agent") || "";
    const isCron = hdr.get("x-vercel-cron") === "1" || /vercel-cron/i.test(ua);

    const token = url.searchParams.get("token") || hdr.get("x-ingest-token") || "";
    const force = url.searchParams.get("force") === "1";
    const mode = (url.searchParams.get("mode") || "").toLowerCase();
    const hours = Math.max(1, Math.min(168, Number(url.searchParams.get("hours") || "24")));

    if (!isCron && token !== MANUAL_TOKEN) {
      // единичный лог поможет в диагностике
      console.error("ingest forbidden", { ua, xvc: hdr.get("x-vercel-cron"), hasToken: !!token });
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

    // апсерт авторов (из поиска)
    try {
      const authorsFromSearch = (items || []).map((c: any) => c?.author).filter(Boolean);
      await upsertAuthors(authorsFromSearch);
    } catch (e) {
      console.warn("users upsert (search) skipped:", e);
    }

    // детали и счётчики
    const withCounts = await fetchBulkCasts(uniq);

    // апсерт авторов (из /casts)
    try {
      const authorsFromCasts = (withCounts || []).map((c: any) => c?.author).filter(Boolean);
      await upsertAuthors(authorsFromCasts);
    } catch (e) {
      console.warn("users upsert (casts) skipped:", e);
    }

    // upsert топ-кастов
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
