import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// какие языки собирать (через запятую). Пример: "en,ru,es"
const TOP_LANGS = (process.env.TOP_LANGS || "en,ru")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// --- эконом-режим Neynar ---
const PAGE_LIMIT = 3;                 // макс. страниц /cast/search за запуск (~300 кастов)
const SLEEP_MS = 2000;                // пауза между вызовами (2s)
const BULK_BATCH = 100;               // размер батча для /casts
const MIN_INTERVAL_MIN = 15;          // пропуск, если прошлый запуск был < 15 мин назад
const BACKFILL_PAGE_LIMIT = 10;       // глубина при hours>0

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function isoMinus(minutes: number) {
  const d = new Date(); d.setUTCMinutes(d.getUTCMinutes() - minutes, 0, 0); return d.toISOString();
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

// ---- CAST SEARCH (без счётчиков) ----
// Сначала пробуем with `since`, если пусто — повторяем с `after`
async function searchCasts(sinceISO: string, cursor?: string) {
  const makeUrl = (timeKey: "since" | "after") => {
    const u = new URL(`${API}/cast/search`);
    const langQuery =
      TOP_LANGS.length > 0 ? TOP_LANGS.map((l) => `lang:${l}`).join(" OR ") : "*";
    u.searchParams.set("q", langQuery);        // фильтр по языкам
    u.searchParams.set("limit", "100");
    u.searchParams.set("sort_type", "desc_chron"); // новейшие вперёд
    u.searchParams.set(timeKey, sinceISO);
    if (cursor) u.searchParams.set("cursor", cursor);
    return u;
  };

  // попытка №1: since
  let r = await fetch(makeUrl("since"), { headers: { "x-api-key": KEY, accept: "application/json" } });
  if (!r.ok) throw new Error(`Neynar cast/search (since) ${r.status}: ${await r.text()}`);
  let data = await r.json();
  let result = data?.result ?? data;
  let casts = Array.isArray(result?.casts) ? result.casts :
              Array.isArray(result?.messages) ? result.messages : [];
  let nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;

  if (casts.length > 0 || cursor) {
    return { casts, cursor: nextCursor } as { casts: any[]; cursor?: string };
  }

  // попытка №2: after
  r = await fetch(makeUrl("after"), { headers: { "x-api-key": KEY, accept: "application/json" } });
  if (!r.ok) throw new Error(`Neynar cast/search (after) ${r.status}: ${await r.text()}`);
  data = await r.json();
  result = data?.result ?? data;
  casts = Array.isArray(result?.casts) ? result.casts :
          Array.isArray(result?.messages) ? result.messages : [];
  nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;

  return { casts, cursor: nextCursor } as { casts: any[]; cursor?: string };
}

// ---- BULK CASTS (со счётчиками) ----
async function fetchBulkCasts(hashes: string[]) {
  const out: any[] = [];
  for (let i = 0; i < hashes.length; i += BULK_BATCH) {
    const chunk = hashes.slice(i, i + BULK_BATCH);
    if (chunk.length === 0) break;

    const u = new URL(`${API}/casts`);
    u.searchParams.set("casts", chunk.join(","));

    const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    if (!r.ok) throw new Error(`Neynar casts ${r.status}: ${await r.text()}`);

    const data = await r.json();
    const result = data?.result ?? data;
    const casts = Array.isArray(result?.casts) ? result.casts : [];
    out.push(...casts);

    if (i + BULK_BATCH < hashes.length) await sleep(SLEEP_MS);
  }
  return out; // у каждого: reactions.likes_count / recasts_count / replies.count
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    // защита: только крон Vercel или ручной запуск с токеном
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);
    const token = url.searchParams.get("token");
    const force = url.searchParams.get("force") === "1";

    // доп. параметры для бэкфилла
    const hours = Math.max(0, Math.min(168, Number(url.searchParams.get("hours") || "0"))); // 0 => инкремент
    const includeExisting = url.searchParams.get("includeExisting") === "1"; // обновлять даже уже существующие хэши
    const pageLimitOverride = Number(url.searchParams.get("pages") || "0");

    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // анти-спам по частоте
    const st = await getState();
    if (!force && st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    await upsertState({ updated_at: new Date().toISOString() });

    // окно времени
    const since = hours
      ? isoMinus(hours * 60)
      : (st?.last_ts
          ? new Date(new Date(st.last_ts).getTime() - 5 * 60 * 1000).toISOString()
          : isoMinus(24 * 60));

    // 1) поиск страниц без счётчиков
    let cursor: string | undefined;
    const found: any[] = [];
    let pages = 0;
    const maxPages =
      pageLimitOverride > 0
        ? Math.min(pageLimitOverride, 15)
        : (hours > 0 ? BACKFILL_PAGE_LIMIT : PAGE_LIMIT);

    do {
      const page = await searchCasts(since, cursor);
      if (page.casts?.length) found.push(...page.casts);
      cursor = page.cursor;
      pages += 1;
      if (pages >= maxPages || !cursor) break;
      await sleep(SLEEP_MS);
    } while (true);

    if (found.length === 0) {
      return NextResponse.json({ upserted: 0, pagesFetched: pages, note: "no casts", since });
    }

    // 2) уникальные хэши; по умолчанию пропускаем те, что уже в БД
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

      if (toFetch.length === 0) {
        return NextResponse.json({ upserted: 0, pagesFetched: pages, requestedBulk: 0, skippedExisting: uniq.length, since });
      }
    }

    // 3) берём полные данные по новым хэшам (включая счётчики)
    const withCounts = await fetchBulkCasts(toFetch);

    // 4) готовим строки для upsert (ВАЖНО: без score — он generated)
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

    // 5) обновим маркер времени
    const maxTs = rows.reduce<string | null>((acc, r: any) => {
      const t = r.timestamp ? new Date(r.timestamp).toISOString() : null;
      if (!t) return acc;
      if (!acc || new Date(t) > new Date(acc)) return t;
      return acc;
    }, st?.last_ts ?? null);
    await upsertState({ last_ts: maxTs || st?.last_ts || new Date().toISOString() });

    return NextResponse.json({
      upserted: rows.length,
      pagesFetched: pages,
      scanned: found.length,
      requestedBulk: toFetch.length,
      skippedExisting: uniq.length - toFetch.length,
      since
    });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
