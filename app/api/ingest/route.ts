import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// --- анти-расход Neynar ---
const PAGE_LIMIT = 8;            // max страниц за прогон (<= 800 кастов)
const SLEEP_MS = 2000;           // пауза между запросами (чтобы не ловить 429)
const REACTION_BATCH = 100;      // батч реакций
const MIN_INTERVAL_MIN = 12;     // пропускаем запуск, если был <12 мин назад

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }
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

// ---- CAST SEARCH ----
async function fetchRecentCasts(sinceISO: string, cursor?: string) {
  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", "lang:en");
  u.searchParams.set("limit", "100");
  u.searchParams.set("since", sinceISO);
  if (cursor) u.searchParams.set("cursor", cursor);

  const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Neynar cast/search ${r.status}: ${text}`);
  }
  const data = await r.json();
  const result = data?.result ?? data;
  const casts = Array.isArray(result?.casts) ? result.casts :
                Array.isArray(result?.messages) ? result.messages : [];
  const nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;
  return { casts, cursor: nextCursor } as { casts: any[]; cursor?: string };
}

// ---- REACTION COUNTS ----
async function fetchReactionCounts(hashes: string[]) {
  const r = await fetch(`${API}/reaction/counts`, {
    method: "POST",
    headers: { "x-api-key": KEY, "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ cast_hashes: hashes })
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Neynar reaction/counts ${r.status}: ${text}`);
  }
  const data = await r.json();
  const counts = Array.isArray(data?.counts) ? data.counts :
                 Array.isArray(data?.result?.counts) ? data.result.counts : [];
  return counts as Array<{ cast_hash: string; likes: number; recasts: number; replies: number }>;
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    // 0) запрет публичного вызова
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";               // приходит от Vercel Cron
    const token = new URL(req.url).searchParams.get("token");
    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }

    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // 1) скипаем, если запускали недавно
    const st = await getState();
    if (st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    // сразу отметим старт
    await upsertState({ updated_at: new Date().toISOString() });

    // окно: с последнего ts - 5 минут буфер; если первый раз — 24ч
    const since = st?.last_ts
      ? new Date(new Date(st.last_ts).getTime() - 5 * 60 * 1000).toISOString()
      : isoMinus(24 * 60);

    // 2) подкачка страниц с лимитом и паузой
    let cursor: string | undefined;
    const all: any[] = [];
    let pages = 0;
    do {
      const page = await fetchRecentCasts(since, cursor);
      if (Array.isArray(page.casts) && page.casts.length) {
        all.push(...page.casts);
      }
      cursor = page.cursor;
      pages += 1;
      if (pages >= PAGE_LIMIT || !cursor) break;
      await sleep(SLEEP_MS);
    } while (true);

    if (all.length === 0) {
      return NextResponse.json({ upserted: 0, pagesFetched: pages, note: "no new casts" });
    }

    // 3) реакции — ТОЛЬКО для новых hash, которых нет в top_casts
    // соберём список уникальных hash
    const uniq = Array.from(new Set(all.map((c: any) => c.hash)));
    // спросим БД, какие уже есть
    const placeholders = uniq.map((_, i) => `$${i + 1}`).join(",");
    const existing = await sql.unsafe(
      `select cast_hash from top_casts where cast_hash in (${placeholders})`,
      uniq
    );
    const existingSet = new Set(existing.map((r: any) => r.cast_hash));
    const onlyNew = uniq.filter((h) => !existingSet.has(h));

    const counts: any[] = [];
    for (let i = 0; i < onlyNew.length; i += REACTION_BATCH) {
      const batch = onlyNew.slice(i, i + REACTION_BATCH);
      const cc = await fetchReactionCounts(batch);
      counts.push(...cc);
      if (i + REACTION_BATCH < onlyNew.length) await sleep(SLEEP_MS);
    }

    // 4) мерж + score
    const byHash = new Map(all.map((c: any) => [c.hash, c]));
    const rows = counts.map((r: any) => {
      const c = byHash.get(r.cast_hash) || {};
      const likes = r.likes ?? 0;
      const recasts = r.recasts ?? 0;
      const replies = r.replies ?? 0;
      const score = replies * 10 + recasts * 3 + likes;
      return {
        cast_hash: r.cast_hash,
        fid: c?.author?.fid ?? null,
        text: c?.text ?? "",
        channel: c?.channel?.id ?? null,
        timestamp: c?.timestamp ?? new Date().toISOString(),
        likes, recasts, replies, score
      };
    });

    // 5) upsert
    if (rows.length) {
      const values = rows.map((_, i) =>
        `($${i*9+1}, $${i*9+2}, $${i*9+3}, $${i*9+4}, $${i*9+5}, $${i*9+6}, $${i*9+7}, $${i*9+8}, $${i*9+9})`
      ).join(",");
      const params = rows.flatMap(r => [
        r.cast_hash, r.fid, r.text, r.channel, r.timestamp, r.likes, r.recasts, r.replies, r.score
      ]);

      await sql.unsafe(
        `
        insert into top_casts
        (cast_hash, fid, text, channel, timestamp, likes, recasts, replies, score)
        values ${values}
        on conflict (cast_hash) do update set
          fid = excluded.fid,
          text = excluded.text,
          channel = excluded.channel,
          timestamp = excluded.timestamp,
          likes = excluded.likes,
          recasts = excluded.recasts,
          replies = excluded.replies,
          score = excluded.score
        `,
        params
      );
    }

    // 6) обновим маркеры в ingest_state
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
      scanned: all.length,
      newHashes: onlyNew.length,
      skippedExisting: uniq.length - onlyNew.length
    });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
