import { NextResponse } from "next/server";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;

// --- настройки лимитов ---
const PAGE_LIMIT = 20;          // максимум страниц cast/search за один запуск (20*100 = 2000 кастов)
const SLEEP_MS = 1100;          // пауза ~1.1s между вызовами, чтобы уважать 60/60s
const REACTION_BATCH = 100;     // батч для reaction/counts

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function isoMinus(minutes: number) {
  const d = new Date();
  d.setUTCMinutes(d.getUTCMinutes() - minutes, 0, 0);
  return d.toISOString();
}

async function getLastTs(): Promise<string | null> {
  const rows = await sql.unsafe(
    `select last_ts from ingest_state where id = 'casts' limit 1`
  );
  return rows?.[0]?.last_ts ?? null;
}

async function setLastTs(ts: string) {
  await sql.unsafe(
    `
    insert into ingest_state (id, last_ts, updated_at)
    values ('casts', $1, now())
    on conflict (id) do update set last_ts = excluded.last_ts, updated_at = now()
    `,
    [ts]
  );
}

// ---- fetch: casts ----
async function fetchRecentCasts(sinceISO: string, cursor?: string) {
  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", "*");       // любые касты
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
  const casts = Array.isArray(result?.casts)
    ? result.casts
    : Array.isArray(result?.messages)
      ? result.messages
      : [];
  const nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;

  return { casts, cursor: nextCursor } as { casts: any[]; cursor?: string };
}

// ---- fetch: reaction counts ----
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
  const counts =
    Array.isArray(data?.counts) ? data.counts :
    Array.isArray(data?.result?.counts) ? data.result.counts :
    [];
  return counts as Array<{ cast_hash: string; likes: number; recasts: number; replies: number }>;
}

export const revalidate = 0;

export async function GET() {
  try {
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // 0) определяем окно: берём всё после последнего run, с 5-минутным буфером
    const lastTs = await getLastTs();
    const since = lastTs ? new Date(new Date(lastTs).getTime() - 5 * 60 * 1000).toISOString()
                         : isoMinus(24 * 60); // если первый запуск — последние 24 часа

    // 1) тянем страницы с ограничением по PAGE_LIMIT и с паузами
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

    // если совсем пусто — выходим красиво
    if (all.length === 0) {
      return NextResponse.json({ upserted: 0, note: "no new casts" });
    }

    // 2) получаем реакции (с паузой между батчами)
    const counts: any[] = [];
    for (let i = 0; i < all.length; i += REACTION_BATCH) {
      const batch = all.slice(i, i + REACTION_BATCH).map((c: any) => c.hash);
      if (batch.length === 0) continue;
      const cc = await fetchReactionCounts(batch);
      counts.push(...cc);
      if (i + REACTION_BATCH < all.length) await sleep(SLEEP_MS);
    }

    // 3) мерж + score
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

    // 4) upsert
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

    // 5) обновим маркер last_ts — возьмём максимальный timestamp из полученных
    const maxTs = rows.reduce<string | null>((acc, r: any) => {
      const t = r.timestamp ? new Date(r.timestamp).toISOString() : null;
      if (!t) return acc;
      if (!acc || new Date(t) > new Date(acc)) return t;
      return acc;
    }, lastTs);
    if (maxTs) await setLastTs(maxTs);

    return NextResponse.json({ upserted: rows.length, pagesFetched: Math.min(pages, PAGE_LIMIT) });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
