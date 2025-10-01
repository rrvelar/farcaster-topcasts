import { NextResponse } from "next/server";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;

function sinceISO(hours = 24) {
  const d = new Date();
  d.setUTCHours(d.getUTCHours() - hours, 0, 0, 0);
  return d.toISOString();
}

async function fetchRecentCasts(cursor?: string) {
  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", "");
  u.searchParams.set("limit", "100");
  u.searchParams.set("since", sinceISO(24));
  if (cursor) u.searchParams.set("cursor", cursor);

  const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Neynar cast/search ${r.status}: ${text}`);
  }
  return r.json() as Promise<{ casts: any[]; cursor?: string }>;
}

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
  return r.json() as Promise<{ counts: Array<{ cast_hash: string; likes: number; recasts: number; replies: number }> }>;
}

export const revalidate = 0;

export async function GET() {
  try {
    if (!KEY) {
      return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });
    }

    let cursor: string | undefined;
    const all: any[] = [];
    do {
      const page = await fetchRecentCasts(cursor);
      all.push(...page.casts);
      cursor = page.cursor;
    } while (cursor);

    const B = 100;
    const counts: any[] = [];
    for (let i = 0; i < all.length; i += B) {
      const batch = all.slice(i, i + B).map((c) => c.hash);
      if (batch.length === 0) continue;
      const { counts: cc } = await fetchReactionCounts(batch);
      counts.push(...cc);
    }

    const byHash = new Map(all.map((c) => [c.hash, c]));
    const rows = counts.map((r) => {
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

    return NextResponse.json({ upserted: rows.length });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
