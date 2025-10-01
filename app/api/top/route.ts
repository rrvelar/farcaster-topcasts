// app/api/top/route.ts
import { NextResponse } from "next/server";
import { sql } from "../../_db";

/**
 * /api/top?by=likes|replies|recasts|score&limit=15&hours=24
 * Возвращает топ кастов за окно часов с данными автора.
 */
export const revalidate = 0;

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const by = (url.searchParams.get("by") || "replies").toLowerCase();
    const limit = Math.max(1, Math.min(50, Number(url.searchParams.get("limit") || "15")));
    const hours = Math.max(1, Math.min(168, Number(url.searchParams.get("hours") || "24")));

    const allowed = new Set(["likes", "recasts", "replies", "score"]);
    const orderCol = allowed.has(by) ? by : "replies";

    const rows = await sql.unsafe(
      `
      select
        t.cast_hash,
        t.fid,
        t.text,
        t.channel,
        t.timestamp,
        t.likes,
        t.recasts,
        t.replies,
        coalesce(t.score, (coalesce(t.replies,0)*10 + coalesce(t.recasts,0)*3 + coalesce(t.likes,0))) as score,
        u.username,
        u.display_name,
        u.pfp_url
      from top_casts t
      left join users u on u.fid = t.fid
      where t.timestamp >= now() - interval '${hours} hours'
      order by ${orderCol} desc nulls last, timestamp desc
      limit ${limit}
      `
    );

    return NextResponse.json({ items: rows, orderBy: orderCol, hours, count: rows.length });
  } catch (e: any) {
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
