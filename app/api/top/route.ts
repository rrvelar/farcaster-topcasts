// app/api/top/route.ts
import { NextResponse } from "next/server";
import { sql } from "../../_db";

/**
 * GET /api/top
 * Параметры:
 *  - metric: likes | replies | recasts | score (по умолчанию replies)
 *  - range:  24h | today | yesterday | 7d   (по умолчанию 24h)
 *  - limit:  1..50 (по умолчанию 15)
 *
 * Возвращает топ кастов с данными автора (LEFT JOIN users).
 */
export const revalidate = 0;

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);

    // Совместимость: поддержим metric/by и range/hours
    const metricParam = (url.searchParams.get("metric") || url.searchParams.get("by") || "replies").toLowerCase();
    const rangeParamRaw = url.searchParams.get("range");
    const hoursParam = url.searchParams.get("hours"); // если вдруг шлёшь hours как раньше

    const limit = Math.max(1, Math.min(50, Number(url.searchParams.get("limit") || "15")));

    const allowedMetrics = new Set(["likes", "replies", "recasts", "score"]);
    const orderCol = allowedMetrics.has(metricParam) ? metricParam : "replies";

    // WHERE по диапазону
    let whereClause = "";
    if (hoursParam) {
      // Явное окно в часах, если передано ?hours=...
      const h = Math.max(1, Math.min(168, Number(hoursParam)));
      whereClause = `t.timestamp >= now() - interval '${h} hours'`;
    } else {
      const range = (rangeParamRaw || "24h").toLowerCase();
      switch (range) {
        case "today":
          // с начала суток до сейчас (UTC в Supabase)
          whereClause = `t.timestamp >= date_trunc('day', now())`;
          break;
        case "yesterday":
          // предыдущие сутки [00:00; 24:00)
          whereClause = `
            t.timestamp >= (date_trunc('day', now()) - interval '1 day')
            and t.timestamp <  date_trunc('day', now())
          `;
          break;
        case "7d":
          whereClause = `t.timestamp >= now() - interval '7 days'`;
          break;
        case "24h":
        default:
          whereClause = `t.timestamp >= now() - interval '24 hours'`;
          break;
      }
    }

    // Сам запрос. ORDER BY — строго whitelisted колонкой.
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
        -- если в схеме score generated, это не нужно; но coalesce не навредит
        coalesce(t.score, (coalesce(t.replies,0)*10 + coalesce(t.recasts,0)*3 + coalesce(t.likes,0))) as score,
        u.username,
        u.display_name,
        u.pfp_url
      from top_casts t
      left join users u on u.fid = t.fid
      where ${whereClause}
      order by ${orderCol} desc nulls last, t.timestamp desc
      limit $1
      `,
      [limit]
    );

    return NextResponse.json({
      items: rows,
      orderBy: orderCol,
      limit,
      range: rangeParamRaw || (hoursParam ? `${hoursParam}h` : "24h"),
      count: rows.length,
    });
  } catch (e: any) {
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
