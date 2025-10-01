import { NextResponse } from "next/server";
import { sql } from "../../_db";

export const revalidate = 0;

function whereByRange(range: string) {
  switch (range) {
    case "today": return `timestamp >= date_trunc('day', now())`;
    case "yesterday": return `timestamp >= date_trunc('day', now()) - interval '1 day' and timestamp < date_trunc('day', now())`;
    case "7d": return `timestamp >= now() - interval '7 days'`;
    default: return `timestamp >= now() - interval '24 hours'`;
  }
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const metric = (searchParams.get("metric") ?? "replies").replace(/[^a-z]/g, "");
  const range = (searchParams.get("range") ?? "24h").replace(/[^a-z0-9]/g, "");
  const limit = Math.min(50, Math.max(1, Number(searchParams.get("limit") ?? 15)));
  const order = ["likes","recasts","replies","score"].includes(metric) ? metric : "replies";
  const where = whereByRange(range);

  const q = `
    with top as (
      select cast_hash, fid, text, channel, timestamp, likes, recasts, replies, score
      from top_casts
      where ${where}
        and coalesce(length(text),0) > 0
        and text !~* '(you just received 1,004 claps|join the fun|swipe right to clap|explore content)'
      order by ${order} desc, timestamp desc
      limit $1
    )
    select t.*, u.username, u.display_name, u.pfp_url
    from top t
    left join fc_users u on u.fid = t.fid
  `;
  const rows = await sql.unsafe(q, [limit]);

  return NextResponse.json({ items: rows });
}
