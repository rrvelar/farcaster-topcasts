import { NextResponse } from "next/server";
import { sql } from "../../_db";

export const revalidate = 0;

function whereByRange(range: string) {
  switch (range) {
    case "today":
      return `timestamp >= date_trunc('day', now())`;
    case "yesterday":
      return `timestamp >= date_trunc('day', now()) - interval '1 day'
              and timestamp < date_trunc('day', now())`;
    case "7d":
      return `timestamp >= now() - interval '7 days'`;
    default:
      return `timestamp >= now() - interval '24 hours'`;
  }
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const metric = (searchParams.get("metric") ?? "score").replace(/[^a-z]/g, "");
  const range = (searchParams.get("range") ?? "24h").replace(/[^a-z0-9]/g, "");
  const allowed = ["score","replies","likes","recasts"];
  const order = allowed.includes(metric) ? metric : "score";
  const where = whereByRange(range);

  const q = `
    select cast_hash, fid, text, channel, timestamp, likes, recasts, replies, score
    from top_casts
    where ${where}
    order by ${order} desc
    limit 3000
  `;
  const rows = await sql.unsafe(q);
  return NextResponse.json({ items: rows });
}
