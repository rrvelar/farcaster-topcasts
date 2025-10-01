import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const TOKEN = process.env.INGEST_TOKEN || "";
const BATCH = 100;

export const revalidate = 0;

async function fetchUsersOnce(fids: number[]): Promise<any[]> {
  // Попытка №1: GET /user/bulk?fids=...
  {
    const u = new URL(`${API}/user/bulk`);
    u.searchParams.set("fids", fids.join(","));
    const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    if (r.ok) {
      const data = await r.json();
      return data?.result?.users ?? data?.users ?? [];
    }
    // если 404/405 — переходим к POST варианту
    if (r.status >= 500) throw new Error(`user/bulk GET ${r.status}: ${await r.text()}`);
  }

  // Попытка №2: POST /user/bulk { fids: [...] }
  {
    const r = await fetch(`${API}/user/bulk`, {
      method: "POST",
      headers: {
        "x-api-key": KEY,
        "content-type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify({ fids }),
    });
    if (!r.ok) throw new Error(`user/bulk POST ${r.status}: ${await r.text()}`);
    const data = await r.json();
    return data?.result?.users ?? data?.users ?? [];
  }
}

async function fetchUsers(fids: number[]) {
  const out: any[] = [];
  for (let i = 0; i < fids.length; i += BATCH) {
    const chunk = fids.slice(i, i + BATCH);
    if (chunk.length === 0) break;
    const users = await fetchUsersOnce(chunk);
    out.push(...users);
    if (i + BATCH < fids.length) {
      await new Promise((res) => setTimeout(res, 1200));
    }
  }
  return out;
}

export async function GET(req: Request) {
  try {
    // защита: только крон или ручной запуск с токеном
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);
    const token = url.searchParams.get("token");
    if (!isCron && token !== TOKEN) return NextResponse.json({ error: "forbidden" }, { status: 403 });
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // берём FID'ы авторов из последних 7 дней
    const rows = await sql.unsafe(`
      select distinct fid from top_casts
      where fid is not null and timestamp >= now() - interval '7 days'
      limit 2000
    `);
    const fids = rows.map((r: any) => Number(r.fid)).filter(Boolean);
    if (fids.length === 0) return NextResponse.json({ updated: 0, note: "no fids" });

    const users = await fetchUsers(fids);
    if (users.length === 0) return NextResponse.json({ updated: 0, note: "no users returned" });

    const values = users.map((_, i) =>
      `($${i*4+1}, $${i*4+2}, $${i*4+3}, $${i*4+4})`
    ).join(",");
    const params = users.flatMap((u: any) => [
      u.fid,
      u.username || null,
      u.display_name || null,
      (u.pfp_url ?? u.profile?.pfp_url) || null,
    ]);

    await sql.unsafe(
      `
      insert into fc_users (fid, username, display_name, pfp_url)
      values ${values}
      on conflict (fid) do update set
        username = excluded.username,
        display_name = excluded.display_name,
        pfp_url = excluded.pfp_url,
        updated_at = now()
      `,
      params
    );

    return NextResponse.json({ updated: users.length });
  } catch (e: any) {
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
