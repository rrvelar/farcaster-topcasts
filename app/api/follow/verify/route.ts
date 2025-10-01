import { NextResponse } from "next/server";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;

// Пример для метода, который есть у Neynar: /user/following?fid=&target_fid=
// Если у вас другой — поправьте URL/параметры.
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const viewer = searchParams.get("viewer");
    const target = searchParams.get("target");
    if (!viewer || !target) return NextResponse.json({ error: "bad params" }, { status: 400 });

    const u = new URL(`${API}/user/following`);
    u.searchParams.set("fid", viewer);
    u.searchParams.set("target_fid", target);

    const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    if (!r.ok) return NextResponse.json({ following: false, status: r.status });

    const data = await r.json();
    // приведите к реальному полю из ответа Neynar
    const following = !!(data?.result?.following ?? data?.following);
    return NextResponse.json({ following });
  } catch (e: any) {
    return NextResponse.json({ following: false, error: String(e?.message || e) }, { status: 200 });
  }
}
