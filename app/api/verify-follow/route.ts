// app/api/verify-follow/route.ts
import { NextResponse } from "next/server";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const PROMO_FID = Number(process.env.NEXT_PUBLIC_PROMO_FID || "0");

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });
    if (!PROMO_FID) return NextResponse.json({ error: "PROMO_FID missing" }, { status: 500 });

    const url = new URL(req.url);
    const viewer = Number(url.searchParams.get("viewer_fid") || "0");
    if (!viewer) return NextResponse.json({ error: "viewer_fid required" }, { status: 400 });

    // --- ВАРИАНТ 1 (точечная проверка отношения):
    // Многие используют endpoint вида /user/follows?fid=..&target_fid=..
    // Если в вашей версии Neynar этот путь другой — см. комментарий ниже (ВАРИАНТ 2).
    const u = new URL(`${API}/user/follows`);
    u.searchParams.set("fid", String(viewer));
    u.searchParams.set("target_fid", String(PROMO_FID));

    const r = await fetch(u, {
      headers: { "x-api-key": KEY, accept: "application/json" }
    });

    if (r.status === 404) {
      // --- ВАРИАНТ 2 (альтернатива, если у вас другой путь):
      // const alt = new URL(`${API}/user/relation`);
      // alt.searchParams.set("fid", String(viewer));
      // alt.searchParams.set("target_fid", String(PROMO_FID));
      // const rr = await fetch(alt, { headers: { "x-api-key": KEY, accept: "application/json" } });
      // if (!rr.ok) return NextResponse.json({ ok:false, status: rr.status, note:"relation endpoint" }, { status: 200 });
      // const jd = await rr.json();
      // const following = !!(jd?.result?.following || jd?.data?.following);
      // return NextResponse.json({ ok: true, following });

      // Если первый endpoint вернул 404 и вы не включили альтернативу, просто отвечаем «не подписан».
      return NextResponse.json({ ok: true, following: false });
    }

    if (!r.ok) {
      const txt = await r.text();
      return NextResponse.json({ ok: false, status: r.status, error: txt }, { status: 200 });
    }

    const data = await r.json();
    // Пытаемся понять «following» из разных возможных форматов ответа
    const following =
      !!(data?.result?.following ?? data?.data?.following ?? data?.following ?? false);

    return NextResponse.json({ ok: true, following });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message || e) }, { status: 200 });
  }
}
