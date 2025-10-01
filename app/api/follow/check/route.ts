// app/api/follow/check/route.ts
import { NextResponse } from "next/server";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;

/**
 * GET /api/follow/check?viewer=123&target=456
 * Возвращает { ok:true, following:boolean }
 */
export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const viewer = Number(url.searchParams.get("viewer") || "0");
    const target = Number(url.searchParams.get("target") || "0");

    if (!viewer || !target) {
      return NextResponse.json(
        { ok: false, error: "bad_params" },
        { status: 400 }
      );
    }
    if (!KEY) {
      return NextResponse.json(
        { ok: false, error: "missing_neynar_key" },
        { status: 500 }
      );
    }

    // Пытаемся использовать relation-эндпоинт Neynar
    // (в разных версиях поля могут называться по-разному — учитываем варианты)
    const u = new URL(`${API}/user/relation`);
    u.searchParams.set("fid", String(viewer));
    u.searchParams.set("target_fid", String(target));

    const r = await fetch(u, {
      headers: { "x-api-key": KEY, accept: "application/json" },
      cache: "no-store",
    });

    if (!r.ok) {
      const t = await r.text();
      return NextResponse.json(
        { ok: false, error: `neynar ${r.status}: ${t}` },
        { status: 502 }
      );
    }

    const data = await r.json();
    // Возможные формы ответа:
    // data.result.relation.following === true
    // data.relation.following === true
    // data.is_following === true
    const following = !!(
      data?.result?.relation?.following ??
      data?.relation?.following ??
      data?.is_following
    );

    return NextResponse.json({ ok: true, following });
  } catch (e: any) {
    return NextResponse.json(
      { ok: false, error: String(e?.message || e) },
      { status: 500 }
    );
  }
}
