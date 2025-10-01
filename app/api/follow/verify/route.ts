import { NextResponse } from "next/server";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const viewer = searchParams.get("viewer");
    const target = searchParams.get("target");
    if (!viewer || !target) {
      return NextResponse.json({ error: "Bad params" }, { status: 400 });
    }

    // Популярные у Neynar варианты (поменяйте на ваш рабочий, если точно знаете):
    // 1) /user/relationships?fid=<viewer>&target_fid=<target>
    // 2) /user/following?fid=<viewer>&target_fid=<target>
    // Сначала пробуем relationships:
    const tryFetch = async (path: string) => {
      const u = new URL(`${API}${path}`);
      u.searchParams.set("fid", viewer);
      u.searchParams.set("target_fid", target);
      const r = await fetch(u, {
        headers: { "x-api-key": KEY, accept: "application/json" },
      });
      const text = await r.text();
      let json: any = null;
      try { json = JSON.parse(text); } catch {}
      return { ok: r.ok, json, text };
    };

    let following = false;

    // relationships
    {
      const { ok, json } = await tryFetch("/user/relationships");
      if (ok && json) {
        // Попытка вытащить флаг из разных возможных полей
        const res = json.result ?? json;
        following = !!(
          res?.is_following ?? res?.following ?? res?.viewer_follows_target
        );
      }
    }

    // если ещё не поняли — попробуем /user/following
    if (!following) {
      const { ok, json } = await tryFetch("/user/following");
      if (ok && json) {
        const res = json.result ?? json;
        following = !!(
          res?.is_following ?? res?.following ?? res?.viewer_follows_target
        );
      }
    }

    return NextResponse.json({ following });
  } catch (e: any) {
    return NextResponse.json(
      { following: false, error: String(e?.message || e) },
      { status: 200 }
    );
  }
}
