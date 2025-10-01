import { NextResponse } from "next/server";
import { sql } from "../../_db";

export const revalidate = 0;

export async function GET() {
  try {
    const hasKey = !!process.env.NEYNAR_API_KEY;
    // Пробуем простейший запрос в БД (без таблиц)
    const now = await sql.unsafe("select now()");
    return NextResponse.json({
      ok: true,
      env: {
        NEYNAR_API_KEY: hasKey ? "set" : "missing",
        DATABASE_URL: process.env.DATABASE_URL ? "set" : "missing"
      },
      dbNow: now?.[0]?.now ?? null
    });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message || e) }, { status: 500 });
  }
}
