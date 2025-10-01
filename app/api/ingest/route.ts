import { NextResponse } from "next/server";
import { headers } from "next/headers";
import { sql } from "../../_db";

const API = "https://api.neynar.com/v2/farcaster";
const KEY = process.env.NEYNAR_API_KEY!;
const MANUAL_TOKEN = process.env.INGEST_TOKEN || "";

// языки (через запятую). Пример: "en,ru,es". Пусто => без фильтра
const TOP_LANGS = (process.env.TOP_LANGS || "en,ru")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// --- лимиты и тайминги ---
const PAGE_LIMIT = 3;                 // обычный инкрементальный прогон
const BACKFILL_PAGE_LIMIT = 10;       // глубина при hours>0 без slicing
const SLEEP_MS = 1500;                // пауза между запросами
const BULK_BATCH = 100;               // размер батча для /casts
const MIN_INTERVAL_MIN = 15;          // анти-частота
const SLICE_MINUTES_DEFAULT = Number(process.env.SLICE_MINUTES || 60); // ширина временного слайса при hours>0
const SLICE_PAGES_PER_CHUNK = Number(process.env.SLICE_PAGES || 2);    // страниц на один слайс

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }
function isoMinus(minutes: number) {
  const d = new Date(); d.setUTCMinutes(d.getUTCMinutes() - minutes, 0, 0); return d.toISOString();
}

async function getState() {
  const rows = await sql.unsafe(`select last_ts, updated_at from ingest_state where id='casts' limit 1`);
  return rows?.[0] ?? null;
}
async function upsertState(fields: { last_ts?: string; updated_at?: string }) {
  const last_ts = fields.last_ts ?? null;
  const updated_at = fields.updated_at ?? new Date().toISOString();
  await sql.unsafe(`
    insert into ingest_state (id, last_ts, updated_at)
    values ('casts', $1, $2)
    on conflict (id) do update set
      last_ts = coalesce(excluded.last_ts, ingest_state.last_ts),
      updated_at = excluded.updated_at
  `, [last_ts, updated_at]);
}

// ---- один шаг поиска (страница) ----
async function searchPage(params: {
  q: string;
  timeKey: "since" | "after";
  sinceISO: string;
  cursor?: string;
  sort: "desc_chron" | "chron" | "algorithmic";
}) {
  const u = new URL(`${API}/cast/search`);
  u.searchParams.set("q", params.q);
  u.searchParams.set("limit", "100");
  u.searchParams.set("sort_type", params.sort);
  u.searchParams.set(params.timeKey, params.sinceISO);
  if (params.cursor) u.searchParams.set("cursor", params.cursor);

  const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`Neynar cast/search (${params.timeKey}, q=${params.q}, sort=${params.sort}) ${r.status}: ${txt}`);
  }

  const data = await r.json();
  const result = data?.result ?? data;
  const casts = Array.isArray(result?.casts) ? result.casts :
                Array.isArray(result?.messages) ? result.messages : [];
  const nextCursor = result?.next?.cursor ?? result?.cursor ?? undefined;
  return { casts, cursor: nextCursor };
}

// ---- умный поиск: несколько q + since/after ----
async function searchSmart(sinceISO: string, cursor?: string, qOverride?: string, sort: "desc_chron" | "chron" | "algorithmic" = "desc_chron") {
  const qLang = TOP_LANGS.length > 0 ? TOP_LANGS.map((l) => `lang:${l}`).join(" OR ") : "*";
  const baseQ = (qOverride?.trim() || qLang).replace(/\s+/g, " ").trim();

  // фан-аут по запросам
  const queries = [
    baseQ,
    `${baseQ} has:replies`,
    `${baseQ} has:likes`,
    `${baseQ} has:recasts`,
    "*",
  ].map(q => q.replace(/\s+/g, " ").trim())
   .filter((q, i, arr) => q && arr.indexOf(q) === i);

  const timeKeys: Array<"since" | "after"> = ["since", "after"];

  for (const q of queries) {
    for (const timeKey of timeKeys) {
      const page = await searchPage({ q, timeKey, sinceISO, cursor, sort });
      if (page.casts.length > 0 || cursor) {
        return { ...page, qUsed: q, timeKeyUsed: timeKey };
      }
      if (cursor) return { ...page, qUsed: q, timeKeyUsed: timeKey }; // при пагинации пусто — выходим
    }
  }
  return { casts: [] as any[], cursor: undefined, qUsed: baseQ, timeKeyUsed: "since" as const };
}

// ---- батч деталей кастов ----
async function fetchBulkCasts(hashes: string[]) {
  const out: any[] = [];
  for (let i = 0; i < hashes.length; i += BULK_BATCH) {
    const chunk = hashes.slice(i, i + BULK_BATCH);
    if (chunk.length === 0) break;

    const u = new URL(`${API}/casts`);
    u.searchParams.set("casts", chunk.join(","));
    const r = await fetch(u, { headers: { "x-api-key": KEY, accept: "application/json" } });
    if (!r.ok) throw new Error(`Neynar casts ${r.status}: ${await r.text()}`);

    const data = await r.json();
    const result = data?.result ?? data;
    const casts = Array.isArray(result?.casts) ? result.casts : [];
    out.push(...casts);

    if (i + BULK_BATCH < hashes.length) await sleep(SLEEP_MS);
  }
  return out;
}

export const revalidate = 0;

export async function GET(req: Request) {
  try {
    const h = headers();
    const isCron = h.get("x-vercel-cron") === "1";
    const url = new URL(req.url);
    const token = url.searchParams.get("token");
    const force = url.searchParams.get("force") === "1";

    // параметры
    const hours = Math.max(0, Math.min(168, Number(url.searchParams.get("hours") || "0"))); // 0 => инкремент
    const includeExisting = url.searchParams.get("includeExisting") === "1";
    const pageLimitOverride = Number(url.searchParams.get("pages") || "0");
    const qOverride = url.searchParams.get("q") || undefined;
    const sliceMinutes = Math.max(15, Math.min(240, Number(url.searchParams.get("slice") || SLICE_MINUTES_DEFAULT)));

    if (!isCron && token !== MANUAL_TOKEN) {
      return NextResponse.json({ error: "forbidden" }, { status: 403 });
    }
    if (!KEY) return NextResponse.json({ error: "NEYNAR_API_KEY missing" }, { status: 500 });

    // анти-частота
    const st = await getState();
    if (!force && st?.updated_at) {
      const diffMin = (Date.now() - new Date(st.updated_at).getTime()) / 60000;
      if (diffMin < MIN_INTERVAL_MIN) {
        return NextResponse.json({ skipped: true, reason: "too_soon", lastRunMinAgo: Number(diffMin.toFixed(1)) });
      }
    }
    await upsertState({ updated_at: new Date().toISOString() });

    // базовое окно
    const since = hours
      ? isoMinus(hours * 60)
      : (st?.last_ts
          ? new Date(new Date(st.last_ts).getTime() - 5 * 60 * 1000).toISOString()
          : isoMinus(24 * 60));

    const found: any[] = [];
    let pagesFetched = 0;

    // --- РЕЖИМ 1: SLICING, когда hours > 0 ---
    if (hours > 0) {
      const end = new Date();                   // now
      const start = new Date(end.getTime() - hours * 60 * 60 * 1000);
      // идём по кускам от старых к новым, сортировка = chron (возрастающая)
      for (let t = start.getTime(); t < end.getTime(); t += sliceMinutes * 60 * 1000) {
        const sliceStart = new Date(t);
        const sliceEnd = new Date(Math.min(t + sliceMinutes * 60 * 1000, end.getTime()));
        let cursor: string | undefined = undefined;
        let pages = 0;

        // на слайсах используем сортировку chron (от старых к новым), чтобы быстрее достучаться до конца слайса
        do {
          const page = await searchSmart(sliceStart.toISOString(), cursor, qOverride, "chron");
          if (page.casts?.length) {
            // отфильтруем по границе слайса
            const inWindow = page.casts.filter((c: any) => {
              const ts = new Date(c.timestamp).getTime();
              return ts >= sliceStart.getTime() && ts <= sliceEnd.getTime();
            });
            found.push(...inWindow);
          }
          cursor = page.cursor;
          pages += 1;
          pagesFetched += 1;

          // условия остановки: вышли за лимит страниц на слайс, или курсор закончился,
          // или самая новая запись страницы уже старше конца слайса (для chron это редкость).
          const stopForLimit = pages >= SLICE_PAGES_PER_CHUNK;
          const stopForCursor = !cursor;
          if (stopForLimit || stopForCursor) break;
          await sleep(SLEEP_MS);
        } while (true);

        // пауза между слайсами
        await sleep(SLEEP_MS);
      }
    } else {
      // --- РЕЖИМ 2: Обычный инкрементальный прогон ---
      let cursor: string | undefined;
      const maxPages =
        pageLimitOverride > 0
          ? Math.min(pageLimitOverride, 15)
          : BACKFILL_PAGE_LIMIT; // делаем его глубже, чтобы не терять страницы
      do {
        const page = await searchSmart(since, cursor, qOverride, "desc_chron");
        if (page.casts?.length) found.push(...page.casts);
        cursor = page.cursor;
        pagesFetched += 1;
        if (pagesFetched >= maxPages || !cursor) break;
        await sleep(SLEEP_MS);
      } while (true);
    }

    if (found.length === 0) {
      return NextResponse.json({ upserted: 0, pagesFetched, note: "no casts", since });
    }

    // уникальные хэши
    const uniq = Array.from(new Set(found.map((c: any) => c.hash)));
    let toFetch = uniq;
    if (!includeExisting) {
      const placeholders = uniq.map((_, i) => `$${i + 1}`).join(",");
      const existing = await sql.unsafe(
        `select cast_hash from top_casts where cast_hash in (${placeholders})`,
        uniq
      );
      const existSet = new Set(existing.map((r: any) => r.cast_hash));
      toFetch = uniq.filter((h) => !existSet.has(h));
    }

    if (toFetch.length === 0) {
      return NextResponse.json({ upserted: 0, pagesFetched, requestedBulk: 0, skippedExisting: uniq.length, since, slices: hours > 0 ? Math.ceil(hours * 60 / sliceMinutes) : 0 });
    }

    // детали и счётчики
    const withCounts = await fetchBulkCasts(toFetch);

    // upsert (без score — он generated)
    const byHash = new Map(withCounts.map((c: any) => [c.hash, c]));
    const rows = toFetch.map((hash) => {
      const c = byHash.get(hash) || {};
      const likes = c?.reactions?.likes_count ?? 0;
      const recasts = c?.reactions?.recasts_count ?? 0;
      const replies = c?.replies?.count ?? 0;
      return {
        cast_hash: hash,
        fid: c?.author?.fid ?? null,
        text: c?.text ?? "",
        channel: c?.channel?.id ?? null,
        timestamp: c?.timestamp ?? new Date().toISOString(),
        likes, recasts, replies
      };
    }).filter(r => r.fid !== null);

    if (rows.length) {
      const values = rows.map((_, i) =>
        `($${i*8+1}, $${i*8+2}, $${i*8+3}, $${i*8+4}, $${i*8+5}, $${i*8+6}, $${i*8+7}, $${i*8+8})`
      ).join(",");
      const params = rows.flatMap(r => [
        r.cast_hash, r.fid, r.text, r.channel, r.timestamp, r.likes, r.recasts, r.replies
      ]);

      await sql.unsafe(
        `
        insert into top_casts
        (cast_hash, fid, text, channel, timestamp, likes, recasts, replies)
        values ${values}
        on conflict (cast_hash) do update set
          fid = excluded.fid,
          text = excluded.text,
          channel = excluded.channel,
          timestamp = excluded.timestamp,
          likes = greatest(excluded.likes, top_casts.likes),
          recasts = greatest(excluded.recasts, top_casts.recasts),
          replies = greatest(excluded.replies, top_casts.replies)
        `,
        params
      );
    }

    // маркер времени — максимум того, что залили (для инкремента)
    const st2 = await getState();
    const maxTs = rows.reduce<string | null>((acc, r: any) => {
      const t = r.timestamp ? new Date(r.timestamp).toISOString() : null;
      if (!t) return acc;
      if (!acc || new Date(t) > new Date(acc)) return t;
      return acc;
    }, st2?.last_ts ?? null);
    await upsertState({ last_ts: maxTs || st2?.last_ts || new Date().toISOString() });

    return NextResponse.json({
      upserted: rows.length,
      pagesFetched,
      scanned: found.length,
      requestedBulk: toFetch.length,
      skippedExisting: uniq.length - toFetch.length,
      since,
      slices: hours > 0 ? Math.ceil(hours * 60 / sliceMinutes) : 0
    });
  } catch (e: any) {
    console.error("INGEST ERROR:", e?.message || e);
    return NextResponse.json({ error: String(e?.message || e) }, { status: 500 });
  }
}
