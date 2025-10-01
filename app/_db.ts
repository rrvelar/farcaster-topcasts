import postgres from "postgres";

// Обязательно поставь в Vercel переменную окружения DATABASE_URL.
// Для Supabase берём Transaction pooler (порт 6543) + sslmode=require.
const url = process.env.DATABASE_URL!;
export const sql = postgres(url, { ssl: "require" });
