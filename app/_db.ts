import postgres from "postgres";

// Для Supabase используй строку вида postgresql://...:6543/... с sslmode=require
const url = process.env.DATABASE_URL!;
export const sql = postgres(url, { ssl: "require" });
