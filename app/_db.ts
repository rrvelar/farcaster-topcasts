import postgres from "postgres";

const url = process.env.DATABASE_URL!;
export const sql = postgres(url, { ssl: "require" });
