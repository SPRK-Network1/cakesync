import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ─────────────────────────────────────────────
// ENV VARS (SET THESE IN RENDER)
// ─────────────────────────────────────────────
const {
  CAKE_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
} = process.env;

if (!CAKE_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing environment variables");
}

// ─────────────────────────────────────────────
// SUPABASE CLIENT
// ─────────────────────────────────────────────
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
const AFFILIATE_ID = "208330";
const START_DATE = "2025-12-03";
const SNAPSHOT_DATE = "1970-01-01";
const SPARK_ID_REGEX = /^SPK-\d{4}-\d{4}$/;

// ─────────────────────────────────────────────
// 🔒 CAKE-SAFE END DATE (NOW − 48 HOURS)
// ─────────────────────────────────────────────
const twoDaysAgo = new Date(Date.now() - 48 * 60 * 60 * 1000);
const END_DATE = twoDaysAgo.toISOString().split("T")[0];

// ─────────────────────────────────────────────
// CAKE REQUEST (KNOWN-GOOD CONTRACT)
// ─────────────────────────────────────────────
const url =
  "https://login.affluentco.com/affiliates/api/Reports/SubAffiliateSummary" +
  `?api_key=${CAKE_API_KEY}` +
  `&affiliate_id=${AFFILIATE_ID}` +
  `&start_date=${START_DATE}` +
  `&end_date=${END_DATE}` +
  `&format=json`;

console.log("Fetching CAKE:", url);

const res = await fetch(url, {
  headers: {
    "Accept": "application/json",
    "User-Agent": "render-cron",
  },
});

if (!res.ok) {
  const text = await res.text();
  throw new Error(`CAKE HTTP ${res.status}: ${text}`);
}

const json = await res.json();

if (!Array.isArray(json.data)) {
  throw new Error("Unexpected CAKE response shape");
}

// ─────────────────────────────────────────────
// TRANSFORM → DB ROWS
// ─────────────────────────────────────────────
const rows = json.data
  .filter(
    r =>
      r.sub_id &&
      SPARK_ID_REGEX.test(String(r.sub_id))
  )
  .map(r => ({
    cake_affiliate_id: String(r.sub_id),
    date: SNAPSHOT_DATE,
    clicks: Number(r.clicks ?? 0),
    conversions: Number(r.conversions ?? 0),
    revenue: Number(r.revenue ?? 0),
    payout: Number(r.events ?? 0),
  }));

if (rows.length === 0) {
  console.log("No SPK rows found. Done.");
  process.exit(0);
}

// ─────────────────────────────────────────────
// UPSERT (user_id preserved)
// ─────────────────────────────────────────────
const { error } = await supabase
  .from("cake_earnings_daily")
  .upsert(rows, {
    onConflict: "cake_affiliate_id,date",
  });

if (error) {
  throw error;
}

console.log(`✔ Synced ${rows.length} SPK lifetime rows`);
