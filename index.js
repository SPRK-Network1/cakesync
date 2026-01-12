import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ─────────────────────────────────────────────
// ENV
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
// SUPABASE
// ─────────────────────────────────────────────
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
const AFFILIATE_ID = "208330";
const START_DATE = new Date("2025-12-01");
const WINDOW_DAYS = 28;
const SNAPSHOT_DATE = "2026-01-04";

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// yesterday only (CAKE completed data)
const today = new Date();
today.setDate(today.getDate() - 1);

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function toISO(d) {
  return d.toISOString().split("T")[0];
}

function addDays(d, days) {
  const x = new Date(d);
  x.setDate(x.getDate() + days);
  return x;
}

function minDate(a, b) {
  return a < b ? a : b;
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function run() {
  const totals = new Map();

  let cursor = new Date(START_DATE);

  while (cursor <= today) {
    const windowStart = new Date(cursor);
    const windowEnd = minDate(
      addDays(cursor, WINDOW_DAYS - 1),
      today
    );

    const url =
      "https://login.affluentco.com/affiliates/api/Reports/SubAffiliateSummary" +
      `?api_key=${CAKE_API_KEY}` +
      `&affiliate_id=${AFFILIATE_ID}` +
      `&start_date=${toISO(windowStart)}` +
      `&end_date=${toISO(windowEnd)}` +
      `&format=json`;

    console.log(`Fetching CAKE: ${toISO(windowStart)} → ${toISO(windowEnd)}`);

    const res = await fetch(url, {
      headers: { Accept: "application/json" },
    });

    if (!res.ok) {
      throw new Error(await res.text());
    }

    const json = await res.json();

    for (const r of json.data || []) {
      if (!r.sub_id || !SPARK_ID_REGEX.test(String(r.sub_id))) continue;

      const key = String(r.sub_id);

      const prev = totals.get(key) || {
        clicks: 0,
        conversions: 0,
        revenue: 0,
        payout: 0,
      };

      totals.set(key, {
        clicks: prev.clicks + Number(r.clicks ?? 0),
        conversions: prev.conversions + Number(r.conversions ?? 0),
        revenue: prev.revenue + Number(r.revenue ?? 0),
        payout: prev.payout + Number(r.events ?? 0),
      });
    }

    cursor = addDays(windowEnd, 1);
  }

  if (!totals.size) {
    console.log("No SPK rows found at all");
    return;
  }

  const rows = Array.from(totals.entries()).map(
    ([sparkId, v]) => ({
      cake_affiliate_id: sparkId,
      date: SNAPSHOT_DATE,
      clicks: v.clicks,
      conversions: v.conversions,
      revenue: v.revenue,
      payout: v.payout,
    })
  );

  const { error } = await supabase
    .from("cake_earnings_daily")
    .upsert(rows, {
      onConflict: "cake_affiliate_id,date",
    });

  if (error) throw error;

  console.log(`✔ Synced ${rows.length} SPK lifetime rows`);
}

run()
  .then(() => process.exit(0))
  .catch(err => {
    console.error("❌ Sync failed:", err);
    process.exit(1);
  });

