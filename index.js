import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ─────────────────────────────────────────────
// ENV VARS (SET IN RENDER)
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
// SUPABASE (SERVICE ROLE)
// ─────────────────────────────────────────────
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: { autoRefreshToken: false, persistSession: false },
  }
);

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
const AFFILIATE_ID = "208330";
const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

const WINDOW_DAYS = 28;

// Start of your system (CAKE-safe)
const START_OF_TIME = new Date("2023-12-03");

// CAKE only allows completed days → yesterday
const TODAY = new Date();
TODAY.setDate(TODAY.getDate() - 1);

// ─────────────────────────────────────────────
// DATE HELPERS
// ─────────────────────────────────────────────
function toISODate(d) {
  return d.toISOString().split("T")[0];
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function minDate(a, b) {
  return a < b ? a : b;
}

// ─────────────────────────────────────────────
// MAIN SYNC
// ─────────────────────────────────────────────
async function syncCakeAllTime() {
  let cursor = new Date(START_OF_TIME);

  while (cursor <= TODAY) {
    const windowStart = new Date(cursor);
    const windowEnd = minDate(
      addDays(cursor, WINDOW_DAYS - 1),
      TODAY
    );

    const START_DATE = toISODate(windowStart);
    const END_DATE = toISODate(windowEnd);

    const url =
      "https://login.affluentco.com/affiliates/api/Reports/SubAffiliateSummary" +
      `?api_key=${CAKE_API_KEY}` +
      `&affiliate_id=${AFFILIATE_ID}` +
      `&start_date=${START_DATE}` +
      `&end_date=${END_DATE}` +
      `&format=json`;

    console.log(`Fetching CAKE: ${START_DATE} → ${END_DATE}`);

    const res = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "render-cron",
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`CAKE HTTP ${res.status}: ${text}`);
    }

    const json = await res.json();

    if (!Array.isArray(json.data)) {
      console.warn("Unexpected CAKE response shape, skipping window");
      cursor = addDays(windowEnd, 1);
      continue;
    }

    const rows = json.data
      .filter(
        (r) =>
          r.sub_id &&
          SPARK_ID_REGEX.test(String(r.sub_id)) &&
          r.date
      )
      .map((r) => ({
        cake_affiliate_id: String(r.sub_id),
        date: r.date,               // REAL DAILY DATE
        clicks: Number(r.clicks ?? 0),
        conversions: Number(r.conversions ?? 0),
        revenue: Number(r.revenue ?? 0),
        payout: Number(r.events ?? 0),
      }));

    if (rows.length > 0) {
      const { error } = await supabase
        .from("cake_earnings_daily")
        .upsert(rows, {
          onConflict: "cake_affiliate_id,date",
        });

      if (error) throw error;

      console.log(`✔ Upserted ${rows.length} rows`);
    } else {
      console.log("No SPK rows in this window");
    }

    // Move to next window
    cursor = addDays(windowEnd, 1);
  }

  console.log("✅ CAKE all-time sync complete");
}

// ─────────────────────────────────────────────
// RUN
// ─────────────────────────────────────────────
syncCakeAllTime()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("❌ CAKE sync failed:", err);
    process.exit(1);
  });
