import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const LOOP_INTERVAL = 15000;

// =============================
// QUERY
// =============================
function buildQueries(med, country, domain) {
  return [
    `${med} travel ${country} medication rules site:${domain}`,
    `${med} prescription required ${country} site:${domain}`,
    `${med} controlled substance ${country} law site:${domain}`
  ];
}

// =============================
// CLEAN
// =============================
function clean(html) {
  return html
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 3000);
}

// =============================
// FILTRO REAL (CORE)
// =============================
function isRegulatoryContent(text) {
  const t = text.toLowerCase();

  const signals = [
    "prescription",
    "controlled",
    "allowed",
    "prohibited",
    "documentation",
    "travel",
    "bring medication"
  ];

  return signals.some(s => t.includes(s));
}

// =============================
// GOOGLE PARSER
// =============================
function extractLinks(html) {
  return [...html.matchAll(/href="\/url\?q=(https:\/\/[^&"]+)/g)]
    .map(m => decodeURIComponent(m[1]))
    .slice(0, 5);
}

// =============================
// PROCESS
// =============================
async function processEvent(event) {
  const med = event.normalized_key || event.medicine_name;
  const country = event.destination_country;

  console.log(`🔍 ${med} → ${country}`);

  const { data: sources } = await supabase
    .from("official_sources")
    .select("*")
    .eq("country_code", country);

  for (const source of sources || []) {
    const domain = new URL(source.base_url).hostname;

    const queries = buildQueries(med, country, domain);

    for (const q of queries) {
      try {
        const searchRes = await fetch(
          `https://www.google.com/search?q=${encodeURIComponent(q)}`,
          { headers: { "User-Agent": "Mozilla/5.0" } }
        );

        const searchHtml = await searchRes.text();
        const links = extractLinks(searchHtml);

        for (const link of links) {
          const page = await fetch(link, {
            headers: { "User-Agent": "Mozilla/5.0" }
          });

          const html = await page.text();
          const cleaned = clean(html);

          // 🔥 FILTRO CRÍTICO
          if (!isRegulatoryContent(cleaned)) continue;

          await supabase.from("evidence_sources").insert({
            source_name: source.source_name,
            source_url: link,
            content_snapshot: cleaned,
            content_hash: Buffer.from(cleaned).toString("base64").slice(0, 50),
            country_code: country
          });

          console.log("✅ EVIDÊNCIA REAL");

          return;
        }
      } catch (err) {
        console.log("❌ falha");
      }
    }
  }
}

// =============================
// LOOP
// =============================
async function run() {
  console.log("🚀 Worker REGULATORY iniciado");

  while (true) {
    const { data } = await supabase
      .from("search_events")
      .select("*")
      .eq("result_status", "NOT_FOUND_OR_INCONCLUSIVE")
      .limit(2);

    for (const ev of data || []) {
      await processEvent(ev);
    }

    await new Promise(r => setTimeout(r, LOOP_INTERVAL));
  }
}

run();
