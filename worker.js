import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const LOOP_INTERVAL = 15000;

// =============================
// GERAR QUERY
// =============================
function buildSearchQueries(medication, country, domain) {
  return [
    `${medication} travel ${country} site:${domain}`,
    `${medication} prescription requirement ${country} site:${domain}`,
    `${medication} controlled substance ${country} site:${domain}`
  ];
}

// =============================
// LIMPAR HTML
// =============================
function cleanHtml(html) {
  return html
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 2000);
}

// =============================
// VALIDAR TEXTO
// =============================
function isValidContent(text) {
  if (!text || text.length < 200) return false;

  const lower = text.toLowerCase();

  if (
    lower.includes("cookie") ||
    lower.includes("login") ||
    lower.includes("menu") ||
    lower.includes("navigation")
  ) {
    return false;
  }

  return true;
}

// =============================
// BUSCA GOOGLE (SIMPLES)
// =============================
async function googleSearch(query) {
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}`;

  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0"
    }
  });

  const html = await res.text();

  // extrair links simples
  const links = [...html.matchAll(/href="\/url\?q=(https:\/\/[^&"]+)/g)]
    .map(m => decodeURIComponent(m[1]));

  return links.slice(0, 3);
}

// =============================
// PROCESS SEARCH EVENT
// =============================
async function processSearchEvent(event) {
  const medication = event.normalized_key || event.medicine_name;
  const country = event.destination_country;

  console.log(`🔍 ${medication} → ${country}`);

  const { data: sources } = await supabase
    .from("official_sources")
    .select("*")
    .eq("country_code", country);

  for (const source of sources || []) {
    const domain = new URL(source.base_url).hostname;

    const queries = buildSearchQueries(medication, country, domain);

    for (const q of queries) {
      try {
        const links = await googleSearch(q);

        for (const link of links) {
          console.log("🌐", link);

          const page = await fetch(link, {
            headers: { "User-Agent": "Mozilla/5.0" }
          });

          const html = await page.text();
          const cleaned = cleanHtml(html);

          if (!isValidContent(cleaned)) continue;

          await supabase.from("evidence_sources").insert({
            source_name: source.source_name,
            source_url: link,
            content_snapshot: cleaned,
            content_hash: Buffer.from(cleaned).toString("base64").slice(0, 50),
            country_code: country
          });

          console.log("✅ evidência real salva");

          return;
        }
      } catch (err) {
        console.log("❌ falha query");
      }
    }
  }
}

// =============================
// LOOP
// =============================
async function run() {
  console.log("🚀 Worker V3 iniciado");

  while (true) {
    const { data } = await supabase
      .from("search_events")
      .select("*")
      .eq("result_status", "NOT_FOUND_OR_INCONCLUSIVE")
      .limit(3);

    for (const event of data || []) {
      await processSearchEvent(event);
    }

    await new Promise(r => setTimeout(r, LOOP_INTERVAL));
  }
}

run();
