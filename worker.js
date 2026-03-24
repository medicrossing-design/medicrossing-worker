import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const LOOP_INTERVAL = 15000;

// =============================
// GERAR QUERY INTELIGENTE
// =============================
function buildSearchQueries(medication, country) {
  return [
    `${medication} travel ${country} medication rules`,
    `can I bring ${medication} to ${country}`,
    `${medication} controlled substance ${country} law`,
    `${medication} prescription requirement ${country}`
  ];
}

// =============================
// CLEAN HTML
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
// VALIDAR CONTEÚDO
// =============================
function isValidContent(text) {
  if (!text || text.length < 200) return false;

  const lower = text.toLowerCase();

  const blacklist = [
    "cookie",
    "captcha",
    "login",
    "menu",
    "navigation",
    "challenge validation",
    "enable javascript"
  ];

  return !blacklist.some(word => lower.includes(word));
}

// =============================
// FETCH
// =============================
async function fetchPage(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept-Language": "en-US,en;q=0.9"
    }
  });

  if (!res.ok) throw new Error("HTTP " + res.status);

  return await res.text();
}

// =============================
// PROCESSAR UMA BUSCA REAL
// =============================
async function processSearchEvent(event) {
  const medication = event.normalized_key || event.medicine_name;
  const country = event.destination_country;

  console.log(`🔍 ${medication} → ${country}`);

  // 1. buscar fontes oficiais
  const { data: sources } = await supabase
    .from("official_sources")
    .select("*")
    .eq("country_code", country)
    .eq("is_active", true)
    .order("priority_score", { ascending: false });

  if (!sources || sources.length === 0) {
    console.log("⚠️ sem fontes oficiais");
    return;
  }

  const queries = buildSearchQueries(medication, country);

  for (const source of sources) {
    for (const q of queries) {
      const searchUrl = `${source.base_url}/search?q=${encodeURIComponent(q)}`;

      try {
        console.log(`🌐 ${source.source_name}`);

        const html = await fetchPage(searchUrl);
        const cleaned = cleanHtml(html);

        if (!isValidContent(cleaned)) continue;

        // salvar evidência
        await supabase.from("evidence_sources").insert({
          source_name: source.source_name,
          source_url: searchUrl,
          content_snapshot: cleaned,
          content_hash: Buffer.from(cleaned).toString("base64").slice(0, 50),
          source_type: source.source_type,
          country_code: country
        });

        console.log("✅ evidência salva");

        return; // para no primeiro válido
      } catch (err) {
        console.log("❌ falha tentativa");
      }
    }
  }
}

// =============================
// LOOP PRINCIPAL
// =============================
async function run() {
  console.log("🚀 Search-driven worker iniciado");

  while (true) {
    const { data } = await supabase
      .from("search_events")
      .select("*")
      .eq("result_status", "NOT_FOUND_OR_INCONCLUSIVE")
      .order("created_at", { ascending: true })
      .limit(3);

    if (!data || data.length === 0) {
      console.log("😴 sem buscas pendentes");
    }

    for (const event of data || []) {
      await processSearchEvent(event);
    }

    await new Promise(r => setTimeout(r, LOOP_INTERVAL));
  }
}

run();
