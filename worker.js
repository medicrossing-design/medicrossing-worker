import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const LOOP_INTERVAL = 20000;

// =============================
// CLEAN
// =============================
function clean(text) {
  return text
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 2000);
}

// =============================
// FILTRO
// =============================
function isUseful(text) {
  const t = text.toLowerCase();

  return (
    t.includes("prescription") ||
    t.includes("controlled") ||
    t.includes("allowed") ||
    t.includes("prohibited")
  );
}

// =============================
// FETCH SIMPLES (SEM GOOGLE)
// =============================
async function fetchPage(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0" }
  });

  if (!res.ok) throw new Error("fail");

  return await res.text();
}

// =============================
// PROCESS
// =============================
async function processEvent(ev) {
  const med = ev.normalized_key || ev.medicine_name;
  const country = ev.destination_country;

  console.log(`🔍 ${med} → ${country}`);

  const { data: sources } = await supabase
    .from("official_sources")
    .select("*")
    .eq("country_code", country);

  for (const s of sources || []) {
    try {
      console.log("🌐 tentando:", s.base_url);

      const html = await fetchPage(s.base_url);
      const text = clean(html);

      if (!isUseful(text)) continue;

      await supabase.from("evidence_sources").insert({
        source_name: s.source_name,
        source_url: s.base_url,
        content_snapshot: text,
        content_hash: Buffer.from(text).toString("base64").slice(0, 50),
        country_code: country
      });

      console.log("✅ evidência salva");

      break;
    } catch (e) {
      console.log("❌ falha fonte");
    }
  }

  // 🔥 MARCA COMO PROCESSADO (CRÍTICO)
  await supabase
    .from("search_events")
    .update({ result_status: "PROCESSED" })
    .eq("id", ev.id);
}

// =============================
// LOOP
// =============================
async function run() {
  console.log("🚀 Worker estável iniciado");

  while (true) {
    const { data } = await supabase
      .from("search_events")
      .select("*")
      .eq("result_status", "NOT_FOUND_OR_INCONCLUSIVE")
      .limit(2);

    if (!data || data.length === 0) {
      console.log("😴 nada pendente");
    }

    for (const ev of data || []) {
      await processEvent(ev);
    }

    await new Promise(r => setTimeout(r, LOOP_INTERVAL));
  }
}

run();
