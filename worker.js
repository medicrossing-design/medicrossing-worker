import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// =============================
// CONFIG
// =============================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const BATCH_SIZE = 5;
const LOOP_INTERVAL = 10000; // 10s
const FETCH_TIMEOUT = 8000; // 8s

// =============================
// CLEAN HTML → TEXTO
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
// HASH SIMPLES (CONSISTENTE)
// =============================
function generateHash(text) {
  return Buffer.from(text).toString("base64").slice(0, 50);
}

// =============================
// FETCH COM TIMEOUT
// =============================
async function fetchWithTimeout(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; MediCrossingBot/1.0)"
      },
      signal: controller.signal
    });

    const text = await res.text();
    return text;
  } finally {
    clearTimeout(timeout);
  }
}

// =============================
// PROCESSAR EVIDÊNCIAS
// =============================
async function processEvidenceBatch() {
  console.log("🔎 Buscando evidências pendentes...");

  const { data, error } = await supabase
    .from("evidence_sources")
    .select("*")
    .eq("content_snapshot", "SNAPSHOT_NOT_CAPTURED_YET")
    .order("created_at", { ascending: true })
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ Erro ao buscar evidências:", error.message);
    return;
  }

  if (!data || data.length === 0) {
    console.log("😴 Nenhuma evidência pendente.");
    return;
  }

  for (const ev of data) {
    console.log(`🌐 Capturando: ${ev.source_url}`);

    try {
      const rawHtml = await fetchWithTimeout(ev.source_url);

      if (!rawHtml || rawHtml.length < 50) {
        console.log("⚠️ Conteúdo muito curto, ignorando:", ev.id);
        continue;
      }

      const cleaned = cleanHtml(rawHtml);
      const hash = generateHash(cleaned);

      await supabase
        .from("evidence_sources")
        .update({
          content_snapshot: cleaned,
          content_hash: hash,
          captured_at: new Date().toISOString()
        })
        .eq("id", ev.id);

      console.log(`✅ Salvo: ${ev.id}`);
    } catch (err) {
      console.error(`❌ Erro ao processar ${ev.id}:`, err.message);
    }
  }
}

// =============================
// LOOP PRINCIPAL
// =============================
async function startWorker() {
  console.log("🚀 MediCrossing Worker iniciado");

  while (true) {
    try {
      await processEvidenceBatch();
    } catch (err) {
      console.error("🔥 Erro no loop:", err.message);
    }

    await new Promise((r) => setTimeout(r, LOOP_INTERVAL));
  }
}

// START
startWorker();
