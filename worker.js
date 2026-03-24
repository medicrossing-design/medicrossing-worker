import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const BATCH_SIZE = 5;
const LOOP_INTERVAL = 10000;
const FETCH_TIMEOUT = 15000; // aumentamos para 15s

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
// HASH
// =============================
function generateHash(text) {
  return Buffer.from(text).toString("base64").slice(0, 50);
}

// =============================
// FETCH COM RETRY
// =============================
async function fetchWithRetry(url, retries = 2) {
  for (let i = 0; i <= retries; i++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Accept-Language": "en-US,en;q=0.9"
        },
        signal: controller.signal
      });

      clearTimeout(timeout);

      if (!res.ok) throw new Error("HTTP " + res.status);

      return await res.text();
    } catch (err) {
      console.log(`⚠️ tentativa ${i + 1} falhou:`, err.message);

      if (i === retries) throw err;

      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

// =============================
// PROCESSAR
// =============================
async function processEvidenceBatch() {
  console.log("🔎 Buscando evidências...");

  const { data, error } = await supabase
    .from("evidence_sources")
    .select("*")
    .eq("content_snapshot", "SNAPSHOT_NOT_CAPTURED_YET")
    .order("created_at", { ascending: true })
    .limit(BATCH_SIZE);

  if (error) {
    console.error("❌ erro:", error.message);
    return;
  }

  if (!data || data.length === 0) {
    console.log("😴 nada pendente");
    return;
  }

  for (const ev of data) {
    console.log("🌐", ev.source_url);

    try {
      const html = await fetchWithRetry(ev.source_url);

      if (!html || html.length < 100) {
        throw new Error("conteúdo inválido");
      }

      const cleaned = cleanHtml(html);
      const hash = generateHash(cleaned);

      await supabase
        .from("evidence_sources")
        .update({
          content_snapshot: cleaned,
          content_hash: hash,
          captured_at: new Date().toISOString()
        })
        .eq("id", ev.id);

      console.log("✅ salvo:", ev.id);
    } catch (err) {
      console.error("❌ falhou:", ev.id);

      // 👇 MARCA COMO FALHA PRA NÃO LOOPAR
      await supabase
        .from("evidence_sources")
        .update({
          content_snapshot: "FAILED_TO_CAPTURE"
        })
        .eq("id", ev.id);
    }
  }
}

// =============================
// LOOP
// =============================
async function startWorker() {
  console.log("🚀 Worker iniciado");

  while (true) {
    try {
      await processEvidenceBatch();
    } catch (err) {
      console.error("🔥 erro loop:", err.message);
    }

    await new Promise((r) => setTimeout(r, LOOP_INTERVAL));
  }
}

startWorker();
