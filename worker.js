import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

async function processEvidence() {
  const { data, error } = await supabase
    .from("evidence_sources")
    .select("*")
    .eq("content_snapshot", "SNAPSHOT_NOT_CAPTURED_YET")
    .limit(5);

  if (error) {
    console.error("Erro ao buscar:", error);
    return;
  }

  for (const ev of data || []) {
    try {
      console.log("Capturando:", ev.source_url);

      const res = await fetch(ev.source_url, {
        headers: {
          "User-Agent": "Mozilla/5.0"
        }
      });

      const text = await res.text();

      const hash = Buffer.from(text).toString("base64").slice(0, 50);

      await supabase
        .from("evidence_sources")
        .update({
          content_snapshot: text.slice(0, 5000),
          content_hash: hash
        })
        .eq("id", ev.id);

      console.log("✔ salvo:", ev.id);
    } catch (err) {
      console.error("Erro ao processar:", ev.id, err.message);
    }
  }
}

setInterval(processEvidence, 10000);
