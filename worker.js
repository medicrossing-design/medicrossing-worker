import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// =============================
// CHAMADA IA
// =============================
async function classifyText(text) {
  const prompt = `
You are a regulatory analyst.

Based ONLY on the text below, classify the rule for transporting medication across borders.

Return JSON:

{
  "status": "PERMITTED | PERMITTED_WITH_RESTRICTION | REQUIRES_PRESCRIPTION_OR_DOCUMENTATION | CONTROLLED_SUBSTANCE | NOT_FOUND_OR_INCONCLUSIVE",
  "confidence": "LOW | MEDIUM | HIGH",
  "reason": "short explanation"
}

TEXT:
${text.slice(0, 1500)}
`;

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0.2
    })
  });

  const json = await res.json();

  const content = json.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("Resposta vazia da IA");
  }

  return JSON.parse(content);
}

// =============================
// PROCESSAR EVIDÊNCIA
// =============================
async function processEvidence() {
  const { data } = await supabase
    .from("evidence_sources")
    .select("*")
    .is("classified", false)
    .limit(3);

  for (const ev of data || []) {
    try {
      console.log("🧠 analisando:", ev.id);

      const result = await classifyText(ev.content_snapshot);

      await supabase.from("curated_decisions").insert({
        identified_medication_key: "unknown",
        country_code: ev.country_code,
        status: result.status,
        confidence: result.confidence,
        source_name: ev.source_name,
        source_url: ev.source_url,
        evidence_id: ev.id,
        review_status: "pending"
      });

      await supabase
        .from("evidence_sources")
        .update({ classified: true })
        .eq("id", ev.id);

      console.log("✅ classificado:", result.status);

    } catch (error) {
      console.error("❌ erro IA DETALHADO:", {
        message: error.message,
        response: error.response?.data,
        stack: error.stack
      });
    }
  }
}

// =============================
// LOOP
// =============================
async function run() {
  console.log("🚀 CLASSIFIER iniciado");

  while (true) {
    try {
      await processEvidence();
    } catch (err) {
      console.error("❌ erro no loop:", err);
    }

    await new Promise(r => setTimeout(r, 10000));
  }
}

run();
