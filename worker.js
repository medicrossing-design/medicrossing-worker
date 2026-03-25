import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch";

// =============================
// ENV CHECK
// =============================
console.log("ENV CHECK:", {
  supabaseUrl: !!process.env.SUPABASE_URL,
  supabaseKey: !!process.env.SUPABASE_KEY,
  openaiKey: !!process.env.OPENAI_API_KEY
});

if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY não encontrada");
  process.exit(1);
}

// =============================
// SUPABASE
// =============================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// =============================
// CLEAN JSON
// =============================
function cleanJson(text) {
  return text
    .replace(/```json/g, "")
    .replace(/```/g, "")
    .trim();
}

// =============================
// CLASSIFICAR TEXTO (IA)
// =============================
async function classifyText(text) {
  const prompt = `
You are a regulatory analyst.

Based ONLY on the text below, classify the rule for transporting medication across borders.

Return ONLY valid JSON:

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
      messages: [
        {
          role: "system",
          content: "Return ONLY valid JSON. No markdown."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0.2
    })
  });

  // erro de API
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`OpenAI error: ${text}`);
  }

  const json = await res.json();

  const content = json.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("Resposta vazia da IA");
  }

  const cleaned = cleanJson(content);

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    console.error("❌ JSON inválido:", cleaned);
    throw err;
  }
}

// =============================
// PROCESSAR EVIDÊNCIAS
// =============================
async function processEvidence() {
  const { data, error } = await supabase
    .from("evidence_sources")
    .select("*")
    .is("classified", false)
    .limit(1); // SAFE pro Railway

  if (error) {
    console.error("❌ erro ao buscar evidências:", error);
    return;
  }

  if (!data || data.length === 0) {
    console.log("⏳ sem evidências pendentes");
    return;
  }

  for (const ev of data) {
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
        stack: error.stack
      });
    }
  }
}

// =============================
// LOOP PRINCIPAL (ESTÁVEL)
// =============================
async function run() {
  console.log("🚀 CLASSIFIER iniciado");

  while (true) {
    try {
      await processEvidence();
    } catch (err) {
      console.error("❌ erro no loop:", err);
    }

    await new Promise(resolve => setTimeout(resolve, 10000));
  }
}

// =============================
// GRACEFUL SHUTDOWN (Railway)
// =============================
process.on("SIGTERM", () => {
  console.log("🛑 Encerrando worker...");
  process.exit(0);
});

run();
