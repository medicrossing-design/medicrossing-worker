import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = 'gpt-4o-mini';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

let isRunning = true;

process.on('SIGINT', () => {
  console.log(JSON.stringify({ level: 'info', message: 'Received SIGINT, shutting down gracefully' }));
  isRunning = false;
});

process.on('SIGTERM', () => {
  console.log(JSON.stringify({ level: 'info', message: 'Received SIGTERM, shutting down gracefully' }));
  isRunning = false;
});

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, { timeout: 10000 });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.text();
    } catch (error) {
      if (i === retries - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

async function processBatch(batch) {
  for (const source of batch) {
    if (!isRunning) break;
    try {
      const html = await fetchWithRetry(source.source_url);
      const $ = cheerio.load(html);
      const raw_text = $('body').text().trim();

      if (raw_text.length < 500) {
        await supabase.from('evidence_sources').update({ classified: true }).eq('id', source.id);
        continue;
      }

      const fetched_at = new Date().toISOString();
      const metadata_json = JSON.stringify({ url: source.source_url, fetched_at });

      const { error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({ source_id: source.id, raw_text, fetched_at, metadata_json });

      if (snapshotError) throw snapshotError;

      const prompt = `Analise o texto e responda APENAS em JSON: {"medicamento_mencionado": "SIM" ou "NÃO", "status": "PERMITTED" ou "CONTROLLED" ou "RESTRICTED" ou "UNKNOWN", "confianca": 0 a 1}. Texto: ${raw_text.substring(0, 2000)}`;

      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 100
      });

      const aiText = aiResponse.choices[0].message.content.trim();
      let parsed = { medicamento_mencionado: 'NÃO', status: 'UNKNOWN', confianca: 0 };

      try {
        parsed = JSON.parse(aiText);
      } catch (e) {}

      const { medicamento_mencionado, status, confianca } = parsed;
      let review_status;

      if (confianca < 0.6) {
        await supabase.from('evidence_sources').update({ classified: true }).eq('id', source.id);
        continue;
      } else if (confianca <= 0.75) {
        review_status = 'pending';
      } else {
        review_status = 'approved';
      }

      const audit_trail = JSON.stringify({
        source_url: source.source_url,
        raw_text_length: raw_text.length,
        ai_response: aiText,
        confidence: confianca,
        decision_at: new Date().toISOString()
      });

      await supabase.from('curated_decisions').insert({
        evidence_id: source.id,
        country_code: source.country_code,
        identified_medication: medicamento_mencionado === 'SIM' ? 'Sim' : 'Não',
        status: status,
        confidence: confianca,
        plain_language_pt: `Baseado em ${source.source_name}, status é ${status} com confiança ${confianca}.`,
        snapshot_id: source.id,
        review_status: review_status,
        source_name: source.source_name,
        source_url: source.source_url,
        audit_trail: audit_trail
      });

      await supabase.from('evidence_sources').update({ classified: true }).eq('id', source.id);

    } catch (error) {
      console.error(`Error processing source ${source.id}:`, error.message);
      await supabase.from('evidence_sources').update({ classified: true }).eq('id', source.id).catch(() => {});
    }
  }
}

async function main() {
  console.log(`Worker initialized with OpenAI ${OPENAI_MODEL}`);
  while (isRunning) {
    try {
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(5);

      if (error) throw error;
      if (batch.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      await processBatch(batch);
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      console.error(`Main loop error: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

main().catch(error => {
  console.error(`Fatal error: ${error.message}`);
  process.exit(1);
});
