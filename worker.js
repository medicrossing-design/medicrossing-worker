import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = 'gpt-4o-mini';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

let isRunning = true;

console.log('=== WORKER START ===');
console.log(`SUPABASE_URL: ${SUPABASE_URL}`);
console.log(`OPENAI_MODEL: ${OPENAI_MODEL}`);

process.on('SIGINT', () => {
  console.log('SIGINT received');
  isRunning = false;
});

process.on('SIGTERM', () => {
  console.log('SIGTERM received');
  isRunning = false;
});

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate',
  'DNT': '1',
  'Connection': 'keep-alive',
  'Upgrade-Insecure-Requests': '1'
};

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[FETCH] Attempt ${i + 1}/${retries}: ${url}`);
      const response = await fetch(url, { 
        timeout: 10000,
        headers: HEADERS
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const text = await response.text();
      console.log(`[FETCH] Success: ${text.length} chars`);
      return text;
    } catch (error) {
      console.log(`[FETCH] Failed: ${error.message}`);
      if (i === retries - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, 2000 * (i + 1)));
    }
  }
}

async function updateClassified(sourceId) {
  try {
    const { error } = await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', sourceId);
    if (error) throw error;
  } catch (error) {
    console.log(`[UPDATE ERROR] ${sourceId}: ${error.message}`);
  }
}

async function processBatch(batch) {
  console.log(`[BATCH] Processing ${batch.length} sources`);
  for (const source of batch) {
    if (!isRunning) break;
    try {
      console.log(`[SOURCE] ${source.id}: ${source.source_url}`);
      const html = await fetchWithRetry(source.source_url);
      const $ = cheerio.load(html);
      const raw_text = $('body').text().trim();
      console.log(`[PARSE] ${raw_text.length} chars extracted`);

      if (raw_text.length < 500) {
        console.log(`[SKIP] Text too short (${raw_text.length} < 500)`);
        await updateClassified(source.id);
        continue;
      }

      const fetched_at = new Date().toISOString();
      const metadata_json = JSON.stringify({ url: source.source_url, fetched_at });

      const { error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({ source_id: source.id, raw_text, fetched_at, metadata_json });

      if (snapshotError) throw snapshotError;
      console.log(`[SNAPSHOT] Saved`);

      const prompt = `Analise o texto e responda APENAS em JSON: {"medicamento_mencionado": "SIM" ou "NÃO", "status": "PERMITTED" ou "CONTROLLED" ou "RESTRICTED" ou "UNKNOWN", "confianca": 0 a 1}. Texto: ${raw_text.substring(0, 2000)}`;

      console.log(`[AI] Calling OpenAI...`);
      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 100
      });

      const aiText = aiResponse.choices[0].message.content.trim();
      console.log(`[AI] Response: ${aiText}`);

      let parsed = { medicamento_mencionado: 'NÃO', status: 'UNKNOWN', confianca: 0 };
      try {
        parsed = JSON.parse(aiText);
      } catch (e) {
        console.log(`[AI] Parse failed, using fallback`);
      }

      const { medicamento_mencionado, status, confianca } = parsed;
      let review_status;

      if (confianca < 0.6) {
        console.log(`[CONFIDENCE] Too low (${confianca} < 0.6), skipping`);
        await updateClassified(source.id);
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

      console.log(`[DECISION] Saving with status: ${review_status}`);
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

      await updateClassified(source.id);
      console.log(`[DONE] Source ${source.id} completed`);

    } catch (error) {
      console.log(`[ERROR] ${source.id}: ${error.message}`);
      await updateClassified(source.id);
    }
  }
}

async function main() {
  console.log(`Worker initialized with OpenAI ${OPENAI_MODEL}`);
  while (isRunning) {
    try {
      console.log(`[LOOP] Fetching batch...`);
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(5);

      if (error) throw error;

      console.log(`[LOOP] Found ${batch.length} sources`);
      if (batch.length === 0) {
        console.log(`[LOOP] Nothing to do, sleeping 5s`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      await processBatch(batch);
      console.log(`[LOOP] Batch done, sleeping 1s`);
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      console.log(`[MAIN ERROR] ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

main().catch(error => {
  console.log(`[FATAL] ${error.message}`);
  process.exit(1);
});
