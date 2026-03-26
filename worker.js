import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-3.5-turbo';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
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
  for (let i = 0; i &lt; retries; i++) {
    try {
      console.log(JSON.stringify({ level: 'info', message: `Fetching URL: ${url}, attempt ${i + 1}` }));
      const response = await fetch(url, { timeout: 10000 });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.text();
    } catch (error) {
      console.log(JSON.stringify({ level: 'warn', message: `Fetch failed for ${url}, attempt ${i + 1}: ${error.message}` }));
      if (i === retries - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

async function processBatch(batch) {
  for (const source of batch) {
    if (!isRunning) break;
    try {
      console.log(JSON.stringify({ level: 'info', message: `Processing source ID: ${source.id}, URL: ${source.url}` }));
      const html = await fetchWithRetry(source.url);
      const $ = cheerio.load(html);
      const raw_text = $('body').text().trim();
      console.log(JSON.stringify({ level: 'info', message: `Extracted raw_text length: ${raw_text.length} for source ${source.id}` }));
      if (raw_text.length &lt; 500) {
        console.log(JSON.stringify({ level: 'warn', message: `Skipping source ${source.id}: raw_text too short (${raw_text.length} &lt; 500)` }));
        continue;
      }
      const fetched_at = new Date().toISOString();
      const metadata_json = JSON.stringify({ url: source.url, fetched_at });
      const { error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({ source_id: source.id, raw_text, fetched_at, metadata_json });
      if (snapshotError) throw snapshotError;
      console.log(JSON.stringify({ level: 'info', message: `Saved snapshot for source ${source.id}` }));
      const prompt = `Analise o texto fornecido e responda APENAS em JSON válido: {"medicamento_mencionado": "SIM" ou "NÃO", "status": "PERMITTED" ou "CONTROLLED" ou "RESTRICTED" ou "UNKNOWN", "confianca": um número entre 0 e 1}. Texto: ${raw_text.substring(0, 2000)}`;
      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 100,
      });
      const aiText = aiResponse.choices[0].message.content.trim();
      console.log(JSON.stringify({ level: 'info', message: `AI response for source ${source.id}: ${aiText}` }));
      let parsed;
      try {
        parsed = JSON.parse(aiText);
      } catch (e) {
        console.log(JSON.stringify({ level: 'warn', message: `JSON parse failed for source ${source.id}, using fallback` }));
        parsed = { medicamento_mencionado: 'não', status: 'UNKNOWN', confianca: 0 };
      }
      const { medicamento_mencionado, status, confianca } = parsed;
      let review_status;
      if (confianca &lt; 0.6) {
        console.log(JSON.stringify({ level: 'info', message: `Skipping source ${source.id}: confidence ${confianca} &lt; 0.6` }));
        continue;
      } else if (confianca &lt;= 0.75) {
        review_status = 'pending';
      } else {
        review_status = 'approved';
      }
      const audit_trail = JSON.stringify({
        source_url: source.url,
        raw_text_length: raw_text.length,
        ai_response: aiText,
        confidence: confianca,
        decision_at: new Date().toISOString()
      });
      const { data: decision, error: decisionError } = await supabase
        .from('curated_decisions')
        .insert({
          source_id: source.id,
          medicamento_mencionado,
          status,
          confianca,
          review_status,
          audit_trail
        })
        .select()
        .single();
      if (decisionError) throw decisionError;
      console.log(JSON.stringify({ level: 'info', message: `Saved curated decision for source ${source.id}, decision ID: ${decision.id}` }));
      const { error: mapError } = await supabase
        .from('decision_evidence_map')
        .insert({ decision_id: decision.id, source_id: source.id });
      if (mapError) throw mapError;
      console.log(JSON.stringify({ level: 'info', message: `Linked decision_evidence_map for decision ${decision.id}` }));
      const { error: updateError } = await supabase
        .from('evidence_sources')
        .update({ classified: true })
        .eq('id', source.id);
      if (updateError) throw updateError;
      console.log(JSON.stringify({ level: 'info', message: `Updated source ${source.id} to classified=true` }));
    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: `Error processing source ${source.id}: ${error.message}`, stack: error.stack }));
    }
  }
}

async function main() {
  console.log(`Worker initialized with Supabase ${SUPABASE_URL} and OpenAI ${OPENAI_MODEL}`);
  while (isRunning) {
    try {
      console.log(JSON.stringify({ level: 'info', message: 'Fetching batch of 5 unclassified sources' }));
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('id, url')
        .eq('classified', false)
        .limit(5);
      if (error) throw error;
      if (batch.length === 0) {
        console.log(JSON.stringify({ level: 'info', message: 'No more sources to process, sleeping for 5s' }));
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }
      await processBatch(batch);
      console.log(JSON.stringify({ level: 'info', message: 'Batch processed, sleeping for 1s' }));
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: `Main loop error: ${error.message}`, stack: error.stack }));
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  console.log(JSON.stringify({ level: 'info', message: 'Worker shutdown complete' }));
}

main().catch(error => {
  console.log(JSON.stringify({ level: 'fatal', message: `Unhandled error: ${error.message}`, stack: error.stack }));
  process.exit(1);
});
