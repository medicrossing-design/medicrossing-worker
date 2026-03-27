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

process.on('SIGINT', () => {
  console.log('[SIGNAL] SIGINT, shutting down');
  isRunning = false;
  setTimeout(() => process.exit(0), 2000);
});

process.on('SIGTERM', () => {
  console.log('[SIGNAL] SIGTERM, shutting down');
  isRunning = false;
  setTimeout(() => process.exit(0), 2000);
});

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
};

async function fetchWithTimeout(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  
  try {
    const response = await fetch(url, {
      headers: HEADERS,
      signal: controller.signal,
      follow: 2
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    
    const text = await response.text();
    return text.substring(0, 50000);
  } catch (error) {
    clearTimeout(timeoutId);
    throw error;
  }
}

function computeHash(text) {
  let hash = 0;
  for (let i = 0; i &lt; text.length; i++) {
    const char = text.charCodeAt(i);
    hash = ((hash &lt;< 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(16);
}

async function updateClassified(sourceId, status = 'error') {
  try {
    await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', sourceId);
  } catch (e) {
    console.log(`[UPDATE] Failed: ${e.message}`);
  }
}

async function processBatch(batch) {
  console.log(`[BATCH] Processing ${batch.length} sources`);
  
  for (const source of batch) {
    if (!isRunning) break;
    
    try {
      console.log(`[SOURCE] ${source.source_name}`);
      
      let html;
      try {
        console.log(`[FETCH] Starting...`);
        html = await fetchWithTimeout(source.source_url, 8000);
        console.log(`[FETCH] Success: ${html.length} bytes`);
      } catch (error) {
        console.log(`[FETCH] Failed: ${error.message}`);
        await updateClassified(source.id, 'fetch_failed');
        continue;
      }
      
      const $ = cheerio.load(html);
      const raw_text = $('body').text().trim();
      console.log(`[PARSE] ${raw_text.length} chars`);

      if (raw_text.length &lt; 100) {
        console.log(`[SKIP] Too short`);
        await updateClassified(source.id, 'short');
        continue;
      }

      const content_hash = computeHash(raw_text);
      const fetched_at = new Date().toISOString();
      
      const { data: snapshot, error: snapError } = await supabase
        .from('source_snapshots')
        .insert({ 
          source_id: source.id, 
          raw_text, 
          fetched_at,
          content_hash: content_hash,
          metadata_json: { url: source.source_url, fetched_at },
          http_status: 200
        })
        .select('id')
        .single();

      if (snapError) {
        console.log(`[SNAPSHOT] Error: ${snapError.message}`);
        await updateClassified(source.id, 'snap_error');
        continue;
      }
      
      console.log(`[SNAPSHOT] Saved ID: ${snapshot.id}`);

      const prompt = `Responda APENAS em JSON: {"medicamento_mencionado": "SIM" ou "NAO", "status": "PERMITTED" ou "CONTROLLED_SUBSTANCE" ou "REQUIRES_PRESCRIPTION_OR_DOCUMENTATION" ou "UNKNOWN", "confianca": "LOW" ou "MEDIUM" ou "HIGH"}. Texto: ${raw_text.substring(0, 2000)}`;

      console.log(`[AI] Calling...`);
      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 150
      });

      const aiText = aiResponse.choices[0].message.content.trim();
      let parsed;
      try {
        parsed = JSON.parse(aiText);
      } catch (e) {
        console.log(`[AI] Parse failed, using fallback`);
        parsed = { medicamento_mencionado: 'NAO', status: 'UNKNOWN', confianca: 'LOW' };
      }

      console.log(`[AI] med=${parsed.medicamento_mencionado}, status=${parsed.status}, conf=${parsed.confianca}`);

      const { medicamento_mencionado, status, confianca } = parsed;
      
      const validConfidence = ['LOW', 'MEDIUM', 'HIGH'].includes(confianca) ? confianca : 'LOW';
      const validStatus = ['PERMITTED', 'CONTROLLED_SUBSTANCE', 'REQUIRES_PRESCRIPTION_OR_DOCUMENTATION', 'UNKNOWN'].includes(status) ? status : 'UNKNOWN';
      
      if (validConfidence === 'LOW') {
        console.log(`[SKIP] Confidence too low`);
        await updateClassified(source.id, 'low_conf');
        continue;
      }

      const review_status = validConfidence === 'MEDIUM' ? 'pending' : 'approved';
      const identified_medication_key = (medicamento_mencionado === 'SIM' ? 'sim' : 'nao') + '-' + (source.country_code || 'UNKNOWN').toLowerCase();
      let confidence_score;
      if (validConfidence === 'HIGH') confidence_score = 0.85;
      else if (validConfidence === 'MEDIUM') confidence_score = 0.65;
      else confidence_score = 0.35;

      const { error: decisionError } = await supabase
        .from('curated_decisions')
        .insert({
          country_code: source.country_code || 'UNKNOWN',
          identified_medication: medicamento_mencionado === 'SIM' ? 'Sim' : 'Nao',
          identified_medication_key: identified_medication_key,
          status: validStatus,
          confidence: validConfidence,
          conditions_json: { source_name: source.source_name, source_url: source.source_url, fetched_at },
          plain_language_pt: `Baseado em ${source.source_name}, o status é ${validStatus}.`,
          plain_language_en: `Based on ${source.source_name}, status is ${validStatus}.`,
          primary_source_id: source.id,
          snapshot_id: snapshot.id,
          review_status: review_status,
          source_name: source.source_name,
          source_url: source.source_url,
          confidence_score: confidence_score,
          evidence_id: source.id,
          audit_trail: { ai_response: aiText, processed_at: fetched_at }
        });

      if (decisionError) {
        console.log(`[DECISION] Error: ${decisionError.message}`);
        await updateClassified(source.id, 'decision_error');
        continue;
      }

      await updateClassified(source.id, 'success');
      console.log(`[DONE] ${source.source_name}\n`);

    } catch (error) {
      console.log(`[ERROR] ${error.message}`);
      await updateClassified(source.id, 'error');
    }
  }
}

async function main() {
  console.log('Worker ready\n');
  
  while (isRunning) {
    try {
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(5);

      if (error) throw error;
      if (batch.length === 0) {
        console.log('[LOOP] Sleeping 10s...');
        await new Promise(resolve => setTimeout(resolve, 10000));
        continue;
      }

      await processBatch(batch);
      await new Promise(resolve => setTimeout(resolve, 2000));

    } catch (error) {
      console.log(`[LOOP ERROR] ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

main().catch(e => {
  console.log(`[FATAL] ${e.message}`);
  process.exit(1);
});
