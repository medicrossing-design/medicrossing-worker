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
        console.log(`[FETCH] Starting (8s timeout)...`);
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

      if (raw_text.length < 100) {
        console.log(`[SKIP] Too short`);
        await updateClassified(source.id, 'short');
        continue;
      }

      const fetched_at = new Date().toISOString();
      
      const { error: snapError } = await supabase
        .from('source_snapshots')
        .insert({ 
          source_id: source.id, 
          raw_text, 
          fetched_at,
          metadata_json: JSON.stringify({ url: source.source_url })
        });

      if (snapError) {
        console.log(`[SNAPSHOT] Error: ${snapError.message}`);
        await updateClassified(source.id, 'snap_error');
        continue;
      }
      
      console.log(`[SNAPSHOT] Saved`);

      const prompt = `Responda APENAS em JSON: {"medicamento_mencionado": "SIM" ou "NÃO", "status": "PERMITTED" ou "CONTROLLED" ou "RESTRICTED" ou "UNKNOWN", "confianca": 0-1}. Texto: ${raw_text.substring(0, 1500)}`;

      console.log(`[AI] Calling...`);
      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 100
      });

      const aiText = aiResponse.choices[0].message.content.trim();
      let parsed = JSON.parse(aiText);
      console.log(`[AI] conf=${parsed.confianca}`);

      const { medicamento_mencionado, status, confianca } = parsed;
      
      if (confianca < 0.5) {
        console.log(`[SKIP] Confidence too low`);
        await updateClassified(source.id, 'low_conf');
        continue;
      }

      const review_status = confianca < 0.75 ? 'pending' : 'approved';

      await supabase.from('curated_decisions').insert({
        evidence_id: source.id,
        country_code: source.country_code || 'UNKNOWN',
        identified_medication: medicamento_mencionado === 'SIM' ? 'Sim' : 'Não',
        status: status,
        confidence: confianca,
        plain_language_pt: `${source.source_name}: ${status} (${(confianca*100).toFixed(0)}%)`,
        snapshot_id: source.id,
        review_status: review_status,
        source_name: source.source_name,
        source_url: source.source_url,
        audit_trail: JSON.stringify({ ai_response: aiText })
      });

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
