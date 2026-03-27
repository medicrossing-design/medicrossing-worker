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
  console.log('[SIGNAL] SIGINT received, shutting down gracefully');
  isRunning = false;
});

process.on('SIGTERM', () => {
  console.log('[SIGNAL] SIGTERM received, shutting down gracefully');
  isRunning = false;
});

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
  'Accept-Encoding': 'gzip, deflate, br',
  'DNT': '1',
  'Connection': 'keep-alive',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'none',
  'Cache-Control': 'max-age=0'
};

async function fetchWithRetry(url, retries = 2) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`[FETCH] Attempt ${i + 1}/${retries}: ${url}`);
      const response = await fetch(url, { 
        timeout: 30000,
        headers: HEADERS,
        follow: 3
      });
      
      if (!response.ok) {
        console.log(`[FETCH] HTTP ${response.status}`);
        if (response.status === 403 || response.status === 429) {
          throw new Error(`BLOCKED: HTTP ${response.status}`);
        }
        throw new Error(`HTTP ${response.status}`);
      }
      
      const text = await response.text();
      console.log(`[FETCH] Success: ${text.length} bytes`);
      return text;
    } catch (error) {
      console.log(`[FETCH] Failed: ${error.message}`);
      if (i === retries - 1) throw error;
      const delay = 3000 * (i + 1);
      console.log(`[FETCH] Waiting ${delay}ms before retry`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

async function updateClassified(sourceId, status = 'error') {
  try {
    const { error } = await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', sourceId);
    if (error) console.log(`[UPDATE] Error: ${error.message}`);
    else console.log(`[UPDATE] Source ${sourceId} marked as classified (${status})`);
  } catch (error) {
    console.log(`[UPDATE] Exception: ${error.message}`);
  }
}

async function processBatch(batch) {
  console.log(`[BATCH] Processing ${batch.length} sources`);
  
  for (const source of batch) {
    if (!isRunning) {
      console.log('[BATCH] Shutdown signal received, stopping batch');
      break;
    }
    
    try {
      console.log(`[SOURCE] ID: ${source.id} | Name: ${source.source_name} | URL: ${source.source_url}`);
      
      let html;
      try {
        html = await fetchWithRetry(source.source_url, 2);
      } catch (fetchError) {
        console.log(`[SOURCE] Fetch failed permanently: ${fetchError.message}`);
        await updateClassified(source.id, 'fetch_failed');
        continue;
      }
      
      const $ = cheerio.load(html);
      const raw_text = $('body').text().trim();
      console.log(`[PARSE] Extracted ${raw_text.length} chars`);

      if (raw_text.length < 200) {
        console.log(`[SKIP] Content too short (${raw_text.length} chars)`);
        await updateClassified(source.id, 'short_content');
        continue;
      }

      const fetched_at = new Date().toISOString();
      const metadata_json = JSON.stringify({ url: source.source_url, fetched_at });

      const { error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({ source_id: source.id, raw_text, fetched_at, metadata_json });

      if (snapshotError) {
        console.log(`[SNAPSHOT] Error: ${snapshotError.message}`);
        await updateClassified(source.id, 'snapshot_error');
        continue;
      }
      
      console.log(`[SNAPSHOT] Saved successfully`);

      const textToAnalyze = raw_text.substring(0, 3000);
      const prompt = `Analise o texto e responda APENAS em JSON válido com estas chaves exatas: {"medicamento_mencionado": "SIM" ou "NÃO", "status": "PERMITTED" ou "CONTROLLED" ou "RESTRICTED" ou "UNKNOWN", "confianca": número entre 0 e 1}. Texto: ${textToAnalyze}`;

      console.log(`[AI] Calling OpenAI gpt-4o-mini...`);
      const aiResponse = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 150,
        temperature: 0.3
      });

      const aiText = aiResponse.choices[0].message.content.trim();
      console.log(`[AI] Raw response: ${aiText.substring(0, 100)}...`);

      let parsed = { medicamento_mencionado: 'NÃO', status: 'UNKNOWN', confianca: 0 };
      try {
        parsed = JSON.parse(aiText);
        console.log(`[AI] Parsed: med=${parsed.medicamento_mencionado}, status=${parsed.status}, conf=${parsed.confianca}`);
      } catch (parseError) {
        console.log(`[AI] JSON parse failed: ${parseError.message}, using fallback`);
      }

      const { medicamento_mencionado, status, confianca } = parsed;
      let review_status;

      if (confianca < 0.5) {
        console.log(`[CONF] Too low (${confianca} < 0.5), skipping decision`);
        await updateClassified(source.id, 'low_confidence');
        continue;
      } else if (confianca < 0.75) {
        review_status = 'pending';
        console.log(`[CONF] Medium (${confianca}), marking as pending`);
      } else {
        review_status = 'approved';
        console.log(`[CONF] High (${confianca}), marking as approved`);
      }

      const audit_trail = JSON.stringify({
        source_url: source.source_url,
        raw_text_length: raw_text.length,
        ai_response: aiText,
        confidence: confianca,
        decision_at: new Date().toISOString()
      });

      const { error: decisionError } = await supabase.from('curated_decisions').insert({
        evidence_id: source.id,
        country_code: source.country_code || 'UNKNOWN',
        identified_medication: medicamento_mencionado === 'SIM' ? 'Sim' : 'Não',
        status: status,
        confidence: confianca,
        plain_language_pt: `Baseado em ${source.source_name}, o status regulatório é ${status} com confiança de ${(confianca * 100).toFixed(0)}%.`,
        snapshot_id: source.id,
        review_status: review_status,
        source_name: source.source_name,
        source_url: source.source_url,
        audit_trail: audit_trail
      });

      if (decisionError) {
        console.log(`[DECISION] Error: ${decisionError.message}`);
        await updateClassified(source.id, 'decision_error');
        continue;
      }

      await updateClassified(source.id, 'success');
      console.log(`[DONE] Source ${source.id} completed successfully\n`);

    } catch (error) {
      console.log(`[ERROR] Unexpected error: ${error.message}`);
      await updateClassified(source.id, 'unexpected_error');
    }
  }
}

async function main() {
  console.log(`Worker initialized with OpenAI ${OPENAI_MODEL}\n`);
  
  while (isRunning) {
    try {
      console.log(`[LOOP] Fetching unclassified sources...`);
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(5);

      if (error) {
        console.log(`[LOOP] Query error: ${error.message}`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      console.log(`[LOOP] Found ${batch.length} sources to process`);
      
      if (batch.length === 0) {
        console.log(`[LOOP] No sources to process, sleeping 10s\n`);
        await new Promise(resolve => setTimeout(resolve, 10000));
        continue;
      }

      await processBatch(batch);
      console.log(`[LOOP] Batch complete, sleeping 2s before next batch\n`);
      await new Promise(resolve => setTimeout(resolve, 2000));

    } catch (error) {
      console.log(`[LOOP] Exception: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  console.log('\n=== WORKER SHUTDOWN ===');
}

main().catch(error => {
  console.log(`[FATAL] ${error.message}`);
  process.exit(1);
});
