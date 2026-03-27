import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';
import crypto from 'crypto';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = 'gpt-4o-mini';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

let isRunning = true;

console.log('=== WORKER START ===');

process.on('SIGINT', () => {
  console.log('[SIGNAL] SIGINT received, shutting down gracefully');
  isRunning = false;
  setTimeout(() => process.exit(0), 2000);
});

process.on('SIGTERM', () => {
  console.log('[SIGNAL] SIGTERM received, shutting down gracefully');
  isRunning = false;
  setTimeout(() => process.exit(0), 2000);
});

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate',
  'DNT': '1',
  'Connection': 'keep-alive',
  'Upgrade-Insecure-Requests': '1'
};

async function fetchWithTimeout(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  
  try {
    console.log(`[FETCH] Attempting to fetch ${url} with ${timeoutMs}ms timeout.`);
    const response = await fetch(url, {
      headers: HEADERS,
      signal: controller.signal,
      follow: 3
    });
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      console.log(`[FETCH] HTTP Error: ${response.status} for ${url}`);
      throw new Error(`HTTP ${response.status}`);
    }
    
    const text = await response.text();
    console.log(`[FETCH] Success: ${text.length} bytes from ${url}`);
    return text.substring(0, 50000);
  } catch (error) {
    clearTimeout(timeoutId);
    console.log(`[FETCH] Failed for ${url}: ${error.message}`);
    throw error;
  }
}

function computeSha256Hash(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function markSourceClassified(sourceId, status = 'processed') {
  try {
    const { error } = await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', sourceId);
    if (error) {
      console.error(`[UPDATE] Error marking source ${sourceId} as classified (${status}):`, error.message);
    } else {
      console.log(`[UPDATE] Source ${sourceId} marked as classified (${status}).`);
    }
  } catch (e) {
    console.error(`[UPDATE] Exception marking source ${sourceId} as classified (${status}):`, e.message);
  }
}

async function processSingleSource(source) {
  console.log(`[PROCESS] Starting source ID: ${source.id}, Name: ${source.source_name}, URL: ${source.source_url}`);
  let snapshotId = null;

  try {
    let htmlContent;
    try {
      htmlContent = await fetchWithTimeout(source.source_url);
    } catch (fetchError) {
      console.error(`[ERROR] Fetch failed for ${source.id}: ${fetchError.message}`);
      return;
    }

    const $ = cheerio.load(htmlContent);
    const raw_text = $('body').text().trim();
    console.log(`[PARSE] Extracted ${raw_text.length} characters from ${source.id}.`);

    if (raw_text.length < 100) {
      console.log(`[SKIP] Content too short (${raw_text.length} chars) for ${source.id}.`);
      return;
    }

    const content_hash = computeSha256Hash(raw_text);
    const fetched_at = new Date().toISOString();
    
    const { data: snapshot, error: snapError } = await supabase
      .from('source_snapshots')
      .insert({ 
        source_id: source.id, 
        raw_text: raw_text, 
        fetched_at: fetched_at,
        http_status: 200,
        content_hash: content_hash,
        metadata_json: { url: source.source_url, fetched_at: fetched_at }
      })
      .select('id')
      .single();

    if (snapError) {
      console.error(`[SNAPSHOT] Error saving snapshot for ${source.id}:`, snapError.message);
      return;
    }
    snapshotId = snapshot.id;
    console.log(`[SNAPSHOT] Saved snapshot ID: ${snapshotId} for source ${source.id}.`);

    const textToAnalyze = raw_text.substring(0, 2000);
    const prompt = `Analise o texto e responda APENAS em JSON válido com estas chaves exatas: {"medicamento_mencionado": "SIM" ou "NAO", "status": "PERMITTED" ou "CONTROLLED_SUBSTANCE" ou "REQUIRES_PRESCRIPTION_OR_DOCUMENTATION" ou "UNKNOWN", "confianca": "LOW" ou "MEDIUM" ou "HIGH"}. Texto: ${textToAnalyze}`;

    console.log(`[AI] Calling OpenAI for source ${source.id}...`);
    const aiResponse = await openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 150,
      temperature: 0.3
    });

    const aiText = aiResponse.choices[0].message.content.trim();
    let parsedAiResponse;
    try {
      parsedAiResponse = JSON.parse(aiText);
    } catch (parseError) {
      console.error(`[AI] JSON parse failed for ${source.id}: ${parseError.message}. Raw AI response: ${aiText}`);
      parsedAiResponse = { medicamento_mencionado: 'NAO', status: 'UNKNOWN', confianca: 'LOW' };
    }

    console.log(`[AI] Response for ${source.id}: Med=${parsedAiResponse.medicamento_mencionado}, Status=${parsedAiResponse.status}, Conf=${parsedAiResponse.confianca}`);

    const validConfidence = ['LOW', 'MEDIUM', 'HIGH'].includes(parsedAiResponse.confianca) ? parsedAiResponse.confianca : 'LOW';
    const validStatus = ['PERMITTED', 'CONTROLLED_SUBSTANCE', 'REQUIRES_PRESCRIPTION_OR_DOCUMENTATION', 'UNKNOWN'].includes(parsedAiResponse.status) ? parsedAiResponse.status : 'UNKNOWN';
    const identifiedMedication = parsedAiResponse.medicamento_mencionado === 'SIM' ? 'Sim' : 'Nao';

    if (validConfidence === 'LOW') {
      console.log(`[SKIP] Confidence is LOW for ${source.id}, skipping decision insertion.`);
      return;
    }

    const review_status = (validConfidence === 'MEDIUM') ? 'pending' : 'approved';
    const identified_medication_key = `${identifiedMedication.toLowerCase()}-${(source.country_code || 'UNKNOWN').toLowerCase()}`;
    
    let confidence_score;
    if (validConfidence === 'HIGH') confidence_score = 0.85;
    else if (validConfidence === 'MEDIUM') confidence_score = 0.65;
    else confidence_score = 0.35;

    const { error: decisionError } = await supabase
      .from('curated_decisions')
      .insert({
        country_code: source.country_code || 'UNKNOWN',
        identified_medication: identifiedMedication,
        identified_medication_key: identified_medication_key,
        status: validStatus,
        confidence: validConfidence,
        conditions_json: {
          source_name: source.source_name,
          source_url: source.source_url,
          fetched_at: fetched_at,
          text_length: raw_text.length
        },
        plain_language_pt: `Baseado em ${source.source_name}, o status regulatório é ${validStatus} com confiança ${validConfidence}.`,
        plain_language_en: `Based on ${source.source_name}, the regulatory status is ${validStatus} with ${validConfidence} confidence.`,
        primary_source_id: source.id,
        snapshot_id: snapshotId,
        review_status: review_status,
        source_name: source.source_name,
        source_url: source.source_url,
        confidence_score: confidence_score,
        evidence_id: source.id,
        audit_trail: {
          ai_response: aiText,
          processed_at: new Date().toISOString(),
          confidence_level: validConfidence
        }
      });

    if (decisionError) {
      console.error(`[DECISION] Error inserting decision for ${source.id}:`, decisionError.message);
      return;
    }
    console.log(`[DECISION] Successfully inserted decision for ${source.id}.`);

  } catch (error) {
    console.error(`[ERROR] Unhandled error processing source ${source.id}:`, error.message);
  } finally {
    await markSourceClassified(source.id, 'completed');
    console.log(`[PROCESS] Finished processing for source ${source.id}.`);
  }
}

async function main() {
  console.log('Worker ready and listening for sources...');
  
  while (isRunning) {
    try {
      const { data: sources, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(1);

      if (error) {
        console.error(`[LOOP] Error fetching unclassified sources:`, error.message);
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      if (sources.length === 0) {
        console.log('[LOOP] No unclassified sources found, waiting 10s...');
        await new Promise(resolve => setTimeout(resolve, 10000));
        continue;
      }

      await processSingleSource(sources[0]);
      
      console.log('[LOOP] Processed one source, waiting 2s before next check...');
      await new Promise(resolve => setTimeout(resolve, 2000));

    } catch (error) {
      console.error(`[LOOP] Unhandled error in main loop:`, error.message);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  console.log('=== WORKER SHUTDOWN ===');
}

main().catch(e => {
  console.error(`[FATAL] Worker encountered a fatal error:`, e.message);
  process.exit(1);
});
