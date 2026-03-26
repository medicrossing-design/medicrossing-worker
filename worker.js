import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const openaiApiKey = process.env.OPENAI_API_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);
const openai = new OpenAI({ apiKey: openaiApiKey });

let isRunning = true;

process.on('SIGINT', () => { isRunning = false; console.log(JSON.stringify({ event: 'shutdown', reason: 'SIGINT' })); process.exit(0); });
process.on('SIGTERM', () => { isRunning = false; console.log(JSON.stringify({ event: 'shutdown', reason: 'SIGTERM' })); process.exit(0); });

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url);
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
      const rawText = await fetchWithRetry(source.url);
      const $ = cheerio.load(rawText);
      const parsedText = $('body').text().trim();
      if (parsedText.length < 500) {
        console.log(JSON.stringify({ event: 'skip', source_id: source.id, reason: 'raw_text too short' }));
        continue;
      }
      const fetchedAt = new Date().toISOString();
      await supabase.from('source_snapshots').insert({ source_id: source.id, raw_text: parsedText, fetched_at: fetchedAt });
      const prompt = `Analise o texto fornecido e responda estritamente em JSON com as chaves: medicamento_mencionado (sim/não), status (APPROVED/PENDING/REJECTED/UNKNOWN), confianca (0-1). Texto: ${parsedText}`;
      let aiResponse;
      try {
        const completion = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content: prompt }],
          response_format: { type: 'json_object' }
        });
        aiResponse = JSON.parse(completion.choices[0].message.content);
      } catch (error) {
        aiResponse = { medicamento_mencionado: 'não', status: 'UNKNOWN', confianca: 0 };
      }
      const confidence = aiResponse.confianca;
      let decision;
      if (confidence < 0.6) continue;
      if (confidence <= 0.75) decision = 'pending';
      else decision = 'approved';
      const auditTrail = {
        source_url: source.url,
        raw_text_length: parsedText.length,
        ai_response: aiResponse,
        confidence: confidence,
        decision_at: new Date().toISOString()
      };
      const { data: decisionData } = await supabase.from('curated_decisions').insert({
        source_id: source.id,
        decision: decision,
        audit_trail: auditTrail
      }).select('id');
      await supabase.from('decision_evidence_map').insert({ decision_id: decisionData[0].id, source_id: source.id });
      await supabase.from('evidence_sources').update({ classified: true }).eq('id', source.id);
      console.log(JSON.stringify({ event: 'processed', source_id: source.id, decision: decision }));
    } catch (error) {
      console.log(JSON.stringify({ event: 'error', source_id: source.id, error: error.message }));
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

async function main() {
  while (isRunning) {
    const { data: sources } = await supabase.from('evidence_sources').select('*').eq('classified', false).limit(5);
    if (!sources || sources.length === 0) {
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }
    await processBatch(sources);
  }
}

main().catch(error => console.log(JSON.stringify({ event: 'fatal_error', error: error.message })));
