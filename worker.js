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
  console.log('[SIGNAL] SIGINT received');
  isRunning = false;
});

process.on('SIGTERM', () => {
  console.log('[SIGNAL] SIGTERM received');
  isRunning = false;
});

const HEADERS = {
  'User-Agent': 'Mozilla/5.0',
};

async function fetchWithTimeout(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { headers: HEADERS, signal: controller.signal });
    clearTimeout(timeoutId);
    return await res.text();
  } catch (err) {
    clearTimeout(timeoutId);
    throw err;
  }
}

function hash(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function insertEvidenceAnalysis(payload) {
  const MAX = 2;

  for (let i = 0; i <= MAX; i++) {
    const { error } = await supabase
      .from('evidence_analysis')
      .insert(payload);

    if (!error) {
      console.log('[EVIDENCE] saved');
      return;
    }

    console.error('[EVIDENCE] error:', error.message);

    if (i < MAX) {
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

async function processSingleSource(source) {
  console.log(`[PROCESS] ${source.id}`);

  try {
    const html = await fetchWithTimeout(source.source_url);
    const $ = cheerio.load(html);
    const raw = $('body').text().trim();

    if (raw.length < 100) return;

    const snapshotHash = hash(raw);

    const { data: snapshot } = await supabase
      .from('source_snapshots')
      .insert({
        source_id: source.id,
        raw_text: raw,
        content_hash: snapshotHash,
        http_status: 200
      })
      .select('id')
      .single();

    const snapshotId = snapshot.id;

    const text = raw.substring(0, 2000);

    // =========================
    // NOVO PROMPT (MELHORADO)
    // =========================
    const prompt = `
Você é um especialista regulatório.

Analise o texto abaixo e identifique regras sobre transporte de medicamentos em viagens internacionais.

Mesmo que nenhum medicamento específico seja citado, identifique regras gerais (ex: necessidade de receita, substâncias controladas, limites).

Responda APENAS em JSON:

{
  "status": "PERMITTED | PERMITTED_WITH_RESTRICTION | REQUIRES_PRESCRIPTION_OR_DOCUMENTATION | CONTROLLED_SUBSTANCE | UNKNOWN",
  "confidence": "LOW | MEDIUM | HIGH",
  "excerpt": "trecho relevante do texto"
}

TEXTO:
${text}
`;

    const ai = await openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.2,
      response_format: { type: "json_object" }
    });

    let parsed;

    try {
      parsed = JSON.parse(ai.choices[0].message.content);
    } catch {
      parsed = { status: 'UNKNOWN', confidence: 'LOW', excerpt: '' };
    }

    const status = parsed.status || 'UNKNOWN';
    const confidence = parsed.confidence || 'LOW';
    const excerpt = parsed.excerpt || raw.substring(0, 500);

    // =========================
    // NOVO: evidence_analysis
    // =========================
    await insertEvidenceAnalysis({
      snapshot_id: snapshotId,
      source_id: source.id,
      medication_identified: null,
      status_found: status,
      confidence_extraction: confidence,
      relevant_excerpt: excerpt,
      source_sentiment: 'official',
      ia_response_raw: parsed
    });

    // =========================
    // DECISION (mantido)
    // =========================
    if (confidence !== 'LOW') {
      await supabase.from('curated_decisions').insert({
        country_code: source.country_code,
        status,
        confidence,
        source_name: source.source_name,
        source_url: source.source_url,
        snapshot_id: snapshotId
      });
    }

    await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', source.id);

  } catch (err) {
    console.error('[ERROR]', err.message);
  }
}

async function main() {
  while (isRunning) {
    const { data } = await supabase
      .from('evidence_sources')
      .select('*')
      .eq('classified', false)
      .limit(1);

    if (!data || data.length === 0) {
      console.log('[LOOP] idle...');
      await new Promise(r => setTimeout(r, 5000));
      continue;
    }

    await processSingleSource(data[0]);
  }
}

main();
