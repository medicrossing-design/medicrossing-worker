import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';
import crypto from 'crypto';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const openaiApiKey = process.env.OPENAI_API_KEY;

const supabase = createClient(supabaseUrl, supabaseKey);
const openai = new OpenAI({ apiKey: openaiApiKey });

let isShuttingDown = false;

process.on('SIGINT', () => { isShuttingDown = true; process.exit(0); });
process.on('SIGTERM', () => { isShuttingDown = true; process.exit(0); });

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

async function processBatch(sources) {
  const promises = sources.map(async (source) => {
    if (isShuttingDown) return;
    try {
      const html = await fetchWithRetry(source.url);
      const $ = cheerio.load(html);
      const rawText = $('body').text().trim();
      const contentHash = crypto.createHash('sha256').update(rawText).digest('hex');
      const fetchedAt = new Date().toISOString();
      const metadataJson = JSON.stringify({ title: $('title').text() });

      const { data: snapshot, error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({
          source_id: source.id,
          fetched_at: fetchedAt,
          raw_text: rawText,
          content_hash: contentHash,
          metadata_json: metadataJson
        })
        .select('id')
        .single();

      if (snapshotError) throw snapshotError;

      const prompt = `Analise o texto: ${rawText}. Identifique medicamentos mencionados, país, status e gere explicação em português brasileiro.`;
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 500
      });

      const response = JSON.parse(completion.choices[0].message.content);
      const { country_code, identified_medication, status, confidence, plain_language_pt } = response;

      const { data: decision, error: decisionError } = await supabase
        .from('curated_decisions')
        .insert({
          country_code,
          identified_medication,
          status,
          confidence,
          plain_language_pt,
          snapshot_id: snapshot.id,
          source_name: source.name,
          source_url: source.url,
          evidence_id: source.evidence_id,
          review_status: 'pending'
        })
        .select('id')
        .single();

      if (decisionError) throw decisionError;

      await supabase
        .from('decision_evidence_map')
        .insert({
          decision_id: decision.id,
          evidence_id: source.evidence_id
        });

      await supabase
        .from('evidence_sources')
        .update({ classified: true })
        .eq('id', source.id);

      console.log(JSON.stringify({ level: 'info', message: `Processed source ${source.id}`, timestamp: new Date().toISOString() }));
    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: error.message, source_id: source.id, timestamp: new Date().toISOString() }));
    }
  });

  await Promise.all(promises);
}

async function main() {
  while (!isShuttingDown) {
    const { data: sources, error } = await supabase
      .from('evidence_sources')
      .select('*')
      .eq('classified', false)
      .limit(5);

    if (error) {
      console.log(JSON.stringify({ level: 'error', message: error.message, timestamp: new Date().toISOString() }));
      await new Promise(resolve => setTimeout(resolve, 1000));
      continue;
    }

    if (sources.length === 0) {
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    await processBatch(sources);
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

main();
