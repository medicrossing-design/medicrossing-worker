import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import crypto from 'crypto';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

let isShuttingDown = false;

process.on('SIGTERM', () => {
  console.log('Received SIGTERM, initiating graceful shutdown...');
  isShuttingDown = true;
});

process.on('SIGINT', () => {
  console.log('Received SIGINT, initiating graceful shutdown...');
  isShuttingDown = true;
});

async function fetchWithTimeout(url, timeout = 8000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    throw error;
  }
}

function extractText(html) {
  const $ = cheerio.load(html);
  return $('body').text().trim();
}

function calculateHash(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function processSource(source) {
  console.log(`Processing source ID: ${source.id}, URL: ${source.url}`);

  try {
    const response = await fetchWithTimeout(source.url);
    const html = await response.text();
    const rawText = extractText(html);
    const contentHash = calculateHash(rawText);
    const fetchedAt = new Date().toISOString();
    const metadataJson = JSON.stringify({ userAgent: 'MediCrossingWorker/1.0' });
    const httpStatus = response.status;

    console.log(`Fetched source: status ${httpStatus}, text length: ${rawText.length}`);

    // Save snapshot
    const { data: snapshot, error: snapshotError } = await supabase
      .from('source_snapshots')
      .insert({
        source_id: source.id,
        raw_text: rawText,
        fetched_at: fetchedAt,
        content_hash: contentHash,
        metadata_json: metadataJson,
        http_status: httpStatus,
      })
      .select()
      .single();

    if (snapshotError) {
      console.error('Error saving snapshot:', snapshotError);
      return;
    }

    console.log(`Snapshot saved with ID: ${snapshot.id}`);

    // Call OpenAI
    const prompt = `Analyze the following text from a medical source and return a JSON object with exactly these keys: medicamento_mencionado (boolean), status (string: 'approved', 'rejected', 'pending', or 'unknown'), confianca (number between 0 and 1). Text: ${rawText.substring(0, 4000)}`;

    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0,
    });

    const aiResponse = JSON.parse(completion.choices[0].message.content);
    console.log('AI Response:', aiResponse);

    // Insert curated decision
    const { error: decisionError } = await supabase
      .from('curated_decisions')
      .insert({
        country_code: source.country_code || 'BR',
        identified_medication: aiResponse.medicamento_mencionado ? 'Unknown' : null,
        identified_medication_key: null,
        status: aiResponse.status,
        confidence: aiResponse.confianca >= 0.8 ? 'high' : aiResponse.confianca >= 0.5 ? 'medium' : 'low',
        conditions_json: JSON.stringify({}),
        plain_language_pt: 'Texto em português',
        plain_language_en: 'Text in English',
        primary_source_id: source.id,
        snapshot_id: snapshot.id,
        review_status: 'pending',
        source_name: source.name,
        source_url: source.url,
        confidence_score: aiResponse.confianca,
        evidence_id: source.evidence_id,
        audit_trail: JSON.stringify({ processed_at: new Date().toISOString() }),
      });

    if (decisionError) {
      console.error('Error inserting decision:', decisionError);
      return;
    }

    console.log('Decision inserted');

    // Mark as classified
    const { error: updateError } = await supabase
      .from('evidence_sources')
      .update({ classified: true })
      .eq('id', source.id);

    if (updateError) {
      console.error('Error updating source:', updateError);
    } else {
      console.log(`Source ${source.id} marked as classified`);
    }

  } catch (error) {
    console.error(`Error processing source ${source.id}:`, error.message);
  }
}

async function mainLoop() {
  console.log('Starting MediCrossing worker...');

  while (!isShuttingDown) {
    try {
      const { data: sources, error } = await supabase
        .from('evidence_sources')
        .select('*')
        .eq('classified', false)
        .limit(1);

      if (error) {
        console.error('Error fetching sources:', error);
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      if (sources.length === 0) {
        console.log('No unclassified sources found, waiting...');
        await new Promise(resolve => setTimeout(resolve, 10000));
        continue;
      }

      await processSource(sources[0]);

    } catch (error) {
      console.error('Error in main loop:', error);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }

  console.log('Worker shutting down gracefully');
}

mainLoop().catch(console.error);
