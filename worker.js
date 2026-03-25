import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const openaiApiKey = process.env.OPENAI_API_KEY;

const supabase = createClient(supabaseUrl, supabaseKey);
const openai = new OpenAI({ apiKey: openaiApiKey });

const BATCH_SIZE = 5;
const RETRY_ATTEMPTS = 3;
const RATE_LIMIT_DELAY = 1000; // 1 second

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, retries = RETRY_ATTEMPTS) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.text();
    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: `Fetch failed for ${url}, attempt ${i + 1}`, error: error.message }));
      if (i === retries - 1) throw error;
      await sleep(1000 * (i + 1)); // Exponential backoff
    }
  }
}

async function processBatch(batch) {
  for (const item of batch) {
    try {
      console.log(JSON.stringify({ level: 'info', message: `Processing evidence_source ID: ${item.id}` }));

      // Fetch URL
      const html = await fetchWithRetry(item.url);

      // Parse with cheerio
      const $ = cheerio.load(html);
      const title = $('title').text() || 'No title';
      const content = $('body').text().substring(0, 5000); // Limit content

      // Save to source_snapshots
      const { data: snapshot, error: snapshotError } = await supabase
        .from('source_snapshots')
        .insert({
          evidence_source_id: item.id,
          url: item.url,
          title,
          content,
          fetched_at: new Date().toISOString()
        })
        .select()
        .single();

      if (snapshotError) throw snapshotError;

      console.log(JSON.stringify({ level: 'info', message: `Saved snapshot for ID: ${item.id}` }));

      // Call OpenAI
      await sleep(RATE_LIMIT_DELAY);
      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Analyze the following content and decide if it is relevant for medical evidence. Respond with JSON: { "decision": "approve" or "reject", "reason": "brief reason" }' },
          { role: 'user', content: `Title: ${title}\nContent: ${content}` }
        ],
        max_tokens: 200
      });

      const response = JSON.parse(completion.choices[0].message.content);

      // Save to curated_decisions
      const { data: decision, error: decisionError } = await supabase
        .from('curated_decisions')
        .insert({
          decision: response.decision,
          reason: response.reason,
          model_used: 'gpt-4o-mini',
          created_at: new Date().toISOString()
        })
        .select()
        .single();

      if (decisionError) throw decisionError;

      console.log(JSON.stringify({ level: 'info', message: `Saved decision for ID: ${item.id}` }));

      // Create decision_evidence_map
      const { error: mapError } = await supabase
        .from('decision_evidence_map')
        .insert({
          curated_decision_id: decision.id,
          evidence_source_id: item.id,
          source_snapshot_id: snapshot.id
        });

      if (mapError) throw mapError;

      // Mark classified=true
      const { error: updateError } = await supabase
        .from('evidence_sources')
        .update({ classified: true })
        .eq('id', item.id);

      if (updateError) throw updateError;

      console.log(JSON.stringify({ level: 'info', message: `Marked classified=true for ID: ${item.id}` }));

    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: `Failed to process ID: ${item.id}`, error: error.message }));
    }
  }
}

async function main() {
  console.log(JSON.stringify({ level: 'info', message: 'Worker started' }));

  while (true) {
    try {
      // Fetch unclassified evidence_sources
      const { data: batch, error } = await supabase
        .from('evidence_sources')
        .select('id, url')
        .eq('classified', false)
        .limit(BATCH_SIZE);

      if (error) throw error;

      if (batch.length === 0) {
        console.log(JSON.stringify({ level: 'info', message: 'No more items to process, sleeping for 10 seconds' }));
        await sleep(10000);
        continue;
      }

      await processBatch(batch);

    } catch (error) {
      console.log(JSON.stringify({ level: 'error', message: 'Error in main loop', error: error.message }));
      await sleep(5000);
    }
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log(JSON.stringify({ level: 'info', message: 'Received SIGINT, shutting down gracefully' }));
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log(JSON.stringify({ level: 'info', message: 'Received SIGTERM, shutting down gracefully' }));
  process.exit(0);
});

main().catch(error => {
  console.log(JSON.stringify({ level: 'error', message: 'Unhandled error in main', error: error.message }));
  process.exit(1);
});
