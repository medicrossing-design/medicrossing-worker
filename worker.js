import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_key = process.env.SUPABASE_ANON_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

if (!SUPABASE_URL || !SUPABASE_key || !OPENAI_API_KEY) {
    console.log(JSON.stringify({ error: 'Missing environment variables' }));
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_key);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let running = true;
process.on('SIGINT', () => { running = false; });
process.on('SIGTERM', () => { running = false; });

(async () => {
    while (running) {
        const { data: sources, error } = await supabase
            .from('evidence_sources')
            .select('id, url')
            .eq('classified', false)
            .limit(5);
        if (error) {
            console.log(JSON.stringify({ error: error.message }));
            continue;
        }
        if (sources.length === 0) {
            await sleep(10000);
            continue;
        }
        for (const source of sources) {
            try {
                const response = await fetch(source.url);
                const html = await response.text();
                const $ = cheerio.load(html);
                const text = $('body').text().trim();
                const { data: snapshot, error: snapError } = await supabase
                    .from('source_snapshots')
                    .insert({ evidence_source_id: source.id, content: text })
                    .select('id')
                    .single();
                if (snapError) {
                    console.log(JSON.stringify({ error: snapError.message }));
                    continue;
                }
                let decision = null;
                for (let attempt = 0; attempt < 3; attempt++) {
                    try {
                        const completion = await openai.chat.completions.create({
                            model: 'gpt-4o-mini',
                            messages: [{ role: 'user', content: `Analyze this text and make a decision: ${text}` }],
                        });
                        decision = completion.choices[0].message.content;
                        break;
                    } catch (e) {
                        if (attempt < 2) {
                            await sleep(Math.pow(2, attempt) * 1000);
                        } else {
                            console.log(JSON.stringify({ error: e.message }));
                        }
                    }
                }
                if (!decision) continue;
                const { data: curDecision, error: decError } = await supabase
                    .from('curated_decisions')
                    .insert({ decision: decision })
                    .select('id')
                    .single();
                if (decError) {
                    console.log(JSON.stringify({ error: decError.message }));
                    continue;
                }
                await supabase
                    .from('decision_evidence_map')
                    .insert({ curated_decision_id: curDecision.id, evidence_source_id: source.id });
                await supabase
                    .from('evidence_sources')
                    .update({ classified: true })
                    .eq('id', source.id);
                await sleep(1000);
            } catch (e) {
                console.log(JSON.stringify({ error: e.message }));
            }
        }
    }
})();
