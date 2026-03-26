import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import * as cheerio from 'cheerio';
import fetch from 'node-fetch';

// Validação das variáveis de ambiente
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_key = process.env.SUPABASE_key;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

if (!SUPABASE_URL) {
console.error(JSON.stringify({ level: 'error', message: 'SUPABASE_URL is missing' }));
process.exit(1);
 }
 if (!SUPABASE_key) {
   console.error(JSON.stringify({ level: 'error', message: 'SUPABASE_key is missing' }));
   process.exit(1);
 }
 if (!OPENAI_API_KEY) {
   console.error(JSON.stringify({ level: 'error', message: 'OPENAI_API_KEY is missing' }));
   process.exit(1);
 }
 
// Inicialização dos clientes
 const supabase = createClient(SUPABASE_URL, SUPABASE_key);
 const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
 
 // Função de delay simples
 const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));
 
 // Função de retry com backoff exponencial
 async function retryWithBackoff(fn, retries = 3, baseDelay = 1000) {
   for (let i = 0; i < retries; i++) {
     try {
       return await fn();
     } catch (error) {
       if (i === retries - 1) throw error;
       const waitTime = baseDelay * Math.pow(2, i);
       console.log(JSON.stringify({ level: 'warn', message: `Retry ${i + 1} failed, waiting ${waitTime}ms`, error: error.message }));
       await delay(waitTime);
     }
   }
 }

 // Função principal
 async function processBatch() {
   console.log(JSON.stringify({ level: 'info', message: 'Starting batch processing' }));
   
   // Buscar 5 evidence_sources com classified=false
   const { data: sources, error } = await supabase
     .from('evidence_sources')
     .select('*')
     .eq('classified', false)
     .limit(5);
   
   if (error) {
     console.error(JSON.stringify({ level: 'error', message: 'Error fetching sources', error: error.message }));
     return;
   }
   
   if (sources.length === 0) {
     console.log(JSON.stringify({ level: 'info', message: 'No sources to process, sleeping 10s' }));
     await delay(10000);
     return;
   }
   
   for (const source of sources) {
     try {
       console.log(JSON.stringify({ level: 'info', message: `Processing source ${source.id}` }));
       
       // Fetch URL
       const response = await fetch(source.url);
       if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);
       const html = await response.text();
       
       // Parse HTML com cheerio
       const $ = cheerio.load(html);
       const textContent = $('body').text().trim();
       
       // Salvar em source_snapshots
       const { data: snapshot, error: snapError } = await supabase
         .from('source_snapshots')
         .insert([{ source_id: source.id, content: textContent }])
         .select()
         .single();
       if (snapError) throw snapError;
       
       // Chamar OpenAI gpt-4o-mini
       await delay(1000); // Rate limit 1s
       const completion = await retryWithBackoff(async () => {
         return await openai.chat.completions.create({
           model: 'gpt-4o-mini',
           messages: [{ role: 'user', content: `Analyze this content and make a decision: ${textContent.substring(0, 2000)}` }],
         });
       });
       const decision = completion.choices[0].message.content;
       
       // Salvar em curated_decisions
       const { data: curated, error: curError } = await supabase
         .from('curated_decisions')
         .insert([{ decision_text: decision }])
         .select()
         .single();
       if (curError) throw curError;
       
       // Criar link em decision_evidence_map
       const { error: mapError } = await supabase
         .from('decision_evidence_map')
         .insert([{ decision_id: curated.id, evidence_id: snapshot.id }]);
       if (mapError) throw mapError;
       
       // Marcar classified=true
       const { error: updateError } = await supabase
         .from('evidence_sources')
         .update({ classified: true })
         .eq('id', source.id);
       if (updateError) throw updateError;
       
       console.log(JSON.stringify({ level: 'info', message: `Processed source ${source.id}` }));
     } catch (err) {
       console.error(JSON.stringify({ level: 'error', message: `Error processing source ${source.id}`, error: err.message }));
     }
   }
 }
 
 // Loop infinito
 async function main() {
   while (true) {
     await processBatch();
   }
 }
 
 // Graceful shutdown
 process.on('SIGINT', () => {
   console.log(JSON.stringify({ level: 'info', message: 'Received SIGINT, shutting down' }));
   process.exit(0);
 });
 process.on('SIGTERM', () => {
   console.log(JSON.stringify({ level: 'info', message: 'Received SIGTERM, shutting down' }));
   process.exit(0);
 });
 
 // Iniciar
 main().catch(err => {
   console.error(JSON.stringify({ level: 'error', message: 'Main loop error', error: err.message }));
   process.exit(1);
 });
