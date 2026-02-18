#!/usr/bin/env node
/**
 * Geographic Enrichment Script for Housing Dashboard
 * 
 * Populates state_abbr, county_name, and metro_area in housing_stats table
 * using multiple data sources:
 *   1. GitHub us-state-county-zip dataset (ZIP → state, county)
 *   2. `zipcodes` npm package (fallback for state)
 *   3. HUD USPS ZIP-CBSA crosswalk (ZIP → metro area)
 * 
 * Usage: node geo-enrich.js [--dry-run] [--batch-size=500]
 * 
 * Ticket: 260217-010
 * Created: 2026-02-17
 */

const https = require('https');
const { Pool } = require('pg');
const zipcodes = require('zipcodes');

const DRY_RUN = process.argv.includes('--dry-run');
const BATCH_SIZE = parseInt((process.argv.find(a => a.startsWith('--batch-size=')) || '').split('=')[1]) || 500;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_gNhrxuR1Uv8S@ep-bold-star-aeeibsjz-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require',
  ssl: { rejectUnauthorized: false },
  max: 5,
});

// ── Download helper with redirect following ──
function download(url) {
  return new Promise((resolve, reject) => {
    const get = (u, depth = 0) => {
      if (depth > 5) return reject(new Error('Too many redirects'));
      const client = u.startsWith('https') ? https : require('http');
      client.get(u, { headers: { 'User-Agent': 'EIR-OS/1.0' } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const loc = res.headers.location.startsWith('http') ? res.headers.location : new URL(res.headers.location, u).href;
          return get(loc, depth + 1);
        }
        if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve(data));
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

// ── Parse simple CSV ──
function parseCSV(text) {
  const lines = text.trim().split('\n');
  const parseRow = (line) => {
    const fields = [];
    let current = '';
    let inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { inQuote = !inQuote; continue; }
      if (c === ',' && !inQuote) { fields.push(current.trim()); current = ''; continue; }
      if (c === '\r') continue;
      current += c;
    }
    fields.push(current.trim());
    return fields;
  };
  const header = parseRow(lines[0]);
  return { header, rows: lines.slice(1).map(parseRow) };
}

async function main() {
  console.log('=== Geographic Enrichment for Housing Dashboard ===');
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'} | Batch size: ${BATCH_SIZE}\n`);

  // Step 1: Check current gaps
  const gaps = await pool.query(`
    SELECT COUNT(*) as total,
           COUNT(*) FILTER (WHERE state_abbr IS NULL OR state_abbr = '') as missing_state,
           COUNT(*) FILTER (WHERE county_name IS NULL OR county_name = '') as missing_county,
           COUNT(*) FILTER (WHERE metro_area IS NULL OR metro_area = '') as missing_metro
    FROM housing_stats
  `);
  const g = gaps.rows[0];
  console.log(`Current gaps: ${g.missing_state}/${g.total} missing state, ${g.missing_county}/${g.total} missing county, ${g.missing_metro}/${g.total} missing metro\n`);

  // Step 2: Download ZIP → State/County dataset
  console.log('[1/3] Downloading ZIP-to-county dataset...');
  const zipGeo = new Map(); // zip → { state_abbr, county_name }
  
  try {
    const csvUrl = 'https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv';
    const csvData = await download(csvUrl);
    const { header, rows } = parseCSV(csvData);
    
    // Fields: state_fips, state, state_abbr, zipcode, county, city
    const stateIdx = header.indexOf('state_abbr');
    const zipIdx = header.indexOf('zipcode');
    const countyIdx = header.indexOf('county');
    
    console.log(`  Downloaded ${rows.length} ZIP-county mappings`);
    
    for (const row of rows) {
      const zip = (row[zipIdx] || '').padStart(5, '0');
      if (zip.length !== 5) continue;
      
      const state = row[stateIdx] || '';
      const county = row[countyIdx] || '';
      
      // Keep the first occurrence (or could deduplicate by most common)
      if (!zipGeo.has(zip)) {
        zipGeo.set(zip, { state_abbr: state, county_name: county ? county + ' County' : '' });
      }
    }
    console.log(`  Built ZIP→county map: ${zipGeo.size} entries`);
  } catch (e) {
    console.error(`  Download failed: ${e.message}`);
  }

  // Step 3: Fill state gaps from `zipcodes` npm package
  console.log('[2/3] Supplementing with zipcodes npm package...');
  let npmFills = 0;
  
  // Get all zips from DB
  const allZips = await pool.query('SELECT DISTINCT zip_code FROM housing_stats');
  for (const row of allZips.rows) {
    const zip = row.zip_code;
    if (!zipGeo.has(zip)) {
      const lookup = zipcodes.lookup(zip);
      if (lookup) {
        zipGeo.set(zip, { 
          state_abbr: lookup.state || '', 
          county_name: '' // zipcodes pkg doesn't have county
        });
        npmFills++;
      }
    } else if (!zipGeo.get(zip).state_abbr) {
      const lookup = zipcodes.lookup(zip);
      if (lookup && lookup.state) {
        zipGeo.get(zip).state_abbr = lookup.state;
        npmFills++;
      }
    }
  }
  console.log(`  Filled ${npmFills} additional ZIPs from npm package`);
  console.log(`  Total ZIP coverage: ${zipGeo.size}/${allZips.rows.length}`);

  // Step 4: Download CBSA metro area data
  console.log('[3/3] Building metro area mapping...');
  const zipMetro = new Map(); // zip → metro_area
  
  // Use HUD crosswalk or Census CBSA. Let's try multiple sources.
  try {
    // Try the Census CBSA delineation
    const cbsaUrl = 'https://raw.githubusercontent.com/mwkracht/zip_to_cbsa/master/zip_to_cbsa/data/zip_cbsa.csv';
    const cbsaData = await download(cbsaUrl);
    const { header, rows } = parseCSV(cbsaData);
    
    const zipCol = header.findIndex(h => /zip/i.test(h));
    const cbsaCol = header.findIndex(h => /cbsa/i.test(h));
    const metroCol = header.findIndex(h => /metro.*name|cbsa.*title|name/i.test(h));
    
    console.log(`  Downloaded ${rows.length} ZIP-CBSA mappings (columns: ${header.join(', ')})`);
    
    if (metroCol >= 0) {
      for (const row of rows) {
        const zip = (row[zipCol] || '').padStart(5, '0');
        const metro = row[metroCol] || '';
        if (zip.length === 5 && metro) zipMetro.set(zip, metro);
      }
    } else if (cbsaCol >= 0) {
      // If no name column, we need a separate CBSA-to-name lookup
      console.log('  ZIP-CBSA has codes but no names, downloading CBSA names...');
      const cbsaCodes = new Map();
      for (const row of rows) {
        const zip = (row[zipCol] || '').padStart(5, '0');
        const cbsa = row[cbsaCol] || '';
        if (zip.length === 5 && cbsa) cbsaCodes.set(zip, cbsa);
      }
      
      // Download CBSA name lookup
      try {
        const namesUrl = 'https://raw.githubusercontent.com/mwkracht/zip_to_cbsa/master/zip_to_cbsa/data/cbsa.csv';
        const namesData = await download(namesUrl);
        const names = parseCSV(namesData);
        const nameMap = new Map();
        const codeCol = names.header.findIndex(h => /cbsa.*code|code/i.test(h));
        const titleCol = names.header.findIndex(h => /title|name/i.test(h));
        
        if (codeCol >= 0 && titleCol >= 0) {
          for (const row of names.rows) {
            nameMap.set(row[codeCol], row[titleCol]);
          }
          console.log(`  Loaded ${nameMap.size} CBSA names`);
          
          for (const [zip, cbsa] of cbsaCodes) {
            const name = nameMap.get(cbsa);
            if (name) zipMetro.set(zip, name);
          }
        }
      } catch (e2) {
        console.log(`  CBSA names download failed: ${e2.message}`);
      }
    }
    
    console.log(`  Metro coverage: ${zipMetro.size} ZIPs`);
  } catch (e) {
    console.error(`  CBSA download failed: ${e.message}`);
    console.log('  Metro area enrichment will be limited.');
  }

  // Step 5: Build update list
  console.log('\nBuilding updates...');
  const dbZips = await pool.query(`
    SELECT zip_code, state_abbr, county_name, metro_area FROM housing_stats 
    WHERE (state_abbr IS NULL OR state_abbr = '')
       OR (county_name IS NULL OR county_name = '')
       OR (metro_area IS NULL OR metro_area = '')
  `);

  const updates = [];
  let stats = { state: 0, county: 0, metro: 0, noMatch: 0 };

  for (const row of dbZips.rows) {
    const geo = zipGeo.get(row.zip_code);
    const metro = zipMetro.get(row.zip_code);
    
    if (!geo && !metro) { stats.noMatch++; continue; }

    const update = { zip_code: row.zip_code, state_abbr: null, county_name: null, metro_area: null };
    let hasUpdate = false;

    if ((!row.state_abbr || row.state_abbr === '') && geo?.state_abbr) {
      update.state_abbr = geo.state_abbr;
      stats.state++;
      hasUpdate = true;
    }
    if ((!row.county_name || row.county_name === '') && geo?.county_name) {
      update.county_name = geo.county_name;
      stats.county++;
      hasUpdate = true;
    }
    if ((!row.metro_area || row.metro_area === '') && metro) {
      update.metro_area = metro;
      stats.metro++;
      hasUpdate = true;
    }

    if (hasUpdate) updates.push(update);
  }

  console.log(`\nEnrichment summary:`);
  console.log(`  State fills:  ${stats.state}`);
  console.log(`  County fills: ${stats.county}`);
  console.log(`  Metro fills:  ${stats.metro}`);
  console.log(`  No match:     ${stats.noMatch}`);
  console.log(`  Total updates: ${updates.length}\n`);

  if (DRY_RUN) {
    console.log('DRY RUN — no database changes made.\n');
    console.log('Sample updates (first 15):');
    for (const u of updates.slice(0, 15)) {
      console.log(`  ${u.zip_code} → state=${u.state_abbr || '—'}, county=${u.county_name || '—'}, metro=${u.metro_area || '—'}`);
    }
    await pool.end();
    return;
  }

  // Step 6: Execute updates in batches
  console.log(`Updating database in batches of ${BATCH_SIZE}...`);
  let totalUpdated = 0;

  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);

    const zips = batch.map(u => u.zip_code);
    const states = batch.map(u => u.state_abbr);
    const counties = batch.map(u => u.county_name);
    const metros = batch.map(u => u.metro_area);

    const result = await pool.query(`
      UPDATE housing_stats h SET
        state_abbr = COALESCE(v.new_state, h.state_abbr),
        county_name = COALESCE(v.new_county, h.county_name),
        metro_area = COALESCE(v.new_metro, h.metro_area),
        updated_at = NOW()
      FROM (
        SELECT unnest($1::text[]) as zip_code,
               unnest($2::text[]) as new_state,
               unnest($3::text[]) as new_county,
               unnest($4::text[]) as new_metro
      ) v
      WHERE h.zip_code = v.zip_code
    `, [zips, states, counties, metros]);

    totalUpdated += result.rowCount;
    const pct = Math.round(((i + batch.length) / updates.length) * 100);
    process.stdout.write(`\r  Progress: ${i + batch.length}/${updates.length} (${pct}%) — ${totalUpdated} rows updated`);
  }

  console.log('\n');

  // Step 7: Verify
  const after = await pool.query(`
    SELECT COUNT(*) as total,
           COUNT(*) FILTER (WHERE state_abbr IS NULL OR state_abbr = '') as missing_state,
           COUNT(*) FILTER (WHERE county_name IS NULL OR county_name = '') as missing_county,
           COUNT(*) FILTER (WHERE metro_area IS NULL OR metro_area = '') as missing_metro
    FROM housing_stats
  `);
  const a = after.rows[0];

  console.log('=== Results ===');
  console.log(`Total records: ${a.total}`);
  console.log(`State:  ${g.missing_state} → ${a.missing_state} missing (filled ${g.missing_state - a.missing_state})`);
  console.log(`County: ${g.missing_county} → ${a.missing_county} missing (filled ${g.missing_county - a.missing_county})`);
  console.log(`Metro:  ${g.missing_metro} → ${a.missing_metro} missing (filled ${g.missing_metro - a.missing_metro})`);
  console.log(`\n✅ Done. ${totalUpdated} rows updated.`);

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
