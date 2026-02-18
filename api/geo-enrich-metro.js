#!/usr/bin/env node
/**
 * Metro Area Enrichment for Housing Dashboard
 * 
 * Populates metro_area using the Census county FIPS → CBSA mapping.
 * Approach: county_name + state_abbr → county FIPS → CBSA title
 * 
 * Uses two data sources:
 *   1. Census county FIPS lookup (state+county name → FIPS)
 *   2. OMB CBSA delineation (county FIPS → metro area name)
 * 
 * Usage: node geo-enrich-metro.js [--dry-run]
 */

const https = require('https');
const { Pool } = require('pg');

const DRY_RUN = process.argv.includes('--dry-run');
const BATCH_SIZE = 500;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_gNhrxuR1Uv8S@ep-bold-star-aeeibsjz-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require',
  ssl: { rejectUnauthorized: false },
  max: 5,
});

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

// State abbr → FIPS
const STATE_FIPS = {
  'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09','DE':'10',
  'DC':'11','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17','IN':'18','IA':'19',
  'KS':'20','KY':'21','LA':'22','ME':'23','MD':'24','MA':'25','MI':'26','MN':'27',
  'MS':'28','MO':'29','MT':'30','NE':'31','NV':'32','NH':'33','NJ':'34','NM':'35',
  'NY':'36','NC':'37','ND':'38','OH':'39','OK':'40','OR':'41','PA':'42','RI':'44',
  'SC':'45','SD':'46','TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53',
  'WV':'54','WI':'55','WY':'56','AS':'60','GU':'66','MP':'69','PR':'72','VI':'78',
};

async function main() {
  console.log('=== Metro Area Enrichment ===');
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}\n`);

  // Step 1: Get unique county+state combos from DB that need metro
  const counties = await pool.query(`
    SELECT DISTINCT state_abbr, county_name 
    FROM housing_stats 
    WHERE state_abbr IS NOT NULL AND state_abbr != ''
      AND county_name IS NOT NULL AND county_name != ''
      AND (metro_area IS NULL OR metro_area = '')
    ORDER BY state_abbr, county_name
  `);
  console.log(`Counties needing metro: ${counties.rows.length}`);

  // Step 2: Download CBSA delineation - try multiple sources
  console.log('\nDownloading CBSA delineation data...');
  
  // Source: Census Bureau CBSA-to-county relationship
  // Try the Gazetteer county file first to get county FIPS, then CBSA
  let countyFipsToMetro = new Map(); // "SSFFF" → metro_name
  
  // Try NBER crosswalk (very reliable)
  try {
    const url = 'https://data.nber.org/cbsa-csa-fips-county-crosswalk/cbsa2fipsxw.csv';
    const data = await download(url);
    const lines = data.trim().split('\n');
    const header = lines[0].split(',').map(h => h.replace(/"/g, '').trim());
    
    console.log(`  NBER crosswalk: ${lines.length - 1} rows, columns: ${header.join(', ')}`);
    
    const fipsCol = header.findIndex(h => /fipscounty|fipscode|fips/i.test(h));
    const stateCol = header.findIndex(h => /fipsstate|statecode/i.test(h));
    const nameCol = header.findIndex(h => /cbsatitle|metroname|cbsa.*name/i.test(h));
    const codeCol = header.findIndex(h => /cbsacode|cbsa(?!title)/i.test(h));
    
    console.log(`  Using columns: fips=${header[fipsCol]}, state=${header[stateCol]}, name=${header[nameCol]}`);
    
    for (let i = 1; i < lines.length; i++) {
      const vals = lines[i].split(',').map(v => v.replace(/"/g, '').trim());
      const sf = (vals[stateCol] || '').padStart(2, '0');
      const cf = (vals[fipsCol] || '').padStart(3, '0');
      const fips = sf + cf;
      const name = vals[nameCol] || '';
      if (fips.length === 5 && name && name !== '""') {
        countyFipsToMetro.set(fips, name);
      }
    }
    console.log(`  Built county FIPS → metro map: ${countyFipsToMetro.size} entries`);
  } catch (e) {
    console.error(`  NBER download failed: ${e.message}`);
    
    // Fallback: try GitHub crosswalk
    try {
      const url2 = 'https://raw.githubusercontent.com/kjhealy/fips-codes/master/county_fips_master.csv';
      const data = await download(url2);
      const lines = data.trim().split('\n');
      console.log(`  Fallback: ${lines.length - 1} rows`);
      // Parse and build map...
    } catch (e2) {
      console.error(`  Fallback also failed: ${e2.message}`);
    }
  }

  if (countyFipsToMetro.size === 0) {
    console.log('\nNo CBSA data available. Aborting metro enrichment.');
    await pool.end();
    return;
  }

  // Step 3: Download county FIPS codes (county name + state → FIPS)
  console.log('\nDownloading county FIPS codes...');
  const countyNameToFips = new Map(); // "state_fips|county_name_lower" → county_fips_5digit
  
  try {
    const url = 'https://raw.githubusercontent.com/kjhealy/fips-codes/master/county_fips_master.csv';
    const data = await download(url);
    const lines = data.trim().split('\n');
    
    // Parse header
    const parseRow = (line) => {
      const fields = [];
      let current = '';
      let inQuote = false;
      for (const c of line) {
        if (c === '"') { inQuote = !inQuote; continue; }
        if (c === ',' && !inQuote) { fields.push(current.trim()); current = ''; continue; }
        if (c === '\r') continue;
        current += c;
      }
      fields.push(current.trim());
      return fields;
    };
    
    const header = parseRow(lines[0]);
    console.log(`  County FIPS: ${lines.length - 1} rows, columns: ${header.slice(0, 8).join(', ')}`);
    
    const fipsCol = header.findIndex(h => /^fips$/i.test(h));
    const nameCol = header.findIndex(h => /county_name|name/i.test(h));
    const stateCol = header.findIndex(h => /state_abbr|state/i.test(h));
    
    for (let i = 1; i < lines.length; i++) {
      const vals = parseRow(lines[i]);
      const fips = (vals[fipsCol] || '').padStart(5, '0');
      const name = (vals[nameCol] || '').toLowerCase();
      const state = (vals[stateCol] || '').toUpperCase();
      
      if (fips.length === 5 && name && state) {
        // Store multiple key formats for fuzzy matching
        countyNameToFips.set(`${state}|${name}`, fips);
        // Also store without "county" suffix
        const short = name.replace(/ county$| parish$| borough$| census area$| municipality$| city and borough$| city$/, '').trim();
        countyNameToFips.set(`${state}|${short}`, fips);
      }
    }
    console.log(`  Built county name → FIPS map: ${countyNameToFips.size} entries`);
  } catch (e) {
    console.error(`  County FIPS download failed: ${e.message}`);
  }

  // Step 4: Match DB counties to metro areas
  console.log('\nMatching counties to metro areas...');
  const updates = []; // { zip_code, metro_area }
  let matched = 0, unmatched = 0;

  // Get all ZIPs needing metro
  const zips = await pool.query(`
    SELECT zip_code, state_abbr, county_name
    FROM housing_stats
    WHERE state_abbr IS NOT NULL AND state_abbr != ''
      AND (metro_area IS NULL OR metro_area = '')
  `);

  for (const row of zips.rows) {
    const state = row.state_abbr;
    const county = (row.county_name || '').toLowerCase();
    const stateFips = STATE_FIPS[state];
    
    if (!stateFips || !county) continue;

    // Try to find county FIPS
    let fips = countyNameToFips.get(`${state}|${county}`);
    if (!fips) {
      // Try without suffix
      const short = county.replace(/ county$| parish$| borough$| census area$| municipality$/, '').trim();
      fips = countyNameToFips.get(`${state}|${short}`);
    }
    
    if (fips) {
      const metro = countyFipsToMetro.get(fips);
      if (metro) {
        updates.push({ zip_code: row.zip_code, metro_area: metro });
        matched++;
      } else {
        unmatched++; // County exists but not in a metro area (rural)
      }
    } else {
      unmatched++;
    }
  }

  console.log(`  Matched: ${matched} ZIPs to metro areas`);
  console.log(`  Unmatched: ${unmatched} (rural or name mismatch)`);
  console.log(`  Total updates: ${updates.length}\n`);

  if (DRY_RUN) {
    console.log('DRY RUN — sample updates (first 15):');
    for (const u of updates.slice(0, 15)) {
      console.log(`  ${u.zip_code} → ${u.metro_area}`);
    }
    await pool.end();
    return;
  }

  // Step 5: Execute updates
  console.log(`Updating ${updates.length} rows...`);
  let totalUpdated = 0;

  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    const zips = batch.map(u => u.zip_code);
    const metros = batch.map(u => u.metro_area);

    const result = await pool.query(`
      UPDATE housing_stats h SET
        metro_area = v.metro,
        updated_at = NOW()
      FROM (SELECT unnest($1::text[]) as zip_code, unnest($2::text[]) as metro) v
      WHERE h.zip_code = v.zip_code
    `, [zips, metros]);

    totalUpdated += result.rowCount;
    const pct = Math.round(((i + batch.length) / updates.length) * 100);
    process.stdout.write(`\r  Progress: ${i + batch.length}/${updates.length} (${pct}%) — ${totalUpdated} rows`);
  }

  console.log('\n');

  // Verify
  const after = await pool.query(`
    SELECT COUNT(*) FILTER (WHERE metro_area IS NULL OR metro_area = '') as missing
    FROM housing_stats
  `);
  console.log(`✅ Metro enrichment complete. ${totalUpdated} rows updated.`);
  console.log(`   Remaining without metro: ${after.rows[0].missing} (expected — rural areas aren't in metro areas)`);

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
