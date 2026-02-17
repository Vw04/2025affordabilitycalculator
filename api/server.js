/**
 * Homium Housing Research Dashboard — API Server
 * Express.js + PostgreSQL (Neon)
 */
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://neondb_owner:npg_gNhrxuR1Uv8S@ep-bold-star-aeeibsjz-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require',
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
});

// Helper: format response
const ok = (data, meta = {}) => ({ success: true, data, meta: { timestamp: new Date().toISOString(), ...meta } });
const err = (msg, code = 400) => ({ success: false, error: msg, code });

// ── Endpoints ──

// 1. GET /api/v1/research/stats/zip/:zipCode
app.get('/api/v1/research/stats/zip/:zipCode', async (req, res) => {
  const { zipCode } = req.params;
  if (!/^\d{5}$/.test(zipCode)) return res.status(400).json(err('Invalid ZIP code format'));
  
  const start = Date.now();
  try {
    const result = await pool.query('SELECT * FROM housing_stats WHERE zip_code = $1', [zipCode]);
    if (result.rows.length === 0) return res.status(404).json(err('ZIP code not found'));
    
    const row = result.rows[0];
    
    // Get national averages for comparison
    const natl = await pool.query(`
      SELECT 
        AVG(homeownership_rate) as avg_homeownership,
        AVG(median_home_price) as avg_home_price,
        AVG(median_rent) as avg_rent,
        AVG(median_household_income) as avg_income
      FROM housing_stats 
      WHERE homeownership_rate IS NOT NULL
    `);
    
    res.json(ok({
      ...row,
      comparison: {
        national_avg_homeownership: parseFloat(natl.rows[0].avg_homeownership).toFixed(2),
        national_avg_home_price: Math.round(parseFloat(natl.rows[0].avg_home_price)),
        national_avg_rent: Math.round(parseFloat(natl.rows[0].avg_rent)),
        national_avg_income: Math.round(parseFloat(natl.rows[0].avg_income)),
      }
    }, { query_time_ms: Date.now() - start }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 2. GET /api/v1/research/stats/state/:stateAbbr
app.get('/api/v1/research/stats/state/:stateAbbr', async (req, res) => {
  const { stateAbbr } = req.params;
  if (!/^[A-Z]{2}$/i.test(stateAbbr)) return res.status(400).json(err('Invalid state abbreviation'));
  
  const start = Date.now();
  try {
    const result = await pool.query(`
      SELECT 
        state_abbr,
        COUNT(*) as zip_count,
        AVG(homeownership_rate) as avg_homeownership,
        AVG(median_home_price) as avg_home_price,
        AVG(median_rent) as avg_rent,
        AVG(median_household_income) as avg_income,
        SUM(population) as total_population,
        AVG(median_age) as avg_age,
        AVG(vacancy_rate) as avg_vacancy
      FROM housing_stats 
      WHERE state_abbr = $1
      GROUP BY state_abbr
    `, [stateAbbr.toUpperCase()]);
    
    if (result.rows.length === 0) return res.status(404).json(err('State not found'));
    
    res.json(ok(result.rows[0], { query_time_ms: Date.now() - start }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 3. GET /api/v1/research/search?q=Austin&type=zip|county|state|metro
app.get('/api/v1/research/search', async (req, res) => {
  const { q, type } = req.query;
  if (!q || q.length < 2) return res.status(400).json(err('Query must be at least 2 characters'));
  
  const start = Date.now();
  const limit = Math.min(parseInt(req.query.limit) || 20, 50);
  
  try {
    let query, params;
    
    if (/^\d+$/.test(q)) {
      // Numeric: search by ZIP code prefix
      query = `SELECT DISTINCT ON (zip_code) zip_code, county_name, state_abbr, metro_area, population, 
               homeownership_rate, median_home_price, median_rent, median_household_income
               FROM housing_stats WHERE zip_code LIKE $1 ORDER BY zip_code, population DESC NULLS LAST LIMIT $2`;
      params = [q + '%', limit];
    } else {
      // Text: search by county, state, or metro area
      const pattern = '%' + q + '%';
      query = `SELECT DISTINCT ON (zip_code) zip_code, county_name, state_abbr, metro_area, population,
               homeownership_rate, median_home_price, median_rent, median_household_income
               FROM housing_stats 
               WHERE county_name ILIKE $1 OR state_name ILIKE $1 OR metro_area ILIKE $1 OR zip_code LIKE $2
               ORDER BY zip_code, population DESC NULLS LAST LIMIT $3`;
      params = [pattern, q + '%', limit];
    }
    
    const result = await pool.query(query, params);
    res.json(ok(result.rows, { query_time_ms: Date.now() - start, count: result.rows.length }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 4. POST /api/v1/research/compare
app.post('/api/v1/research/compare', async (req, res) => {
  const { zip_codes } = req.body;
  if (!zip_codes || !Array.isArray(zip_codes) || zip_codes.length < 2) {
    return res.status(400).json(err('Provide at least 2 zip_codes'));
  }
  if (zip_codes.length > 10) return res.status(400).json(err('Maximum 10 ZIP codes'));
  
  const start = Date.now();
  try {
    const placeholders = zip_codes.map((_, i) => `$${i + 1}`).join(', ');
    const result = await pool.query(
      `SELECT * FROM housing_stats WHERE zip_code IN (${placeholders}) ORDER BY population DESC NULLS LAST`,
      zip_codes
    );
    res.json(ok(result.rows, { query_time_ms: Date.now() - start }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 5. GET /api/v1/research/export/csv
app.get('/api/v1/research/export/csv', async (req, res) => {
  const { zip_codes, state } = req.query;
  
  const start = Date.now();
  try {
    let result;
    if (zip_codes) {
      const zips = zip_codes.split(',').slice(0, 100);
      const placeholders = zips.map((_, i) => `$${i + 1}`).join(', ');
      result = await pool.query(`SELECT * FROM housing_stats WHERE zip_code IN (${placeholders})`, zips);
    } else if (state) {
      result = await pool.query('SELECT * FROM housing_stats WHERE state_abbr = $1', [state.toUpperCase()]);
    } else {
      return res.status(400).json(err('Provide zip_codes or state parameter'));
    }
    
    if (result.rows.length === 0) return res.status(404).json(err('No data found'));
    
    // Generate CSV
    const headers = Object.keys(result.rows[0]);
    const csv = [
      headers.join(','),
      ...result.rows.map(row => headers.map(h => {
        const v = row[h];
        if (v === null) return '';
        if (typeof v === 'string' && v.includes(',')) return `"${v}"`;
        return v;
      }).join(','))
    ].join('\n');
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=housing-stats-${Date.now()}.csv`);
    res.send(csv);
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 6. GET /api/v1/research/quality/report
app.get('/api/v1/research/quality/report', async (req, res) => {
  const start = Date.now();
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(homeownership_rate) as has_homeownership,
        COUNT(median_home_price) as has_price,
        COUNT(median_rent) as has_rent,
        COUNT(median_household_income) as has_income,
        COUNT(population) as has_population,
        COUNT(state_abbr) as has_state,
        COUNT(county_name) as has_county,
        COUNT(metro_area) as has_metro,
        COUNT(redfin_median_sale_price) as has_redfin
      FROM housing_stats
    `);
    
    const r = result.rows[0];
    const total = parseInt(r.total);
    const pct = (n) => ((parseInt(n) / total) * 100).toFixed(1);
    const status = (p) => p >= 95 ? 'excellent' : p >= 80 ? 'good' : p >= 50 ? 'fair' : 'poor';
    
    const metrics = {
      homeownership_rate: { completeness: pct(r.has_homeownership), status: status(pct(r.has_homeownership)) },
      median_home_price: { completeness: pct(r.has_price), status: status(pct(r.has_price)) },
      median_rent: { completeness: pct(r.has_rent), status: status(pct(r.has_rent)) },
      median_household_income: { completeness: pct(r.has_income), status: status(pct(r.has_income)) },
      population: { completeness: pct(r.has_population), status: status(pct(r.has_population)) },
      state_abbr: { completeness: pct(r.has_state), status: status(pct(r.has_state)) },
      county_name: { completeness: pct(r.has_county), status: status(pct(r.has_county)) },
      metro_area: { completeness: pct(r.has_metro), status: status(pct(r.has_metro)) },
      redfin_data: { completeness: pct(r.has_redfin), status: status(pct(r.has_redfin)) },
    };
    
    res.json(ok({ total_records: total, metrics }, { query_time_ms: Date.now() - start }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// 7. GET /api/v1/research/summary — National overview
app.get('/api/v1/research/summary', async (req, res) => {
  const start = Date.now();
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_zips,
        COUNT(DISTINCT state_abbr) as states,
        AVG(homeownership_rate) as avg_homeownership,
        AVG(median_home_price) as avg_home_price,
        AVG(median_rent) as avg_rent,
        AVG(median_household_income) as avg_income,
        SUM(population) as total_population,
        AVG(vacancy_rate) as avg_vacancy
      FROM housing_stats
      WHERE homeownership_rate IS NOT NULL
    `);
    res.json(ok(result.rows[0], { query_time_ms: Date.now() - start }));
  } catch (e) {
    console.error(e);
    res.status(500).json(err('Database error'));
  }
});

// Health check
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', db: 'connected' });
  } catch (e) {
    res.status(500).json({ status: 'error', db: e.message });
  }
});

// Serve static frontend in production
if (process.env.NODE_ENV === 'production') {
  app.use(express.static('../docs'));
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Housing Research API running on port ${PORT}`));
