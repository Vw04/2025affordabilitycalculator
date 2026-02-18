/**
 * Homium Housing Research Dashboard — Frontend
 */

// API base URL — change for production
const API_BASE = window.location.hostname === 'localhost' 
  ? 'http://localhost:3000' 
  : 'https://unabashed-empathy.onrender.com';

// State
let currentData = null;
let compareZips = [];

// ── DOM Elements ──
const searchInput = document.getElementById('searchInput');
const searchBtn = document.getElementById('searchBtn');
const searchResults = document.getElementById('searchResults');
const statsGrid = document.getElementById('statsGrid');
const detailGrid = document.getElementById('detailGrid');
const mainTitle = document.getElementById('mainTitle');
const mainSubtitle = document.getElementById('mainSubtitle');
const compareList = document.getElementById('compareList');
const addCompareBtn = document.getElementById('addCompareBtn');
const compareBtn = document.getElementById('compareBtn');
const exportCsvBtn = document.getElementById('exportCsvBtn');
const qualityBtn = document.getElementById('qualityBtn');

// ── Formatters ──
const fmt = {
  currency: (v) => v == null ? '—' : '$' + Number(v).toLocaleString(),
  pct: (v) => v == null ? '—' : parseFloat(v).toFixed(1) + '%',
  num: (v) => v == null ? '—' : Number(v).toLocaleString(),
  compact: (v) => {
    if (v == null) return '—';
    const n = Number(v);
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return n.toString();
  },
};

// ── API Calls ──
async function fetchAPI(endpoint) {
  try {
    const res = await fetch(API_BASE + endpoint);
    const data = await res.json();
    if (!data.success) throw new Error(data.error || 'API error');
    return data;
  } catch (e) {
    console.error('API error:', e);
    throw e;
  }
}

// ── Search ──
searchBtn.addEventListener('click', () => doSearch());
searchInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doSearch(); });
searchInput.addEventListener('input', debounce(doAutocomplete, 300));

async function doSearch() {
  const q = searchInput.value.trim();
  if (!q) return;
  searchResults.classList.add('hidden');
  
  if (/^\d{5}$/.test(q)) {
    await loadZip(q);
  } else if (/^\d{1,4}$/.test(q)) {
    await doAutocomplete();
  }
}

async function doAutocomplete() {
  const q = searchInput.value.trim();
  if (q.length < 2) { searchResults.classList.add('hidden'); return; }
  
  try {
    const data = await fetchAPI(`/api/v1/research/search?q=${encodeURIComponent(q)}&limit=10`);
    if (data.data.length === 0) {
      searchResults.innerHTML = '<div class="search-result-item"><span class="meta">No results</span></div>';
    } else {
      searchResults.innerHTML = data.data.map(r => `
        <div class="search-result-item" onclick="loadZip('${r.zip_code}')">
          <span class="zip">${r.zip_code}</span>
          <span class="meta">${[r.county_name, r.state_abbr].filter(Boolean).join(', ') || 'US'} · Pop ${fmt.compact(r.population)}</span>
        </div>
      `).join('');
    }
    searchResults.classList.remove('hidden');
  } catch (e) {
    searchResults.classList.add('hidden');
  }
}

// ── Load ZIP Data ──
async function loadZip(zipCode) {
  searchResults.classList.add('hidden');
  searchInput.value = zipCode;
  
  // Show loading
  statsGrid.querySelectorAll('.stat-value').forEach(el => {
    el.innerHTML = '<span class="loading"></span>';
  });
  
  try {
    const data = await fetchAPI(`/api/v1/research/stats/zip/${zipCode}`);
    currentData = data.data;
    renderStats(data.data);
    renderComparison(data.data);
    renderRedfin(data.data);
    
    // Update title
    const location = [data.data.county_name, data.data.state_abbr].filter(Boolean).join(', ');
    mainTitle.textContent = `ZIP ${zipCode}` + (location ? ` — ${location}` : '');
    mainSubtitle.textContent = [data.data.metro_area, `Pop. ${fmt.num(data.data.population)}`].filter(Boolean).join(' · ');
    
  } catch (e) {
    mainTitle.textContent = 'ZIP Not Found';
    mainSubtitle.textContent = `No data for ${zipCode}`;
  }
}

// ── Render Stats Cards ──
function renderStats(d) {
  const cards = [
    { label: 'Homeownership Rate', value: fmt.pct(d.homeownership_rate), compare: d.comparison ? `Nat'l avg: ${d.comparison.national_avg_homeownership}%` : null },
    { label: 'Median Home Price', value: fmt.currency(d.median_home_price), compare: d.comparison ? `Nat'l avg: ${fmt.currency(d.comparison.national_avg_home_price)}` : null },
    { label: 'Median Rent', value: fmt.currency(d.median_rent), compare: d.comparison ? `Nat'l avg: ${fmt.currency(d.comparison.national_avg_rent)}` : null },
    { label: 'Median Income', value: fmt.currency(d.median_household_income), compare: d.comparison ? `Nat'l avg: ${fmt.currency(d.comparison.national_avg_income)}` : null },
  ];
  
  statsGrid.innerHTML = cards.map(c => `
    <div class="stat-card">
      <div class="stat-label">${c.label}</div>
      <div class="stat-value">${c.value}</div>
      ${c.compare ? `<div class="stat-compare">${c.compare}</div>` : ''}
    </div>
  `).join('');
  
  // Detail row
  const setCard = (id, val) => {
    const el = document.querySelector(`#${id} .stat-value`);
    if (el) el.textContent = val;
  };
  setCard('cardPopulation', fmt.num(d.population));
  setCard('cardAge', d.median_age != null ? parseFloat(d.median_age).toFixed(1) : '—');
  setCard('cardVacancy', fmt.pct(d.vacancy_rate));
  setCard('cardOwnerUnits', fmt.compact(d.owner_occupied_units));
  setCard('cardRenterUnits', fmt.compact(d.renter_occupied_units));
  setCard('cardTotalUnits', fmt.compact(d.total_housing_units));
  detailGrid.classList.remove('hidden');
}

// ── Render National Comparison Bars ──
function renderComparison(d) {
  if (!d.comparison) return;
  
  const comp = d.comparison;
  const bars = [
    { label: 'Homeownership', zip: parseFloat(d.homeownership_rate), natl: parseFloat(comp.national_avg_homeownership), unit: '%', max: 100 },
    { label: 'Home Price', zip: Number(d.median_home_price), natl: Number(comp.national_avg_home_price), unit: '$', max: Math.max(Number(d.median_home_price), Number(comp.national_avg_home_price)) * 1.2 },
    { label: 'Rent', zip: Number(d.median_rent), natl: Number(comp.national_avg_rent), unit: '$', max: Math.max(Number(d.median_rent), Number(comp.national_avg_rent)) * 1.2 },
    { label: 'Income', zip: Number(d.median_household_income), natl: Number(comp.national_avg_income), unit: '$', max: Math.max(Number(d.median_household_income), Number(comp.national_avg_income)) * 1.2 },
  ];
  
  const section = document.getElementById('comparisonSection');
  const barsDiv = document.getElementById('comparisonBars');
  
  barsDiv.innerHTML = bars.map(b => {
    const zipPct = Math.min((b.zip / b.max) * 100, 100);
    const natlPct = Math.min((b.natl / b.max) * 100, 100);
    const fmtVal = b.unit === '$' ? fmt.currency(b.zip) : fmt.pct(b.zip);
    
    return `
      <div class="comparison-bar">
        <div class="bar-label">${b.label}</div>
        <div class="bar-track">
          <div class="bar-fill national" style="width: ${natlPct}%"></div>
          <div class="bar-fill zip" style="width: ${zipPct}%"></div>
        </div>
        <div class="bar-value">${fmtVal}</div>
      </div>
    `;
  }).join('');
  
  section.classList.remove('hidden');
}

// ── Render Redfin Market Data ──
function renderRedfin(d) {
  const section = document.getElementById('redfinSection');
  const grid = document.getElementById('redfinGrid');
  
  if (!d.redfin_median_sale_price) { section.classList.add('hidden'); return; }
  
  const cards = [
    { label: 'Redfin Sale Price', value: fmt.currency(d.redfin_median_sale_price) },
    { label: 'Redfin List Price', value: fmt.currency(d.redfin_median_list_price) },
    { label: 'Homes Sold', value: fmt.num(d.redfin_homes_sold) },
    { label: 'Days on Market', value: d.redfin_median_days_on_market != null ? d.redfin_median_days_on_market + ' days' : '—' },
  ];
  
  grid.innerHTML = cards.map(c => `
    <div class="stat-card small">
      <div class="stat-label">${c.label}</div>
      <div class="stat-value">${c.value}</div>
    </div>
  `).join('');
  
  section.classList.remove('hidden');
}

// ── Compare ──
addCompareBtn.addEventListener('click', () => {
  if (!currentData) return;
  const zip = currentData.zip_code;
  if (compareZips.includes(zip)) return;
  if (compareZips.length >= 10) return;
  
  compareZips.push(zip);
  renderCompareList();
});

compareBtn.addEventListener('click', async () => {
  if (compareZips.length < 2) return;
  
  try {
    const data = await fetchAPI('/api/v1/research/compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zip_codes: compareZips }),
    });
    // Fallback: use direct fetch for POST
  } catch (e) {}
  
  // Use direct fetch for POST
  try {
    const res = await fetch(API_BASE + '/api/v1/research/compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zip_codes: compareZips }),
    });
    const data = await res.json();
    if (data.success) renderCompareTable(data.data);
  } catch (e) {
    console.error('Compare error:', e);
  }
});

function renderCompareList() {
  compareList.innerHTML = compareZips.map((zip, i) => `
    <div class="compare-chip">
      <span>${zip}</span>
      <span class="remove" onclick="removeCompare(${i})">&times;</span>
    </div>
  `).join('');
  compareBtn.classList.toggle('hidden', compareZips.length < 2);
}

function removeCompare(idx) {
  compareZips.splice(idx, 1);
  renderCompareList();
  if (compareZips.length < 2) document.getElementById('compareSection').classList.add('hidden');
}

function renderCompareTable(rows) {
  const section = document.getElementById('compareSection');
  const table = document.getElementById('compareTable');
  
  const metrics = [
    { key: 'homeownership_rate', label: 'Homeownership %', fmt: fmt.pct },
    { key: 'median_home_price', label: 'Home Price', fmt: fmt.currency },
    { key: 'median_rent', label: 'Rent', fmt: fmt.currency },
    { key: 'median_household_income', label: 'Income', fmt: fmt.currency },
    { key: 'population', label: 'Population', fmt: fmt.num },
    { key: 'vacancy_rate', label: 'Vacancy %', fmt: fmt.pct },
  ];
  
  table.querySelector('thead').innerHTML = `
    <tr>
      <th>Metric</th>
      ${rows.map(r => `<th>ZIP ${r.zip_code}</th>`).join('')}
    </tr>
  `;
  
  table.querySelector('tbody').innerHTML = metrics.map(m => `
    <tr>
      <td>${m.label}</td>
      ${rows.map(r => `<td class="mono">${m.fmt(r[m.key])}</td>`).join('')}
    </tr>
  `).join('');
  
  section.classList.remove('hidden');
}

// ── Export CSV ──
exportCsvBtn.addEventListener('click', async () => {
  if (!currentData) return;
  const zip = currentData.zip_code;
  window.open(API_BASE + `/api/v1/research/export/csv?zip_codes=${zip}`, '_blank');
});

// ── Data Quality ──
qualityBtn.addEventListener('click', async () => {
  const modal = document.getElementById('qualityModal');
  const body = document.getElementById('qualityBody');
  
  try {
    const data = await fetchAPI('/api/v1/research/quality/report');
    const metrics = data.data.metrics;
    
    body.innerHTML = `
      <p style="margin-bottom: 16px; color: var(--slate); font-size: 0.85rem;">
        Total records: <strong>${fmt.num(data.data.total_records)}</strong>
      </p>
      ${Object.entries(metrics).map(([key, m]) => {
        const pct = parseFloat(m.completeness);
        const color = m.status === 'excellent' ? 'var(--green)' : m.status === 'good' ? 'var(--blue)' : m.status === 'fair' ? 'var(--amber)' : 'var(--red)';
        return `
          <div class="quality-row">
            <div class="quality-label">${key.replace(/_/g, ' ')}</div>
            <div class="quality-bar"><div class="quality-fill" style="width: ${pct}%; background: ${color}"></div></div>
            <div class="quality-pct">${m.completeness}%</div>
            <div class="quality-status ${m.status}">${m.status}</div>
          </div>
        `;
      }).join('')}
    `;
    
    modal.classList.remove('hidden');
  } catch (e) {
    alert('Failed to load quality report');
  }
});

// Close modal on backdrop click
document.getElementById('qualityModal').addEventListener('click', (e) => {
  if (e.target.classList.contains('modal')) e.target.classList.add('hidden');
});

// ── Utilities ──
function debounce(fn, ms) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); };
}

// ── Init ──
// Load Austin by default
loadZip('78701');
