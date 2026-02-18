/**
 * Homium Housing Research Dashboard — Frontend
 * v2: ZIP search + state/county browsing
 */

// API base URL
const API_BASE = window.location.hostname === 'localhost' 
  ? 'http://localhost:3000' 
  : 'https://unabashed-empathy.onrender.com';

// State
let currentData = null;
let compareZips = [];
let currentView = 'zip'; // 'zip' | 'state' | 'county'

// State name lookup
const STATE_NAMES = {
  AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',CO:'Colorado',CT:'Connecticut',
  DE:'Delaware',DC:'District of Columbia',FL:'Florida',GA:'Georgia',HI:'Hawaii',ID:'Idaho',IL:'Illinois',
  IN:'Indiana',IA:'Iowa',KS:'Kansas',KY:'Kentucky',LA:'Louisiana',ME:'Maine',MD:'Maryland',MA:'Massachusetts',
  MI:'Michigan',MN:'Minnesota',MS:'Mississippi',MO:'Missouri',MT:'Montana',NE:'Nebraska',NV:'Nevada',
  NH:'New Hampshire',NJ:'New Jersey',NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',
  OH:'Ohio',OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',PR:'Puerto Rico',RI:'Rhode Island',SC:'South Carolina',
  SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',VA:'Virginia',WA:'Washington',
  WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming'
};

// ── DOM Elements ──
const searchInput = document.getElementById('searchInput');
const searchBtn = document.getElementById('searchBtn');
const searchResults = document.getElementById('searchResults');
const statsGrid = document.getElementById('statsGrid');
const mainTitle = document.getElementById('mainTitle');
const mainSubtitle = document.getElementById('mainSubtitle');
const compareList = document.getElementById('compareList');
const addCompareBtn = document.getElementById('addCompareBtn');
const compareBtn = document.getElementById('compareBtn');
const exportCsvBtn = document.getElementById('exportCsvBtn');
const qualityBtn = document.getElementById('qualityBtn');
const stateSelect = document.getElementById('stateSelect');
const countySelect = document.getElementById('countySelect');
const browseZips = document.getElementById('browseZips');

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

// Close search results on outside click
document.addEventListener('click', (e) => {
  if (!e.target.closest('.search-section')) searchResults.classList.add('hidden');
});

async function doSearch() {
  const q = searchInput.value.trim();
  if (!q) return;
  searchResults.classList.add('hidden');
  
  if (/^\d{5}$/.test(q)) {
    await loadZip(q);
  } else if (q.length >= 2) {
    await doAutocomplete();
  }
}

async function doAutocomplete() {
  const q = searchInput.value.trim();
  if (q.length < 2) { searchResults.classList.add('hidden'); return; }
  
  try {
    const data = await fetchAPI(`/api/v1/research/search?q=${encodeURIComponent(q)}&limit=15`);
    if (data.data.length === 0) {
      searchResults.innerHTML = '<div class="search-result-item"><span class="meta">No results</span></div>';
    } else {
      searchResults.innerHTML = data.data.map(r => `
        <div class="search-result-item" onclick="loadZip('${r.zip_code}')">
          <span class="zip">${r.zip_code}</span>
          <span class="meta">${[r.county_name, r.state_abbr, r.metro_area].filter(Boolean).join(', ') || 'US'} · Pop ${fmt.compact(r.population)}</span>
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
  currentView = 'zip';
  
  statsGrid.querySelectorAll('.stat-value').forEach(el => {
    el.innerHTML = '<span class="loading"></span>';
  });
  
  try {
    const data = await fetchAPI(`/api/v1/research/stats/zip/${zipCode}`);
    currentData = data.data;
    renderZipStats(data.data);
    renderComparison(data.data);
    
    const location = [data.data.county_name, data.data.state_abbr].filter(Boolean).join(', ');
    mainTitle.textContent = `ZIP ${zipCode}` + (location ? ` — ${location}` : '');
    
    const parts = [];
    if (data.data.metro_area) parts.push(data.data.metro_area);
    if (data.data.population) parts.push(`Pop. ${fmt.num(data.data.population)}`);
    
    // Add breadcrumb links
    const crumbs = [];
    if (data.data.state_abbr) crumbs.push(`<a onclick="loadState('${data.data.state_abbr}')">${STATE_NAMES[data.data.state_abbr] || data.data.state_abbr}</a>`);
    if (data.data.county_name && data.data.state_abbr) crumbs.push(`<a onclick="loadCounty('${encodeURIComponent(data.data.county_name)}', '${data.data.state_abbr}')">${data.data.county_name}</a>`);
    
    mainSubtitle.innerHTML = (crumbs.length ? crumbs.join(' › ') + ' · ' : '') + parts.join(' · ');
    
  } catch (e) {
    mainTitle.textContent = 'ZIP Not Found';
    mainSubtitle.textContent = `No data for ${zipCode}`;
  }
}

// ── Load State View ──
async function loadState(stateAbbr) {
  currentView = 'state';
  searchResults.classList.add('hidden');
  
  // Update sidebar select
  stateSelect.value = stateAbbr;
  
  statsGrid.innerHTML = Array(4).fill('<div class="stat-card"><div class="stat-label">&nbsp;</div><div class="stat-value"><span class="loading"></span></div></div>').join('');
  
  try {
    const data = await fetchAPI(`/api/v1/research/stats/state/${stateAbbr}`);
    const d = data.data;
    
    mainTitle.textContent = STATE_NAMES[stateAbbr] || stateAbbr;
    mainSubtitle.innerHTML = `${fmt.num(d.zip_count)} ZIP codes · Pop. ${fmt.compact(d.total_population)}`;
    
    renderAggregateStats(d);
    
    // Hide ZIP-specific sections
    document.getElementById('comparisonSection').classList.add('hidden');
    
    // Load counties
    await loadCountyList(stateAbbr);
    
  } catch (e) {
    mainTitle.textContent = 'State Not Found';
    mainSubtitle.textContent = '';
  }
}

// ── Load County View ──
async function loadCounty(countyName, stateAbbr) {
  currentView = 'county';
  searchResults.classList.add('hidden');
  
  statsGrid.innerHTML = Array(4).fill('<div class="stat-card"><div class="stat-label">&nbsp;</div><div class="stat-value"><span class="loading"></span></div></div>').join('');
  
  try {
    const data = await fetchAPI(`/api/v1/research/stats/county/${countyName}?state=${stateAbbr}`);
    const d = data.data;
    
    mainTitle.textContent = `${decodeURIComponent(d.county_name)}, ${d.state_abbr}`;
    mainSubtitle.innerHTML = `<a onclick="loadState('${stateAbbr}')">${STATE_NAMES[stateAbbr] || stateAbbr}</a> › ${decodeURIComponent(d.county_name)} · ${fmt.num(d.zip_count)} ZIPs · Pop. ${fmt.compact(d.total_population)}`;
    
    renderAggregateStats(d);
    renderComparison(d);
    
    // Load ZIPs in county
    await loadZipList(stateAbbr, decodeURIComponent(d.county_name));
    
  } catch (e) {
    mainTitle.textContent = 'County Not Found';
    mainSubtitle.textContent = '';
  }
}

// ── Render Aggregate Stats (state/county) ──
function renderAggregateStats(d) {
  const cards = [
    { label: 'Total Population', value: fmt.compact(d.total_population) },
    { label: 'Avg Home Price', value: fmt.currency(Math.round(d.avg_home_price)) },
    { label: 'Avg Rent', value: fmt.currency(Math.round(d.avg_rent)) },
    { label: 'Avg Income', value: fmt.currency(Math.round(d.avg_income)) },
  ];
  
  statsGrid.innerHTML = cards.map(c => `
    <div class="stat-card">
      <div class="stat-label">${c.label}</div>
      <div class="stat-value">${c.value}</div>
    </div>
  `).join('');
}

// ── Render ZIP Stats Cards ──
function renderZipStats(d) {
  const cards = [
    { label: 'Population', value: fmt.num(d.population) },
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
}

// ── Render National Comparison Bars ──
function renderComparison(d) {
  if (!d.comparison) return;
  
  const comp = d.comparison;
  const isAggregate = currentView !== 'zip';
  const priceVal = isAggregate ? Number(d.avg_home_price) : Number(d.median_home_price);
  const rentVal = isAggregate ? Number(d.avg_rent) : Number(d.median_rent);
  const incomeVal = isAggregate ? Number(d.avg_income) : Number(d.median_household_income);

  const bars = [
    { label: 'Home Price', zip: priceVal, natl: Number(comp.national_avg_home_price), unit: '$', max: Math.max(priceVal, Number(comp.national_avg_home_price)) * 1.2 },
    { label: 'Rent', zip: rentVal, natl: Number(comp.national_avg_rent), unit: '$', max: Math.max(rentVal, Number(comp.national_avg_rent)) * 1.2 },
    { label: 'Income', zip: incomeVal, natl: Number(comp.national_avg_income), unit: '$', max: Math.max(incomeVal, Number(comp.national_avg_income)) * 1.2 },
  ];
  
  const section = document.getElementById('comparisonSection');
  const barsDiv = document.getElementById('comparisonBars');
  
  barsDiv.innerHTML = bars.map(b => {
    const zipPct = Math.min((b.zip / b.max) * 100, 100);
    const natlPct = Math.min((b.natl / b.max) * 100, 100);
    const fmtVal = b.unit === '$' ? fmt.currency(Math.round(b.zip)) : fmt.pct(b.zip);
    
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

// ── State/County Browse ──

// Load state list on init
async function loadStateList() {
  try {
    const data = await fetchAPI('/api/v1/research/list/states');
    stateSelect.innerHTML = '<option value="">Select a state...</option>' +
      data.data.map(s => `<option value="${s.state_abbr}">${STATE_NAMES[s.state_abbr] || s.state_abbr} (${s.zip_count} ZIPs)</option>`).join('');
  } catch (e) {
    console.error('Failed to load states:', e);
  }
}

stateSelect.addEventListener('change', async () => {
  const state = stateSelect.value;
  countySelect.classList.add('hidden');
  browseZips.classList.add('hidden');
  
  if (!state) return;
  await loadState(state);
});

async function loadCountyList(stateAbbr) {
  try {
    const data = await fetchAPI(`/api/v1/research/list/counties?state=${stateAbbr}`);
    
    countySelect.innerHTML = '<option value="">Select a county...</option>' +
      data.data.map(c => `<option value="${c.county_name}">${c.county_name} (${c.zip_count} ZIPs)</option>`).join('');
    countySelect.classList.remove('hidden');
    browseZips.classList.add('hidden');
    
    // Also render county cards in the main area
    renderCountyGrid(data.data, stateAbbr);
    
  } catch (e) {
    console.error('Failed to load counties:', e);
  }
}

countySelect.addEventListener('change', async () => {
  const county = countySelect.value;
  if (!county) return;
  const state = stateSelect.value;
  await loadCounty(encodeURIComponent(county), state);
});

function renderCountyGrid(counties, stateAbbr) {
  // Show county cards in a section below stats
  let section = document.getElementById('countySection');
  if (!section) {
    section = document.createElement('div');
    section.id = 'countySection';
    section.className = 'section';
    // Insert after statsGrid
    statsGrid.parentNode.insertBefore(section, statsGrid.nextSibling);
  }
  
  section.innerHTML = `
    <h3 class="section-title">📍 Counties in ${STATE_NAMES[stateAbbr] || stateAbbr} (${counties.length})</h3>
    <div class="table-wrap">
      <table class="compare-table">
        <thead>
          <tr>
            <th>County</th>
            <th>ZIPs</th>
            <th>Population</th>
            <th>Avg Home Price</th>
          </tr>
        </thead>
        <tbody>
          ${counties.map(c => `
            <tr style="cursor:pointer" onclick="loadCounty('${encodeURIComponent(c.county_name)}', '${stateAbbr}')">
              <td><strong>${c.county_name}</strong></td>
              <td class="mono">${c.zip_count}</td>
              <td class="mono">${fmt.compact(c.total_population)}</td>
              <td class="mono">${fmt.currency(Math.round(c.avg_home_price))}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
  section.classList.remove('hidden');
}

async function loadZipList(stateAbbr, countyName) {
  try {
    const data = await fetchAPI(`/api/v1/research/list/zips?state=${stateAbbr}&county=${encodeURIComponent(countyName)}`);
    
    // Show ZIP list in a section
    let section = document.getElementById('countySection');
    if (!section) {
      section = document.createElement('div');
      section.id = 'countySection';
      section.className = 'section';
      statsGrid.parentNode.insertBefore(section, statsGrid.nextSibling);
    }
    
    section.innerHTML = `
      <h3 class="section-title">📍 ZIP Codes in ${countyName}, ${stateAbbr} (${data.data.length})</h3>
      <div class="table-wrap">
        <table class="compare-table">
          <thead>
            <tr>
              <th>ZIP</th>
              <th>Population</th>
              <th>Home Price</th>
              <th>Rent</th>
              <th>Income</th>
            </tr>
          </thead>
          <tbody>
            ${data.data.map(z => `
              <tr style="cursor:pointer" onclick="loadZip('${z.zip_code}')">
                <td><strong>${z.zip_code}</strong></td>
                <td class="mono">${fmt.compact(z.population)}</td>
                <td class="mono">${fmt.currency(z.median_home_price)}</td>
                <td class="mono">${fmt.currency(z.median_rent)}</td>
                <td class="mono">${fmt.currency(z.median_household_income)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;
    section.classList.remove('hidden');
    
  } catch (e) {
    console.error('Failed to load ZIP list:', e);
  }
}

// ── Compare ──
addCompareBtn.addEventListener('click', () => {
  if (!currentData) return;
  const zip = currentData.zip_code;
  if (!zip || compareZips.includes(zip)) return;
  if (compareZips.length >= 10) return;
  
  compareZips.push(zip);
  renderCompareList();
});

compareBtn.addEventListener('click', async () => {
  if (compareZips.length < 2) return;
  
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
  
  const seen = new Set();
  rows = rows.filter(r => {
    if (seen.has(r.zip_code)) return false;
    seen.add(r.zip_code);
    return true;
  });
  
  const metrics = [
    { key: 'population', label: 'Population', fmt: fmt.num },
    { key: 'median_home_price', label: 'Home Price', fmt: fmt.currency },
    { key: 'median_rent', label: 'Rent', fmt: fmt.currency },
    { key: 'median_household_income', label: 'Income', fmt: fmt.currency },
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
  if (currentView === 'state' && stateSelect.value) {
    window.open(API_BASE + `/api/v1/research/export/csv?state=${stateSelect.value}`, '_blank');
  } else if (currentData && currentData.zip_code) {
    window.open(API_BASE + `/api/v1/research/export/csv?zip_codes=${currentData.zip_code}`, '_blank');
  }
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

document.getElementById('qualityModal').addEventListener('click', (e) => {
  if (e.target.classList.contains('modal')) e.target.classList.add('hidden');
});

// ── Utilities ──
function debounce(fn, ms) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); };
}

// ── Init ──
loadStateList();
loadZip('78701');
