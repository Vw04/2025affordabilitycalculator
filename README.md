# Homium Housing Research Dashboard

Explore US housing statistics across 66,362 ZIP codes. Built on Census ACS 2023 data + Redfin market data.

## Live Dashboard

**Frontend:** Deployed to GitHub Pages  
**API:** Express.js + Neon PostgreSQL

## Architecture

```
docs/           → Static frontend (HTML/CSS/JS) — GitHub Pages
api/            → Express.js API server
  server.js     → 8 REST endpoints connecting Neon PostgreSQL
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/research/stats/zip/:zipCode` | GET | ZIP code statistics + national comparison |
| `/api/v1/research/stats/state/:stateAbbr` | GET | State-level aggregations |
| `/api/v1/research/search?q=787` | GET | Search by ZIP prefix or geography name |
| `/api/v1/research/compare` | POST | Compare multiple ZIP codes side-by-side |
| `/api/v1/research/export/csv` | GET | CSV export |
| `/api/v1/research/quality/report` | GET | Data completeness metrics |
| `/api/v1/research/summary` | GET | National overview statistics |
| `/health` | GET | Health check |

## Local Development

```bash
# API
cd api/
npm install
node server.js  # Runs on http://localhost:3000

# Frontend
# Open docs/index.html in a browser, or serve with:
npx serve docs/
```

## Data

- **66,362 records** from Census ACS 5-Year Estimates (2023)
- **Redfin market data** for ~69% of ZIP codes
- Stored in Neon PostgreSQL (serverless)
- Key metrics: homeownership rate, home prices, rent, income, population, vacancy

## Built By

[Homium](https://homium.com) · Powered by EIR-OS
