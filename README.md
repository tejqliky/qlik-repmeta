# Qlik RepMeta (Customer Inventory + Ops Insights)

FastAPI (Python) + React (Vite + TS + Tailwind) + PostgreSQL app to ingest Qlik Replicate Repository JSONs and QEM TSVs, explore inventory/metrics, and export a modern **Customer Technical Overview** (.docx).

## Stack
- **Backend:** FastAPI, async psycopg3
- **Frontend:** React, Vite, TypeScript, Tailwind
- **DB:** PostgreSQL (schema: `repmeta`)
- **Docs Export:** python-docx

## Features
- Ingest **Repo JSONs** and **QEM TSVs** per customer
- Normalize & persist into Postgres (dim tables, QEM metrics)
- Browse counts + upload status
- Export slick **Customer Technical Overview (.docx)**
  - KPI cards, versions/posture vs latest GA train
  - Endpoint mix (from TSV with repo fallback)
  - License usage (sources/targets vs license)
  - Insights (null targets, duplicate endpoint configs)
  - Coverage matrix & server deep dives

---

## Quick start (Windows)

### 1) Database
```sql
-- Create DB and schema
CREATE DATABASE repmeta;
\c repmeta
CREATE SCHEMA IF NOT EXISTS repmeta;

-- Create tables (ingest, qem, dim_*)
-- If you have migration scripts, run them here.

### 2) backend
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Copy env template and set DATABASE_URL etc.
copy .env.example .env

# Run
python -m uvicorn app.main:app --host 127.0.0.1 --port 8002 --reload


### 3) frontend
cd frontend\repmeta-ui
npm install
# Copy env template and set VITE_API_BASE
copy ..\..\backend\.env.example NUL >NUL
copy .env.example .env
npm run dev

Open: http://localhost:5173


Typical workflow

Add Customer

Upload Repo JSON (per server)

Upload QEM Servers TSV (mapping for Name ↔ Host if needed)

Upload QEM Metrics TSV

(Optional) Upload one Replicate task log to extract license info

Export Customer Technical Overview (.docx)

Environment & config

backend/.env — DB URL, schema, optional GitHub token for release cache

frontend/.env — VITE_API_BASE to your backend

