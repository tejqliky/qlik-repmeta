# Qlik RepMeta  
> **Customer Inventory + Operational Insights for Qlik Replicate**

![FastAPI](https://img.shields.io/badge/backend-FastAPI-green)
![React](https://img.shields.io/badge/frontend-React%20%2B%20Vite-blue)
![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-blueviolet)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## 🌟 Overview

**Qlik RepMeta** is a self-service internal tool for CSEs and Solution Architects to analyze Qlik Replicate environments.  
Upload your **Replicate Repository JSONs**, **QEM TSV metrics**, and (optionally) a **Replicate task log** to:

- Normalize data into **PostgreSQL**  
- **Browse** counts and trends via a React UI  
- Generate a **modern Customer Technical Overview (.docx)** including:
  - KPI cards and posture vs. the latest GA Replicate train  
  - Endpoint mix (source & target usage)  
  - License coverage vs. customer usage  
  - Insights (null targets, duplicate endpoint configs)  
  - Server roll-ups & Source×Target coverage matrix  

---

## 🏗 Architecture

```
C:\qlik-repmeta\
├─ backend\app\
│  ├─ main.py          # FastAPI routes
│  ├─ ingest.py        # Repo JSON ingest
│  ├─ ingest_qem.py    # QEM TSV ingest
│  ├─ export_report.py # Generates Customer Technical Overview
│  ├─ db.py            # psycopg3 async DB helper
│  └─ .env.example
└─ frontend\repmeta-ui\src\
   ├─ App.tsx          # Main UI
   ├─ DataBrowser.tsx  # Browse counts
   ├─ ReportExport.tsx # Export logic
   └─ .env.example
```

---

## ✨ Key Features

- **FastAPI** backend (async psycopg3) for ingestion and export  
- **React + Vite + Tailwind** UI for uploads, browsing and download  
- **PostgreSQL** normalized schema for:
  - Customers, servers, runs
  - Replicate tasks, endpoints, QEM performance metrics  
  - License details and master endpoint lists  
- **Modern docx export** with:
  - KPI cards  
  - Posture vs. latest GA Replicate release  
  - Source & Target endpoint mix  
  - License usage matrix  
  - Null target detection and duplicate endpoint configs  

---

## 🚀 Quick Start (Windows)

### 1. Clone and Setup

```powershell
git clone https://github.com/tejqliky/qlik-repmeta.git
cd qlik-repmeta
```

### 2. Database

```sql
-- Create DB and schema
CREATE DATABASE repmeta;
\c repmeta
CREATE SCHEMA IF NOT EXISTS repmeta;

-- Apply migration scripts or run provided SQL
```

### 3. Backend

```powershell
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

copy .env.example .env  # update DATABASE_URL etc.

# Start API
python -m uvicorn app.main:app --host 127.0.0.1 --port 8002 --reload
```

### 4. Frontend

```powershell
cd ..\frontend\repmeta-ui
npm install
copy .env.example .env  # set VITE_API_BASE to backend URL
npm run dev
```

Open **http://localhost:5173** in your browser.

---

## 🔄 Typical Workflow

1. Add **Customer**  
2. Upload **Repo JSON** per server  
3. Upload **QEM TSV** (mapping TSV if needed)  
4. (Optional) Upload **Replicate task log** (extract license info)  
5. Export **Customer Technical Overview (.docx)**  

---

## 🧩 Environment Variables

### Backend (`backend/.env`)

| Variable           | Description                                              |
|-------------------|----------------------------------------------------------|
| `DATABASE_URL`     | Postgres URL with credentials                             |
| `REPMETA_SCHEMA`   | Schema name (default `repmeta`)                           |
| `GITHUB_TOKEN`     | Optional token for higher GitHub API rate limits          |

### Frontend (`frontend/repmeta-ui/.env`)

| Variable         | Description                       |
|-----------------|-----------------------------------|
| `VITE_API_BASE`  | Base URL of the backend FastAPI    |

---

## 📊 Database Schema Highlights

- `dim_customer` - Customers  
- `dim_server` - Servers  
- `ingest_run` - Replicate ingestion runs  
- `rep_database` / `rep_task` - Endpoints & tasks  
- `qem_task_perf` - QEM TSV metrics  
- `replicate_latest_release_cache` - Latest GA Replicate version (auto-cached)  
- `endpoint_master_sources` / `endpoint_master_targets` - Master endpoint lists  
- `endpoint_alias_map` - Canonical alias mapping  

---

## 📝 Customer Technical Overview (.docx)

- **Executive Summary** - KPI cards + posture vs. GA train  
- **Customer Insights** - Null targets & duplicate endpoint configs  
- **Environment & Inventory** - Server roll-up & last ingests  
- **Coverage Matrix** - Source × Target  
- **License Usage** - Licensed vs. used endpoints  
- **Server Deep Dives** - Top pairs and metrics  

---

## 🤝 Contributing

- Use feature branches (`feature/<topic>`).  
- Run Black/isort (Python) and Prettier/Eslint (TS) before PRs.  
- Open PRs to `dev`; merge to `main` after review.  

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## 📦 Deployment / Hosting

- The app can run locally or in Docker containers.  
- For internal distribution, host backend on a Windows or Linux server and serve frontend via Netlify, Vercel or GitHub Pages.  
- Database can be self-hosted Postgres or managed cloud Postgres.

---

## 🛡 Security

- `.env` files are **not committed**.  
- Use GitHub Secrets for CI/CD.  
- Rotate credentials periodically.

---

## 📚 Roadmap / Ideas

- [ ] Inline dashboard in UI for quick KPIs  
- [ ] Automated email of .docx report to CSEs  
- [ ] Extended license analysis vs. actual usage  
- [ ] Multi-customer dashboards & trending  

---

## 📄 License

MIT License - see [LICENSE](LICENSE).

---

## 🙌 Acknowledgments

Built by Tej and collaborators at Qlik.  
Inspired by real-world CSE needs to streamline Qlik Replicate platform reviews.

---
