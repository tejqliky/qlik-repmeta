# Qlik RepMeta

> Customer inventory + operational insights for Qlik Replicate

![Qlik RepMeta banner](./Qlik_Banner.png)

---

## Table of Contents

- [Overview](#overview)
- [What RepMeta Gives You](#what-repmeta-gives-you)
- [Architecture](#architecture)
- [Inputs & Outputs](#inputs--outputs)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Clone the Repo](#clone-the-repo)
  - [Database Setup](#database-setup)
  - [Backend Setup (FastAPI)](#backend-setup-fastapi)
  - [Frontend Setup (React)](#frontend-setup-react)
- [Typical Workflow](#typical-workflow)
- [Database Schema Highlights](#database-schema-highlights)
- [Customer Technical Overview (.docx)](#customer-technical-overview-docx)
- [Roadmap / Ideas](#roadmap--ideas)
- [Contributing](#contributing)
- [Notes & Limitations](#notes--limitations)
- [License](#license)

---

## Overview

Qlik RepMeta is a self-service internal tool for Qlik CSEs, Solution Architects and PS to quickly understand a customer’s Qlik Replicate footprint.

You upload **Replicate Repository JSONs**, **QEM TSV metrics**, and (optionally) a **Replicate task log**. RepMeta:

- Normalizes everything into **PostgreSQL**
- Exposes a **React UI** to browse counts, trends and inventory
- Generates a **Qlik-branded Customer Technical Overview (.docx)** that you can drop straight into a customer meeting

> ⚠️ RepMeta is **not an official Qlik product** – it’s a field tool to speed up platform reviews, scoping conversations and quarterly health checks.

---

## What RepMeta Gives You

From a handful of artifacts, RepMeta builds:

- **Inventory view**
  - Servers, tasks, endpoints and endpoint families
  - Source/target mix across the environment
- **Operational posture**
  - Task counts by status
  - QEM-derived throughput and latency metrics (where available)
- **License vs usage**
  - How licensed endpoint families compare to what the customer is actually using
- **Data-driven insights**
  - Tasks with **null targets** or “dangling” configurations
  - **Duplicate endpoint configurations** worth consolidating
  - Source × Target coverage matrix to spot gaps and common patterns
- **Customer-ready document**
  - Single click export of a **Customer Technical Overview** branded with Qlik logo and banner

---

## Architecture

At a high level:

```text
Qlik artifacts  ──►  FastAPI (backend/app)  ──►  PostgreSQL (repmeta schema)
                        │
                        ▼
                 React + Vite UI (frontend/repmeta-ui)
qlik-repmeta/
├─ backend/
│  └─ app/
│     ├─ main.py          # FastAPI routes & dependency wiring
│     ├─ ingest.py        # Replicate repository JSON ingest
│     ├─ ingest_qem.py    # QEM TSV ingest
│     ├─ export_report.py # Generates Customer Technical Overview (.docx)
│     ├─ db.py            # psycopg3 async DB helper
│     └─ .env.example     # Backend env vars template
├─ frontend/
│  └─ repmeta-ui/
│     ├─ src/
│     │  ├─ App.tsx          # Main UI shell
│     │  ├─ DataBrowser.tsx  # Simple counts / lists
│     │  ├─ ReportExport.tsx # Report export flows
│     │  └─ ...              # Other React components
│     └─ .env.example        # Frontend env vars template
├─ schema.sql                # PostgreSQL schema for repmeta
├─ postgres - repmeta.png    # Logical schema diagram
├─ Basic_Template.docx       # Base Word template for export
├─ Qlik_Banner.png           # Banner used in exported docs
└─ Qlik_logo.png             # Qlik logo used in exported docs
