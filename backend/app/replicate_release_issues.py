#!/usr/bin/env python3
"""
replicate_release_issues.py

Discovery strategy:
  (1) Community hub scrape (b-ReleaseNotes) for anchors to articles
  (2) Slug-targeted lookup on the "permanent link" page with client-managed label
  (3) Fallback to Qlik Help release notes pages for the same train
"""

from __future__ import annotations

import argparse
import csv
import dataclasses as dc
import json
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Logging — resilient (never raises NameError)
# -----------------------------------------------------------------------------
def _ensure_logger() -> logging.Logger:
    lvl_name = (os.getenv("RELEASE_LOG_LEVEL") or "INFO").upper()
    lvl = getattr(logging, lvl_name, logging.INFO)
    lg = logging.getLogger("repmeta.release_issues")
    if not lg.handlers:
        logging.basicConfig(level=lvl, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    lg.setLevel(lvl)
    return lg

LOG = _ensure_logger()

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

# Help page used to detect the latest GA train (e.g., "May 2025")
HELP_TRAIN_URL = "https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Release_Notes/features.htm"

# Community (board) hub we first try
COMMUNITY_RELEASE_NOTES_HUB = "https://community.qlik.com/t5/b-ReleaseNotes/Qlik%2BReplicate/pd-p/qlikReplicate"

# Community "permanent link" filtered to client-managed label (user provided)
COMMUNITY_PERMA_LINK = (
    "https://community.qlik.com/t5/Release-Notes/"
    "tkb-p/ReleaseNotes/label-name/client%20managed?labels=client+managed"
)

ARTICLE_TITLE_PATTERNS = [
    r"Qlik Replicate May 20\d{2}.*(Initial Release|Service Release|SR\s*\d+)",
    r"Qlik Replicate November 20\d{2}.*(Initial Release|Service Release|SR\s*\d+)",
    r"Qlik Replicate .*Technical Preview",
]

RESOLVED_HDR_RX = re.compile(r"\b(Resolved\s+issues|Issues\s+resolved)\b", re.I)

CANONICAL_ENDPOINTS: Dict[str, List[str]] = {
    "Oracle": [r"\bOracle\b", r"\bASM\b", r"\bGoldenGate\b"],
    "Microsoft SQL Server": [r"\bSQL Server\b", r"\bMSSQL\b", r"\bAzure SQL\b", r"\bManaged Instance\b"],
    "PostgreSQL": [r"\bPostgreSQL\b", r"\bAurora PostgreSQL\b", r"\bRDS for PostgreSQL\b"],
    "MySQL": [r"\bMySQL\b", r"\bPercona\b", r"\bAurora MySQL\b", r"\bRDS for MySQL\b"],
    "Snowflake": [r"\bSnowflake\b"],
    "Databricks Lakehouse (Delta)": [r"\bDatabricks\b", r"\bDelta\b"],
    "Google BigQuery": [r"\bBigQuery\b", r"\bGoogle Cloud BigQuery\b"],
    "Kafka / MSK / Confluent": [r"\bKafka\b", r"\bMSK\b", r"\bConfluent\b", r"\bSchema Registry\b"],
    "IBM Db2 LUW": [r"\bDB2\b", r"\bDb2\b", r"\bLUW\b"],
    "IBM Db2 z/OS": [r"\bz/OS\b", r"\bDB2 z/OS\b", r"\bDb2 for z/OS\b"],
    "SAP HANA": [r"\bSAP HANA\b", r"\bHANA\b"],
    "SAP OData": [r"\bSAP OData\b"],
    "MongoDB": [r"\bMongoDB\b"],
    "Microsoft ADLS / HDInsight": [r"\bADLS\b", r"\bHDInsight\b"],
    "Amazon S3": [r"\bAmazon S3\b", r"\bS3-compatible\b"],
    "Google Cloud SQL (MySQL|PostgreSQL)": [r"\bGoogle Cloud SQL\b"],
    "MariaDB": [r"\bMariaDB\b"],
    "IMS": [r"\bIBM IMS\b", r"\bIMS\b"],
}

GENERAL_BUCKETS: Dict[str, List[str]] = {
    "Engine/Task": [r"\btask\b", r"\bFull Load\b", r"\bCDC\b", r"\bperformance\b", r"\blatency\b", r"\bapply\b", r"\bmetadata\b"],
    "Console/UI": [r"\bConsole\b", r"\bUI\b", r"\bWeb UI\b"],
    "Install/Upgrade": [r"\binstall\b", r"\bupgrade\b", r"\bsetup\b", r"\bmigration\b"],
    "Licensing": [r"\blicense\b", r"\blicensing\b"],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonScraper/1.0",
    "Accept-Language": "en-US,en;q=0.9",
}

JIRA_RX = re.compile(r"\b(?:RECOB|QID|QEM|ATT|QDMC|QCB|QRS)[-–—]\s*\d+\b", re.I)

# -----------------------------------------------------------------------------
# Data
# -----------------------------------------------------------------------------

@dc.dataclass
class Issue:
    version: str
    date: Optional[str]
    title: str
    url: str
    text: str
    jira: Optional[str]
    endpoints: List[str]
    buckets: List[str]

# -----------------------------------------------------------------------------
# HTTP helpers
# -----------------------------------------------------------------------------

def http_get(url: str, cookie: Optional[str] = None) -> str:
    headers = dict(HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    LOG.debug("HTTP GET %s (cookie_set=%s)", url, bool(cookie))
    r = requests.get(url, headers=headers, timeout=45)
    r.raise_for_status()
    return r.text

# -----------------------------------------------------------------------------
# Train helpers
# -----------------------------------------------------------------------------

def parse_latest_train(help_html: str) -> str:
    soup = BeautifulSoup(help_html, "html.parser")
    nav = soup.find(string=re.compile(r"(May|November)\s+20\d{2}"))
    if nav:
        m = re.search(r"(May|November)\s+20\d{2}", nav)
        if m:
            return m.group(0)
    return "May 2025"

def train_label_to_slug_prefix(train_label: str) -> str:
    # "May 2025" -> "Qlik-Replicate-May-2025-Initial-Release-until-Service-Release-1"
    month, year = train_label.split()
    return f"Qlik-Replicate-{month}-{year}-Initial-Release-until-Service-Release-1"

# -----------------------------------------------------------------------------
# Community discovery
# -----------------------------------------------------------------------------

def _title_looks_like_release_notes(t: str) -> bool:
    if any(re.search(pat, t, re.I) for pat in ARTICLE_TITLE_PATTERNS):
        return True
    return bool(
        re.search(r"Qlik\s*Replicate\s+(May|November)\s+20\d{2}", t, re.I)
        and re.search(r"(Release\s+Notes|Initial\s+Release|Service\s+Release|SR\s*\d+|Technical\s+Preview)", t, re.I)
    )

def find_candidate_articles(hub_html: str, include_previews: bool) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(hub_html, "html.parser")
    matches: List[Tuple[str, str]] = []

    # a) simple anchors
    for a in soup.find_all("a", href=True):
        t = a.get_text(strip=True) or a.get("title", "").strip()
        href = a["href"]
        if not t or not href:
            continue
        if not _title_looks_like_release_notes(t):
            continue
        if not include_previews and re.search(r"Technical\s+Preview", t, re.I):
            continue
        if any(seg in href for seg in ("/ta-p/", "/td-p/", "/kb/")):
            url = "https://community.qlik.com" + href if href.startswith("/") else href
            matches.append((t, url))

    # b) card blocks
    for card in soup.select("[data-t], [data-slingo], .lia-tile, .lia-card"):
        a = card.find("a", href=True)
        if not a:
            continue
        t = a.get_text(strip=True) or a.get("title", "").strip()
        href = a["href"]
        if not t or not href:
            continue
        if not _title_looks_like_release_notes(t):
            continue
        if not include_previews and re.search(r"Technical\s+Preview", t, re.I):
            continue
        if any(seg in href for seg in ("/ta-p/", "/td-p/", "/kb/")):
            url = "https://community.qlik.com" + href if href.startswith("/") else href
            matches.append((t, url))

    # de-dupe
    seen = set()
    out: List[Tuple[str, str]] = []
    for t, u in matches:
        if u in seen:
            continue
        seen.add(u)
        out.append((t, u))

    LOG.debug("find_candidate_articles: matched=%d", len(out))
    return out

def find_article_via_permanent_page(train_label: str, cookie: Optional[str]) -> Optional[Tuple[str, str]]:
    """
    Fallback: fetch the "permanent" client-managed listing page and look for:
      /t5/Release-Notes/Qlik-Replicate-{Month}-{Year}-Initial-Release-until-Service-Release-1/ta-p/<id>
    """
    html = http_get(COMMUNITY_PERMA_LINK, cookie)
    soup = BeautifulSoup(html, "html.parser")
    slug_prefix = train_label_to_slug_prefix(train_label)
    rx = re.compile(rf"/t5/Release-Notes/{re.escape(slug_prefix)}/ta-p/\d+", re.I)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        t = a.get_text(strip=True) or a.get("title", "").strip()
        if rx.search(href):
            url = "https://community.qlik.com" + href if href.startswith("/") else href
            LOG.info("permanent-page matched article: %s", url)
            return (t or slug_prefix, url)

    LOG.warning("permanent-page: no href matched slug %s", slug_prefix)
    return None

# -----------------------------------------------------------------------------
# Extraction (lists, tables, paragraphs, JIRA fallback)
# -----------------------------------------------------------------------------

def extract_resolved_issues_from_article(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[str] = []

    def clean(txt: str) -> str:
        return " ".join((txt or "").split())

    def add_txt(txt: str):
        t = clean(txt)
        if t and t not in items:
            items.append(t)

    def harvest_from(node):
        for ul in node.find_all(["ul", "ol"], recursive=True):
            for li in ul.find_all("li", recursive=True):
                add_txt(li.get_text(" ", strip=True))
        for table in node.find_all("table", recursive=True):
            for tr in table.find_all("tr"):
                cells = [clean(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
                rowtxt = " – ".join([c for c in cells if c])
                if rowtxt:
                    add_txt(rowtxt)
        for p in node.find_all("p", recursive=True):
            txt = clean(p.get_text(" ", strip=True))
            if len(txt.split()) >= 5:
                add_txt(txt)

    heads = [h for h in soup.find_all(re.compile(r"^h[1-6]$", re.I))
             if RESOLVED_HDR_RX.search(h.get_text(" ", strip=True) or "")]
    LOG.debug("extract_resolved_issues_from_article: resolved headings found=%d", len(heads))
    for h in heads:
        for sib in h.find_all_next():
            if sib.name and re.match(r"^h[1-6]$", sib.name, re.I):
                break
            harvest_from(sib)

    if not items:
        LOG.debug("no list/table/para under resolved headings -> global JIRA-key fallback")
        body = soup.body or soup
        for li in body.find_all("li"):
            txt = clean(li.get_text(" ", strip=True))
            if JIRA_RX.search(txt):
                add_txt(txt)
        for p in body.find_all("p"):
            txt = clean(p.get_text(" ", strip=True))
            if JIRA_RX.search(txt):
                add_txt(txt)
        for tr in body.find_all("tr"):
            cells = [clean(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            rowtxt = " – ".join([c for c in cells if c])
            if rowtxt and JIRA_RX.search(rowtxt):
                add_txt(rowtxt)

    LOG.debug("extract_resolved_issues_from_article: items=%d", len(items))
    return items

# -----------------------------------------------------------------------------
# Help fallback
# -----------------------------------------------------------------------------

def _train_code_from_label(train_label: str) -> str:
    return train_label.replace(" ", "")

def candidate_help_pages(train_label: str) -> List[Tuple[str, str]]:
    base = f"https://help.qlik.com/en-US/replicate/{_train_code_from_label(train_label)}/Content/Replicate/Main/Release_Notes/"
    pages = [
        "Resolved-issues.htm",
        "release-notes.htm",
        "Resolved-issues-Service-Release-1.htm",
        "Service-Release-1.htm",
        "Resolved-issues-Service-Release-2.htm",
        "Service-Release-2.htm",
        "Resolved-issues-Initial-Release.htm",
        "Initial-Release.htm",
        "Updates.htm",
    ]
    return [(p, base + p) for p in pages]

def extract_resolved_issues_from_help(html: str) -> List[str]:
    return extract_resolved_issues_from_article(html)

# -----------------------------------------------------------------------------
# Classification
# -----------------------------------------------------------------------------

def classify(text: str) -> Tuple[List[str], List[str]]:
    endpoints: List[str] = []
    for canon, pats in CANONICAL_ENDPOINTS.items():
        if any(re.search(p, text, re.I) for p in pats):
            endpoints.append(canon)
    if not endpoints and re.search(r"\bendpoint\b", text, re.I):
        endpoints.append("Unspecified endpoint")
    buckets: List[str] = []
    for bname, pats in GENERAL_BUCKETS.items():
        if any(re.search(p, text, re.I) for p in pats):
            buckets.append(bname)
    if not buckets:
        buckets.append("General")
    return sorted(set(endpoints)), sorted(set(buckets))

# -----------------------------------------------------------------------------
# DB persistence
# -----------------------------------------------------------------------------

def _import_pg_driver():
    try:
        import psycopg  # type: ignore
        return psycopg, True
    except Exception:
        try:
            import psycopg2  # type: ignore
            return psycopg2, False
        except Exception:
            return None, False

def persist_postgres_upsert(rows: List[Issue], dsn: str) -> int:
    LOG.info("persist_postgres_upsert: rows=%d dsn_set=%s", len(rows), bool(dsn))
    drv, _ = _import_pg_driver()
    if drv is None:
        LOG.error("no Postgres driver (psycopg/psycopg2) installed; skipping DB persistence")
        return 0

    conn = drv.connect(dsn)
    cur = conn.cursor()
    LOG.debug("ensuring table and unique index exist")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS replicate_release_issue (
        id BIGSERIAL PRIMARY KEY,
        version TEXT,
        issue_date DATE NULL,
        title TEXT,
        url TEXT,
        jira TEXT NULL,
        endpoints TEXT[],
        buckets TEXT[],
        text TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_rep_issue_vut
    ON replicate_release_issue (version, url, text);
    """)
    inserted = 0
    for r in rows:
        try:
            cur.execute("""
                INSERT INTO replicate_release_issue
                  (version, issue_date, title, url, jira, endpoints, buckets, text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (version, url, text) DO NOTHING;
            """, (r.version, r.date, r.title, r.url, r.jira, r.endpoints, r.buckets, r.text))
            inserted += 1
        except Exception:
            LOG.exception("upsert failed for url=%s jira=%s", r.url, r.jira)
    conn.commit()
    cur.close()
    conn.close()
    LOG.info("persist_postgres_upsert: attempted=%d", len(rows))
    return inserted

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------

def fetch_issues(include_previews: bool = False,
                 community_cookie: str | None = None,
                 from_html: str | None = None) -> List[Issue]:
    """
    Fetch and parse latest "Resolved issues".
      1) Community hub discovery (b-ReleaseNotes)
      2) Community permanent page slug-targeted discovery (client-managed)
      3) Help site fallback
    """
    LOG.info("fetch_issues: include_previews=%s from_html=%s", include_previews, bool(from_html))

    # Identify latest train
    try:
        help_html = http_get(HELP_TRAIN_URL)
        latest_train = parse_latest_train(help_html)
        LOG.info("detected latest_train=%s from HELP page", latest_train)
    except Exception as e:
        latest_train = "May 2025"
        LOG.warning("failed to fetch HELP_TRAIN_URL (%s); defaulting train=%s", type(e).__name__, latest_train)

    rows: List[Issue] = []

    # Offline
    if from_html:
        LOG.info("offline mode: parsing %s", from_html)
        with open(from_html, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
        fixes = extract_resolved_issues_from_article(html)
        LOG.info("offline extracted fixes: %d", len(fixes))
        for txt in fixes:
            m = JIRA_RX.search(txt)
            jira = m.group(0).replace("—", "-").replace("–", "-") if m else None
            endpoints, buckets = classify(txt)
            rows.append(Issue(version=latest_train, date=None, title="Manual", url="(file)",
                              text=txt, jira=jira, endpoints=endpoints, buckets=buckets))
        return sorted(rows, key=lambda r: (r.version, r.title, r.jira or "", r.text))

    # Pass 1: Community hub
    hub_html = http_get(COMMUNITY_RELEASE_NOTES_HUB, community_cookie)
    articles = find_candidate_articles(hub_html, include_previews=include_previews)
    LOG.info("community candidates found: %d", len(articles))

    preferred = [a for a in articles if latest_train in a[0]]
    scan_list = preferred or articles[:4]
    LOG.info("community scanning %d article(s)", len(scan_list))

    for title, url in scan_list:
        try:
            html = http_get(url, community_cookie)
            LOG.info("community fetched article: %s", url)
        except Exception:
            LOG.exception("community fetch failed: %s", url)
            continue
        fixes = extract_resolved_issues_from_article(html)
        LOG.info("community parsed: title=%s fixes_found=%d", title, len(fixes))
        for txt in fixes:
            m = JIRA_RX.search(txt)
            jira = m.group(0).replace("—", "-").replace("–", "-") if m else None
            endpoints, buckets = classify(txt)
            rows.append(Issue(version=latest_train, date=None, title=title, url=url,
                              text=txt, jira=jira, endpoints=endpoints, buckets=buckets))

    # Pass 2: Permanent page slug-targeted (only if still empty)
    if not rows:
        LOG.warning("community yielded 0 fixes -> trying permanent client-managed page (slug-targeted)")
        slug_pair = find_article_via_permanent_page(latest_train, community_cookie)
        if slug_pair:
            title, url = slug_pair
            try:
                html = http_get(url, community_cookie)
                LOG.info("permanent-page fetched article: %s", url)
                fixes = extract_resolved_issues_from_article(html)
                LOG.info("permanent-page parsed: title=%s fixes_found=%d", title, len(fixes))
                for txt in fixes:
                    m = JIRA_RX.search(txt)
                    jira = m.group(0).replace("—", "-").replace("–", "-") if m else None
                    endpoints, buckets = classify(txt)
                    rows.append(Issue(version=latest_train, date=None, title=title, url=url,
                                      text=txt, jira=jira, endpoints=endpoints, buckets=buckets))
            except Exception:
                LOG.exception("permanent-page fetch/parse failed: %s", url)

    # Pass 3: Help fallback
    if not rows:
        LOG.warning("no community matches -> trying Help fallback for %s", latest_train)
        for name, url in candidate_help_pages(latest_train):
            try:
                html = http_get(url)
                LOG.info("help fetched page: %s", url)
            except Exception:
                LOG.exception("help fetch failed: %s", url)
                continue
            fixes = extract_resolved_issues_from_help(html)
            LOG.info("help parsed: page=%s fixes_found=%d", name, len(fixes))
            for txt in fixes:
                m = JIRA_RX.search(txt)
                jira = m.group(0).replace("—", "-").replace("–", "-") if m else None
                endpoints, buckets = classify(txt)
                rows.append(Issue(version=latest_train, date=None, title=name, url=url,
                                  text=txt, jira=jira, endpoints=endpoints, buckets=buckets))

    LOG.info("TOTAL fixes parsed across sources: %d", len(rows))
    return sorted(rows, key=lambda r: (r.version, r.title, r.jira or "", r.text))

def fetch_and_persist(pg_dsn: Optional[str] = None,
                      include_previews: bool = False,
                      community_cookie: Optional[str] = None,
                      from_html: Optional[str] = None) -> List[Issue]:
    rows = fetch_issues(include_previews=include_previews,
                        community_cookie=community_cookie,
                        from_html=from_html)
    if pg_dsn:
        try:
            inserted = persist_postgres_upsert(rows, pg_dsn)
            LOG.info("fetch_and_persist: upsert attempted=%d inserted_calls=%d", len(rows), inserted)
        except Exception as e:
            # Print as well as log so you’ll still see the reason even if logging misconfigures
            try:
                LOG.exception("DB upsert failed: %s", e)
            except Exception:
                pass
            print(f"DB upsert failed: {type(e).__name__}: {e}", file=sys.stderr)
    else:
        LOG.warning("fetch_and_persist: no pg_dsn provided -> skipping DB persistence")
    return rows

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def to_csv(rows: List[Issue], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["version", "date", "title", "url", "jira", "endpoints", "buckets", "text"])
        for r in rows:
            w.writerow([r.version, r.date or "", r.title, r.url, r.jira or "",
                        "; ".join(r.endpoints), "; ".join(r.buckets), r.text])

def to_json(rows: List[Issue], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([dc.asdict(r) for r in rows], f, ensure_ascii=False, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-previews", action="store_true")
    ap.add_argument("--community-cookie")
    ap.add_argument("--from-html")
    ap.add_argument("--out", default="issues.json")
    ap.add_argument("--csv", default="issues.csv")
    ap.add_argument("--pg")
    args = ap.parse_args()

    rows = fetch_and_persist(
        pg_dsn=args.pg,
        include_previews=args.include_previews,
        community_cookie=args.community_cookie,
        from_html=args.from_html,
    )

    to_json(rows, args.out)
    to_csv(rows, args.csv)

    by_ep: Dict[str, int] = {}
    for r in rows:
        for ep in (r.endpoints or ["(Unspecified)"]):
            by_ep[ep] = by_ep.get(ep, 0) + 1
    print(f"Extracted {len(rows)} resolved issues.")
    for ep, cnt in sorted(by_ep.items(), key=lambda x: (-x[1], x[0])):
        print(f"{ep}: {cnt}")

if __name__ == "__main__":
    main()
