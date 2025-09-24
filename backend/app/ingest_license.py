import re
from typing import List, Tuple

# Minimal alias/canon; catalog will handle the final canonicalization
CANON = {
    "sqlserver":"SQLServer","microsoft sql server":"SQLServer","mssql":"SQLServer",
    "postgresql":"PostgreSQL","postgres":"PostgreSQL",
    "adls":"ADLS","azure data lake":"ADLS",
    "s3":"S3","file":"File","kafka":"Kafka","gcs":"GCS","google cloud storage":"GCS",
    "oracle":"Oracle","mysql":"MySQL","db2":"DB2","snowflake":"Snowflake",
    "azure sql":"AzureSQL","synapse":"Synapse","bigquery":"BigQuery",
}

def _canon(s: str) -> str:
    k = s.strip().lower()
    return CANON.get(k, s.strip())

def parse_license_from_log(text: str) -> Tuple[bool,bool,List[str],List[str],str]:
    """
    Returns: (all_sources, all_targets, sources_list, targets_list, raw_line)
    Grabs the 2nd `]I: Licensed to` line if present; else the first.
    Supports:
      - "... all sources, all targets ..."
      - "... sources: (A,B), targets: (X,Y) ..."
    """
    lines = [ln for ln in text.splitlines() if "]I:" in ln and "Licensed to " in ln]
    if not lines:
        raise ValueError("No 'Licensed to' line found")
    raw = lines[1] if len(lines) > 1 else lines[0]

    all_src = bool(re.search(r"\ball sources\b", raw, re.I))
    all_tgt = bool(re.search(r"\ball targets\b", raw, re.I))

    srcs: List[str] = []
    tgts: List[str] = []
    m_src = re.search(r"sources:\s*\(([^)]+)\)", raw, re.I)
    m_tgt = re.search(r"targets:\s*\(([^)]+)\)", raw, re.I)
    if m_src:
        srcs = [_canon(x) for x in m_src.group(1).split(",") if x.strip()]
    if m_tgt:
        tgts = [_canon(x) for x in m_tgt.group(1).split(",") if x.strip()]
    # dedupe + sort for stable output
    return all_src, all_tgt, sorted(set(srcs)), sorted(set(tgts)), raw