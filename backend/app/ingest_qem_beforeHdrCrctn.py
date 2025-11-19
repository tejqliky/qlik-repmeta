
import csv
import io
import os
import re
import logging
from datetime import timedelta, datetime
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row
from psycopg.types.json import Json  # adapt Python dict to JSONB

from .db import connection

LOG = logging.getLogger("ingest_qem")
SCHEMA = os.getenv("REPMETA_SCHEMA", "repmeta")


# ---------- helpers ----------

async def _set_row_factory(conn):
    try:
        await conn.set_row_factory(dict_row)
    except Exception:
        try:
            conn.row_factory = dict_row  # type: ignore[attr-defined]
        except Exception:
            pass


def _decode_bytes_to_text(data: bytes) -> str:
    """Be forgiving: try utf-8-sig → utf-16 → utf-16-le → latin-1."""
    for enc in ("utf-8-sig", "utf-16", "utf-16-le", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def _to_int(s: Any) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, (int,)):
        return int(s)
    text = str(s).strip()
    if text == "" or text.lower() in ("null", "none", "na", "n/a"):
        return None
    text = text.replace(",", "")
    try:
        return int(float(text))
    except Exception:
        return None


def _to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    text = str(s).strip()
    if text == "" or text.lower() in ("null", "none", "na", "n/a"):
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except Exception:
        return None


def _hms_to_timedelta(s: Any) -> Optional[timedelta]:
    """Parse 'HH:MM:SS' (HH can be >24, e.g. 167:49:18)."""
    if s is None:
        return None
    text = str(s).strip()
    if not text:
        return None
    m = re.match(r"^\s*(\d+):(\d{1,2}):(\d{1,2})\s*$", text)
    if not m:
        return None
    h, mnt, sec = map(int, m.groups())
    return timedelta(hours=h, minutes=mnt, seconds=sec)


def _parse_collected_at_from_filename(name: str) -> Optional[datetime]:
    """
    Examples:
      AemTasks_2025-03-31_10.10.49.646.tsv
      AemServers_2025-09-18_23.28.52.772.tsv
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{2})\.(\d{2})\.(\d{2})", name)
    if not m:
        return None
    try:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}", "%Y-%m-%d %H:%M:%S")
        return dt
    except Exception:
        return None


def _get_any(row: Dict[str, Any], *names: str) -> Optional[str]:
    """Return row[name] for the first header name that exists (case-sensitive DictReader keys)."""
    for n in names:
        if n in row:
            return row.get(n)
    return None


def _norm_name(s: Optional[str]) -> str:
    """Normalize for fuzzy compare: lowercase, strip, remove dots/underscores/dashes and spaces."""
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[.\-_ ]+", "", s)
    return s


def _short_host(s: Optional[str]) -> str:
    """Return the host part before the first dot (e.g., 'srv1' from 'srv1.acme.local')."""
    if not s:
        return ""
    return s.strip().split(".", 1)[0]


# ---------- DB helpers ----------

async def _get_or_create_customer(conn, name: str) -> int:
    row = await (await conn.execute(
        f"SELECT customer_id FROM {SCHEMA}.dim_customer WHERE customer_name=%s", (name,)
    )).fetchone()
    if row:
        return int(row["customer_id"])
    row = await (await conn.execute(
        f"INSERT INTO {SCHEMA}.dim_customer(customer_name) VALUES (%s) RETURNING customer_id", (name,)
    )).fetchone()
    return int(row["customer_id"])


async def _find_server_id_ci(conn, customer_id: int, server_name: str) -> Optional[int]:
    cur = await conn.execute(
        f"""
        SELECT server_id
        FROM {SCHEMA}.dim_server
        WHERE customer_id=%s AND LOWER(server_name)=LOWER(%s)
        LIMIT 1
        """,
        (customer_id, server_name)
    )
    row = await cur.fetchone()
    return int(row["server_id"]) if row else None


async def _get_or_create_server(conn, customer_id: int, server_name: str) -> int:
    sid = await _find_server_id_ci(conn, customer_id, server_name)
    if sid:
        return sid
    row = await (await conn.execute(
        f"INSERT INTO {SCHEMA}.dim_server(customer_id, server_name) VALUES (%s,%s) RETURNING server_id",
        (customer_id, server_name)
    )).fetchone()
    return int(row["server_id"])


async def _load_customer_servers(conn, customer_id: int) -> List[Dict[str, Any]]:
    cur = await conn.execute(
        f"SELECT server_id, server_name FROM {SCHEMA}.dim_server WHERE customer_id=%s",
        (customer_id,)
    )
    rows = await cur.fetchall()
    result = []
    for r in rows or []:
        nm = r["server_name"]
        result.append({
            "server_id": int(r["server_id"]),
            "server_name": nm,
            "short": _short_host(nm),
            "norm": _norm_name(nm),
        })
    return result


def _best_server_match(known: List[Dict[str, Any]], host_value: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Choose the best server record for a given host_value using:
      1) exact case-insensitive match on full name  -> 'exact'
      2) exact match on short host (before dot)     -> 'short'
      3) substring match on normalized names        -> 'substr' (pick the longest server_name)
      else                                         -> 'none'
    """
    hv = host_value.strip()
    hv_short = _short_host(hv)
    hv_norm = _norm_name(hv)

    # 1) exact (ci) on full name
    for s in known:
        if s["server_name"].lower() == hv.lower():
            return "exact", s

    # 2) exact on short
    for s in known:
        if s["short"].lower() == hv_short.lower() and hv_short != "":
            return "short", s

    # 3) substring on normalized
    candidates = [s for s in known if hv_norm and (hv_norm in s["norm"] or s["norm"] in hv_norm)]
    if candidates:
        candidates.sort(key=lambda x: len(x["server_name"]), reverse=True)
        return "substr", candidates[0]

    return "none", None


async def _create_qem_batch(conn, customer_id: int, file_name: str, collected_at: Optional[datetime]) -> int:
    # Some deployments may not have qem_batch; create lazily if needed
    try:
        row = await (await conn.execute(
            f"INSERT INTO {SCHEMA}.qem_batch (customer_id, file_name, collected_at) VALUES (%s,%s,%s) RETURNING qem_batch_id",
            (customer_id, file_name, collected_at)
        )).fetchone()
        return int(row["qem_batch_id"])
    except Exception:
        # Fall back to a sentinel 0 (not ideal, but keeps compatibility if table doesn't exist)
        return 0


async def _create_qem_run(conn, customer_id: int, server_id: int, file_name: str, collected_at: Optional[datetime], qem_batch_id: Optional[int]) -> int:
    # Ensure columns exist (some schemas don't)
    row = await (await conn.execute(
        f"""
        INSERT INTO {SCHEMA}.qem_ingest_run
          (customer_id, server_id, file_name, collected_at, created_at, qem_batch_id)
        VALUES (%s,%s,%s,%s, NOW(), %s)
        RETURNING qem_run_id
        """,
        (customer_id, server_id, file_name, collected_at, qem_batch_id)
    )).fetchone()
    return int(row["qem_run_id"])


async def _resolve_task_id(conn, customer_id: int, server_id: int, task_name: str) -> Optional[int]:
    """Find the most recent rep_task for this (customer, server, task_name). Try exact lower() then normalized."""
    # Exact lower()
    cur = await conn.execute(
        f"""
        SELECT t.task_id
        FROM {SCHEMA}.rep_task t
        JOIN {SCHEMA}.ingest_run r ON r.run_id = t.run_id
        WHERE r.customer_id=%s AND r.server_id=%s
          AND LOWER(t.task_name) = LOWER(%s)
        ORDER BY r.created_at DESC
        LIMIT 1
        """,
        (customer_id, server_id, task_name)
    )
    row = await cur.fetchone()
    if row:
        return int(row["task_id"])

    # Normalized compare
    cur = await conn.execute(
        f"""
        WITH latest AS (
          SELECT MAX(created_at) AS max_created_at
          FROM {SCHEMA}.ingest_run
          WHERE customer_id=%s AND server_id=%s
        )
        SELECT t.task_id
        FROM {SCHEMA}.rep_task t
        JOIN {SCHEMA}.ingest_run r ON r.run_id=t.run_id
        JOIN latest L ON L.max_created_at=r.created_at
        WHERE LOWER(regexp_replace(t.task_name, '[-_. ]', '', 'g')) = %s
        LIMIT 1
        """,
        (customer_id, server_id, _norm_name(task_name))
    )
    row = await cur.fetchone()
    return int(row["task_id"]) if row else None


# ---------- server map ingestion (NEW) ----------

def _read_tsv(text: str) -> Tuple[List[str], List[Dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    headers = reader.fieldnames or []
    rows = [dict(r) for r in reader]
    return headers, rows


async def _load_server_map(conn, customer_id: int) -> Dict[str, str]:
    """Return { norm(Name) : Host } for the customer."""
    try:
        cur = await conn.execute(
            f"SELECT name, host FROM {SCHEMA}.qem_server_map WHERE customer_id=%s",
            (customer_id,),
        )
    except Exception:
        # table missing
        return {}
    rows = await cur.fetchall() or []
    m: Dict[str, str] = {}
    for r in rows:
        name = r["name"] if isinstance(r, dict) else r[0]
        host = r["host"] if isinstance(r, dict) else r[1]
        m[_norm_name(name)] = host
    return m


async def ingest_qem_servers_map_tsv(data_bytes: bytes, customer_name: str, file_name: str) -> Dict[str, Any]:
    """
    Ingest the *AemServers* TSV which provides the mapping:
      QEM 'Server' (-> column 'Name' here)  ===>  Repo Server Name (-> column 'Host' here)

    We persist into {SCHEMA}.qem_server_map with upsert by (customer_id, name).
    """
    text = _decode_bytes_to_text(data_bytes)
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    headers = reader.fieldnames or []
    if "Name" not in headers or "Host" not in headers:
        raise ValueError("Servers TSV must include 'Name' and 'Host' columns.")
    rows = [dict(r) for r in reader]

    async with connection() as conn:
        await _set_row_factory(conn)
        async with conn.transaction():
            customer_id = await _get_or_create_customer(conn, customer_name)

            # Ensure table exists (best-effort; ignore if already present via migrations)
            try:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA}.qem_server_map(
                      map_id bigserial PRIMARY KEY,
                      customer_id bigint NOT NULL REFERENCES {SCHEMA}.dim_customer(customer_id) ON DELETE CASCADE,
                      name text NOT NULL,
                      host text NOT NULL,
                      created_at timestamptz NOT NULL DEFAULT now(),
                      updated_at timestamptz NOT NULL DEFAULT now(),
                      UNIQUE(customer_id, name)
                    )
                    """
                )
            except Exception:
                pass

            upserts = 0
            for r in rows:
                name = (r.get("Name") or "").strip()
                host = (r.get("Host") or "").strip()
                if not name or not host:
                    continue
                await conn.execute(
                    f"""
                    INSERT INTO {SCHEMA}.qem_server_map (customer_id, name, host)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (customer_id, name) DO UPDATE
                      SET host = EXCLUDED.host,
                          updated_at = now()
                    """,
                    (customer_id, name, host),
                )
                upserts += 1

            return {
                "customer_id": customer_id,
                "file_name": file_name,
                "rows": len(rows),
                "upserts": upserts,
                "ok": True,
                "note": "Mapping stored: used when QEM TSV lacks 'Host' per row (new default).",
            }


# ---------- main QEM TSV ingest (NEW default: no Host column) ----------

# Accepted headers (alias-aware for a few)
EXPECTED_HEADER_GROUPS = [
    ["State"], ["Server"], ["Task"], ["Server Type"], ["Stage"],
    ["Source Name"], ["Source Type"], ["Target Name"], ["Target Type"],
    ["Tables with Error", "Tables with Errors"],
    ["Memory (KB)"], ["Disk Usage (KB)"], ["CPU (%)"],
    ["FL Progress (%)"], ["FL Load Duration"], ["FL Total Tables"], ["FL Total Records"],
    ["FL Target Throughput (rec/sec)"],
    ["CDC Incoming Changes"], ["CDC INSERTs"], ["CDC UPDATEs"], ["CDC DELETEs"],
    ["CDC Applied Changes"], ["CDC COMMIT Change Records"], ["CDC COMMIT Change Volume"],
    ["CDC Apply Throughput (rec/sec)"], ["CDC Source Latency"], ["CDC Apply Latency"]
]


async def ingest_qem_tsv(data_bytes: bytes, customer_name: str, file_name: str) -> Dict[str, Any]:
    """
    Parse the QEM TSV and load into:
      - qem_batch           (one record per TSV upload)
      - qem_ingest_run      (one run per resolved server)
      - qem_task_perf       (one row per task per server, typed metrics)

    Server resolution (new default; no 'Host' in TSV):
      Resolve per row using qem_server_map:
        host = SELECT host FROM qem_server_map WHERE customer_id=:cid AND name ILIKE row['Server'].
      If mapping missing:
        - If REPMETA_REQUIRE_QEM_SERVER_MAP=true => fail fast listing missing 'Server' values.
        - Else best-effort fallback: try 'Server' as host; if still unmatched, create server.
    """
    text = _decode_bytes_to_text(data_bytes)
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    headers = reader.fieldnames or []
    rows = [dict(r) for r in reader]

    # Header sanity (non-fatal)
    missing = []
    for group in EXPECTED_HEADER_GROUPS:
        if not any(h in headers for h in group):
            missing.append(group[0])
    if missing:
        LOG.warning("[QEM] TSV missing headers: %s", ", ".join(missing))

    async with connection() as conn:
        await _set_row_factory(conn)
        async with conn.transaction():
            customer_id = await _get_or_create_customer(conn, customer_name)

            known_servers = await _load_customer_servers(conn, customer_id)
            server_map = await _load_server_map(conn, customer_id)

            require_map = os.getenv("REPMETA_REQUIRE_QEM_SERVER_MAP", "false").lower() in ("1", "true", "yes")
            unmapped_servers: set[str] = set()

            # Group by resolved host
            groups: Dict[str, List[Dict[str, Any]]] = {}

            for r in rows:
                server_display = (r.get("Server") or "").strip()
                host_val: Optional[str] = None

                if server_display:
                    host_val = server_map.get(_norm_name(server_display))

                    if not host_val:
                        if require_map:
                            unmapped_servers.add(server_display)
                            # don't fallback; skip grouping now; we'll error after scan
                            continue
                        # best effort: treat 'Server' as a host name
                        host_val = server_display

                if host_val:
                    groups.setdefault(host_val.strip(), []).append(r)

            # Enforce mapping if requested
            if require_map and unmapped_servers:
                raise ValueError(
                    "QEM TSV contains 'Server' values with no mapping. "
                    "Upload AemServers TSV first. Missing: " + ", ".join(sorted(unmapped_servers))
                )

            collected_at = _parse_collected_at_from_filename(file_name)
            qem_batch_id = await _create_qem_batch(conn, customer_id, file_name, collected_at)

            # Resolve/ensure server_id and create per-server run
            server_for_host: Dict[str, Dict[str, Any]] = {}
            created_runs: List[Dict[str, Any]] = []

            for host, host_rows in groups.items():
                mode, match = _best_server_match(known_servers, host)
                if match is None:
                    server_id = await _get_or_create_server(conn, customer_id, host)
                    known_servers.append({
                        "server_id": server_id,
                        "server_name": host,
                        "short": _short_host(host),
                        "norm": _norm_name(host),
                    })
                    server_name = host
                    match_mode = "created"
                else:
                    server_id = int(match["server_id"])
                    server_name = str(match["server_name"])
                    match_mode = "mapped"  # resolution came via mapping path

                qem_run_id = await _create_qem_run(conn, customer_id, server_id, file_name, collected_at, qem_batch_id)
                server_for_host[host] = {
                    "server_id": server_id,
                    "server_name": server_name,
                    "qem_run_id": qem_run_id,
                    "match_mode": match_mode,
                }
                created_runs.append({
                    "server_name": server_name,
                    "server_id": server_id,
                    "host": host,
                    "match_mode": match_mode,
                    "qem_run_id": qem_run_id,
                    "rows": len(host_rows),
                    "inserted": 0,
                    "matched": 0
                })

            total_inserted = 0
            total_matched = 0
            run_stats_by_run: Dict[int, Dict[str, Any]] = {r["qem_run_id"]: r for r in created_runs}

            for host, host_rows in groups.items():
                server_ctx = server_for_host[host]
                server_id = server_ctx["server_id"]
                qem_run_id = server_ctx["qem_run_id"]

                for r in host_rows:
                    task_name = (r.get("Task") or "").strip()
                    if not task_name:
                        continue

                    task_id = await _resolve_task_id(conn, customer_id, server_id, task_name)
                    if task_id:
                        run_stats_by_run[qem_run_id]["matched"] += 1
                        total_matched += 1

                    sql = f"""
                    INSERT INTO {SCHEMA}.qem_task_perf (
                      qem_run_id, customer_id, server_id, task_name, task_id,
                      state, stage, server_type, source_name, source_type, target_name, target_type,
                      tables_with_error, memory_kb, disk_usage_kb, cpu_pct, fl_progress_pct,
                      fl_load_duration, fl_total_tables, fl_total_records, fl_target_throughput_rec_sec,
                      cdc_incoming_changes, cdc_inserts, cdc_updates, cdc_deletes, cdc_applied_changes,
                      cdc_commit_change_records, cdc_commit_change_volume, cdc_apply_throughput_rec_sec,
                      cdc_source_latency, cdc_apply_latency, raw
                    )
                    VALUES (
                      %s,%s,%s,%s,%s,
                      %s,%s,%s,%s,%s,%s,%s,
                      %s,%s,%s,%s,%s,
                      %s,%s,%s,%s,
                      %s,%s,%s,%s,%s,
                      %s,%s,%s,
                      %s,%s,%s
                    )
                    ON CONFLICT (qem_run_id, task_name) DO UPDATE SET
                      task_id = EXCLUDED.task_id,
                      state = EXCLUDED.state,
                      stage = EXCLUDED.stage,
                      server_type = EXCLUDED.server_type,
                      source_name = EXCLUDED.source_name,
                      source_type = EXCLUDED.source_type,
                      target_name = EXCLUDED.target_name,
                      target_type = EXCLUDED.target_type,
                      tables_with_error = EXCLUDED.tables_with_error,
                      memory_kb = EXCLUDED.memory_kb,
                      disk_usage_kb = EXCLUDED.disk_usage_kb,
                      cpu_pct = EXCLUDED.cpu_pct,
                      fl_progress_pct = EXCLUDED.fl_progress_pct,
                      fl_load_duration = EXCLUDED.fl_load_duration,
                      fl_total_tables = EXCLUDED.fl_total_tables,
                      fl_total_records = EXCLUDED.fl_total_records,
                      fl_target_throughput_rec_sec = EXCLUDED.fl_target_throughput_rec_sec,
                      cdc_incoming_changes = EXCLUDED.cdc_incoming_changes,
                      cdc_inserts = EXCLUDED.cdc_inserts,
                      cdc_updates = EXCLUDED.cdc_updates,
                      cdc_deletes = EXCLUDED.cdc_deletes,
                      cdc_applied_changes = EXCLUDED.cdc_applied_changes,
                      cdc_commit_change_records = EXCLUDED.cdc_commit_change_records,
                      cdc_commit_change_volume = EXCLUDED.cdc_commit_change_volume,
                      cdc_apply_throughput_rec_sec = EXCLUDED.cdc_apply_throughput_rec_sec,
                      cdc_source_latency = EXCLUDED.cdc_source_latency,
                      cdc_apply_latency = EXCLUDED.cdc_apply_latency,
                      raw = EXCLUDED.raw
                    """
                    await conn.execute(
                        sql,
                        (
                            qem_run_id, customer_id, server_id, task_name, task_id,
                            (r.get("State") or None),
                            (r.get("Stage") or None),
                            (r.get("Server Type") or None),
                            (r.get("Source Name") or None),
                            (r.get("Source Type") or None),
                            (r.get("Target Name") or None),
                            (r.get("Target Type") or None),

                            _to_int(_get_any(r, "Tables with Error", "Tables with Errors")),
                            _to_int(r.get("Memory (KB)")),
                            _to_int(r.get("Disk Usage (KB)")),
                            _to_float(r.get("CPU (%)")),
                            _to_float(r.get("FL Progress (%)")),

                            _hms_to_timedelta(r.get("FL Load Duration")),
                            _to_int(r.get("FL Total Tables")),
                            _to_int(r.get("FL Total Records")),
                            _to_float(r.get("FL Target Throughput (rec/sec)")),

                            _to_int(r.get("CDC Incoming Changes")),
                            _to_int(r.get("CDC INSERTs")),
                            _to_int(r.get("CDC UPDATEs")),
                            _to_int(r.get("CDC DELETEs")),
                            _to_int(r.get("CDC Applied Changes")),

                            _to_int(r.get("CDC COMMIT Change Records")),
                            _to_int(r.get("CDC COMMIT Change Volume")),
                            _to_float(r.get("CDC Apply Throughput (rec/sec)")),

                            _hms_to_timedelta(r.get("CDC Source Latency")),
                            _hms_to_timedelta(r.get("CDC Apply Latency")),
                            Json(r)
                        )
                    )
                    run_stats_by_run[qem_run_id]["inserted"] += 1
                    total_inserted += 1

            total_rows = len(rows)
            result_runs = []
            for r in created_runs:
                result_runs.append({
                    "server_name": r["server_name"],
                    "server_id": r["server_id"],
                    "host": r["host"],
                    "match_mode": r["match_mode"],
                    "qem_run_id": r["qem_run_id"],
                    "rows": r["rows"],
                    "metrics_inserted": r["inserted"],
                    "tasks_matched": r["matched"],
                    "tasks_unmatched": max(0, r["rows"] - r["matched"]),
                })

            return {
                "customer_id": customer_id,
                "qem_batch_id": qem_batch_id,
                "file_name": file_name,
                "total_rows_processed": total_rows,
                "total_metrics_inserted": total_inserted,
                "total_tasks_matched": total_matched,
                "total_tasks_unmatched": max(0, total_inserted - total_matched),
                "runs": result_runs
            }
