import os, json, hashlib, socket, asyncio
from typing import Any, Dict, Optional, List, Literal, Tuple
from datetime import datetime

import httpx
from pydantic import BaseModel, Field, ValidationError
from psycopg.rows import dict_row

from .db import connection, SCHEMA

# ----------------------------
# Schema (Pydantic validation)
# ----------------------------

class Finding(BaseModel):
    title: str
    severity: int = Field(ge=1, le=5)
    what_happened: str
    why_it_matters: str
    evidence: List[str] = Field(default_factory=list)
    next_steps: List[str] = Field(default_factory=list)

class Risk(BaseModel):
    risk: str
    severity: Literal["low", "medium", "high"]
    evidence: str
    mitigation: List[str] = Field(default_factory=list)

class Recommendation(BaseModel):
    action: str
    impact: str
    effort: Literal["low", "medium", "high"]
    owner_suggestion: str

class InsightOutput(BaseModel):
    summary: List[str] = Field(default_factory=list)
    findings: List[Finding] = Field(default_factory=list)
    risks: List[Risk] = Field(default_factory=list)
    recommendations: List[Recommendation] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)

INSIGHT_SCHEMA_SHAPE = {
  "summary": ["string"],
  "findings": [{
    "title": "string",
    "severity": 1,
    "what_happened": "string",
    "why_it_matters": "string",
    "evidence": ["string"],
    "next_steps": ["string"]
  }],
  "risks": [{
    "risk": "string",
    "severity": "low|medium|high",
    "evidence": "string",
    "mitigation": ["string"]
  }],
  "recommendations": [{
    "action": "string",
    "impact": "string",
    "effort": "low|medium|high",
    "owner_suggestion": "string"
  }],
  "confidence": 0.0
}

# ----------------------------
# Config
# ----------------------------
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
AI_POLL_SECONDS = float(os.getenv("AI_WORKER_POLL_SECONDS", "2.0"))
AI_MAX_ITEMS = int(os.getenv("AI_CONTEXT_TOP_N", "10"))
AI_HTTP_TIMEOUT = float(os.getenv("AI_OLLAMA_TIMEOUT_SECONDS", "120"))

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# ----------------------------
# DB helpers
# ----------------------------
async def ensure_job_created(run_id: int) -> None:
    """Create job row in 'created' state if it doesn't exist."""
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                INSERT INTO {SCHEMA}.ai_insight_job (run_id, status, requested_by, model)
                VALUES (%s, 'created', 'system', %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id, OLLAMA_MODEL),
            )

async def start_job(run_id: int, requested_by: str, model: Optional[str]) -> Dict[str, Any]:
    """Idempotent start: transitions created/failed -> pending, bumps attempts."""
    m = model or OLLAMA_MODEL
    async with connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # ensure exists
            await cur.execute(
                f"""
                INSERT INTO {SCHEMA}.ai_insight_job (run_id, status, requested_by, model)
                VALUES (%s, 'created', %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                  requested_by = EXCLUDED.requested_by,
                  model = COALESCE(EXCLUDED.model, {SCHEMA}.ai_insight_job.model)
                RETURNING *
                """,
                (run_id, requested_by, m),
            )
            job = await cur.fetchone()
            if not job:
                raise RuntimeError("Failed to upsert ai_insight_job")

            if job["status"] in ("created", "failed"):
                await cur.execute(
                    f"""
                    UPDATE {SCHEMA}.ai_insight_job
                    SET status='pending',
                        attempts = attempts + 1,
                        error_message=NULL,
                        error_detail=NULL,
                        locked_by=NULL,
                        locked_at=NULL
                    WHERE run_id=%s
                    RETURNING *
                    """,
                    (run_id,),
                )
                job = await cur.fetchone()

            return dict(job)

async def retry_job(run_id: int) -> Dict[str, Any]:
    async with connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"SELECT * FROM {SCHEMA}.ai_insight_job WHERE run_id=%s", (run_id,))
            job = await cur.fetchone()
            if not job:
                raise KeyError("Job not found")

            await cur.execute(
                f"""
                UPDATE {SCHEMA}.ai_insight_job
                SET status='pending',
                    attempts = attempts + 1,
                    error_message=NULL,
                    error_detail=NULL,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE run_id=%s
                RETURNING *
                """,
                (run_id,),
            )
            return dict(await cur.fetchone())

async def get_insights(run_id: int) -> Dict[str, Any]:
    async with connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"SELECT * FROM {SCHEMA}.ai_insight_job WHERE run_id=%s", (run_id,))
            job = await cur.fetchone()

            result = None
            if job and job.get("latest_result_id"):
                await cur.execute(
                    f"SELECT schema_version, result_json FROM {SCHEMA}.ai_insight_result WHERE result_id=%s",
                    (job["latest_result_id"],),
                )
                rr = await cur.fetchone()
                if rr:
                    result = {"schema_version": rr["schema_version"], "json": rr["result_json"]}

            await cur.execute(
                f"""
                SELECT source, execution_id, status, message, payload, created_at
                FROM {SCHEMA}.automation_event
                WHERE run_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (run_id,),
            )
            automation = await cur.fetchone()
            return {
                "job": dict(job) if job else None,
                "result": result,
                "automation": dict(automation) if automation else None,
            }

# ----------------------------
# Context builder (compact)
# ----------------------------
async def _view_exists(cur, view_name: str) -> bool:
    """Return True if the given view exists in the configured schema.

    Note: our cursors commonly use row_factory=dict_row; this helper must work with both dict and tuple rows.
    """
    await cur.execute("SELECT to_regclass(%s) AS r", (f"{SCHEMA}.{view_name}",))
    row = await cur.fetchone()
    if not row:
        return False
    if isinstance(row, dict):
        return bool(row.get("r"))
    # tuple-like
    return bool(row[0])

async def _relation_columns(cur, rel_name: str) -> set[str]:
    """Fetch column names for a table/view in {SCHEMA}."""
    await cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        """,
        (SCHEMA, rel_name),
    )
    rows = await cur.fetchall() or []
    if not rows:
        return set()
    if isinstance(rows[0], dict):
        return {r["column_name"] for r in rows if r.get("column_name")}
    return {r[0] for r in rows if r and r[0]}

def _sha256_json(d: Dict[str, Any]) -> str:
    raw = json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

async def build_context(run_id: int) -> Dict[str, Any]:
    """Build a compact, schema-tolerant context payload for the LLM.

    Key goals:
      - Prefer views (stable contract), but tolerate column variations between environments.
      - Never raise on missing views/columns; instead, record warnings and continue (demo-first resilience).
      - Keep payload small (top-N per section).
    """
    ctx: Dict[str, Any] = {"run_id": run_id, "warnings": []}

    async with connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # ---------------- identity ----------------
            try:
                await cur.execute(
                    f"""
                    SELECT r.run_id, r.customer_id, r.server_id, r.created_at, r.replicate_version,
                           c.customer_name, s.server_name
                    FROM {SCHEMA}.ingest_run r
                    JOIN {SCHEMA}.dim_customer c ON c.customer_id=r.customer_id
                    JOIN {SCHEMA}.dim_server   s ON s.server_id=r.server_id
                    WHERE r.run_id=%s
                    """,
                    (run_id,),
                )
                ident = await cur.fetchone()
                ctx["identity"] = dict(ident) if ident else {}
            except Exception as e:
                ctx["identity"] = {}
                ctx["warnings"].append({"section": "identity", "error": str(e)})

            customer_id = ctx.get("identity", {}).get("customer_id")
            server_id = ctx.get("identity", {}).get("server_id")

            # ---------------- endpoints (mix) ----------------
            ctx["endpoints"] = []
            if await _view_exists(cur, "v_current_endpoints"):
                try:
                    cols = await _relation_columns(cur, "v_current_endpoints")
                    id_col = "endpoint_id" if "endpoint_id" in cols else ("database_id" if "database_id" in cols else None)
                    name_col = "endpoint_name" if "endpoint_name" in cols else ("name" if "name" in cols else None)
                    role_col = "endpoint_role" if "endpoint_role" in cols else ("role" if "role" in cols else None)

                    family_col = None
                    for cand in ("endpoint_family", "family_id", "type_id", "db_settings_type"):
                        if cand in cols:
                            family_col = cand
                            break

                    select_parts = []
                    if id_col:
                        select_parts.append(f"{id_col} AS endpoint_id")
                    if name_col:
                        select_parts.append(f"{name_col} AS endpoint_name")
                    if role_col:
                        select_parts.append(f"{role_col} AS endpoint_role")
                    if family_col:
                        select_parts.append(f"{family_col} AS endpoint_family")
                    # keep db_settings_type if present (helpful disambiguator)
                    if "db_settings_type" in cols and family_col != "db_settings_type":
                        select_parts.append("db_settings_type")

                    if select_parts:
                        order_by = []
                        if role_col:
                            order_by.append("endpoint_role")
                        if family_col:
                            order_by.append("endpoint_family")
                        if name_col:
                            order_by.append("endpoint_name")
                        order_by.append("endpoint_id")

                        await cur.execute(
                            f"""
                            SELECT {", ".join(select_parts)}
                            FROM {SCHEMA}.v_current_endpoints
                            WHERE run_id=%s
                            ORDER BY {", ".join(order_by)}
                            LIMIT %s
                            """,
                            (run_id, AI_MAX_ITEMS * 3),
                        )
                        ctx["endpoints"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "endpoints", "error": str(e)})

            # ---------------- tasks ----------------
            ctx["tasks"] = []
            if await _view_exists(cur, "v_current_tasks"):
                try:
                    await cur.execute(
                        f"""
                        SELECT task_id, task_name, task_type, source_name, target_names
                        FROM {SCHEMA}.v_current_tasks
                        WHERE run_id=%s
                        ORDER BY task_name
                        LIMIT %s
                        """,
                        (run_id, AI_MAX_ITEMS * 3),
                    )
                    ctx["tasks"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "tasks", "error": str(e)})

            # ---------------- task settings overview (top fields only) ----------------
            ctx["task_settings_overview"] = []
            if await _view_exists(cur, "v_task_settings_overview"):
                try:
                    await cur.execute(
                        f"""
                        SELECT task_name, task_type, source_name, target_names,
                               write_full_logging,
                               batch_apply_memory_limit,
                               batch_apply_timeout,
                               batch_apply_timeout_min,
                               transaction_consistency_timeout
                        FROM {SCHEMA}.v_task_settings_overview
                        WHERE run_id=%s
                        ORDER BY task_name
                        LIMIT %s
                        """,
                        (run_id, AI_MAX_ITEMS * 3),
                    )
                    ctx["task_settings_overview"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "task_settings_overview", "error": str(e)})

            # ---------------- health/perf (worst first) ----------------
            ctx["task_health_t90"] = []
            if customer_id is not None and server_id is not None and await _view_exists(cur, "v_task_health_t90"):
                try:
                    cols = await _relation_columns(cur, "v_task_health_t90")
                    sessions_col = "tasks" if "tasks" in cols else ("session_count" if "session_count" in cols else None)
                    rate_col = "error_stop_rate" if "error_stop_rate" in cols else ("err_stop_rate" if "err_stop_rate" in cols else None)

                    select_parts = ["tkey"]
                    if sessions_col:
                        select_parts.append(f"{sessions_col} AS sessions")
                    if "rows_moved" in cols:
                        select_parts.append("rows_moved")
                    if rate_col:
                        select_parts.append(f"{rate_col} AS error_stop_rate")
                    if "uptime_pct" in cols:
                        select_parts.append("uptime_pct")
                    if "throughput_rps" in cols:
                        select_parts.append("throughput_rps")
                    if "restarts_per_day" in cols:
                        select_parts.append("restarts_per_day")
                    if "median_session_minutes" in cols:
                        select_parts.append("median_session_minutes")

                    order_rate = "error_stop_rate" if rate_col else "rows_moved"
                    await cur.execute(
                        f"""
                        SELECT {", ".join(select_parts)}
                        FROM {SCHEMA}.v_task_health_t90
                        WHERE customer_id=%s AND server_id=%s
                        ORDER BY {order_rate} DESC NULLS LAST, rows_moved DESC NULLS LAST
                        LIMIT %s
                        """,
                        (customer_id, server_id, AI_MAX_ITEMS),
                    )
                    ctx["task_health_t90"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "task_health_t90", "error": str(e)})

            ctx["endpoint_perf_t90"] = []
            if customer_id is not None and server_id is not None and await _view_exists(cur, "v_endpoint_perf_t90"):
                try:
                    cols = await _relation_columns(cur, "v_endpoint_perf_t90")
                    rate_col = "err_stop_rate" if "err_stop_rate" in cols else ("error_stop_rate" if "error_stop_rate" in cols else None)

                    select_parts = ["role", "family_id"]
                    for c in ("tasks", "rows_moved", "uptime_pct", "median_rps", "median_session_minutes"):
                        if c in cols:
                            select_parts.append(c)
                    if rate_col:
                        select_parts.append(f"{rate_col} AS error_stop_rate")

                    order_rate = "error_stop_rate" if rate_col else "rows_moved"
                    await cur.execute(
                        f"""
                        SELECT {", ".join(select_parts)}
                        FROM {SCHEMA}.v_endpoint_perf_t90
                        WHERE customer_id=%s AND server_id=%s
                        ORDER BY {order_rate} DESC NULLS LAST, rows_moved DESC NULLS LAST
                        LIMIT %s
                        """,
                        (customer_id, server_id, AI_MAX_ITEMS),
                    )
                    ctx["endpoint_perf_t90"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "endpoint_perf_t90", "error": str(e)})

            # ---------------- unmapped signals (scoped to this run) ----------------
            # We intentionally scope these "global" views back to the current run/customer to avoid noise.
            ctx["v_unmapped_component_types"] = []
            ctx["v_unmapped_endpoints"] = []
            ctx["v_unmapped_license_tickers"] = []
            ctx["v_unknown_counts"] = []

            # Values observed in this run (to filter the global "unmapped" views)
            try:
                await cur.execute(
                    f"""
                    SELECT DISTINCT role, type_id
                    FROM {SCHEMA}.rep_database
                    WHERE run_id=%s AND type_id IS NOT NULL
                    LIMIT %s
                    """,
                    (run_id, AI_MAX_ITEMS * 50),
                )
                seen_component_types = await cur.fetchall() or []
            except Exception as e:
                seen_component_types = []
                ctx["warnings"].append({"section": "seen_component_types", "error": str(e)})

            try:
                await cur.execute(
                    f"""
                    SELECT DISTINCT role, db_settings_type
                    FROM {SCHEMA}.rep_database
                    WHERE run_id=%s AND db_settings_type IS NOT NULL
                    LIMIT %s
                    """,
                    (run_id, AI_MAX_ITEMS * 50),
                )
                seen_endpoints = await cur.fetchall() or []
            except Exception as e:
                seen_endpoints = []
                ctx["warnings"].append({"section": "seen_endpoints", "error": str(e)})

            # helper: group values by role
            def _group_by_role(rows, key_field: str) -> Dict[str, List[str]]:
                out: Dict[str, List[str]] = {}
                for r in rows:
                    if not r:
                        continue
                    role = r.get("role") if isinstance(r, dict) else r[0]
                    val = r.get(key_field) if isinstance(r, dict) else r[1]
                    if role and val:
                        out.setdefault(role, []).append(val)
                return out

            seen_component_types_by_role = _group_by_role(seen_component_types, "type_id")
            seen_endpoints_by_role = _group_by_role(seen_endpoints, "db_settings_type")

            if await _view_exists(cur, "v_unmapped_component_types"):
                try:
                    rows_out = []
                    for role, vals in seen_component_types_by_role.items():
                        # Avoid empty arrays (ANY({}) blows up)
                        if not vals:
                            continue
                        await cur.execute(
                            f"""
                            SELECT role, type_id
                            FROM {SCHEMA}.v_unmapped_component_types
                            WHERE role=%s AND type_id = ANY(%s)
                            LIMIT %s
                            """,
                            (role, vals, AI_MAX_ITEMS),
                        )
                        rows_out.extend([dict(r) for r in (await cur.fetchall() or [])])
                    ctx["v_unmapped_component_types"] = rows_out[:AI_MAX_ITEMS]
                except Exception as e:
                    ctx["warnings"].append({"section": "v_unmapped_component_types", "error": str(e)})

            if await _view_exists(cur, "v_unmapped_endpoints"):
                try:
                    rows_out = []
                    for role, vals in seen_endpoints_by_role.items():
                        if not vals:
                            continue
                        await cur.execute(
                            f"""
                            SELECT role, db_settings_type
                            FROM {SCHEMA}.v_unmapped_endpoints
                            WHERE role=%s AND db_settings_type = ANY(%s)
                            LIMIT %s
                            """,
                            (role, vals, AI_MAX_ITEMS),
                        )
                        rows_out.extend([dict(r) for r in (await cur.fetchall() or [])])
                    ctx["v_unmapped_endpoints"] = rows_out[:AI_MAX_ITEMS]
                except Exception as e:
                    ctx["warnings"].append({"section": "v_unmapped_endpoints", "error": str(e)})

            # License tickers: filter to the latest snapshot for this customer (best effort)
            if customer_id is not None:
                try:
                    await cur.execute(
                        f"""
                        WITH latest AS (
                            SELECT license_id
                            FROM {SCHEMA}.license_snapshot
                            WHERE customer_id=%s
                            ORDER BY extracted_at DESC NULLS LAST
                            LIMIT 1
                        )
                        SELECT DISTINCT i.role, i.alias_value AS ticker
                        FROM {SCHEMA}.license_snapshot_item i
                        JOIN latest l ON l.license_id=i.license_id
                        LIMIT %s
                        """,
                        (customer_id, AI_MAX_ITEMS * 50),
                    )
                    seen_tickers = await cur.fetchall() or []
                except Exception as e:
                    seen_tickers = []
                    ctx["warnings"].append({"section": "seen_license_tickers", "error": str(e)})

                if await _view_exists(cur, "v_unmapped_license_tickers") and seen_tickers:
                    try:
                        # group by role for filtering
                        by_role: Dict[str, List[str]] = {}
                        for r in seen_tickers:
                            role = r.get("role")
                            t = r.get("ticker")
                            if role and t:
                                by_role.setdefault(role, []).append(t)

                        rows_out = []
                        for role, vals in by_role.items():
                            if not vals:
                                continue
                            await cur.execute(
                                f"""
                                SELECT role, ticker
                                FROM {SCHEMA}.v_unmapped_license_tickers
                                WHERE role=%s AND ticker = ANY(%s)
                                LIMIT %s
                                """,
                                (role, vals, AI_MAX_ITEMS),
                            )
                            rows_out.extend([dict(r) for r in (await cur.fetchall() or [])])
                        ctx["v_unmapped_license_tickers"] = rows_out[:AI_MAX_ITEMS]
                    except Exception as e:
                        ctx["warnings"].append({"section": "v_unmapped_license_tickers", "error": str(e)})

            if await _view_exists(cur, "v_unknown_counts"):
                try:
                    await cur.execute(
                        f"""
                        SELECT run_id, entity, unknown_key_count
                        FROM {SCHEMA}.v_unknown_counts
                        WHERE run_id=%s
                        ORDER BY unknown_key_count DESC
                        LIMIT %s
                        """,
                        (run_id, AI_MAX_ITEMS),
                    )
                    ctx["v_unknown_counts"] = [dict(r) for r in (await cur.fetchall() or [])]
                except Exception as e:
                    ctx["warnings"].append({"section": "v_unknown_counts", "error": str(e)})

    return ctx

# ----------------------------
# Prompt builder + Ollama
# ----------------------------
def build_prompt(context: Dict[str, Any]) -> List[Dict[str, str]]:
    system = (
        "You are a senior Qlik Replicate operations consultant.\n"
        "Return ONLY valid JSON (no markdown, no prose outside JSON).\n"
        "The JSON must conform exactly to this shape:\n"
        f"{json.dumps(INSIGHT_SCHEMA_SHAPE, indent=2)}\n\n"
        "Rules:\n"
        "- severity in findings is integer 1..5 (5 = highest)\n"
        "- confidence is 0.0..1.0\n"
        "- If unknown, use empty arrays/strings rather than inventing specifics.\n"
        "- Evidence must reference the provided context fields.\n"
    )
    user = (
        "Analyze this Replicate metadata context and produce insights JSON.\n"
        "Context:\n"
        + json.dumps(context, ensure_ascii=False)
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

async def ollama_chat(messages: List[Dict[str, str]], model: str) -> str:
    url = f"{OLLAMA_BASE_URL}/api/chat"
    payload = {"model": model, "messages": messages, "stream": False}
    async with httpx.AsyncClient(timeout=AI_HTTP_TIMEOUT) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["message"]["content"]

async def _validate_or_fix(raw: str, model: str, context: Dict[str, Any]) -> Tuple[InsightOutput, str]:
    # 1) direct parse/validate
    try:
        obj = json.loads(raw)
        validated = InsightOutput.model_validate(obj)
        return validated, raw
    except Exception:
        pass

    # 2) one retry: "fix JSON"
    fix_system = (
        "You are a strict JSON repair bot.\n"
        "Return ONLY corrected JSON matching this exact shape:\n"
        f"{json.dumps(INSIGHT_SCHEMA_SHAPE, indent=2)}\n"
        "No markdown. No explanations."
    )
    fix_user = (
        "Fix this output into valid JSON only.\n"
        "Invalid output:\n" + raw + "\n\n"
        "Original context (do not invent beyond this):\n" + json.dumps(context, ensure_ascii=False)
    )
    fixed = await ollama_chat([{"role": "system", "content": fix_system}, {"role": "user", "content": fix_user}], model=model)

    obj2 = json.loads(fixed)
    validated2 = InsightOutput.model_validate(obj2)
    return validated2, fixed

# ----------------------------
# Worker tick
# ----------------------------
async def _claim_one_pending_job() -> Optional[Dict[str, Any]]:
    async with connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    SELECT job_id, run_id, attempts, model, prompt_version
                    FROM {SCHEMA}.ai_insight_job
                    WHERE status='pending'
                    ORDER BY updated_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                job = await cur.fetchone()
                if not job:
                    return None

                await cur.execute(
                    f"""
                    UPDATE {SCHEMA}.ai_insight_job
                    SET status='running',
                        locked_by=%s,
                        locked_at=now(),
                        started_at=COALESCE(started_at, now())
                    WHERE job_id=%s
                    """,
                    (WORKER_ID, job["job_id"]),
                )
                return dict(job)

async def _mark_failed(job_id: int, err: str, detail: Dict[str, Any]) -> None:
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE {SCHEMA}.ai_insight_job
                SET status='failed',
                    error_message=%s,
                    error_detail=%s,
                    finished_at=now()
                WHERE job_id=%s
                """,
                (err, json.dumps(detail), job_id),
            )

async def _mark_succeeded(job_id: int, attempt_no: int, validated: InsightOutput, raw_text: str, context: Dict[str, Any]) -> None:
    async with connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # insert result
                await cur.execute(
                    f"""
                    INSERT INTO {SCHEMA}.ai_insight_result (job_id, attempt_no, schema_version, result_json, raw_response_text)
                    VALUES (%s, %s, 'v1', %s, %s)
                    RETURNING result_id
                    """,
                    (job_id, attempt_no, validated.model_dump(), raw_text),
                )
                rid = (await cur.fetchone())["result_id"]

                # update job
                sha = _sha256_json(context)
                await cur.execute(
                    f"""
                    UPDATE {SCHEMA}.ai_insight_job
                    SET status='succeeded',
                        latest_result_id=%s,
                        context_sha256=%s,
                        context_json=%s,
                        finished_at=now(),
                        error_message=NULL,
                        error_detail=NULL
                    WHERE job_id=%s
                    """,
                    (rid, sha, json.dumps(context), job_id),
                )

async def process_one_job() -> bool:
    job = await _claim_one_pending_job()
    if not job:
        return False

    job_id = int(job["job_id"])
    run_id = int(job["run_id"])
    attempt_no = int(job["attempts"])
    model = (job.get("model") or OLLAMA_MODEL)

    try:
        context = await build_context(run_id)
        messages = build_prompt(context)
        raw = await ollama_chat(messages, model=model)
        validated, final_raw = await _validate_or_fix(raw, model=model, context=context)
        await _mark_succeeded(job_id, attempt_no, validated, final_raw, context)
        return True
    except (ValidationError, json.JSONDecodeError) as ve:
        await _mark_failed(job_id, "Invalid JSON from model", {"error": str(ve)})
    except httpx.TimeoutException:
        await _mark_failed(job_id, "Ollama timeout", {"base_url": OLLAMA_BASE_URL, "model": model})
    except Exception as e:
        await _mark_failed(job_id, "Worker error", {"error": str(e)})

    return True

async def worker_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        did = await process_one_job()
        if not did:
            await asyncio.sleep(AI_POLL_SECONDS)
