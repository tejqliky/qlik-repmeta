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
    await cur.execute("SELECT to_regclass(%s) AS r", (f"{SCHEMA}.{view_name}",))
    row = await cur.fetchone()
    return bool(row and row[0])

def _sha256_json(d: Dict[str, Any]) -> str:
    raw = json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

async def build_context(run_id: int) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"run_id": run_id}

    async with connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # identity
            await cur.execute(
                f"""
                SELECT r.run_id, r.created_at, r.replicate_version,
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

            # endpoints (mix)
            if await _view_exists(cur, "v_current_endpoints"):
                await cur.execute(
                    f"""
                    SELECT endpoint_id, endpoint_name, endpoint_role, endpoint_family
                    FROM {SCHEMA}.v_current_endpoints
                    WHERE run_id=%s
                    ORDER BY endpoint_role, endpoint_family, endpoint_name
                    LIMIT %s
                    """,
                    (run_id, AI_MAX_ITEMS * 2),
                )
                ctx["endpoints"] = [dict(r) for r in (await cur.fetchall() or [])]
            else:
                ctx["endpoints"] = []

            # tasks
            if await _view_exists(cur, "v_current_tasks"):
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
            else:
                ctx["tasks"] = []

            # settings overview (top fields only)
            if await _view_exists(cur, "v_task_settings_overview"):
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
            else:
                ctx["task_settings_overview"] = []

            # health/perf (worst first)
            if await _view_exists(cur, "v_task_health_t90"):
                await cur.execute(
                    f"""
                    SELECT tkey, tasks, rows_moved, error_stop_rate
                    FROM {SCHEMA}.v_task_health_t90
                    WHERE customer_id = (SELECT customer_id FROM {SCHEMA}.ingest_run WHERE run_id=%s)
                      AND server_id   = (SELECT server_id   FROM {SCHEMA}.ingest_run WHERE run_id=%s)
                    ORDER BY error_stop_rate DESC NULLS LAST, rows_moved DESC NULLS LAST
                    LIMIT %s
                    """,
                    (run_id, run_id, AI_MAX_ITEMS),
                )
                ctx["task_health_t90"] = [dict(r) for r in (await cur.fetchall() or [])]
            else:
                ctx["task_health_t90"] = []

            if await _view_exists(cur, "v_endpoint_perf_t90"):
                await cur.execute(
                    f"""
                    SELECT role, family_id, tasks, rows_moved, error_stop_rate
                    FROM {SCHEMA}.v_endpoint_perf_t90
                    WHERE customer_id = (SELECT customer_id FROM {SCHEMA}.ingest_run WHERE run_id=%s)
                      AND server_id   = (SELECT server_id   FROM {SCHEMA}.ingest_run WHERE run_id=%s)
                    ORDER BY error_stop_rate DESC NULLS LAST, rows_moved DESC NULLS LAST
                    LIMIT %s
                    """,
                    (run_id, run_id, AI_MAX_ITEMS),
                )
                ctx["endpoint_perf_t90"] = [dict(r) for r in (await cur.fetchall() or [])]
            else:
                ctx["endpoint_perf_t90"] = []

            # unmapped signals
            for vname in ("v_unmapped_component_types", "v_unmapped_endpoints", "v_unmapped_license_tickers", "v_unknown_counts"):
                if await _view_exists(cur, vname):
                    await cur.execute(
                        f"SELECT * FROM {SCHEMA}.{vname} WHERE run_id=%s LIMIT %s",
                        (run_id, AI_MAX_ITEMS),
                    )
                    ctx[vname] = [dict(r) for r in (await cur.fetchall() or [])]
                else:
                    ctx[vname] = []

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
