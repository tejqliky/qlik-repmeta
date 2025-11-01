import os
import re
import io
import json
import uuid
import asyncio
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Awaitable, List, Tuple

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

from psycopg.rows import dict_row  # noqa: F401  (kept for compatibility)

from .ingest_qem import ingest_qem_tsv, ingest_qem_servers_map_tsv
from .export_report import (
    generate_summary_docx,         # (server-scoped; kept for backward-compat)
    generate_customer_report_docx, # customer-wide report
)
# Ingest entrypoints we actually call
from .ingest import (
    ingest_repository,   # JSON ingest (we pass server_name explicitly)
    ingest_metrics_log,  # metrics TSV ingest
)

from .license_routes import router as license_router  # license upload routes

LOG = logging.getLogger("api")

# DB connection alias + schema
try:
    from .db import connection, SCHEMA
except Exception:
    from .db import get_db_connection as connection  # type: ignore
    SCHEMA = os.getenv("REPMETA_SCHEMA", "repmeta")

# ---------------- logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("INGEST")

API_TITLE = "Qlik RepMeta API"
API_VERSION = os.getenv("API_VERSION", "2.5")  # bumped

app = FastAPI(title=API_TITLE, version=API_VERSION)

# Register sub-routers
app.include_router(license_router)


# ---------------- CORS (VM-friendly & env-driven) ----------------
def _parse_csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "")
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

# Consolidate origins from either variable (both supported)
origins_env = _parse_csv_env("CORS_ORIGINS") or _parse_csv_env("ALLOW_ORIGINS")

# Default allow-list aims to cover your local & VM usage out of the box
default_origins = [
    "http://172.20.18.221",   # VM access
    "http://localhost:5173",  # Vite dev
    "http://127.0.0.1:5173",
    "http://localhost",
    "http://127.0.0.1",
]
allow_origins = origins_env or default_origins

# Optional regex (e.g., r"^https?://172\.20\.18\.221(:\d+)?$")
allow_origin_regex = os.getenv("ALLOW_ORIGIN_REGEX", "").strip() or None

# Credentials & headers
allow_credentials = _parse_bool_env("CORS_ALLOW_CREDENTIALS", True)
expose_headers = _parse_csv_env("CORS_EXPOSE_HEADERS") or ["Content-Disposition"]
allow_methods = _parse_csv_env("CORS_ALLOW_METHODS") or ["*"]
allow_headers = _parse_csv_env("CORS_ALLOW_HEADERS") or ["*"]

# Starlette/browser nuance:
# If credentials are allowed, do NOT use wildcard "*" origins. Ensure we return a concrete origin.
if allow_credentials and any(o == "*" for o in allow_origins):
    # Remove "*" and fall back to defaults if list becomes empty.
    allow_origins = [o for o in allow_origins if o != "*"] or default_origins
    # If caller provided ALLOW_ORIGIN_REGEX, it will be used to match and echo request Origin.

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=allow_credentials,
    allow_methods=allow_methods,
    allow_headers=allow_headers,
    expose_headers=expose_headers,
)


# ---------------- Models ----------------
class IngestBody(BaseModel):
    payload: Dict[str, Any]
    file_name: Optional[str] = "repository.json"
    uploaded_by: Optional[str] = None
    customer_name: Optional[str] = None
    # optional override; if not provided we extract using "Host name: <SERVER>" from payload text
    server_name: Optional[str] = None


class CustomerBody(BaseModel):
    customer_name: str


# ---------------- Helpers ----------------
def _infer_server_from_description_text(raw_text: str) -> Optional[str]:
    """
    Extracts the server from lines like:
      "description": "Host name: USREM-HXT2, Time: 2025-08-22 ..."
    STRICT: We do not scan generic 'server/host' keys.
    """
    if not raw_text:
        return None
    matches = re.findall(r'Host\s*name\s*:\s*([A-Za-z0-9._-]+)', raw_text, re.IGNORECASE)
    return matches[-1].strip() if matches else None


# ---------- ZIP safety (mirror approach used previously in ingest; kept in API now) ----------
MAX_ZIP_FILES = int(os.getenv("REPMETA_MAX_ZIP_FILES", "250"))
MAX_ZIP_UNCOMPRESSED = int(os.getenv("REPMETA_MAX_ZIP_UNCOMPRESSED", str(1_000_000_000)))  # 1 GB
ALLOWED_ZIP_EXTS = {".json"}

def _is_safe_member(name: str) -> bool:
    if not name or name.startswith(("/", "\\")):
        return False
    # prevent traversal
    if ".." in name.replace("\\", "/"):
        return False
    return Path(name).suffix.lower() in ALLOWED_ZIP_EXTS

def _safe_zip_members(zf: zipfile.ZipFile) -> List[zipfile.ZipInfo]:
    infos = [i for i in zf.infolist() if not i.is_dir() and _is_safe_member(i.filename)]
    if len(infos) > MAX_ZIP_FILES:
        raise ValueError(f"Zip has too many files (>{MAX_ZIP_FILES}).")
    total_uncompressed = 0
    for i in infos:
        total_uncompressed += i.file_size
        if total_uncompressed > MAX_ZIP_UNCOMPRESSED:
            raise ValueError("Zip uncompressed size exceeds allowed limit.")
    return infos


# ---------------- Diagnostics / Health ----------------
@app.get("/health")
async def health():
    """
    Lightweight health endpoint for Kubernetes/Docker and manual curl checks.
    """
    return {
        "ok": True,
        "service": API_TITLE,
        "version": API_VERSION,
        "schema": SCHEMA,
    }

@app.get("/_debug/cors")
async def debug_cors():
    """
    Returns the active CORS configuration to simplify troubleshooting in Docker/VM.
    """
    return {
        "allow_origins": allow_origins,
        "allow_origin_regex": allow_origin_regex,
        "allow_credentials": allow_credentials,
        "allow_methods": allow_methods,
        "allow_headers": allow_headers,
        "expose_headers": expose_headers,
    }


# ---------------- Tenancy ----------------
@app.post("/customers")
async def create_customer(body: CustomerBody):
    """
    Creates the customer if it doesn't exist and returns both id and name.
    """
    name = body.customer_name.strip()
    async with connection() as conn:
        cur = await conn.execute(
            f"""INSERT INTO {SCHEMA}.dim_customer (customer_name)
                VALUES (%s) ON CONFLICT (customer_name) DO NOTHING
                RETURNING customer_id""",
            (name,),
        )
        row = await cur.fetchone()
        if not row:
            cur = await conn.execute(
                f"SELECT customer_id FROM {SCHEMA}.dim_customer WHERE customer_name = %s",
                (name,),
            )
            row = await cur.fetchone()

        # Row can be tuple or dict depending on row factory
        cid = row[0] if not isinstance(row, dict) else (row.get("customer_id") or row.get("customer_id".upper()))
        return {"customer_id": cid, "customer_name": name}


@app.get("/customers")
async def list_customers():
    async with connection() as conn:
        cur = await conn.execute(
            f"SELECT customer_id, customer_name FROM {SCHEMA}.dim_customer ORDER BY customer_name"
        )
        rows = await cur.fetchall() or []
        return [{"customer_id": r[0], "customer_name": r[1]} if not isinstance(r, dict) else r for r in rows]


@app.get("/customers/{customer_id}/servers")
async def list_servers(customer_id: int):
    async with connection() as conn:
        cur = await conn.execute(
            f"""SELECT server_id, server_name, environment
                FROM {SCHEMA}.dim_server WHERE customer_id = %s ORDER BY server_name""",
            (customer_id,),
        )
        rows = await cur.fetchall() or []
        return [
            {"server_id": r[0], "server_name": r[1], "environment": r[2]}
            if not isinstance(r, dict) else r
            for r in rows
        ]


# ---------------- Ingest (JSON body) ----------------
@app.post("/ingest")
async def ingest(body: IngestBody):
    """
    Ingest a repository JSON provided in the request body.
    STRICT server resolution:
      1) If body.server_name is provided, use it.
      2) Else parse from payload text using regex: 'Host name: <SERVER>'.
      3) If neither are available, return 400 (we DO NOT scan generic JSON keys or filename).
    """
    try:
        # Resolve names
        customer_name_eff = (body.customer_name or "").strip() or "UNKNOWN"

        server_name_eff = (body.server_name or "").strip()
        if not server_name_eff:
            payload_text = json.dumps(body.payload, ensure_ascii=False)
            server_name_eff = _infer_server_from_description_text(payload_text) or ""

        if not server_name_eff:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Server name not found. Expected a description string like "
                    "'Host name: <SERVER>' in the payload, or pass 'server_name' explicitly."
                ),
            )

        log.info("INGEST /ingest start file=%s customer=%s server=%s",
                 body.file_name, customer_name_eff, server_name_eff)

        result = await ingest_repository(
            repo_json=body.payload,
            customer_name=customer_name_eff,
            server_name=server_name_eff,
        )
        log.info("INGEST /ingest ok result=%s", {k: result.get(k) for k in ("run_id", "endpoints_inserted", "tasks_inserted")})
        return result

    except HTTPException:
        raise
    except Exception as e:
        log.exception("INGEST /ingest failed")
        raise HTTPException(status_code=400, detail=str(e))


# ---------------- Ingest (legacy multipart file) ----------------
@app.post("/ingest-file")
async def ingest_file(
    file: UploadFile = File(...),
    customer_name: str = Form(None),
    server_name: str = Form(None),    # optional override; required if description is missing
    uploaded_by: str = Form(None),
):
    """
    Accepts a .json file (Qlik Replicate repo export) and ingests it.

    STRICT server resolution:
      1) If 'server_name' form field is provided, use it.
      2) Else parse from raw text using regex: 'Host name: <SERVER>'.
      3) If neither are available, return 400 (we DO NOT scan generic JSON keys or filename).

    NOTE: This legacy endpoint remains strict-by-design. For flexible JSON-or-ZIP
    ingest with per-file progress, use:
        POST /ingest/repository-upload
        GET  /ingest/repository-upload/stream/{job_id}
    """
    try:
        raw = await file.read()
        text = raw.decode("utf-8", errors="replace")

        # Parse JSON payload
        try:
            payload = json.loads(text)
        except Exception as je:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {je}")

        # Normalize names
        customer_name_eff = (customer_name or "").strip() or "UNKNOWN"

        # Strict server extraction
        server_name_eff = (server_name or "").strip()
        if not server_name_eff:
            server_name_eff = _infer_server_from_description_text(text) or ""

        if not server_name_eff:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Server name not found. Expected a description line like "
                    "'Host name: <SERVER>' in the export, or pass 'server_name' "
                    "as a form field."
                ),
            )

        log.info(
            "INGEST /ingest-file start file=%s bytes=%s customer=%s server=%s",
            file.filename, len(raw), customer_name_eff, server_name_eff
        )

        result = await ingest_repository(
            repo_json=payload,
            customer_name=customer_name_eff,
            server_name=server_name_eff,
        )
        return result

    except HTTPException:
        raise
    except Exception as e:
        log.exception("INGEST /ingest-file failed")
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------- JSON-or-ZIP repository upload with SSE progress (server parsed here) -------------------------

# Simple in-memory job registry (per-process)
class _JobState:
    __slots__ = ("queue", "done", "result")
    def __init__(self, max_queue: int = 1000):
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self.done: asyncio.Event = asyncio.Event()
        self.result: Optional[Dict[str, Any]] = None

_JOBS: Dict[str, _JobState] = {}
_JOBS_LOCK = asyncio.Lock()
KEEPALIVE_SECONDS = int(os.getenv("REPMETA_SSE_KEEPALIVE_SEC", "15"))

async def _jobs_create() -> str:
    job_id = uuid.uuid4().hex
    async with _JOBS_LOCK:
        _JOBS[job_id] = _JobState()
    return job_id

async def _jobs_get(job_id: str) -> Optional[_JobState]:
    async with _JOBS_LOCK:
        return _JOBS.get(job_id)

async def _jobs_finish(job_id: str, result: Optional[Dict[str, Any]] = None):
    js = await _jobs_get(job_id)
    if js:
        js.result = result
        js.done.set()

async def _emit(job: _JobState, payload: Dict[str, Any]):
    data = json.dumps(payload, ensure_ascii=False)
    try:
        job.queue.put_nowait(f"data: {data}\n\n")
    except asyncio.QueueFull:
        try:
            _ = job.queue.get_nowait()
        except Exception:
            pass
        try:
            job.queue.put_nowait(f"data: {data}\n\n")
        except Exception:
            pass

def _make_progress_cb(job_id: str) -> Callable[[str, Dict[str, Any]], Awaitable[None]]:
    async def _cb(event_type: str, payload: Dict[str, Any]) -> None:
        js = await _jobs_get(job_id)
        if not js:
            return
        await _emit(js, {"type": event_type, **payload})
    return _cb


async def _ingest_single_json_bytes(
    job: _JobState,
    data_bytes: bytes,
    filename: str,
    customer_name: str,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Parse server name from *text* ('Host name: ...') and ingest this single JSON.
    """
    await _emit(job, {"type": "file_found", "fileName": filename, "index": 1, "total": 1})

    text = data_bytes.decode("utf-8", errors="replace")
    try:
        payload = json.loads(text)
    except Exception as je:
        await _emit(job, {"type": "error", "fileName": filename, "message": f"Invalid JSON: {je}"})
        return False, None

    server = _infer_server_from_description_text(text) or ""
    if not server:
        await _emit(job, {
            "type": "error",
            "fileName": filename,
            "message": "Server name not found in description ('Host name: ...')."
        })
        return False, None

    await _emit(job, {"type": "server_resolved", "fileName": filename, "serverName": server})
    await _emit(job, {"type": "ingest_started", "fileName": filename, "serverName": server})

    try:
        result = await ingest_repository(payload, customer_name, server)
        await _emit(job, {
            "type": "ingest_completed",
            "fileName": filename,
            "serverName": server,
            "runId": result.get("run_id"),
            "endpoints": result.get("endpoints_inserted"),
            "tasks": result.get("tasks_inserted"),
        })
        return True, result
    except Exception as e:
        await _emit(job, {"type": "error", "fileName": filename, "message": str(e)})
        return False, None


async def _run_repository_upload_job(job_id: str, data_bytes: bytes, filename: str, customer_name: str):
    """
    Background job: detects JSON vs ZIP, parses server via 'Host name: ...' from each JSON,
    ingests via ingest_repository, emits per-file progress, and guarantees a terminal 'job_completed'.
    """
    js = await _jobs_get(job_id)
    if not js:
        return
    await _emit(js, {"type": "job_started", "filename": filename})

    try:
        if filename.lower().endswith(".zip"):
            # ZIP flow
            with zipfile.ZipFile(io.BytesIO(data_bytes)) as zf:
                members = _safe_zip_members(zf)
                total = len(members)
                await _emit(js, {"type": "zip_summary", "total": total})

                success = 0
                failed = 0
                results: List[Dict[str, Any]] = []

                for idx, info in enumerate(members, start=1):
                    name = info.filename
                    await _emit(js, {"type": "file_found", "fileName": name, "index": idx, "total": total})
                    try:
                        with zf.open(info, "r") as f:
                            raw = f.read()
                        text = raw.decode("utf-8", errors="replace")

                        try:
                            payload = json.loads(text)
                        except Exception as je:
                            failed += 1
                            await _emit(js, {"type": "error", "fileName": name, "message": f"Invalid JSON: {je}"})
                            continue

                        server = _infer_server_from_description_text(text) or ""
                        if not server:
                            failed += 1
                            await _emit(js, {
                                "type": "error",
                                "fileName": name,
                                "message": "Server name not found in description ('Host name: ...')."
                            })
                            continue

                        await _emit(js, {"type": "server_resolved", "fileName": name, "serverName": server})
                        await _emit(js, {"type": "ingest_started", "fileName": name, "serverName": server})

                        res = await ingest_repository(payload, customer_name, server)
                        results.append(res)
                        success += 1

                        await _emit(js, {
                            "type": "ingest_completed",
                            "fileName": name,
                            "serverName": server,
                            "runId": res.get("run_id"),
                            "endpoints": res.get("endpoints_inserted"),
                            "tasks": res.get("tasks_inserted"),
                        })

                    except Exception as e:
                        failed += 1
                        await _emit(js, {"type": "error", "fileName": name, "message": str(e)})

                await _emit(js, {"type": "job_completed", "total": total, "success": success, "failed": failed})
                await _jobs_finish(job_id, result={"total": total, "success": success, "failed": failed, "results": results})
                return

        # Single JSON flow
        ok, res = await _ingest_single_json_bytes(js, data_bytes, filename, customer_name)
        total = 1
        success = 1 if ok else 0
        failed = 0 if ok else 1
        await _emit(js, {"type": "job_completed", "total": total, "success": success, "failed": failed})
        await _jobs_finish(job_id, result={"total": total, "success": success, "failed": failed, "results": [res] if res else []})
    except Exception as e:
        await _emit(js, {"type": "error", "message": str(e)})
        await _emit(js, {"type": "job_completed", "total": 1, "success": 0, "failed": 1})
        await _jobs_finish(job_id, result={"total": 1, "success": 0, "failed": 1})


@app.post("/ingest/repository-upload")
async def repository_upload(
    background: BackgroundTasks,
    file: UploadFile = File(...),
    customer_name: str = Form(...),
):
    """
    Accepts either:
      - Single Repository JSON (.json)
      - ZIP containing multiple Repository JSONs (.zip) — one per Replicate server

    Server name is always parsed here from each file's description:
      "Host name: <SERVER>"

    Returns a job_id; stream progress via:
      GET /ingest/repository-upload/stream/{job_id}
    """
    try:
        job_id = await _jobs_create()
        raw = await file.read()
        filename = file.filename or "repository.json"
        customer = customer_name.strip() or "UNKNOWN"
        background.add_task(_run_repository_upload_job, job_id, raw, filename, customer)
        return {"job_id": job_id}
    except Exception as e:
        LOG.exception("repository-upload init failed")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/ingest/repository-upload/stream/{job_id}")
async def repository_upload_stream(job_id: str):
    """
    Server-Sent Events stream for per-file progress.
    Emits JSON events:
      - job_started
      - zip_summary
      - file_found
      - server_resolved
      - ingest_started
      - ingest_completed
      - error
      - job_completed (terminal)
    """
    js = await _jobs_get(job_id)
    if not js:
        raise HTTPException(status_code=404, detail="Unknown job_id")

    async def event_gen():
        # Initial hello so the client UI can bind quickly
        yield "event: open\ndata: {}\n\n"
        # Drain queue until job is done and queue emptied
        last_keepalive = asyncio.get_event_loop().time()
        while True:
            try:
                item = await asyncio.wait_for(js.queue.get(), timeout=KEEPALIVE_SECONDS)
                yield item
            except asyncio.TimeoutError:
                # Keep-alive ping to keep proxies from closing the stream
                now = asyncio.get_event_loop().time()
                if now - last_keepalive >= KEEPALIVE_SECONDS:
                    yield ": ping\n\n"
                    last_keepalive = now
            # Exit when done and no more buffered events
            if js.done.is_set() and js.queue.empty():
                break
        # Final comment to mark stream end
        yield ": done\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",   # nginx: disable response buffering for SSE
        },
    )


# ---------------- Reporting ----------------
@app.get("/servers/{server_id}/overview")
async def server_overview(server_id: int):
    async with connection() as conn:
        # Latest run for server
        cur = await conn.execute(
            f"""SELECT run_id FROM {SCHEMA}.ingest_run
                WHERE server_id = %s ORDER BY created_at DESC LIMIT 1""",
            (server_id,),
        )
        row = await cur.fetchone()
        if not row:
            return {"run_id": None, "tasks": 0, "endpoints": 0, "by_endpoint": []}
        run_id = row[0] if not isinstance(row, dict) else row["run_id"]

        # Counts
        cur = await conn.execute(
            f"SELECT count(*) FROM {SCHEMA}.rep_task WHERE run_id = %s", (run_id,)
        )
        tasks = (await cur.fetchone())[0]

        cur = await conn.execute(
            f"SELECT count(*) FROM {SCHEMA}.rep_database WHERE run_id = %s", (run_id,)
        )
        endpoints = (await cur.fetchone())[0]

        # Drill-down: tasks by endpoint (counts of links)
        cur = await conn.execute(
            f"""
            SELECT d.name as endpoint, te.role, count(*) as task_count
            FROM {SCHEMA}.rep_task_endpoint te
            JOIN {SCHEMA}.rep_database d ON d.endpoint_id = te.endpoint_id
            WHERE d.run_id = %s
            GROUP BY d.name, te.role
            ORDER BY d.name, te.role
            """,
            (run_id,),
        )
        rows = await cur.fetchall() or []
        by_endpoint = []
        for r in rows:
            if isinstance(r, dict):
                by_endpoint.append(r)
            else:
                by_endpoint.append({"endpoint": r[0], "role": r[1], "task_count": r[2]})

        return {"run_id": run_id, "tasks": tasks, "endpoints": endpoints, "by_endpoint": by_endpoint}


# --------------- Export Doc (server-scoped; kept for compatibility) -----
@app.get("/export/summary-docx")
async def export_summary_docx(customer: str, server: str):
    """
    Generates and streams the Technical Deployment Review as a Word .docx
    for the latest run of (customer, server).
    """
    try:
        content, filename = await generate_summary_docx(customer, server)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {e}")

    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --------------- Export Doc (customer-wide) -------------------------
@app.get("/export/customer-docx")
async def export_customer_docx(customer: str, include_license: int = Query(1, ge=0, le=1)):
    """
    Generates and streams a Word .docx for the given customer.
    include_license: 1 (default) includes the License Usage section; 0 excludes it.
    """
    try:
        content, filename = await generate_customer_report_docx(customer, include_license=bool(include_license))
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {e}")

    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# Alias to match frontend expectation
@app.get("/export/customer")
async def export_customer(customer: str, include_license: int = Query(1, ge=0, le=1)):
    # Forward to the canonical handler, preserving the toggle
    return await export_customer_docx(customer, include_license)


# ---------------- QEM ingest (multipart) --------------------------------
@app.post("/ingest-qem-file")
async def ingest_qem_file(
    file: UploadFile = File(...),
    customer_name: str = Form(...),
):
    """
    Upload a QEM TSV. We no longer take server_name; the TSV may be multi-server.
    The backend resolves the server per-row using:
      - 'Host' column if present (legacy), else
      - 'Server' column mapped via qem_server_map (upload via /ingest-qem-servers-file).
    """
    try:
        data_bytes = await file.read()
        result = await ingest_qem_tsv(
            data_bytes=data_bytes,
            customer_name=customer_name.strip(),
            file_name=file.filename or "qem.tsv",
        )
        return JSONResponse(result)
    except Exception as e:
        LOG.exception("QEM ingest failed")
        raise HTTPException(status_code=500, detail=f"QEM ingest failed: {e}")


# ---------------- QEM SERVERS MAP ingest -------------------------------
@app.post("/ingest-qem-servers-file")
async def ingest_qem_servers_file(
    file: UploadFile = File(...),
    customer_name: str = Form(...),
):
    """
    Upload the *AemServers_*.tsv file (Server inventory). We persist a mapping of:
       Name (== 'Server' value in QEM metrics)  ->  Host (== Repo server_name)
    """
    try:
        data_bytes = await file.read()
        result = await ingest_qem_servers_map_tsv(
            data_bytes=data_bytes,
            customer_name=customer_name.strip(),
            file_name=file.filename or "AemServers.tsv",
        )
        return JSONResponse(result)
    except Exception as e:
        LOG.exception("QEM servers map ingest failed")
        raise HTTPException(status_code=500, detail=f"QEM servers map ingest failed: {e}")


# ---------------- Metrics purge helper ----------------
async def _purge_metrics_for_customer(conn, customer_id: int) -> dict[str, int]:
    """
    Delete all metrics-log data for a customer in correct dependency order:
    1) rep_metrics_event -> 2) rep_metrics_run
    """
    counts: dict[str, int] = {}
    # Delete events tied to the customer's metrics runs
    cur = await conn.execute(
        f"""
        WITH m_runs AS (
          SELECT metrics_run_id
          FROM {SCHEMA}.rep_metrics_run
          WHERE customer_id = %s
        )
        DELETE FROM {SCHEMA}.rep_metrics_event e
        USING m_runs mr
        WHERE e.metrics_run_id = mr.metrics_run_id
        """,
        (customer_id,),
    )
    try:
        counts["rep_metrics_event"] = cur.rowcount or 0
    except Exception:
        counts["rep_metrics_event"] = 0

    # Delete the runs
    cur = await conn.execute(
        f"DELETE FROM {SCHEMA}.rep_metrics_run WHERE customer_id = %s",
        (customer_id,),
    )
    try:
        counts["rep_metrics_run"] = cur.rowcount or 0
    except Exception:
        counts["rep_metrics_run"] = 0

    return counts


# ---------------- Cleanup ---------------
@app.delete("/customers/{customer_id}/data")
async def delete_customer_data(customer_id: int, drop_servers: bool = True):
    """
    Delete all ingested data for a customer (keeps the dim_customer row).
    Includes repo, QEM, and metrics-log data.
    """
    try:
        async with connection() as conn:
            async with conn.transaction():
                deleted: dict[str, int] = {}

                # 0) Metrics Log data (events -> runs)
                metrics_counts = await _purge_metrics_for_customer(conn, customer_id)
                deleted.update(metrics_counts)

                # 1) QEM metrics
                r = await conn.execute(
                    f"DELETE FROM {SCHEMA}.qem_task_perf WHERE customer_id = %s",
                    (customer_id,),
                )
                deleted["qem_task_perf"] = r.rowcount or 0

                r = await conn.execute(
                    f"DELETE FROM {SCHEMA}.qem_ingest_run WHERE customer_id = %s",
                    (customer_id,),
                )
                deleted["qem_ingest_run"] = r.rowcount or 0

                r = await conn.execute(
                    f"""
                    DELETE FROM {SCHEMA}.qem_batch b
                    WHERE b.customer_id = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM {SCHEMA}.qem_ingest_run r
                        WHERE r.qem_batch_id = b.qem_batch_id
                      )
                    """,
                    (customer_id,),
                )
                deleted["qem_batch"] = r.rowcount or 0

                # 2) Replicate task relations then tasks (by runs of this customer)
                r = await conn.execute(
                    f"""
                    DELETE FROM {SCHEMA}.rep_task_endpoint te
                    USING {SCHEMA}.ingest_run r
                    WHERE te.run_id = r.run_id AND r.customer_id = %s
                    """,
                    (customer_id,),
                )
                deleted["rep_task_endpoint"] = r.rowcount or 0

                r = await conn.execute(
                    f"""
                    DELETE FROM {SCHEMA}.rep_task t
                    USING {SCHEMA}.ingest_run r
                    WHERE t.run_id = r.run_id AND r.customer_id = %s
                    """,
                    (customer_id,),
                )
                deleted["rep_task"] = r.rowcount or 0

                # 3) Endpoint detail tables dynamically (rep_db_% with endpoint_id)
                cur = await conn.execute(
                    """
                    SELECT n.nspname AS schema, c.relname AS table
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s
                      AND c.relkind = 'r'
                      AND c.relname LIKE %s
                    """,
                    (SCHEMA, "rep_db_%"),
                )
                rows = await cur.fetchall()  # tuples: (schema, table)
                for _schema_name, table_name in rows or []:
                    sql = f"""
                        DELETE FROM {SCHEMA}.{table_name} d
                        USING {SCHEMA}.rep_database b
                        WHERE d.endpoint_id = b.endpoint_id
                          AND b.customer_id = %s
                    """
                    rdel = await conn.execute(sql, (customer_id,))
                    deleted[table_name] = (deleted.get(table_name, 0) or 0) + (rdel.rowcount or 0)

                # 4) Endpoints
                r = await conn.execute(
                    f"DELETE FROM {SCHEMA}.rep_database WHERE customer_id = %s",
                    (customer_id,),
                )
                deleted["rep_database"] = r.rowcount or 0

                # 5) Ingest runs (repo JSON)
                r = await conn.execute(
                    f"DELETE FROM {SCHEMA}.ingest_run WHERE customer_id = %s",
                    (customer_id,),
                )
                deleted["ingest_run"] = r.rowcount or 0

                # 6) Servers (optional)
                if drop_servers:
                    r = await conn.execute(
                        f"DELETE FROM {SCHEMA}.dim_server WHERE customer_id = %s",
                        (customer_id,),
                    )
                    deleted["dim_server"] = r.rowcount or 0

                msg = ", ".join(f"{k}={v}" for k, v in deleted.items())
                logging.getLogger("api").info("Customer %s cleanup: %s", customer_id, msg)
                return {"ok": True, "deleted_summary": msg, "deleted": deleted}

    except Exception as e:
        logging.getLogger("api").exception("Cleanup failed for customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {e}")


# --- Replicate Metrics Log (multipart file) ---
@app.post("/ingest-metrics-log")
async def ingest_metrics_log_file(
    file: UploadFile = File(...),
    customer_name: str = Form(...),
    server_name: str = Form(...),   # user must pick the Replicate server
):
    try:
        raw = await file.read()
        res = await ingest_metrics_log(
            data_bytes=raw,
            customer_name=customer_name.strip(),
            server_name=server_name.strip(),
            file_name=file.filename or "metricsLog.tsv",
        )
        return res
    except HTTPException:
        raise
    except Exception as e:
        log.exception("INGEST /ingest-metrics-log failed")
        raise HTTPException(status_code=400, detail=str(e))
