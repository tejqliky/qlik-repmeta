
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from typing import Optional, List
import os, tempfile
import psycopg
from psycopg.rows import dict_row

from .ingest_qliksense import ingest_zip_bytes, ingest_from_buffers, _conninfo
from .report_qliksense import generate_qs_report

router = APIRouter(prefix="/qliksense", tags=["Qlik Sense"])

@router.get("/snapshots")
async def list_snapshots(customer_id: int):
    async with await psycopg.AsyncConnection.connect(_conninfo()) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            rows = await (await cur.execute(
                "SELECT snapshot_id, snapshot_ts, notes FROM repmeta_qs.snapshots WHERE customer_id = %s ORDER BY snapshot_ts DESC",
                (customer_id,)
            )).fetchall()
            return rows

@router.post("/ingest")
async def ingest(
    customer_id: int = Form(...),
    notes: Optional[str] = Form(None),
    file: UploadFile = File(None),
    files: Optional[List[UploadFile]] = None
):
    if file is None and not files:
        raise HTTPException(status_code=400, detail="Provide either a ZIP (file) or one or more Qlik*.json files (files).")
    if file is not None:
        if not file.filename.lower().endswith(".zip"):
            raise HTTPException(status_code=400, detail="The 'file' field must be a .zip containing Qlik*.json files.")
        snapshot_id = await ingest_zip_bytes(await file.read(), customer_id, notes)
        return {"snapshot_id": snapshot_id}

    buffers = {}
    for f in (files or []):
        base = os.path.basename(f.filename).lower()
        if not base.endswith(".json") or not base.startswith("qlik"):
            raise HTTPException(status_code=400, detail=f"Unexpected file: {f.filename}. Only Qlik*.json accepted.")
        buffers[os.path.basename(f.filename)] = await f.read()
    snapshot_id = await ingest_from_buffers(buffers, customer_id, notes)
    return {"snapshot_id": snapshot_id}

@router.get("/summary")
async def summary(snapshot_id: str):
    async with await psycopg.AsyncConnection.connect(_conninfo()) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            env = await (await cur.execute("SELECT * FROM repmeta_qs.v_environment_overview WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            app_ct = await (await cur.execute("SELECT count(*) FROM repmeta_qs.v_apps WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            stream_ct = await (await cur.execute("SELECT count(*) FROM repmeta_qs.v_streams WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            user_ct = await (await cur.execute("SELECT count(*) FROM repmeta_qs.v_users WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            task_ct = await (await cur.execute("SELECT count(*) FROM repmeta_qs.v_reload_tasks WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            gov = await (await cur.execute("SELECT * FROM repmeta_qs.v_governance_checks WHERE snapshot_id = %s", (snapshot_id,))).fetchone()
            return {
                "environment": env,
                "counts": {
                    "apps": app_ct["count"],
                    "streams": stream_ct["count"],
                    "users": user_ct["count"],
                    "reload_tasks": task_ct["count"],
                },
                "governance": gov,
            }

@router.delete("/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str):
    async with await psycopg.AsyncConnection.connect(_conninfo()) as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM repmeta_qs.snapshots WHERE snapshot_id = %s", (snapshot_id,))
            return {"deleted": True}

@router.post("/purge")
async def purge(customer_id: int = Form(...)):
    async with await psycopg.AsyncConnection.connect(_conninfo()) as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM repmeta_qs.snapshots WHERE customer_id = %s", (customer_id,))
            return {"purged": True}

@router.get("/report")
async def download_report(snapshot_id: str):
    td = tempfile.gettempdir()
    out = os.path.join(td, f"QlikSense_Report_{snapshot_id}.docx")
    path = generate_qs_report(snapshot_id, out)
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", filename="QlikSense_Report.docx")
