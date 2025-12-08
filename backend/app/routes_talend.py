from typing import List, Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from .talend_service import get_all_accounts, process_talend_run_request
from .db import connection

# NOTE:
# main.py should do:
#   from .routes_talend import router as talend_router
#   app.include_router(talend_router, prefix="/talend")
#
# Effective paths:
#   GET  /talend/accounts
#   POST /talend/run
#   GET  /talend/runs/{account_id}
#   GET  /talend/run/{run_id}
router = APIRouter(tags=["talend"])


# ============================================================
#  GET /talend/accounts
# ============================================================

@router.get("/accounts")
async def fetch_accounts():
    """
    Return all account entries from qtcmeta.ACCOUNT.

    Each entry includes:
      - account_id
      - tenant_id
      - account_name
    """
    return await get_all_accounts()


# ============================================================
#  POST /talend/run
# ============================================================

@router.post("/run")
async def run_talend(
    account_id: str = Form(...),
    tenant_id: str = Form(...),
    cseat_files: Optional[List[UploadFile]] = File(default=None),
    qtcmt_file: Optional[UploadFile] = File(default=None),
):
    """
    Execute the Talend orchestration workflow using:
      - account_id
      - tenant_id
      - list of CSEAT CSV files (optional)
      - optional QTCMT H2 file

    Customer selection is removed entirely.
    Staging is tied to <account_id>/<tenant_id>.
    """
    if not account_id or not tenant_id:
        raise HTTPException(
            status_code=400,
            detail="Both account_id and tenant_id are required.",
        )

    # Normalize to a list for the service layer
    cseat_files = cseat_files or []

    return await process_talend_run_request(
        account_id=account_id,
        tenant_id=tenant_id,
        cseat_files=cseat_files,
        qtcmt_file=qtcmt_file,
    )


# ============================================================
#  GET /talend/runs/{account_id}
# ============================================================

@router.get("/runs/{account_id}")
async def get_talend_runs(account_id: str):
    """
    Return the 20 most recent Talend runs for a given account_id,
    persisted in qtcmeta.talend_run.

    Used by the UI to show historical runs for the selected account.
    """
    sql = """
        SELECT
            run_id,
            created_at,
            finished_at,
            account_id,
            tenant_id,
            account_name,
            artifact_name,
            status,
            exit_code
        FROM qtcmeta.talend_run
        WHERE account_id = %s
        ORDER BY created_at DESC
        LIMIT 20;
    """

    async with connection() as conn:
        cur = await conn.execute(sql, (account_id,))
        rows = await cur.fetchall()

    runs = []
    for r in rows:
        runs.append(
            {
                "run_id": r[0],
                "created_at": r[1],
                "finished_at": r[2],
                "account_id": r[3],
                "tenant_id": r[4],
                "account_name": r[5],
                "artifact_name": r[6],
                "status": r[7],
                "exit_code": r[8],
            }
        )

    return runs


# ============================================================
#  GET /talend/run/{run_id}
# ============================================================

@router.get("/run/{run_id}")
async def get_talend_run(run_id: int):
    """
    Return detailed information for a single Talend run, including stdout/stderr,
    from qtcmeta.talend_run.

    This powers the Talend run detail drawer in the UI.
    """
    sql = """
        SELECT
            run_id,
            created_at,
            finished_at,
            account_id,
            tenant_id,
            account_name,
            artifact_name,
            status,
            exit_code,
            raw_stdout,
            raw_stderr
        FROM qtcmeta.talend_run
        WHERE run_id = %s;
    """

    async with connection() as conn:
        cur = await conn.execute(sql, (run_id,))
        row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Talend run not found.")

    return {
        "run_id": row[0],
        "created_at": row[1],
        "finished_at": row[2],
        "account_id": row[3],
        "tenant_id": row[4],
        "account_name": row[5],
        "artifact_name": row[6],
        "status": row[7],
        "exit_code": row[8],
        "raw_stdout": row[9],
        "raw_stderr": row[10],
    }
