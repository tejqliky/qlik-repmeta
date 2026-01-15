import asyncio
import os
import shutil
import sys
import json
import subprocess
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import UploadFile, HTTPException

from .db import connection  # existing async DB helper


# ============================================================
#  Configuration & Constants
# ============================================================

# OS-specific defaults for local dev staging
DEFAULT_QTCMT_ROOT = r"C:\qtcmt" if os.name == "nt" else "/qtcmt"
DEFAULT_TMP_ROOT = r"C:\tmp\CS_AUTO" if os.name == "nt" else "/tmp/CS_AUTO"

# Where Talend expects the H2 file and CSEAT CSVs (local staging)
TALEND_QTCMT_ROOT = Path(os.getenv("TALEND_QTCMT_ROOT", DEFAULT_QTCMT_ROOT))
TALEND_TMP_ROOT = Path(os.getenv("TALEND_TMP_ROOT", DEFAULT_TMP_ROOT))

# Default location of run_artifact.py depending on OS
DEFAULT_RUN_ARTIFACT_PATH = (
    r"C:\talend_job\run_artifact.py"
    if os.name == "nt"
    else "/home/qmi/talend_job/run_artifact.py"
)
RUN_ARTIFACT_PATH = Path(
    os.getenv("TALEND_RUN_ARTIFACT_PATH", DEFAULT_RUN_ARTIFACT_PATH)
)

# Env JSON file (Talend credentials) – by default next to run_artifact.py
TALEND_ENV_FILE = Path(
    os.getenv("TALEND_ENV_FILE", str(RUN_ARTIFACT_PATH.with_name("talend_env.json")))
)

# Python binary – default to the same one running uvicorn
PYTHON_BIN = os.getenv("TALEND_PYTHON_BIN", sys.executable)

# Config + artifact names, aligned to your working CLI
DEFAULT_CONFIG_PATH = RUN_ARTIFACT_PATH.with_name("tmc_artifacts.json")
CONFIG_PATH = Path(os.getenv("TALEND_CONFIG_PATH", str(DEFAULT_CONFIG_PATH)))

ARTIFACT_NAME = os.getenv("TALEND_ARTIFACT_NAME", "QTCMT_uploadAssetsData")

# Values passed to --input database_path=... and --input tmp_folder=...
DEFAULT_DB_PATH_ARG = r"C:\qtcmt" if os.name == "nt" else "/qtcmt"
DEFAULT_TMP_FOLDER_ARG = r"C:\tmp\CS_AUTO" if os.name == "nt" else "/tmp/CS_AUTO"

DB_PATH_ARG = os.getenv("TALEND_DB_PATH_ARG", DEFAULT_DB_PATH_ARG)
TMP_FOLDER_ARG = os.getenv("TALEND_TMP_FOLDER_ARG", DEFAULT_TMP_FOLDER_ARG)

# Timeout (seconds) for the Talend runner (maps to --timeout)
# You already have TALEND_TIMEOUT_SECONDS=1 in your env for snappy returns.
TIMEOUT_SECONDS = int(os.getenv("TALEND_TIMEOUT_SECONDS", "1"))

# Ensure local staging dirs exist
TALEND_QTCMT_ROOT.mkdir(parents=True, exist_ok=True)
TALEND_TMP_ROOT.mkdir(parents=True, exist_ok=True)


# ============================================================
#  Account Retrieval (qtcmeta.ACCOUNT)
# ============================================================

async def _get_account_name(account_id: str) -> Optional[str]:
    """
    Helper to fetch the human-friendly account_name from qtcmeta.ACCOUNT.
    """
    sql = """
        SELECT account_name
        FROM qtcmeta."ACCOUNT"
        WHERE account_id = %s
        LIMIT 1;
    """
    async with connection() as conn:
        cur = await conn.execute(sql, (account_id,))
        row = await cur.fetchone()
    return row[0] if row else None


async def get_all_accounts() -> List[Dict[str, str]]:
    """
    Retrieve all accounts from qtcmeta.ACCOUNT and return
    a structured list of dicts:
      { account_id, tenant_id, account_name }
    """
    sql = """
        SELECT account_id, tenant_id, account_name
        FROM qtcmeta."ACCOUNT"
        ORDER BY account_name;
    """

    try:
        async with connection() as conn:
            cur = await conn.execute(sql)
            rows = await cur.fetchall()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch accounts: {e}",
        )

    return [
        {"account_id": r[0], "tenant_id": r[1], "account_name": r[2]}
        for r in rows
    ]


# ============================================================
#  Internal Helpers
# ============================================================

def _save_upload(file_obj: UploadFile, dest: Path) -> None:
    """Stream an uploaded file to disk."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as buffer:
        shutil.copyfileobj(file_obj.file, buffer)


def _resolve_runner() -> Dict[str, Any]:
    """
    Try to find a usable run_artifact script.
    Returns { found: bool, path: Path, tried: [str, ...] }.
    """
    candidates: List[Path] = [RUN_ARTIFACT_PATH]
    if RUN_ARTIFACT_PATH.suffix == "":
        candidates.append(RUN_ARTIFACT_PATH.with_suffix(".py"))
    else:
        candidates.append(RUN_ARTIFACT_PATH.with_suffix(""))

    tried: List[str] = []
    for p in candidates:
        tried.append(str(p))
        if p.is_file():
            return {"found": True, "path": p, "tried": tried}

    return {"found": False, "path": RUN_ARTIFACT_PATH, "tried": tried}


def _resolve_config() -> Dict[str, Any]:
    """
    Resolve tmc_artifacts.json next to run_artifact.py by default.
    Returns { path: Path, exists: bool }.
    """
    cfg = CONFIG_PATH
    return {"path": cfg, "exists": cfg.is_file()}


def _build_talend_env() -> Dict[str, str]:
    """
    Build environment for the Talend runner:
    - start from current process env
    - overlay any keys from talend_env.json
    """
    env: Dict[str, str] = dict(os.environ)

    try:
        if TALEND_ENV_FILE.is_file():
            with TALEND_ENV_FILE.open("r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
            for key, value in cfg.items():
                if value is None:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    env[str(key)] = str(value)
    except Exception as e:  # non-fatal; just surface a warning
        env["TALEND_ENV_WARNING"] = f"Failed to load {TALEND_ENV_FILE}: {repr(e)}"

    return env


# ============================================================
#  Staging Logic (local paths)
# ============================================================

async def stage_cseat_files(
    account_id: str,
    tenant_id: str,
    files: List[UploadFile],
) -> List[str]:
    """
    Save uploaded CSEAT CSV files into the Talend tmp_folder root.

    Windows default:
        C:\\tmp\\CS_AUTO\\<filename>
    Linux default:
        /tmp/CS_AUTO/<filename>
    """
    tmp_dir = TALEND_TMP_ROOT
    tmp_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: List[str] = []
    for idx, f in enumerate(files, start=1):
        fname = f.filename or f"CSEAT_{idx}.csv"
        dest = tmp_dir / fname
        _save_upload(f, dest)
        saved_paths.append(str(dest))

    return saved_paths


async def stage_qtcmt_file(
    account_id: str,
    tenant_id: str,
    file: Optional[UploadFile],
) -> Optional[str]:
    """
    Save uploaded QTCMT H2 file into the Talend database_path root.

    Windows default:
        C:\\qtcmt\\qtcmt.mv.db
    Linux default:
        /qtcmt/qtcmt.mv.db
    """
    if not file:
        return None

    qtcmt_dir = TALEND_QTCMT_ROOT
    qtcmt_dir.mkdir(parents=True, exist_ok=True)

    dest = qtcmt_dir / "qtcmt.mv.db"
    _save_upload(file, dest)

    return str(dest)


# ============================================================
#  Talend Job Runner (run_artifact.py) + Persistence
# ============================================================

async def run_talend_job(
    account_id: str,
    tenant_id: str,
    staged_cseat: List[str],
    staged_qtcmt: Optional[str],
) -> Dict[str, Any]:
    """
    Execute the Talend artifact runner.

    Pattern aligned with your working Linux CLI:

        python run_artifact.py
          --config tmc_artifacts.json
          --artifact QTCMT_uploadAssetsData
          --account-id <id>
          --tenant-id <id>
          --input database_path=/qtcmt
          --input tmp_folder=/tmp/CS_AUTO
          --timeout <TALEND_TIMEOUT_SECONDS>

    We *never* raise HTTPException here for runner errors; instead we
    return a structured payload with:

        status: "success" | "error"
        exit_code: int
        stdout: str
        stderr: str
        command: List[str]
        runner_path: str
        config_path: str
        database_path_arg: str
        tmp_folder_arg: str
        run_id: Optional[int]
    """
    # No artifacts -> no DB row; just return an error payload
    if not staged_cseat and not staged_qtcmt:
        return {
            "status": "error",
            "exit_code": -1,
            "stdout": "",
            "stderr": (
                "No artifacts provided. Upload at least one CSEAT CSV or a "
                "QTCMT H2 file before running the Talend job."
            ),
            "command": [],
            "runner_path": str(RUN_ARTIFACT_PATH),
            "config_path": str(CONFIG_PATH),
            "database_path_arg": DB_PATH_ARG,
            "tmp_folder_arg": TMP_FOLDER_ARG,
            "run_id": None,
        }

    runner_info = _resolve_runner()
    cfg_info = _resolve_config()

    if not runner_info["found"]:
        return {
            "status": "error",
            "exit_code": -1,
            "stdout": "",
            "stderr": (
                "Talend runner not found. "
                f"Tried: {', '.join(runner_info['tried'])}. "
                "Set TALEND_RUN_ARTIFACT_PATH to the full path of run_artifact.py."
            ),
            "command": [],
            "runner_path": str(RUN_ARTIFACT_PATH),
            "config_path": str(cfg_info["path"]),
            "database_path_arg": DB_PATH_ARG,
            "tmp_folder_arg": TMP_FOLDER_ARG,
            "run_id": None,
        }

    runner_path: Path = runner_info["path"]
    config_path: Path = cfg_info["path"]

    config_warning = ""
    if not cfg_info["exists"]:
        config_warning = (
            f"Warning: config file not found at {config_path}. "
            "The Talend runner may fail if it requires this file.\n"
        )

    # Build command matching your reference CLI
    cmd: List[str] = [
        PYTHON_BIN,
        str(runner_path),
        "--config",
        str(config_path),
        "--artifact",
        ARTIFACT_NAME,
        "--account-id",
        account_id,
        "--tenant-id",
        tenant_id,
        "--input",
        f"database_path={DB_PATH_ARG}",
        "--input",
        f"tmp_folder={TMP_FOLDER_ARG}",
        "--timeout",
        str(TIMEOUT_SECONDS),
    ]

    env = _build_talend_env()

    # Persist run metadata in qtcmeta.talend_run
    run_id: Optional[int] = None
    try:
        account_name = await _get_account_name(account_id)
        insert_sql = """
            INSERT INTO qtcmeta.talend_run (
                account_id,
                tenant_id,
                account_name,
                artifact_name,
                status
            )
            VALUES (%s, %s, %s, %s, 'running')
            RETURNING run_id;
        """
        async with connection() as conn:
            cur = await conn.execute(
                insert_sql,
                (account_id, tenant_id, account_name, ARTIFACT_NAME),
            )
            row = await cur.fetchone()
            if row:
                run_id = row[0]
    except Exception:
        # If logging fails, don't block the job; run_id stays None.
        run_id = None

    try:
        loop = asyncio.get_running_loop()

        def _run_subprocess() -> subprocess.CompletedProcess:
            return subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
            )

        completed: subprocess.CompletedProcess = await loop.run_in_executor(
            None, _run_subprocess
        )

        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""

        if config_warning:
            stderr_text = config_warning + stderr_text

        status_str = "success" if completed.returncode == 0 else "error"

        result: Dict[str, Any] = {
            "status": status_str,
            "exit_code": completed.returncode,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "command": cmd,
            "runner_path": str(runner_path),
            "config_path": str(config_path),
            "database_path_arg": DB_PATH_ARG,
            "tmp_folder_arg": TMP_FOLDER_ARG,
            "run_id": run_id,
        }

        # Update run record if we have one
        if run_id is not None:
            update_sql = """
                UPDATE qtcmeta.talend_run
                SET finished_at = now(),
                    status = %s,
                    exit_code = %s,
                    raw_stdout = %s,
                    raw_stderr = %s
                WHERE run_id = %s;
            """
            async with connection() as conn:
                await conn.execute(
                    update_sql,
                    (
                        result["status"],
                        result["exit_code"],
                        result["stdout"],
                        result["stderr"],
                        run_id,
                    ),
                )

        return result

    except Exception as e:
        error_result: Dict[str, Any] = {
            "status": "error",
            "exit_code": -1,
            "stdout": "",
            "stderr": f"Exception launching Talend runner: {repr(e)}",
            "command": cmd,
            "runner_path": str(runner_path),
            "config_path": str(config_path),
            "database_path_arg": DB_PATH_ARG,
            "tmp_folder_arg": TMP_FOLDER_ARG,
            "run_id": run_id,
        }

        # Mark run as failed if we created a row
        if run_id is not None:
            try:
                fail_sql = """
                    UPDATE qtcmeta.talend_run
                    SET finished_at = now(),
                        status = 'failed',
                        raw_stderr = %s
                    WHERE run_id = %s;
                """
                async with connection() as conn:
                    await conn.execute(
                        fail_sql,
                        (error_result["stderr"], run_id),
                    )
            except Exception:
                # swallow logging errors
                pass

        return error_result


# ============================================================
#  Facade for Routes
# ============================================================

async def process_talend_run_request(
    account_id: str,
    tenant_id: str,
    cseat_files: List[UploadFile],
    qtcmt_file: Optional[UploadFile],
) -> Dict[str, Any]:
    """
    Unified orchestrator called from routes:
    1. Stage CSEAT + QTCMT artifacts into local dirs
    2. Execute Talend run_artifact.py with the agreed CLI
    3. Return structured response payload (including run_id for history)
    """
    staged_cseat = await stage_cseat_files(account_id, tenant_id, cseat_files)
    staged_qtcmt = await stage_qtcmt_file(account_id, tenant_id, qtcmt_file)

    return await run_talend_job(
        account_id=account_id,
        tenant_id=tenant_id,
        staged_cseat=staged_cseat,
        staged_qtcmt=staged_qtcmt,
    )
