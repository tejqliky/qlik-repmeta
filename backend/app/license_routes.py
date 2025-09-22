import os
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from .db import connection
from .ingest_license import parse_license_from_log

SCHEMA = os.getenv("REPMETA_SCHEMA", "repmeta")
router = APIRouter()


@router.post("/ingest-license-log")
async def ingest_license_log(
    file: UploadFile = File(...),
    customer_name: str = Form(...),
):
    """
    Parse a Replicate task log and persist per-customer license capabilities.
    Returns a concise summary of what was parsed.
    """
    # Read file
    text = (await file.read()).decode("utf-8", errors="replace")

    # Parse license line(s)
    try:
        all_src, all_tgt, srcs, tgts, raw = parse_license_from_log(text)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse license line: {e}")

    # Persist
    async with connection() as conn:
        cur = await conn.execute(
            f"SELECT customer_id FROM {SCHEMA}.dim_customer WHERE customer_name = %s",
            (customer_name,),
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Unknown customer: {customer_name}")

        # Row may be tuple or dict depending on row_factory
        customer_id = row[0] if not isinstance(row, dict) else row.get("customer_id")

        await conn.execute(
            f"""
            INSERT INTO {SCHEMA}.customer_license_capabilities
              (customer_id, licensed_all_sources, licensed_all_targets,
               licensed_sources, licensed_targets, raw_line)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (customer_id, all_src, all_tgt, srcs, tgts, raw),
        )

    return {
        "customer": customer_name,
        "licensed_all_sources": all_src,
        "licensed_all_targets": all_tgt,
        "licensed_sources": srcs,
        "licensed_targets": tgts,
        "raw": raw,
    }
