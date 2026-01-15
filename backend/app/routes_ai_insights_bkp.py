import os
from typing import Optional, Any, Dict
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from .ai_insights import start_job, retry_job, get_insights
from .db import connection, SCHEMA

router = APIRouter(prefix="/api", tags=["ai-insights"])

AUTOMATION_SECRET = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")

class StartReq(BaseModel):
    requested_by: str = "ui"
    model: Optional[str] = None

class AutomationEventReq(BaseModel):
    source: str = "n8n"
    execution_id: str
    status: str
    message: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None

@router.post("/runs/{run_id}/insights/start")
async def insights_start(run_id: int, body: StartReq):
    try:
        job = await start_job(run_id=run_id, requested_by=body.requested_by, model=body.model)
        return {"job": job}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/runs/{run_id}/insights")
async def insights_get(run_id: int):
    data = await get_insights(run_id)
    return data

@router.post("/runs/{run_id}/insights/retry")
async def insights_retry(run_id: int):
    try:
        job = await retry_job(run_id)
        return {"job": job}
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/runs/{run_id}/automation-events")
async def automation_events(
    run_id: int,
    body: AutomationEventReq,
    x_repmeta_automation_secret: Optional[str] = Header(default=None),
):
    if AUTOMATION_SECRET and x_repmeta_automation_secret != AUTOMATION_SECRET:
        raise HTTPException(status_code=401, detail="Invalid automation secret")

    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                INSERT INTO {SCHEMA}.automation_event (run_id, source, execution_id, status, message, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, execution_id) DO UPDATE SET
                  status=EXCLUDED.status,
                  message=EXCLUDED.message,
                  payload=EXCLUDED.payload,
                  created_at=now()
                """,
                (run_id, body.source, body.execution_id, body.status, body.message, None if body.payload is None else __import__("json").dumps(body.payload)),
            )

    return {"ok": True}
