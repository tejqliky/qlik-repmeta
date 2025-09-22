import os
import psycopg
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

# Required envs:
#   DATABASE_URL=postgresql+psycopg://user:pass@host:port/dbname
#   REPMETA_SCHEMA=repmeta   (optional; defaults to 'repmeta')
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")

SCHEMA = os.getenv("REPMETA_SCHEMA", "repmeta")

@asynccontextmanager
async def connection():
    """
    Open an async psycopg3 connection that:
      - starts with autocommit OFF (psycopg default)
      - COMMITs on successful exit
      - ROLLBACKs on exception
      - always closes cleanly
    Use:
        async with connection() as conn:
            async with conn.transaction():
                await conn.execute("INSERT ...")
    """
    conn = await psycopg.AsyncConnection.connect(DATABASE_URL)
    try:
        # Some code paths set row factory themselves; leave it flexible.
        yield conn
        # If we got here without exception, make sure the outermost transaction is committed.
        # psycopg3 won't auto-commit when the connection context exits.
        if not conn.autocommit:
            try:
                await conn.commit()
            except Exception:
                # if commit fails, roll back to not leave the connection in limbo
                await conn.rollback()
                raise
    except Exception:
        # Ensure we don't leak uncommitted work
        try:
            await conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            await conn.close()
        except Exception:
            pass

# Optional helper for quick sanity checks
async def test_database_connection():
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            await cur.fetchone()
