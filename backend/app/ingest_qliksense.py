
import os, io, json, zipfile, hashlib, asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional
import psycopg
from psycopg.rows import dict_row

def _conninfo() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("Set DATABASE_URL or PG_DSN for Postgres connection")
    return dsn

def _read_json_bytes(b: bytes):
    return json.loads(b.decode("utf-8"))

def _safe_id(obj: Dict[str, Any]) -> str:
    if isinstance(obj, dict) and obj.get("id"):
        return str(obj["id"])
    digest = hashlib.sha1(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()
    return digest

async def _ensure_schema(cur):
    sql_path_env = os.getenv("QS_SCHEMA_SQL")
    if sql_path_env and Path(sql_path_env).exists():
        await cur.execute(Path(sql_path_env).read_text(encoding="utf-8"))
        return
    default = Path(__file__).parent / "sql" / "repmeta_qs_schema.sql"
    if default.exists():
        await cur.execute(default.read_text(encoding="utf-8"))

async def _insert_single(cur, table: str, snapshot_id: str, data: Dict[str, Any]):
    await cur.execute(
        f"INSERT INTO repmeta_qs.{table} (snapshot_id, data) VALUES (%s, %s) "
        f"ON CONFLICT (snapshot_id) DO UPDATE SET data = EXCLUDED.data",
        (snapshot_id, json.dumps(data)),
    )

async def _insert_collection(cur, table: str, key_name: str, rows: List[Dict[str, Any]], snapshot_id: str, app_id_key: Optional[str] = None):
    if not rows:
        return
    if app_id_key:
        await cur.executemany(
            f"INSERT INTO repmeta_qs.{table} (snapshot_id, {key_name}, app_id, data) VALUES (%s, %s, %s, %s) "
            f"ON CONFLICT (snapshot_id, {key_name}) DO UPDATE SET data = EXCLUDED.data, app_id = EXCLUDED.app_id",
            [(snapshot_id, str(r.get('id') or r.get(key_name) or _safe_id(r)), r.get(app_id_key), json.dumps(r)) for r in rows],
        )
    else:
        await cur.executemany(
            f"INSERT INTO repmeta_qs.{table} (snapshot_id, {key_name}, data) VALUES (%s, %s, %s) "
            f"ON CONFLICT (snapshot_id, {key_name}) DO UPDATE SET data = EXCLUDED.data",
            [(snapshot_id, str(r.get('id') or r.get(key_name) or _safe_id(r)), json.dumps(r)) for r in rows],
        )

FILES_MAP = [
    ("about", "QlikAbout.json", None),
    ("system_info", "QlikSystemInfo.json", None),
    ("license", "QlikLicense.json", None),
    ("apps", "QlikApp.json", None),
    ("app_objects", "QlikAppObject.json", "appId"),
    ("streams", "QlikStream.json", None),
    ("users", "QlikUser.json", None),
    ("extensions", "QlikExtension.json", None),
    ("access_professional", "QlikProfessionalAccessType.json", None),
    ("access_analyzer_time", "QlikAnalyzerTimeAccessType.json", None),
    ("reload_tasks", "QlikReloadTask.json", "appId"),
    ("servernode_config", "QlikServernodeConfiguration.json", None),
    ("system_rules", "QlikSystemRule.json", None),
]

def _classify_files(files: Dict[str, bytes]) -> Dict[str, Optional[bytes]]:
    out: Dict[str, Optional[bytes]] = {fname: None for (_, fname, _) in FILES_MAP}
    for canon in out.keys():
        for k, v in files.items():
            if k.lower().endswith(canon.lower()):
                out[canon] = v
                break
    return out

async def ingest_from_buffers(buffers: Dict[str, bytes], customer_id: int, notes: Optional[str]) -> str:
    async with await psycopg.AsyncConnection.connect(_conninfo()) as conn:
        await conn.set_autocommit(False)
        async with conn.cursor(row_factory=dict_row) as cur:
            await _ensure_schema(cur)
            row = await (await cur.execute(
                "INSERT INTO repmeta_qs.snapshots (customer_id, notes) VALUES (%s, %s) RETURNING snapshot_id",
                (customer_id, notes),
            )).fetchone()
            snapshot_id = row["snapshot_id"]
            filemap = _classify_files(buffers)

            # Singletons
            for table, canon, _ in FILES_MAP[:3]:
                data = filemap.get(canon)
                if data:
                    await _insert_single(cur, table, snapshot_id, _read_json_bytes(data))

            # Collections
            for table, canon, app_id_key in FILES_MAP[3:]:
                b = filemap.get(canon)
                if not b:
                    continue
                data = _read_json_bytes(b)
                key_name = "id"
                if table in ("apps", "streams", "users", "extensions", "reload_tasks", "servernode_config", "system_rules", "app_objects"):
                    key_name = {"apps":"app_id","streams":"stream_id","users":"user_id","extensions":"extension_id","reload_tasks":"task_id","servernode_config":"node_id","system_rules":"rule_id","app_objects":"object_id"}[table]
                if isinstance(data, list):
                    await _insert_collection(cur, table, key_name, data, snapshot_id, app_id_key=app_id_key)
                elif isinstance(data, dict):
                    await _insert_collection(cur, table, key_name, [data], snapshot_id, app_id_key=app_id_key)

            await conn.commit()
            return str(snapshot_id)

async def ingest_zip_bytes(zip_bytes: bytes, customer_id: int, notes: Optional[str]) -> str:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    buffers: Dict[str, bytes] = {}
    for name in zf.namelist():
        base = name.split("/")[-1]
        if base.lower().endswith(".json") and base.lower().startswith("qlik"):
            buffers[base] = zf.read(name)
    return await ingest_from_buffers(buffers, customer_id, notes)
