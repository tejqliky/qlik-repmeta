import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set
from decimal import Decimal
import csv
import io

from psycopg.rows import dict_row

from .db import connection

LOG = logging.getLogger("ingest")
SCHEMA = os.getenv("REPMETA_SCHEMA", "repmeta")


# ------------------------------
# Helpers
# ------------------------------
async def _set_row_factory(conn):
    try:
        await conn.set_row_factory(dict_row)  # psycopg3 async
    except Exception:
        try:
            conn.row_factory = dict_row  # type: ignore[attr-defined]
        except Exception:
            pass


def _norm_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


def _get(dct: Dict[str, Any], *path, default=None):
    cur = dct
    for p in path:
        if cur is None:
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def _first_str(*vals) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v:
            return v
    return None


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _extract_replicate_version(payload: Dict[str, Any]) -> Optional[str]:
    """
    Reads the Replicate version string from the repository JSON.
    Expected path: payload["_version"]["version"] == "2025.5.0.308".
    Returns None if not found.
    """
    try:
        v = payload.get("_version")
        if isinstance(v, dict):
            s = v.get("version")
            if s and isinstance(s, str):
                return s.strip()
        if isinstance(v, str):
            return v.strip()
    except Exception:
        pass
    return None


# ---- new helpers for task-settings ----
def _sub(d: Dict[str, Any], *path) -> Dict[str, Any]:
    """Return nested dict or {} if missing/not a dict."""
    cur = d or {}
    for p in path:
        v = cur.get(p)
        if not isinstance(v, dict):
            return {}
        cur = v
    return cur


def _flatten(prefix: str, obj: Any):
    """
    Yield (full_path, type, bool, num, text, json) for rep_task_settings_kv.
    """
    if obj is None:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            yield from _flatten(p, v)
    elif isinstance(obj, list):
        yield (prefix, "json", None, None, None, obj)
    elif isinstance(obj, bool):
        yield (prefix, "bool", obj, None, None, None)
    elif isinstance(obj, (int, float, Decimal)):
        yield (prefix, "num", None, Decimal(str(obj)), None, None)
    elif isinstance(obj, str):
        yield (prefix, "text", None, None, obj, None)
    else:
        yield (prefix, "json", None, None, None, obj)


async def _exec(conn, sql: str, params: tuple):
    """
    Execute with best-effort logging; never break ingest if schema drifts.
    """
    try:
        await conn.execute(sql, params)
    except Exception as e:
        LOG.debug(
            "Non-fatal settings insert skipped: %s | SQL=%s | params(head)=%s",
            e, sql[:90], params[:3] if isinstance(params, tuple) else "?"
        )


# NEW: safe bulk insert
async def _bulk_insert(conn, sql: str, rows: List[Tuple[Any, ...]]) -> None:
    """
    Try connection.executemany(sql, rows). If not available (common for async wrappers),
    fall back to per-row execute inside a short transaction. Always batches in its own txn.
    """
    try:
        em = getattr(conn, "executemany", None)
        if callable(em):
            async with conn.transaction():
                await em(sql, rows)
            return
    except Exception:
        # fall through to per-row execution
        pass

    async with conn.transaction():
        for params in rows:
            await conn.execute(sql, params)


# ------------------------------
# Type → Family mapper
# ------------------------------
TYPE_TO_FAMILY: Dict[str, str] = {
    # PostgreSQL family (source)
    "PostgresqlsourceSettings": "postgresql",
    "RdspostgresqlSettings": "postgresql",
    "GooglepostgresqlsourceSettings": "postgresql",
    "GooglealloydbpostgresqlsourceSettings": "postgresql",
    "AWSAuroracloudpostgressourceSettings": "postgresql",
    # PostgreSQL family (target)
    "PostgresqlSettings": "postgresql",
    "GooglepostgresqlSettings": "postgresql",
    "AzurepostgresqlSettings": "postgresql",

    # SQL Server family
    "SqlserverSettings": "sqlserver",
    "MicrosoftsqlservermscdcSettings": "sqlserver",
    "MicrosoftazuresqlmscdcSettings": "sqlserver",
    "AzuresqlmanagedinstanceSettings": "sqlserver",
    "RdssqlserverSettings": "sqlserver",
    "SqlservergooglecloudsourceSettings": "sqlserver",
    "SqlservergooglecloudSettings": "sqlserver",
    "SqlazureSettings": "sqlserver",

    # MySQL family
    "MysqlSettings": "mysql",
    "RdsmysqlSettings": "mysql",
    "GooglemysqlsourceSettings": "mysql",
    "AzuremysqlsourceSettings": "mysql",
    "MysqltargetSettings": "mysql",
    "GoogleCloudSQLSettings": "mysql",
    "AzuremysqlSettings": "mysql",

    # Oracle
    "OracleSettings": "oracle",
    "OraclexstreamSettings": "oracle",

    # Snowflake (all variants)
    "SnowflakeSettings": "snowflake",
    "SnowflakeazureSettings": "snowflake",
    "SnowflakegoogleSettings": "snowflake",
    "SnowflakeTargetSettings": "snowflake",

    # Redshift
    "RedshiftDirectSettings": "redshift",

    # S3 Object storage (Amazon S3)
    "Amazons3Settings": "s3",

    # Databricks
    "DatabricksdeltaSettings": "databricks_delta",
    "DatabrickscloudstorageSettings": "databricks_cloud_storage",

    # Azure ADLS / Hadoop-ish
    "AzureadlsSettings": "azure_adls",
    "HdinsightSettings": "hdinsight",
    "HortonworkshadoopSettings": "hdinsight",
    "HadoopSettings": "hadoop",

    # Streaming & Queues
    "KafkaSettings": "kafka",
    "AmazonmskSettings": "amazon_msk",
    "ConfluentcloudSettings": "confluent_cloud",
    "EventhubsSettings": "eventhubs",
    "GooglecloudepubsubSettings": "pubsub",
    "LogstreamSettings": "logstream",

    # File
    "FileSettings": "file",
    "FilechannelSettings": "file_channel",

    # DB2
    "Db2luwSettings": "db2_luw",
    "Db2zosSettings": "db2_zos",
    "Db2zostargetSettings": "db2_zos",
    "Db2iSettings": "db2_iseries",

    # Teradata
    "TeradatasourceSettings": "teradata",
    "TeradataSettings": "teradata",

    # ODBC
    "OdbcSettings": "odbc",
    "OdbcwithcdcSettings": "odbc",

    # Informix
    "InformixSettings": "informix",

    # VSAM / IMS
    "VsamaisSettings": "vsam",
    "ImsaisSettings": "ims",

    # GCS
    "GooglestorageSettings": "gcs",

    # Google Dataproc
    "DataprocSettings": "hadoop",

    # Microsoft Fabric
    "FabricDataWarehouseSettings": "ms_fabric_dw",
}


def _family_table(family: str, role: str) -> Optional[str]:
    role = role.upper()
    if family == "postgresql":
        return f"{SCHEMA}.rep_db_postgresql_{'source' if role=='SOURCE' else 'target'}"
    if family == "sqlserver":
        return f"{SCHEMA}.rep_db_sqlserver_{'source' if role=='SOURCE' else 'target'}"
    if family == "mysql":
        return f"{SCHEMA}.rep_db_mysql_{'source' if role=='SOURCE' else 'target'}"
    if family == "oracle":
        return f"{SCHEMA}.rep_db_oracle_{'source' if role=='SOURCE' else 'target'}"
    if family == "snowflake":
        return f"{SCHEMA}.rep_db_snowflake_target"
    if family == "redshift":
        return f"{SCHEMA}.rep_db_redshift_target"
    if family == "s3":
        return f"{SCHEMA}.rep_db_s3_target"
    if family == "databricks_delta":
        return f"{SCHEMA}.rep_db_databricks_delta_target"
    if family == "databricks_cloud_storage":
        return f"{SCHEMA}.rep_db_databricks_cloud_storage_target"
    if family == "azure_adls":
        return f"{SCHEMA}.rep_db_azure_adls_target"
    if family == "hdinsight":
        return f"{SCHEMA}.rep_db_hdinsight_target"
    if family == "hadoop":
        return f"{SCHEMA}.rep_db_hadoop_target"
    if family == "kafka":
        return f"{SCHEMA}.rep_db_kafka_target"
    if family == "amazon_msk":
        return f"{SCHEMA}.rep_db_amazon_msk_target"
    if family == "confluent_cloud":
        return f"{SCHEMA}.rep_db_confluent_cloud_target"
    if family == "eventhubs":
        return f"{SCHEMA}.rep_db_eventhubs_target"
    if family == "pubsub":
        return f"{SCHEMA}.rep_db_pubsub_target"
    if family == "logstream":
        return f"{SCHEMA}.rep_db_logstream_target"
    if family == "file":
        return f"{SCHEMA}.rep_db_file_{'source' if role=='SOURCE' else 'target'}"
    if family == "file_channel":
        return f"{SCHEMA}.rep_db_file_channel_target"
    if family == "db2_luw":
        return f"{SCHEMA}.rep_db_db2_luw_source"
    if family == "db2_zos":
        return f"{SCHEMA}.rep_db_db2_zos_{'source' if role=='SOURCE' else 'target'}"
    if family == "db2_iseries":
        return f"{SCHEMA}.rep_db_db2_iseries_source"
    if family == "teradata":
        return f"{SCHEMA}.rep_db_teradata_{'source' if role=='SOURCE' else 'target'}"
    if family == "odbc":
        return f"{SCHEMA}.rep_db_odbc_{'source' if role=='SOURCE' else 'target'}"
    if family == "informix":
        return f"{SCHEMA}.rep_db_informix_source"
    if family == "vsam":
        return f"{SCHEMA}.rep_db_vsam_source"
    if family == "ims":
        return f"{SCHEMA}.rep_db_ims_source"
    if family == "gcs":
        return f"{SCHEMA}.rep_db_gcs_target"
    if family == "ms_fabric_dw":
        return f"{SCHEMA}.rep_db_ms_fabric_dw_target"
    if family == "hana":
        return f"{SCHEMA}.rep_db_hana_target"
    return None


# ------------------------------
# Base inserts
# ------------------------------
async def _get_or_create_customer(conn, customer_name: str) -> int:
    row = await (await conn.execute(
        f"SELECT customer_id FROM {SCHEMA}.dim_customer WHERE customer_name=%s",
        (customer_name,)
    )).fetchone()
    if row:
        return int(row["customer_id"])
    row = await (await conn.execute(
        f"INSERT INTO {SCHEMA}.dim_customer(customer_name) VALUES (%s) RETURNING customer_id",
        (customer_name,)
    )).fetchone()
    return int(row["customer_id"])


async def _get_or_create_server(conn, customer_id: int, server_name: str) -> int:
    row = await (await conn.execute(
        f"SELECT server_id FROM {SCHEMA}.dim_server WHERE customer_id=%s AND server_name=%s",
        (customer_id, server_name)
    )).fetchone()
    if row:
        return int(row["server_id"])
    row = await (await conn.execute(
        f"INSERT INTO {SCHEMA}.dim_server(customer_id, server_name) VALUES (%s,%s) RETURNING server_id",
        (customer_id, server_name)
    )).fetchone()
    return int(row["server_id"])


async def _create_run(conn, customer_id: int, server_id: int, replicate_version: Optional[str] = None) -> int:
    """
    Creates an ingest run row. If the 'replicate_version' column exists, populate it.
    Falls back automatically if your schema doesn't have that column yet.
    """
    try:
        row = await (await conn.execute(
            f"""
            INSERT INTO {SCHEMA}.ingest_run (customer_id, server_id, replicate_version, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING run_id
            """,
            (customer_id, server_id, replicate_version)
        )).fetchone()
        return int(row["run_id"]) if isinstance(row, dict) else int(row[0])
    except Exception as e:
        LOG.debug("ingest_run.replicate_version not available; falling back. err=%s", e)

    row = await (await conn.execute(
        f"""
        INSERT INTO {SCHEMA}.ingest_run (customer_id, server_id, created_at)
        VALUES (%s, %s, NOW())
        RETURNING run_id
        """,
        (customer_id, server_id)
    )).fetchone()
    return int(row["run_id"]) if isinstance(row, dict) else int(row[0])


# rep_database (base row)
async def _insert_rep_database(conn, run_id: int, customer_id: int, server_id: int, db: Dict[str, Any]) -> int:
    name = db.get("name")
    role = (db.get("role") or "").upper()
    is_licensed = db.get("is_licensed")
    type_id = db.get("type_id")
    settings = db.get("db_settings") or {}
    settings_type = settings.get("$type") or "Unknown"

    if not name:
        raise ValueError("Endpoint missing 'name'.")
    if role not in ("SOURCE", "TARGET"):
        raise ValueError(f"Endpoint {name} has invalid role '{role}'.")

    row = await (await conn.execute(
        f"""
        INSERT INTO {SCHEMA}.rep_database
            (run_id, customer_id, server_id, name, role, is_licensed, type_id, db_settings_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING endpoint_id
        """,
        (run_id, customer_id, server_id, name, role, is_licensed, type_id, settings_type)
    )).fetchone()

    return int(row["endpoint_id"]) if isinstance(row, dict) else int(row[0])


# ------------------------------
# Detail loaders
# ------------------------------
async def _detail_insert_or_json_fallback(conn, endpoint_id: int, role: str, type_label: str, family: str, dbs: Dict[str, Any]):
    """Insert into the per-family detail table when it exists; otherwise write to generic JSON."""
    table = _family_table(family, role)
    sjson = _json(dbs)

    if not table:
        await conn.execute(
            f"""INSERT INTO {SCHEMA}.rep_db_settings_json(endpoint_id, role, type_label, settings_json)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (endpoint_id) DO UPDATE
                SET role=EXCLUDED.role, type_label=EXCLUDED.type_label, settings_json=EXCLUDED.settings_json
            """,
            (endpoint_id, role, type_label, sjson)
        )
        return

    cols: List[str] = []
    vals: List[Any] = []

    def add(col: str, value: Any):
        cols.append(col)
        vals.append(value)

    r = role.upper()

    username = _first_str(dbs.get("username"))
    server = _first_str(dbs.get("server"))
    host = _first_str(dbs.get("host"))
    database = _first_str(dbs.get("database"))
    port = dbs.get("port")

    if family == "postgresql":
        if r == "SOURCE":
            add("username", username)
            add("server", server)
            add("host", host)
            add("port", port)
            add("database", database)
            add("heartbeat", _norm_bool(dbs.get("heartbeatEnable")))
            add("ssl_cert", _first_str(dbs.get("sslCert")))
            add("ssl_key", _first_str(dbs.get("sslKey")))
            add("ssl_root_cert", _first_str(dbs.get("sslRootCert")))
        else:
            add("username", username)
            add("host", host)
            add("database", database)
            add("ssl_cert", _first_str(dbs.get("sslCert")))
            add("ssl_key", _first_str(dbs.get("sslKey")))
            add("ssl_root_cert", _first_str(dbs.get("sslRootCert")))

    elif family == "sqlserver":
        add("username", username)
        add("server", server)
        add("database", database)
        add("safeguard_policy", _first_str(dbs.get("safeguardPolicy")))
        add("suspend_computed", _norm_bool(dbs.get("suspendTableWithComputedColumn")))

    elif family == "mysql":
        add("username", username)
        add("server", server)
        add("database", database)
        add("ssl_root_cert", _first_str(dbs.get("sslRootCert")))
        add("ssl_client_key", _first_str(dbs.get("sslClientKey")))
        add("ssl_client_cert", _first_str(dbs.get("sslClientCert")))

    elif family == "oracle":
        add("username", username)
        add("server", server)
        if r == "SOURCE":
            add("use_logminer", _norm_bool(dbs.get("useLogminerReader")))

    elif family == "snowflake":
        add("username", username)
        add("server", server)
        add("database", database)
        add("staging_type", _first_str(dbs.get("stagingtype")))
        add("files_in_batch", dbs.get("filesInBatch"))
        oauth = dbs.get("oauth") or {}
        add("oauth_type", _first_str(oauth.get("$type")))
        add("private_key_file", _first_str(dbs.get("privateKeyFile")))

    elif family == "redshift":
        add("username", username)
        add("server", server)
        add("database", database)
        add("s3_bucket", _first_str(dbs.get("bucketName")))
        add("s3_region", _first_str(dbs.get("s3Region")))
        add("files_in_batch", dbs.get("multiLoadNumberFiles"))

    elif family == "s3":
        add("bucket_name", _first_str(dbs.get("bucketName")))
        add("bucket_folder", _first_str(dbs.get("bucketFolder")))
        add("s3_region", _first_str(dbs.get("s3Region")))
        add("encryption_mode", _first_str(dbs.get("encryptionMode")))
        add("access_type", _first_str(dbs.get("accessType")))

    elif family == "databricks_delta":
        add("server", server)
        add("database", database)
        add("http_path", _first_str(dbs.get("httpPath")))
        add("staging_dir", _first_str(dbs.get("stagingdirectory")))
        add("s3_bucket", _first_str(dbs.get("s3Bucket")))

    elif family == "databricks_cloud_storage":
        add("server", server)
        add("database", database)
        add("http_path", _first_str(dbs.get("httpPath")))
        add("warehouse_type", _first_str(dbs.get("DatabricksClusterType")))
        add("s3_bucket_name", _first_str(dbs.get("s3BucketName")))

    elif family == "azure_adls":
        add("storage_account", _first_str(dbs.get("storageAccount")))
        add("file_system", _first_str(dbs.get("fileSystem")))
        add("adls_folder", _first_str(dbs.get("adlsFolder")))
        add("tenant_id", _first_str(dbs.get("adlstenantid") or dbs.get("tenantId")))
        add("client_app_id", _first_str(dbs.get("adlsclientappid")))

    elif family == "hdinsight":
        add("username", username)
        add("hdfs_path", _first_str(dbs.get("hdfsPath")))
        add("hive_odbc_host", _first_str(dbs.get("hiveODBCHost")))

    elif family == "hadoop":
        add("username", username)
        add("webhdfs_host", _first_str(dbs.get("webHDFSHost")))
        add("hdfs_path", _first_str(dbs.get("hdfsPath")))

    elif family == "kafka":
        add("username", username)
        add("brokers", _first_str(dbs.get("brokers")))
        add("topic", _first_str(dbs.get("topic")))
        add("compression", _first_str(dbs.get("compression")))
        add("auth_type", _first_str(dbs.get("authType")))
        add("message_format", _first_str(dbs.get("messageFormat")))

    elif family == "amazon_msk":
        add("username", username)
        add("brokers", _first_str(dbs.get("brokers")))
        add("topic", _first_str(dbs.get("topic")))
        add("partition_mapping", _first_str(dbs.get("partitionMapping")))
        add("message_key", _first_str(dbs.get("messageKey")))
        add("compression", _first_str(dbs.get("compression")))

    elif family == "confluent_cloud":
        add("username", username)
        add("brokers", _first_str(dbs.get("brokers")))
        add("topic", _first_str(dbs.get("topic")))
        add("compression", _first_str(dbs.get("compression")))

    elif family == "eventhubs":
        add("namespace", _first_str(dbs.get("namespace")))
        add("topic", _first_str(dbs.get("topic")))
        add("partition_mapping", _first_str(dbs.get("partitionMapping")))
        add("message_format", _first_str(dbs.get("messageFormat")))
        add("publish_option", _first_str(dbs.get("messagePublishOption")))
        add("policy_name", _first_str(dbs.get("sharedPolicyName")))

    elif family == "pubsub":
        add("topic", _first_str(dbs.get("topic")))
        add("project_id", _first_str(dbs.get("projectId")))
        add("region", _first_str(dbs.get("region")))

    elif family == "logstream":
        add("path", _first_str(dbs.get("path")))
        add("compression_level", dbs.get("compressionlevel"))

    elif family == "file":
        add("csv_string_escape", _first_str(dbs.get("csvStringEscape")))
        add("quote_empty_string", _norm_bool(dbs.get("quoteEmptyString")))
        if r == "TARGET":
            add("data_path", _first_str(dbs.get("dataPath")))
    elif family == "file_channel":
        add("path", _first_str(dbs.get("Path")))

    elif family == "db2_luw":
        add("username", username)
        add("database_alias", _first_str(dbs.get("databaseAlias")))

    elif family == "db2_zos":
        if r == "SOURCE":
            add("username", username)
            add("database_alias", _first_str(dbs.get("databaseAlias")))
            add("ifi306_sp_name", _first_str(dbs.get("ifi306SpName")))
        else:
            add("username", username)
            add("server", server)
            add("database_name", _first_str(dbs.get("databaseName")))

    elif family == "db2_iseries":
        add("username", username)
        add("database_alias", _first_str(dbs.get("databaseAlias")))
        add("journal_library", _first_str(dbs.get("JournalLibrary")))

    elif family == "teradata":
        add("username", username)
        add("server", server)
        add("database", database)

    elif family == "odbc":
        add("username", username)
        add("additional_connection_properties", _first_str(dbs.get("additionalConnectionProperties")))

    elif family == "informix":
        add("username", username)
        add("server", server)
        add("database", database)

    elif family == "vsam":
        add("username", username)
        add("source_name", _first_str(dbs.get("sourceName")))

    elif family == "ims":
        add("username", username)
        add("source_name", _first_str(dbs.get("sourceName")))

    elif family == "gcs":
        add("bucket_name", _first_str(dbs.get("bucketName")))
        add("bucket_folder", _first_str(dbs.get("bucketFolder")))
        add("json_credentials", _first_str(dbs.get("jsonCredentials")))

    elif family == "ms_fabric_dw":
        add("server", server)
        add("database", database)
        add("tenant_id", _first_str(dbs.get("tenantId")))
        add("client_id", _first_str(dbs.get("clientId")))
        add("storage_account", _first_str(dbs.get("storageAccount")))
        add("container", _first_str(dbs.get("container")))

    cols.append("settings_json")
    vals.append(sjson)

    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(vals))
    sql = f"""
        INSERT INTO {table} (endpoint_id, {col_list})
        VALUES (%s, {placeholders})
        ON CONFLICT (endpoint_id) DO UPDATE SET
        {", ".join([f"{c}=EXCLUDED.{c}" for c in cols])}
    """
    await conn.execute(sql, (endpoint_id, *vals))


async def _load_database_detail(conn, endpoint_id: int, role: str, settings: Dict[str, Any]):
    type_label = settings.get("$type") or "Unknown"
    family = TYPE_TO_FAMILY.get(type_label)
    if not family:
        await conn.execute(
            f"""INSERT INTO {SCHEMA}.rep_db_settings_json(endpoint_id, role, type_label, settings_json)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (endpoint_id) DO UPDATE
                SET role=EXCLUDED.role, type_label=EXCLUDED.type_label, settings_json=EXCLUDED.settings_json
            """,
            (endpoint_id, role.upper(), type_label, _json(settings))
        )
        return
    await _detail_insert_or_json_fallback(conn, endpoint_id, role.upper(), type_label, family, settings)


# ------------------------------
# Tasks (link to endpoints)
# ------------------------------
async def _index_endpoints_by_name(conn, run_id: int) -> Dict[str, Dict[str, Any]]:
    cur = await conn.execute(
        f"SELECT endpoint_id, name, role FROM {SCHEMA}.rep_database WHERE run_id=%s",
        (run_id,)
    )
    idx: Dict[str, Dict[str, Any]] = {}
    for row in await cur.fetchall():
        idx[str(row["name"])] = {"endpoint_id": int(row["endpoint_id"]), "role": str(row["role"])}
    return idx


async def _insert_task(conn, run_id: int, customer_id: int, server_id: int, task_obj: Dict[str, Any]) -> int:
    task = task_obj.get("task") or {}
    name = task.get("name")
    task_type = task.get("task_type") or _first_str(task.get("type"), task.get("taskType"), "Unknown")
    source_name = task.get("source_name")

    raw_targets = task.get("target_names") or task.get("targets") or []
    targets_list: List[str] = []
    if isinstance(raw_targets, list):
        for t in raw_targets:
            if isinstance(t, str) and t.strip():
                targets_list.append(t.strip())
            elif isinstance(t, dict):
                n = _first_str(t.get("name"), t.get("endpoint"))
                if n:
                    targets_list.append(n.strip())
    elif isinstance(raw_targets, str) and raw_targets.strip():
        targets_list = [raw_targets.strip()]

    row = await (await conn.execute(
        f"""
        INSERT INTO {SCHEMA}.rep_task
            (run_id, customer_id, server_id, task_name, task_type, source_name, target_names)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING task_id
        """,
        (run_id, customer_id, server_id, name, task_type, source_name, targets_list)
    )).fetchone()

    return int(row["task_id"]) if isinstance(row, dict) else int(row[0])


async def _link_task_endpoints(conn, run_id: int, task_id: int, endpoints_by_name: Dict[str, Dict[str, Any]], source_name: Optional[str], target_names: List[str]):
    if source_name and source_name in endpoints_by_name:
        src = endpoints_by_name[source_name]
        await conn.execute(
            f"""INSERT INTO {SCHEMA}.rep_task_endpoint(task_id, role, endpoint_id, run_id)
                VALUES (%s,'SOURCE',%s,%s)
                ON CONFLICT DO NOTHING
            """,
            (task_id, src["endpoint_id"], run_id)
        )
    for tname in target_names or []:
        if tname and tname in endpoints_by_name:
            tgt = endpoints_by_name[tname]
            await conn.execute(
                f"""INSERT INTO {SCHEMA}.rep_task_endpoint(task_id, role, endpoint_id, run_id)
                    VALUES (%s,'TARGET',%s,%s)
                    ON CONFLICT DO NOTHING
                """,
                (task_id, tgt["endpoint_id"], run_id)
            )


# ------------------------------
# FIXED: Persist explicit table list per task (correct path)
# ------------------------------
async def _insert_task_tables(conn, run_id: int, task_id: int, task_obj: Dict[str, Any]) -> None:
    """
    Extract explicit table list from a task and insert into rep_task_table.
    Treat each (owner, name) pair as one table.

    Robust JSON search:
      - Look for 'source' at the top-level (task_obj['source']) and, if present,
        scan that dict AND its 'rep_source' child (some repos nest here).
      - Also tolerate the (rare) 'task' → 'source' nesting.

      Within each candidate, read 'source_tables' (dict or list) and its
      'explicit_included_tables' array of { owner, name, estimated_size, orig_db_id }.
    """
    if not isinstance(task_obj, dict):
        return

    # Where can 'source' live?
    source_candidates: List[Dict[str, Any]] = []
    if isinstance(task_obj.get("source"), dict):
        source_candidates.append(task_obj["source"])        # common shape
    if isinstance(task_obj.get("task"), dict) and isinstance(task_obj["task"].get("source"), dict):
        source_candidates.append(task_obj["task"]["source"])  # rare shape

    if not source_candidates:
        return

    seen: Set[Tuple[str, str]] = set()
    rows: List[Tuple[int, int, str, str, Optional[float], Optional[int]]] = []

    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def _to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    for src in source_candidates:
        # IMPORTANT: 'source_tables' may be a sibling of 'rep_source' (not inside it),
        # so we must check BOTH the src itself and its 'rep_source' child.
        for holder in (src, src.get("rep_source") if isinstance(src.get("rep_source"), dict) else {}):
            st = holder.get("source_tables")
            groups: List[Dict[str, Any]] = []
            if isinstance(st, dict):
                groups = [st]
            elif isinstance(st, list):
                groups = [g for g in st if isinstance(g, dict)]

            for g in groups:
                arr = g.get("explicit_included_tables") or g.get("explicit_tables") or []
                if not isinstance(arr, list):
                    continue
                for t in arr:
                    if not isinstance(t, dict):
                        continue
                    owner = _first_str(t.get("owner"))
                    name = _first_str(t.get("name"))
                    if not owner or not name:
                        continue
                    key = (owner.strip(), name.strip())
                    if key in seen:
                        continue
                    seen.add(key)
                    est = _to_float(t.get("estimated_size"))
                    orig = _to_int(t.get("orig_db_id"))
                    rows.append((run_id, task_id, owner.strip(), name.strip(), est, orig))

    if not rows:
        return

    sql = f"""
        INSERT INTO {SCHEMA}.rep_task_table
            (run_id, task_id, owner, table_name, estimated_size, orig_db_id)
        VALUES (%s,%s,%s,%s,%s,%s)
    """
    inserted = 0
    for r in rows:
        await conn.execute(sql, r)
        inserted += 1
    LOG.debug("rep_task_table: task_id=%s -> inserted %s table rows", task_id, inserted)


async def _ensure_task_logger_table(conn) -> None:
    """
    Ensure the auxiliary table for task loggers exists.
    No-op if it already exists or if privileges don't allow DDL.
    """
    try:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.rep_task_logger (
              task_id     BIGINT REFERENCES {SCHEMA}.rep_task(task_id) ON DELETE CASCADE,
              run_id      BIGINT REFERENCES {SCHEMA}.ingest_run(run_id),
              logger_name TEXT NOT NULL,
              level       TEXT,
              PRIMARY KEY (task_id, run_id, logger_name)
            )
        """)
    except Exception as e:
        LOG.debug("rep_task_logger table creation skipped (non-fatal): %s", e)

async def _insert_task_loggers(conn, run_id: int, task_id: int, task_obj: Dict[str, Any]) -> None:
    """
    Flatten task.loggers into rep_task_logger (one row per logger per task per run).
    Accept shapes:
      - task_obj["loggers"] = { "TARGET_LOAD": "DEBUG", ... }
      - task_obj["task"]["loggers"] = { ... }
    Ignores keys: "$type", "loggers_configuration".
    """
    if not isinstance(task_obj, dict):
        return

    candidates = []
    lg = task_obj.get("loggers")
    if isinstance(lg, dict):
        candidates.append(lg)
    t = task_obj.get("task") or {}
    if isinstance(t.get("loggers"), dict):
        candidates.append(t["loggers"])

    rows = []
    for d in candidates:
        for k, v in (d or {}).items():
            if k in ("$type", "loggers_configuration"):
                continue
            if not isinstance(k, str):
                continue
            level = None if v is None else str(v)
            rows.append((task_id, run_id, k.strip(), level.strip() if isinstance(level, str) else level))

    if not rows:
        return

    # Ensure table exists (best-effort)
    await _ensure_task_logger_table(conn)

    sql = f"""INSERT INTO {SCHEMA}.rep_task_logger (task_id, run_id, logger_name, level)
              VALUES (%s,%s,%s,%s)
              ON CONFLICT (task_id, run_id, logger_name) DO UPDATE SET level=EXCLUDED.level"""
    for r in rows:
        try:
            await conn.execute(sql, r)
        except Exception as e:
            LOG.debug("rep_task_logger insert skipped (non-fatal): %s / row=%s", e, r)


# ------------------------------
# NEW: Task settings – sections / normalized / KV
# ------------------------------
async def _upsert_task_settings_sections(conn, run_id: int, task_id: int, tset: Dict[str, Any]):
    for sec in ("common_settings", "target_settings", "source_settings", "sorter_settings"):
        body = _sub(tset, sec)
        await _exec(
            conn,
            f"""INSERT INTO {SCHEMA}.rep_task_settings_section(task_id, run_id, section_path, body)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (task_id, section_path) DO UPDATE
                SET run_id=EXCLUDED.run_id, body=EXCLUDED.body""",
            (task_id, run_id, f"task_settings.{sec}", json.dumps(body, ensure_ascii=False)),
        )


async def _upsert_task_settings_common(conn, run_id: int, task_id: int, cs: Dict[str, Any]):
    sql = f"""INSERT INTO {SCHEMA}.rep_task_settings_common(
                task_id, run_id,
                write_full_logging,
                status_table_name, suspended_tables_table_name, exception_table_name,
                save_changes_enabled,
                batch_apply_memory_limit, batch_apply_timeout, batch_apply_timeout_min,
                status_table_enabled, suspended_tables_table_enabled, history_table_enabled,
                exception_table_enabled, recovery_table_enabled, ddl_history_table_enabled,
                batch_apply_use_parallel_bulk, parallel_bulk_max_num_threads, batch_optimize_by_merge,
                use_inserts_for_status_table_updates, task_uuid)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (task_id) DO UPDATE SET
                run_id=EXCLUDED.run_id,
                write_full_logging=EXCLUDED.write_full_logging,
                status_table_name=EXCLUDED.status_table_name,
                suspended_tables_table_name=EXCLUDED.suspended_tables_table_name,
                exception_table_name=EXCLUDED.exception_table_name,
                save_changes_enabled=EXCLUDED.save_changes_enabled,
                batch_apply_memory_limit=EXCLUDED.batch_apply_memory_limit,
                batch_apply_timeout=EXCLUDED.batch_apply_timeout,
                batch_apply_timeout_min=EXCLUDED.batch_apply_timeout_min,
                status_table_enabled=EXCLUDED.status_table_enabled,
                suspended_tables_table_enabled=EXCLUDED.suspended_tables_table_enabled,
                history_table_enabled=EXCLUDED.history_table_enabled,
                exception_table_enabled=EXCLUDED.exception_table_enabled,
                recovery_table_enabled=EXCLUDED.recovery_table_enabled,
                ddl_history_table_enabled=EXCLUDED.ddl_history_table_enabled,
                batch_apply_use_parallel_bulk=EXCLUDED.batch_apply_use_parallel_bulk,
                parallel_bulk_max_num_threads=EXCLUDED.parallel_bulk_max_num_threads,
                batch_optimize_by_merge=EXCLUDED.batch_optimize_by_merge,
                use_inserts_for_status_table_updates=EXCLUDED.use_inserts_for_status_table_updates,
                task_uuid=EXCLUDED.task_uuid
        """
    await _exec(conn, sql, (
        task_id, run_id,
        _norm_bool(cs.get("write_full_logging")),
        None, None, None,
        _norm_bool(cs.get("save_changes_enabled")),
        cs.get("batch_apply_memory_limit"),
        cs.get("batch_apply_timeout"),
        cs.get("batch_apply_timeout_min"),
        _norm_bool(cs.get("status_table_enabled")),
        _norm_bool(cs.get("suspended_tables_table_enabled")),
        _norm_bool(cs.get("history_table_enabled")),
        _norm_bool(cs.get("exception_table_enabled")),
        _norm_bool(cs.get("recovery_table_enabled")),
        _norm_bool(cs.get("ddl_history_table_enabled")),
        _norm_bool(cs.get("batch_apply_use_parallel_bulk")),
        cs.get("parallel_bulk_max_num_threads"),
        _norm_bool(cs.get("batch_optimize_by_merge")),
        _norm_bool(cs.get("use_inserts_for_status_table_updates")),
        cs.get("task_uuid"),
    ))


async def _upsert_task_settings_change_table(conn, run_id: int, task_id: int, cs: Dict[str, Any]):
    ddl = _sub(cs, "change_table_settings")
    await _exec(
        conn,
        f"""INSERT INTO {SCHEMA}.rep_task_settings_change_table(task_id, run_id, handle_ddl)
            VALUES (%s,%s,%s)
            ON CONFLICT (task_id) DO UPDATE SET run_id=EXCLUDED.run_id, handle_ddl=EXCLUDED.handle_ddl""",
        (task_id, run_id, _norm_bool(ddl.get("handle_ddl"))),
    )


async def _upsert_task_settings_target(conn, run_id: int, task_id: int, ts: Dict[str, Any]):
    await _exec(
        conn,
        f"""INSERT INTO {SCHEMA}.rep_task_settings_target(
                task_id, run_id,
                create_pk_after_data_load, artifacts_cleanup_enabled,
                handle_truncate_ddl, handle_drop_ddl, max_transaction_size,
                ddl_handling_policy, ftm_settings)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (task_id) DO UPDATE SET
                run_id=EXCLUDED.run_id,
                create_pk_after_data_load=EXCLUDED.create_pk_after_data_load,
                artifacts_cleanup_enabled=EXCLUDED.artifacts_cleanup_enabled,
                handle_truncate_ddl=EXCLUDED.handle_truncate_ddl,
                handle_drop_ddl=EXCLUDED.handle_drop_ddl,
                max_transaction_size=EXCLUDED.max_transaction_size,
                ddl_handling_policy=EXCLUDED.ddl_handling_policy,
                ftm_settings=EXCLUDED.ftm_settings
        """,
        (
            task_id, run_id,
            _norm_bool(ts.get("create_pk_after_data_load")),
            _norm_bool(ts.get("artifacts_cleanup_enabled")),
            _norm_bool(ts.get("handle_truncate_ddl")),
            _norm_bool(ts.get("handle_drop_ddl")),
            ts.get("max_transaction_size"),
            json.dumps(ts.get("ddl_handling_policy") or {}, ensure_ascii=False),
            ts.get("ftm_settings"),
        ),
    )


async def _upsert_task_sorter_settings(conn, run_id: int, task_id: int, ss: Dict[str, Any]):
    lts = _sub(ss, "local_transactions_storage")
    await _exec(
        conn,
        f"""INSERT INTO {SCHEMA}.rep_task_sorter_settings(
                task_id, run_id, memory_keep_time, memory_limit_total, transaction_consistency_timeout)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (task_id) DO UPDATE SET
                run_id=EXCLUDED.run_id,
                memory_keep_time=EXCLUDED.memory_keep_time,
                memory_limit_total=EXCLUDED.memory_limit_total,
                transaction_consistency_timeout=EXCLUDED.transaction_consistency_timeout
        """,
        (
            task_id, run_id,
            lts.get("memory_keep_time"),
            lts.get("memory_limit_total"),
            ss.get("transaction_consistency_timeout"),
        ),
    )


async def _upsert_task_settings_kv(conn, run_id: int, task_id: int, tset: Dict[str, Any]):
    rows = []
    for full_path, typ, vbool, vnum, vtext, vjson in _flatten("task_settings", tset):
        rows.append((
            task_id, run_id, full_path, typ,
            vbool,
            vnum,
            vtext,
            json.dumps(vjson) if vjson is not None and typ == "json" else None
        ))
    if not rows:
        return
    sql = f"""INSERT INTO {SCHEMA}.rep_task_settings_kv
                (task_id, run_id, full_path, val_type, val_bool, val_num, val_text, val_json)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (task_id, full_path) DO UPDATE SET
                run_id=EXCLUDED.run_id,
                val_type=EXCLUDED.val_type,
                val_bool=EXCLUDED.val_bool,
                val_num=EXCLUDED.val_num,
                val_text=EXCLUDED.val_text,
                val_json=EXCLUDED.val_json"""
    try:
        await _bulk_insert(conn, sql, rows)
    except Exception as e:
        LOG.debug("rep_task_settings_kv upsert skipped (non-fatal): %s", e)


async def _insert_task_settings(conn, run_id: int, task_id: int, task_obj: Dict[str, Any]):
    """
    Persist task_settings:
      - sections JSON
      - normalized tables
      - KV (full path capture)
    """
    tset = task_obj.get("task_settings") or {}
    # sections
    await _upsert_task_settings_sections(conn, run_id, task_id, tset)
    # normalized
    cs = _sub(tset, "common_settings")
    await _upsert_task_settings_common(conn, run_id, task_id, cs)
    await _upsert_task_settings_change_table(conn, run_id, task_id, cs)
    await _upsert_task_settings_target(conn, run_id, task_id, _sub(tset, "target_settings"))
    await _upsert_task_sorter_settings(conn, run_id, task_id, _sub(tset, "sorter_settings"))
    # kv
    await _upsert_task_settings_kv(conn, run_id, task_id, tset)


# ------------------------------
# NEW: MetricsLog filtering + rollups
# ------------------------------
def _make_reader(file_obj=None, data_bytes: Optional[bytes] = None) -> csv.DictReader:
    """
    Create a fresh CSV DictReader (tab-delimited) from either a seekable file_obj
    or from in-memory bytes.
    """
    if file_obj is not None:
        try:
            file_obj.seek(0)
        except Exception:
            pass
        text_iter = io.TextIOWrapper(file_obj, encoding="utf-8", errors="replace")
        return csv.DictReader(text_iter, delimiter="\t")
    else:
        return csv.DictReader(io.StringIO((data_bytes or b"").decode("utf-8", errors="replace")), delimiter="\t")


def _parse_ts_opt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            pass
    return None


def _keep_metrics_row(event_type: Optional[str], status: Optional[str]) -> bool:
    """
    Omit rows where eventType=STOP AND status != 'Ok' (case-insensitive).
    """
    et = (event_type or "").strip().upper()
    st = (status or "").strip().lower()
    if et == "STOP" and st != "ok":
        return False
    return True


def _n_int(x) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return None


def _bump(acc: Dict[Any, Dict[str, Any]], key: Any,
          start_ts: Optional[datetime],
          load_rows: Optional[int], load_bytes: Optional[int],
          cdc_rows: Optional[int], cdc_bytes: Optional[int]) -> None:
    """
    Accumulate totals for a key (task_uuid or (src_fam_id, tgt_fam_id)).
    """
    a = acc.get(key)
    if a is None:
        a = {"lr": 0, "lb": 0, "cr": 0, "cb": 0, "ev": 0, "first": start_ts, "last": start_ts}
        acc[key] = a
    a["lr"] += load_rows or 0
    a["lb"] += load_bytes or 0
    a["cr"] += cdc_rows or 0
    a["cb"] += cdc_bytes or 0
    a["ev"] += 1
    if start_ts is not None:
        if a["first"] is None or start_ts < a["first"]:
            a["first"] = start_ts
        if a["last"] is None or start_ts > a["last"]:
            a["last"] = start_ts


async def _create_metrics_run(conn, customer_id: int, server_id: int, file_name: str, collected_at: Optional[datetime]) -> int:
    row = await (await conn.execute(
        f"""
        INSERT INTO {SCHEMA}.rep_metrics_run
          (customer_id, server_id, file_name, collected_at, created_at)
        VALUES (%s,%s,%s,%s, NOW())
        RETURNING metrics_run_id
        """,
        (customer_id, server_id, file_name, collected_at)
    )).fetchone()
    return int(row["metrics_run_id"])


async def _load_alias_map(conn) -> Dict[str, int]:
    """
    Load endpoint_alias_map once; key is lower(alias_value).
    """
    cur = await conn.execute(
        f"SELECT LOWER(alias_value) AS a, family_id FROM {SCHEMA}.endpoint_alias_map"
    )
    m: Dict[str, int] = {}
    for r in await cur.fetchall() or []:
        a = r["a"] if isinstance(r, dict) else r[0]
        fid = int(r["family_id"] if isinstance(r, dict) else r[1])
        if a:
            m[a] = fid
    return m


async def _latest_run_ts(conn, customer_id: int, server_id: int) -> Optional[datetime]:
    cur = await conn.execute(
        f"""SELECT MAX(created_at) AS mx
               FROM {SCHEMA}.ingest_run
              WHERE customer_id=%s AND server_id=%s""",
        (customer_id, server_id)
    )
    row = await cur.fetchone()
    return row["mx"] if row and row["mx"] else None


async def _task_map_by_uuid(conn, customer_id: int, server_id: int, uuids: List[str]) -> Dict[str, List[int]]:
    """
    Map {uuid: [task_id,...]} from the LATEST repo ingest on this (customer, server).
    """
    latest = await _latest_run_ts(conn, customer_id, server_id)
    if not latest or not uuids:
        return {}

    cur = await conn.execute(
        f"""
        SELECT s.task_uuid, t.task_id
          FROM {SCHEMA}.rep_task_settings_common s
          JOIN {SCHEMA}.rep_task t     ON t.task_id = s.task_id
          JOIN {SCHEMA}.ingest_run r   ON r.run_id  = t.run_id
         WHERE r.customer_id=%s
           AND r.server_id=%s
           AND r.created_at = %s
           AND s.task_uuid = ANY(%s)
        """,
        (customer_id, server_id, latest, uuids)
    )
    rows = await cur.fetchall() or []
    m: Dict[str, List[int]] = {}
    for r in rows:
        u = r["task_uuid"] if isinstance(r, dict) else r[0]
        t = int(r["task_id"] if isinstance(r, dict) else r[1])
        m.setdefault(u, []).append(t)
    return m


# ------------------------------
# NEW: Replicate Metrics Log ingest (stream + batch + rollups)
# ------------------------------
async def ingest_metrics_log(
    data_bytes: Optional[bytes],
    customer_name: str,
    server_name: str,
    file_name: str,
    file_obj=None,  # optional streaming input; keeps backward compatibility
) -> Dict[str, Any]:
    """
    Ingest a Replicate MetricsLog TSV exported per Replicate Server.

    Matching:
      - MetricsLog.taskID → rep_task_settings_common.task_uuid
      - We match within the *latest* Repository ingest for (customer, server).

    Family mapping:
      - sourceType / targetType → endpoint_alias_map.alias_value → endpoint_family.family_id

    Performance:
      - Two-pass scan (pre-scan for earliest ts + UUID set), then batch inserts (BATCH_SIZE).
      - Each batch is its own transaction to avoid one giant, long-running transaction.
      - While inserting raw events, we also accumulate per-task and per-pair totals and flush once.
    """
    BATCH_SIZE = int(os.getenv("METRICS_BATCH_SIZE", "1000"))

    # ---------- PASS 1: pre-scan for earliest ts, uuids (with early filter) ----------
    uuids_set: Set[str] = set()
    earliest_ts: Optional[datetime] = None

    rdr1 = _make_reader(file_obj=file_obj, data_bytes=data_bytes)
    for r in rdr1:
        if not _keep_metrics_row(r.get("eventType"), r.get("status")):
            continue
        u = (r.get("taskID") or "").strip()
        if u:
            uuids_set.add(u)
        ts = _parse_ts_opt(r.get("startTimestamp"))
        if ts is not None and (earliest_ts is None or ts < earliest_ts):
            earliest_ts = ts

    # ---------- DB setup ----------
    async with connection() as conn:
        await _set_row_factory(conn)

        # Resolve customer/server
        customer_id = await _get_or_create_customer(conn, customer_name)
        server_id   = await _get_or_create_server(conn, customer_id, server_name)

        # Header row (standalone insert)
        metrics_run_id = await _create_metrics_run(conn, customer_id, server_id, file_name, earliest_ts)

        # Build uuid → task_id[] map once (latest run)
        uuid_to_tasks = await _task_map_by_uuid(conn, customer_id, server_id, sorted(uuids_set))

        # Load alias family map once
        alias_map = await _load_alias_map(conn)

        # ---------- PASS 2: batch insert + rollups ----------
        rows_inserted = 0
        matched = 0
        duplicate_conflicts: List[Dict[str, Any]] = []

        # Raw events insert
        sql_evt = f"""
            INSERT INTO {SCHEMA}.rep_metrics_event
              (metrics_run_id, task_uuid, task_id,
               source_type, target_type, source_family_id, target_family_id,
               start_ts, stop_ts, event_type, load_rows, load_bytes, cdc_rows, cdc_bytes, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        batch: List[Tuple[Any, ...]] = []

        async def flush_batch():
            nonlocal batch, rows_inserted
            if not batch:
                return
            await _bulk_insert(conn, sql_evt, batch)
            rows_inserted += len(batch)
            batch = []

        # In-memory accumulators
        task_acc: Dict[str, Dict[str, Any]] = {}
        pair_acc: Dict[Tuple[int, int], Dict[str, Any]] = {}

        rdr2 = _make_reader(file_obj=file_obj, data_bytes=data_bytes)
        for r in rdr2:
            if not _keep_metrics_row(r.get("eventType"), r.get("status")):
                continue

            task_uuid   = (r.get("taskID") or "").strip()
            source_type = (r.get("sourceType") or "").strip() or None
            target_type = (r.get("targetType") or "").strip() or None
            event_type  = (r.get("eventType") or "").strip() or None
            status      = (r.get("status") or "").strip() or None

            start_ts = _parse_ts_opt(r.get("startTimestamp"))
            stop_ts  = _parse_ts_opt(r.get("stopTimestamp"))

            load_rows  = _n_int(r.get("loadRows"))
            load_bytes = _n_int(r.get("loadBytes"))
            cdc_rows   = _n_int(r.get("cdcRows"))
            cdc_bytes  = _n_int(r.get("cdcBytes"))

            task_ids = uuid_to_tasks.get(task_uuid, [])
            task_id: Optional[int] = None
            if len(task_ids) == 1:
                task_id = task_ids[0]
                matched += 1
            elif len(task_ids) > 1:
                duplicate_conflicts.append({"task_uuid": task_uuid, "task_ids": task_ids})

            src_fam_id = alias_map.get((source_type or "").lower()) if source_type else None
            tgt_fam_id = alias_map.get((target_type or "").lower()) if target_type else None

            # Accumulate rollups
            if task_uuid:
                _bump(task_acc, task_uuid, start_ts, load_rows, load_bytes, cdc_rows, cdc_bytes)
            if src_fam_id is not None and tgt_fam_id is not None:
                _bump(pair_acc, (src_fam_id, tgt_fam_id), start_ts, load_rows, load_bytes, cdc_rows, cdc_bytes)

            # Buffer raw event
            batch.append((
                metrics_run_id, task_uuid, task_id,
                source_type, target_type, src_fam_id, tgt_fam_id,
                start_ts, stop_ts, event_type, load_rows, load_bytes, cdc_rows, cdc_bytes, status
            ))

            if len(batch) >= BATCH_SIZE:
                await flush_batch()

        # final flush of raw events
        await flush_batch()

        # ---------- Write rollups (best-effort) ----------
        # 1) Per-task totals
        if task_acc:
            rows_task = []
            for u, a in task_acc.items():
                # attach first matched task_id when available, else None
                tid = None
                ids = uuid_to_tasks.get(u) or []
                if ids:
                    tid = ids[0]
                rows_task.append((
                    metrics_run_id, u, tid,
                    a["lr"], a["lb"], a["cr"], a["cb"], a["ev"], a["first"], a["last"]
                ))
            try:
                await _bulk_insert(conn, f"""
                    INSERT INTO {SCHEMA}.rep_metrics_task_total
                      (metrics_run_id, task_uuid, task_id,
                       load_rows_total, load_bytes_total, cdc_rows_total, cdc_bytes_total,
                       events_count, first_ts, last_ts)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (metrics_run_id, task_uuid) DO UPDATE SET
                       task_id           = COALESCE(EXCLUDED.task_id, {SCHEMA}.rep_metrics_task_total.task_id),
                       load_rows_total   = EXCLUDED.load_rows_total,
                       load_bytes_total  = EXCLUDED.load_bytes_total,
                       cdc_rows_total    = EXCLUDED.cdc_rows_total,
                       cdc_bytes_total   = EXCLUDED.cdc_bytes_total,
                       events_count      = EXCLUDED.events_count,
                       first_ts          = LEAST({SCHEMA}.rep_metrics_task_total.first_ts, EXCLUDED.first_ts),
                       last_ts           = GREATEST({SCHEMA}.rep_metrics_task_total.last_ts,  EXCLUDED.last_ts)
                """, rows_task)
            except Exception as e:
                LOG.debug("rep_metrics_task_total write skipped (non-fatal): %s", e)

        # 2) Source×Target totals (skip rows with NULL family ids to avoid PK/FK issues)
        if pair_acc:
            rows_pair = []
            for (sfid, tfid), a in pair_acc.items():
                if sfid is None or tfid is None:
                    continue
                rows_pair.append((
                    metrics_run_id, sfid, tfid,
                    a["lr"], a["lb"], a["cr"], a["cb"], a["ev"], a["first"], a["last"]
                ))
            if rows_pair:
                try:
                    await _bulk_insert(conn, f"""
                        INSERT INTO {SCHEMA}.rep_metrics_pair_total
                          (metrics_run_id, source_family_id, target_family_id,
                           load_rows_total, load_bytes_total, cdc_rows_total, cdc_bytes_total,
                           events_count, first_ts, last_ts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (metrics_run_id, source_family_id, target_family_id) DO UPDATE SET
                           load_rows_total   = EXCLUDED.load_rows_total,
                           load_bytes_total  = EXCLUDED.load_bytes_total,
                           cdc_rows_total    = EXCLUDED.cdc_rows_total,
                           cdc_bytes_total   = EXCLUDED.cdc_bytes_total,
                           events_count      = EXCLUDED.events_count,
                           first_ts          = LEAST({SCHEMA}.rep_metrics_pair_total.first_ts, EXCLUDED.first_ts),
                           last_ts           = GREATEST({SCHEMA}.rep_metrics_pair_total.last_ts,  EXCLUDED.last_ts)
                    """, rows_pair)
                except Exception as e:
                    LOG.debug("rep_metrics_pair_total write skipped (non-fatal): %s", e)

        LOG.info("[METRICSLOG] metrics_run_id=%s rows=%s matched=%s dup_uuids=%s (task_totals=%s, pair_totals=%s)",
                 metrics_run_id, rows_inserted, matched, len(duplicate_conflicts),
                 len(task_acc), len(pair_acc))

        return {
            "metrics_run_id": metrics_run_id,
            "file": file_name,
            "rows": rows_inserted,
            "inserted": rows_inserted,
            "matched_by_uuid": matched,
            "duplicate_uuid_conflicts": duplicate_conflicts,
        }


# ------------------------------
# Public API
# ------------------------------
async def ingest_repository(repo_json: Dict[str, Any], customer_name: str, server_name: str) -> Dict[str, Any]:
    """
    Ingest the uploaded repository JSON:
      - create ingest_run
      - flatten databases to rep_database + per-family detail tables (or JSON fallback)
      - flatten tasks, and link to endpoints by name
      - persist per-task explicit tables to rep_task_table
      - NEW: persist task_settings (sections + normalized + KV)
    """
    cmd = (
        _get(repo_json, "cmd", "replication_definition")
        or _get(repo_json, "replication_definition")
        or _get(repo_json, "cmd.replication_definition")
        or {}
    )
    databases = cmd.get("databases") or []
    tasks = cmd.get("tasks") or []

    replicate_version = _extract_replicate_version(repo_json)
    LOG.info("[INGEST] Starting ingest: customer=%s server=%s dbs=%s tasks=%s replicate_version=%s",
             customer_name, server_name, len(databases), len(tasks), replicate_version or "(none)")

    async with connection() as conn:
        await _set_row_factory(conn)
        async with conn.transaction():
            customer_id = await _get_or_create_customer(conn, customer_name)
            server_id = await _get_or_create_server(conn, customer_id, server_name)

            run_id = await _create_run(conn, customer_id, server_id, replicate_version)

            # --- Databases
            endpoint_ids: List[int] = []
            for db in databases:
                endpoint_id = await _insert_rep_database(conn, run_id, customer_id, server_id, db)
                endpoint_ids.append(endpoint_id)
                settings = db.get("db_settings") or {}
                role = (db.get("role") or "UNKNOWN").upper()
                await _load_database_detail(conn, endpoint_id, role, settings)

            # --- Tasks + endpoint links + tables per task + loggers + settings
            endpoints_by_name = await _index_endpoints_by_name(conn, run_id)
            task_ids: List[int] = []
            for obj in tasks:
                task_id = await _insert_task(conn, run_id, customer_id, server_id, obj)
                task_ids.append(task_id)

                t = obj.get("task") or {}
                source_name = t.get("source_name")
                target_names = t.get("target_names") or []
                await _link_task_endpoints(conn, run_id, task_id, endpoints_by_name, source_name, target_names)

                # explicit table list for this task (if present)
                await _insert_task_tables(conn, run_id, task_id, obj)

                # logger levels for this task (if present)
                await _insert_task_loggers(conn, run_id, task_id, obj)

                # NEW: task settings (sections + normalized + kv)
                await _insert_task_settings(conn, run_id, task_id, obj)

            LOG.info("[INGEST] Completed run_id=%s endpoints=%s tasks=%s", run_id, len(endpoint_ids), len(task_ids))
            return {
                "run_id": run_id,
                "endpoints_inserted": len(endpoint_ids),
                "tasks_inserted": len(task_ids)
            }
