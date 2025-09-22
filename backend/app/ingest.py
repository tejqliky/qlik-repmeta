import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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


# ------------------------------
# Type → Family mapper
# ------------------------------
# We map raw db_settings["$type"] into a "family" + role-specific table name.
# This keeps schema size manageable while still giving you per-endpoint tables.
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
    "DataprocSettings": "hadoop",  # goes with hadoop-like

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
    # Try with replicate_version
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

    # Fallback without replicate_version (older schema)
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

    # If no dedicated table → send to JSON fallback
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

    # Family-specific extraction
    def add(col: str, value: Any):
        cols.append(col)
        vals.append(value)

    r = role.upper()

    # Common candidates
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

    # Always include settings_json at the end
    cols.append("settings_json")
    vals.append(sjson)

    # Build upsert
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
        # Unknown → dump whole object
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
    """Return {endpoint_name -> {endpoint_id, role, ...}} for quick linking."""
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

    # Normalize targets to a Python list of strings (for text[] column)
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
    # source (if present)
    if source_name and source_name in endpoints_by_name:
        src = endpoints_by_name[source_name]
        await conn.execute(
            f"""INSERT INTO {SCHEMA}.rep_task_endpoint(task_id, role, endpoint_id, run_id)
                VALUES (%s,'SOURCE',%s,%s)
                ON CONFLICT DO NOTHING
            """,
            (task_id, src["endpoint_id"], run_id)
        )
    # targets
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
# Public API
# ------------------------------
async def ingest_repository(repo_json: Dict[str, Any], customer_name: str, server_name: str) -> Dict[str, Any]:
    """
    Ingest the uploaded repository JSON:
      - create ingest_run
      - flatten databases to rep_database + per-family detail tables (or JSON fallback)
      - flatten tasks, and link to endpoints by name
    """
    # Prefer the clean path-style lookup; keeps dotted key as harmless fallback
    cmd = _get(repo_json, "cmd", "replication_definition") or _get(repo_json, "replication_definition") or _get(repo_json, "cmd.replication_definition") or {}
    databases = cmd.get("databases") or []
    tasks = cmd.get("tasks") or []

    # Extract Replicate version (e.g., "2025.5.0.308") from top-level _version.version
    replicate_version = _extract_replicate_version(repo_json)
    LOG.info("[INGEST] Starting ingest: customer=%s server=%s dbs=%s tasks=%s replicate_version=%s",
             customer_name, server_name, len(databases), len(tasks), replicate_version or "(none)")

    async with connection() as conn:
        await _set_row_factory(conn)
        async with conn.transaction():
            customer_id = await _get_or_create_customer(conn, customer_name)
            server_id = await _get_or_create_server(conn, customer_id, server_name)

            # Create run (stores replicate_version when column exists)
            run_id = await _create_run(conn, customer_id, server_id, replicate_version)

            # --- Databases
            endpoint_ids: List[int] = []
            for db in databases:
                try:
                    endpoint_id = await _insert_rep_database(conn, run_id, customer_id, server_id, db)
                    endpoint_ids.append(endpoint_id)
                    settings = db.get("db_settings") or {}
                    role = (db.get("role") or "UNKNOWN").upper()
                    await _load_database_detail(conn, endpoint_id, role, settings)
                except Exception as e:
                    LOG.exception("Failed to insert endpoint %s (%s): %s", db.get("name"), db.get("role"), e)
                    raise

            # --- Tasks + endpoint links
            endpoints_by_name = await _index_endpoints_by_name(conn, run_id)
            task_ids: List[int] = []
            for obj in tasks:
                task_id = await _insert_task(conn, run_id, customer_id, server_id, obj)
                task_ids.append(task_id)
                t = obj.get("task") or {}
                source_name = t.get("source_name")
                target_names = t.get("target_names") or []
                await _link_task_endpoints(conn, run_id, task_id, endpoints_by_name, source_name, target_names)

            # Finish
            LOG.info("[INGEST] Completed run_id=%s endpoints=%s tasks=%s", run_id, len(endpoint_ids), len(task_ids))
            return {
                "run_id": run_id,
                "endpoints_inserted": len(endpoint_ids),
                "tasks_inserted": len(task_ids)
            }
