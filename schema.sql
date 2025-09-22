-- DROP SCHEMA repmeta;

CREATE SCHEMA repmeta AUTHORIZATION postgres;

-- DROP SEQUENCE repmeta.dim_customer_customer_id_seq;

CREATE SEQUENCE repmeta.dim_customer_customer_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.dim_server_server_id_seq;

CREATE SEQUENCE repmeta.dim_server_server_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.endpoint_endpoint_id_seq;

CREATE SEQUENCE repmeta.endpoint_endpoint_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.feature_flag_value_id_seq;

CREATE SEQUENCE repmeta.feature_flag_value_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.ingest_alert_alert_id_seq;

CREATE SEQUENCE repmeta.ingest_alert_alert_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.ingest_run_run_id_seq;

CREATE SEQUENCE repmeta.ingest_run_run_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_task_task_id_seq;

CREATE SEQUENCE repmeta.rep_task_task_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.source_table_id_seq;

CREATE SEQUENCE repmeta.source_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.task_id_seq;

CREATE SEQUENCE repmeta.task_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.task_target_id_seq;

CREATE SEQUENCE repmeta.task_target_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.unknown_field_unknown_id_seq;

CREATE SEQUENCE repmeta.unknown_field_unknown_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;-- repmeta.dim_customer definition

-- Drop table

-- DROP TABLE repmeta.dim_customer;

CREATE TABLE repmeta.dim_customer (
	customer_id bigserial NOT NULL,
	customer_name text NOT NULL,
	CONSTRAINT dim_customer_customer_name_key UNIQUE (customer_name),
	CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);


-- repmeta.dim_server definition

-- Drop table

-- DROP TABLE repmeta.dim_server;

CREATE TABLE repmeta.dim_server (
	server_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_name text NOT NULL,
	environment text DEFAULT 'prod'::text NOT NULL,
	CONSTRAINT dim_server_customer_id_server_name_key UNIQUE (customer_id, server_name),
	CONSTRAINT dim_server_pkey PRIMARY KEY (server_id),
	CONSTRAINT dim_server_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id)
);


-- repmeta.ingest_run definition

-- Drop table

-- DROP TABLE repmeta.ingest_run;

CREATE TABLE repmeta.ingest_run (
	run_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	filename text NOT NULL,
	loaded_at timestamptz DEFAULT now() NOT NULL,
	uploaded_by text NULL,
	CONSTRAINT ingest_run_pkey PRIMARY KEY (run_id),
	CONSTRAINT ingest_run_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id),
	CONSTRAINT ingest_run_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id)
);


-- repmeta.rep_task definition

-- Drop table

-- DROP TABLE repmeta.rep_task;

CREATE TABLE repmeta.rep_task (
	task_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	task_name text NOT NULL,
	source_name text NULL,
	task_type text NULL,
	task_uuid uuid NULL,
	description text NULL,
	target_names _text NULL,
	CONSTRAINT rep_task_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_run_id_task_name_key UNIQUE (run_id, task_name),
	CONSTRAINT rep_task_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id),
	CONSTRAINT rep_task_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id)
);


-- repmeta.rep_task_configuration definition

-- Drop table

-- DROP TABLE repmeta.rep_task_configuration;

CREATE TABLE repmeta.rep_task_configuration (
	task_id int8 NOT NULL,
	config_name text NOT NULL,
	CONSTRAINT rep_task_configuration_pkey PRIMARY KEY (task_id, config_name),
	CONSTRAINT rep_task_configuration_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_configuration_item definition

-- Drop table

-- DROP TABLE repmeta.rep_task_configuration_item;

CREATE TABLE repmeta.rep_task_configuration_item (
	task_id int8 NOT NULL,
	config_name text NOT NULL,
	"key" text NOT NULL,
	value text NULL,
	CONSTRAINT rep_task_configuration_item_pkey PRIMARY KEY (task_id, config_name, key),
	CONSTRAINT rep_task_configuration_item_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_feature_flag definition

-- Drop table

-- DROP TABLE repmeta.rep_task_feature_flag;

CREATE TABLE repmeta.rep_task_feature_flag (
	task_id int8 NOT NULL,
	"name" text NOT NULL,
	array_values _text NOT NULL,
	CONSTRAINT rep_task_feature_flag_pkey PRIMARY KEY (task_id, name),
	CONSTRAINT rep_task_feature_flag_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_logger definition

-- Drop table

-- DROP TABLE repmeta.rep_task_logger;

CREATE TABLE repmeta.rep_task_logger (
	task_id int8 NOT NULL,
	logger_name text NOT NULL,
	"level" text NOT NULL,
	CONSTRAINT rep_task_logger_pkey PRIMARY KEY (task_id, logger_name),
	CONSTRAINT rep_task_logger_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_pk_manip definition

-- Drop table

-- DROP TABLE repmeta.rep_task_pk_manip;

CREATE TABLE repmeta.rep_task_pk_manip (
	task_id int8 NOT NULL,
	table_name text NOT NULL,
	pk_origin text NULL,
	CONSTRAINT rep_task_pk_manip_pkey PRIMARY KEY (task_id, table_name),
	CONSTRAINT rep_task_pk_manip_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_pk_segment definition

-- Drop table

-- DROP TABLE repmeta.rep_task_pk_segment;

CREATE TABLE repmeta.rep_task_pk_segment (
	task_id int8 NOT NULL,
	table_name text NOT NULL,
	seg_name text NOT NULL,
	"position" int4 NULL,
	seg_id int4 NULL,
	CONSTRAINT rep_task_pk_segment_pkey PRIMARY KEY (task_id, table_name, seg_name),
	CONSTRAINT rep_task_pk_segment_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_change_table definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_change_table;

CREATE TABLE repmeta.rep_task_settings_change_table (
	task_id int8 NOT NULL,
	handle_ddl bool NULL,
	CONSTRAINT rep_task_settings_change_table_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_change_table_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_common definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_common;

CREATE TABLE repmeta.rep_task_settings_common (
	task_id int8 NOT NULL,
	full_load_enabled bool NULL,
	batch_apply_enabled bool NULL,
	write_full_logging bool NULL,
	status_table_name text NULL,
	suspended_tables_table_name text NULL,
	exception_table_name text NULL,
	CONSTRAINT rep_task_settings_common_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_common_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_target definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_target;

CREATE TABLE repmeta.rep_task_settings_target (
	task_id int8 NOT NULL,
	default_schema text NULL,
	control_schema text NULL,
	truncate_table_if_exists bool NULL,
	drop_table_if_exists bool NULL,
	handle_truncate_ddl bool NULL,
	handle_drop_ddl bool NULL,
	handle_column_ddl bool NULL,
	artifacts_cleanup_enabled bool NULL,
	CONSTRAINT rep_task_settings_target_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_target_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_target_queue definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_target_queue;

CREATE TABLE repmeta.rep_task_settings_target_queue (
	task_id int8 NOT NULL,
	use_custom_message bool NULL,
	use_custom_key bool NULL,
	header_fields_flag int4 NULL,
	include_before_data bool NULL,
	set_data_record_namespace bool NULL,
	data_record_namespace text NULL,
	data_record_container text NULL,
	data_prefix text NULL,
	include_headers bool NULL,
	set_headers_namespace bool NULL,
	headers_namespace text NULL,
	CONSTRAINT rep_task_settings_target_queue_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_target_queue_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_source_table definition

-- Drop table

-- DROP TABLE repmeta.rep_task_source_table;

CREATE TABLE repmeta.rep_task_source_table (
	task_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	estimated_size int8 NULL,
	orig_db_id int8 NULL,
	extra jsonb NULL,
	CONSTRAINT rep_task_source_table_pkey PRIMARY KEY (task_id, owner, table_name),
	CONSTRAINT rep_task_source_table_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_table_manip definition

-- Drop table

-- DROP TABLE repmeta.rep_task_table_manip;

CREATE TABLE repmeta.rep_task_table_manip (
	task_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	fl_passthru_filter text NULL,
	CONSTRAINT rep_task_table_manip_pkey PRIMARY KEY (task_id, owner, table_name),
	CONSTRAINT rep_task_table_manip_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_target definition

-- Drop table

-- DROP TABLE repmeta.rep_task_target;

CREATE TABLE repmeta.rep_task_target (
	task_id int8 NOT NULL,
	target_name text NOT NULL,
	target_state text NULL,
	database_name text NULL,
	CONSTRAINT rep_task_target_pkey PRIMARY KEY (task_id, target_name),
	CONSTRAINT rep_task_target_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.source_table definition

-- Drop table

-- DROP TABLE repmeta.source_table;

CREATE TABLE repmeta.source_table (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	estimated_size int8 NULL,
	orig_db_id int8 NULL,
	CONSTRAINT source_table_pkey PRIMARY KEY (id),
	CONSTRAINT source_table_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);


-- repmeta.task definition

-- Drop table

-- DROP TABLE repmeta.task;

CREATE TABLE repmeta.task (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	"name" text NOT NULL,
	source_name text NULL,
	task_type text NULL,
	description text NULL,
	status_table text NULL,
	suspended_tables_table text NULL,
	task_uuid text NULL,
	raw jsonb NOT NULL,
	CONSTRAINT task_pkey PRIMARY KEY (id),
	CONSTRAINT task_run_id_name_key UNIQUE (run_id, name),
	CONSTRAINT task_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);


-- repmeta.task_target definition

-- Drop table

-- DROP TABLE repmeta.task_target;

CREATE TABLE repmeta.task_target (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NOT NULL,
	target_name text NOT NULL,
	target_state text NULL,
	database_name text NULL,
	CONSTRAINT task_target_pkey PRIMARY KEY (id),
	CONSTRAINT task_target_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);


-- repmeta.unknown_field definition

-- Drop table

-- DROP TABLE repmeta.unknown_field;

CREATE TABLE repmeta.unknown_field (
	unknown_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	entity text NOT NULL,
	"path" text NOT NULL,
	"key" text NOT NULL,
	value jsonb NULL,
	seen_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT unknown_field_pkey PRIMARY KEY (unknown_id),
	CONSTRAINT unknown_field_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX ix_unknown_field_gin_value ON repmeta.unknown_field USING gin (value);
CREATE INDEX ix_unknown_field_run_entity ON repmeta.unknown_field USING btree (run_id, entity);


-- repmeta.endpoint definition

-- Drop table

-- DROP TABLE repmeta.endpoint;

CREATE TABLE repmeta.endpoint (
	endpoint_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	"role" text NOT NULL,
	type_id text NOT NULL,
	"name" text NULL,
	database_name text NULL,
	CONSTRAINT endpoint_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT endpoint_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id),
	CONSTRAINT endpoint_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT endpoint_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id)
);
CREATE UNIQUE INDEX uq_endpoint_run_role_type_name ON repmeta.endpoint USING btree (run_id, role, type_id, COALESCE(name, ''::text));


-- repmeta.endpoint_target_kafka definition

-- Drop table

-- DROP TABLE repmeta.endpoint_target_kafka;

CREATE TABLE repmeta.endpoint_target_kafka (
	endpoint_id int8 NOT NULL,
	brokers text NULL,
	security_mode text NULL,
	topic_prefix text NULL,
	extra jsonb NULL,
	CONSTRAINT endpoint_target_kafka_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT endpoint_target_kafka_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.endpoint(endpoint_id) ON DELETE CASCADE
);


-- repmeta.endpoint_target_postgresql definition

-- Drop table

-- DROP TABLE repmeta.endpoint_target_postgresql;

CREATE TABLE repmeta.endpoint_target_postgresql (
	endpoint_id int8 NOT NULL,
	host text NULL,
	port int4 NULL,
	db text NULL,
	username text NULL,
	sslmode text NULL,
	extra jsonb NULL,
	CONSTRAINT endpoint_target_postgresql_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT endpoint_target_postgresql_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.endpoint(endpoint_id) ON DELETE CASCADE
);


-- repmeta.feature_flag_value definition

-- Drop table

-- DROP TABLE repmeta.feature_flag_value;

CREATE TABLE repmeta.feature_flag_value (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NOT NULL,
	"name" text NOT NULL,
	value text NULL,
	CONSTRAINT feature_flag_value_pkey PRIMARY KEY (id),
	CONSTRAINT feature_flag_value_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);


-- repmeta.ingest_alert definition

-- Drop table

-- DROP TABLE repmeta.ingest_alert;

CREATE TABLE repmeta.ingest_alert (
	alert_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NULL,
	"path" text NOT NULL,
	severity text DEFAULT 'WARN'::text NOT NULL,
	details jsonb NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT ingest_alert_pkey PRIMARY KEY (alert_id),
	CONSTRAINT ingest_alert_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);


-- repmeta.v_unknown_counts source

CREATE OR REPLACE VIEW repmeta.v_unknown_counts
AS SELECT unknown_field.run_id,
    unknown_field.entity,
    count(*) AS unknown_key_count
   FROM repmeta.unknown_field
  GROUP BY unknown_field.run_id, unknown_field.entity;



-- DROP PROCEDURE repmeta.clean_run(int8);

CREATE OR REPLACE PROCEDURE repmeta.clean_run(IN p_run_id bigint)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
  -- Delete in dependency order; keep adding child tables here as we wire them
  DELETE FROM repmeta.task_config_item      WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_logger           WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_pk_segment       WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_transform_column WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_manipulation     WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_error_behavior   WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_source_table     WHERE run_id = p_run_id;
  DELETE FROM repmeta.task_target           WHERE run_id = p_run_id;
  DELETE FROM repmeta.task                  WHERE run_id = p_run_id;

  DELETE FROM repmeta.endpoint_target_postgresql WHERE endpoint_id IN
    (SELECT endpoint_id FROM repmeta.endpoint WHERE run_id = p_run_id);
  DELETE FROM repmeta.endpoint WHERE run_id = p_run_id;

  DELETE FROM repmeta.unknown_field WHERE run_id = p_run_id;

  DELETE FROM repmeta.ingest_run WHERE run_id = p_run_id;
END;
$procedure$
;

-- DROP FUNCTION repmeta.parse_server_name(text);

CREATE OR REPLACE FUNCTION repmeta.parse_server_name(desc_txt text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT NULLIF(
           regexp_replace(COALESCE($1,''), '.*Host name:\s*([A-Za-z0-9._-]+).*', '\1'),
           COALESCE($1,'')
         );
$function$
;