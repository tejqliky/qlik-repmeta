-- DROP SCHEMA repmeta;

CREATE SCHEMA repmeta AUTHORIZATION postgres;

-- DROP TYPE repmeta."talend_run_status";

CREATE TYPE repmeta."talend_run_status" AS ENUM (
	'pending',
	'running',
	'success',
	'failed',
	'timeout');

-- DROP SEQUENCE repmeta.customer_license_capabilities_license_id_seq;

CREATE SEQUENCE repmeta.customer_license_capabilities_license_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
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
-- DROP SEQUENCE repmeta.dim_talend_tenant_talend_tenant_key_seq;

CREATE SEQUENCE repmeta.dim_talend_tenant_talend_tenant_key_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.endpoint_alias_alias_id_seq;

CREATE SEQUENCE repmeta.endpoint_alias_alias_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.endpoint_alias_map_alias_id_seq;

CREATE SEQUENCE repmeta.endpoint_alias_map_alias_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.endpoint_catalog_endpoint_id_seq;

CREATE SEQUENCE repmeta.endpoint_catalog_endpoint_id_seq
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
-- DROP SEQUENCE repmeta.endpoint_family_family_id_seq;

CREATE SEQUENCE repmeta.endpoint_family_family_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.endpoint_family_map_map_id_seq;

CREATE SEQUENCE repmeta.endpoint_family_map_map_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.fact_talend_run_talend_run_id_seq;

CREATE SEQUENCE repmeta.fact_talend_run_talend_run_id_seq
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
-- DROP SEQUENCE repmeta.license_snapshot_item_license_item_id_seq;

CREATE SEQUENCE repmeta.license_snapshot_item_license_item_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.license_snapshot_license_id_seq;

CREATE SEQUENCE repmeta.license_snapshot_license_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.qem_batch_qem_batch_id_seq;

CREATE SEQUENCE repmeta.qem_batch_qem_batch_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.qem_ingest_run_qem_run_id_seq;

CREATE SEQUENCE repmeta.qem_ingest_run_qem_run_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.qem_server_map_map_id_seq;

CREATE SEQUENCE repmeta.qem_server_map_map_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.qem_task_perf_qem_perf_id_seq;

CREATE SEQUENCE repmeta.qem_task_perf_qem_perf_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_database_database_id_seq;

CREATE SEQUENCE repmeta.rep_database_database_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_database_endpoint_id_seq;

CREATE SEQUENCE repmeta.rep_database_endpoint_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_endpoint_server_endpoint_server_id_seq;

CREATE SEQUENCE repmeta.rep_endpoint_server_endpoint_server_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_metrics_event_event_id_seq;

CREATE SEQUENCE repmeta.rep_metrics_event_event_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_metrics_run_metrics_run_id_seq;

CREATE SEQUENCE repmeta.rep_metrics_run_metrics_run_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_scheduler_job_job_id_seq;

CREATE SEQUENCE repmeta.rep_scheduler_job_job_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_task_source_table_id_seq;

CREATE SEQUENCE repmeta.rep_task_source_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_task_table_task_table_id_seq;

CREATE SEQUENCE repmeta.rep_task_table_task_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.rep_task_target_id_seq;

CREATE SEQUENCE repmeta.rep_task_target_id_seq
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
-- DROP SEQUENCE repmeta.replicate_latest_release_cache_cache_id_seq;

CREATE SEQUENCE repmeta.replicate_latest_release_cache_cache_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE repmeta.replicate_release_issue_id_seq;

CREATE SEQUENCE repmeta.replicate_release_issue_id_seq
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
	CONSTRAINT endpoint_pkey PRIMARY KEY (endpoint_id)
);
CREATE UNIQUE INDEX uq_endpoint_run_role_type_name ON repmeta.endpoint USING btree (run_id, role, type_id, COALESCE(name, ''::text));


-- repmeta.endpoint_catalog definition

-- Drop table

-- DROP TABLE repmeta.endpoint_catalog;

CREATE TABLE repmeta.endpoint_catalog (
	endpoint_id bigserial NOT NULL,
	"name" text NOT NULL,
	category text NOT NULL,
	direction text NOT NULL,
	icon_key text NULL,
	CONSTRAINT endpoint_catalog_direction_check CHECK ((direction = ANY (ARRAY['source'::text, 'target'::text, 'both'::text]))),
	CONSTRAINT endpoint_catalog_name_key UNIQUE (name),
	CONSTRAINT endpoint_catalog_pkey PRIMARY KEY (endpoint_id)
);


-- repmeta.endpoint_family definition

-- Drop table

-- DROP TABLE repmeta.endpoint_family;

CREATE TABLE repmeta.endpoint_family (
	family_id bigserial NOT NULL,
	"role" text NOT NULL,
	family_name text NOT NULL,
	vendor_hint text NULL,
	CONSTRAINT endpoint_family_pkey PRIMARY KEY (family_id),
	CONSTRAINT endpoint_family_role_check CHECK ((role = ANY (ARRAY['SOURCE'::text, 'TARGET'::text]))),
	CONSTRAINT endpoint_family_role_family_name_key UNIQUE (role, family_name)
);


-- repmeta.endpoint_family_map definition

-- Drop table

-- DROP TABLE repmeta.endpoint_family_map;

CREATE TABLE repmeta.endpoint_family_map (
	map_id bigserial NOT NULL,
	pattern text NOT NULL,
	"role" text NULL,
	"family" text NOT NULL,
	priority int4 DEFAULT 100 NOT NULL,
	active bool DEFAULT true NOT NULL,
	notes text NULL,
	CONSTRAINT endpoint_family_map_pkey PRIMARY KEY (map_id),
	CONSTRAINT uniq_endpoint_family_map_pattern_role UNIQUE (pattern, role)
);
CREATE INDEX idx_endpoint_family_map_active ON repmeta.endpoint_family_map USING btree (active, priority);


-- repmeta.endpoint_master_sources definition

-- Drop table

-- DROP TABLE repmeta.endpoint_master_sources;

CREATE TABLE repmeta.endpoint_master_sources (
	"name" text NOT NULL,
	CONSTRAINT endpoint_master_sources_pkey PRIMARY KEY (name)
);


-- repmeta.endpoint_master_targets definition

-- Drop table

-- DROP TABLE repmeta.endpoint_master_targets;

CREATE TABLE repmeta.endpoint_master_targets (
	"name" text NOT NULL,
	CONSTRAINT endpoint_master_targets_pkey PRIMARY KEY (name)
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
	CONSTRAINT ingest_alert_pkey PRIMARY KEY (alert_id)
);


-- repmeta.rep_metrics_run definition

-- Drop table

-- DROP TABLE repmeta.rep_metrics_run;

CREATE TABLE repmeta.rep_metrics_run (
	metrics_run_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	file_name text NOT NULL,
	collected_at timestamptz NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT rep_metrics_run_pkey PRIMARY KEY (metrics_run_id)
);
CREATE INDEX idx_metrics_run_customer_server_created ON repmeta.rep_metrics_run USING btree (customer_id, server_id, created_at DESC);
CREATE INDEX ix_metrics_run_cust_srv_created ON repmeta.rep_metrics_run USING btree (customer_id, server_id, created_at DESC, metrics_run_id DESC);
CREATE INDEX rep_metrics_run_cust_srv ON repmeta.rep_metrics_run USING btree (customer_id, server_id);


-- repmeta.replicate_latest_release_cache definition

-- Drop table

-- DROP TABLE repmeta.replicate_latest_release_cache;

CREATE TABLE repmeta.replicate_latest_release_cache (
	cache_id bigserial NOT NULL,
	tag text NOT NULL,
	"year" int4 NOT NULL,
	month_code int4 NOT NULL,
	sr int4 NOT NULL,
	fetched_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT replicate_latest_release_cache_pkey PRIMARY KEY (cache_id)
);
CREATE INDEX ix_replicate_latest_release_cache_fetched_at ON repmeta.replicate_latest_release_cache USING btree (fetched_at DESC);


-- repmeta.replicate_release_issue definition

-- Drop table

-- DROP TABLE repmeta.replicate_release_issue;

CREATE TABLE repmeta.replicate_release_issue (
	id bigserial NOT NULL,
	"version" text NULL,
	issue_date date NULL,
	title text NULL,
	url text NULL,
	jira text NULL,
	endpoints _text NULL,
	buckets _text NULL,
	"text" text NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT replicate_release_issue_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_rep_issues_created_at ON repmeta.replicate_release_issue USING btree (created_at DESC);
CREATE INDEX idx_rep_issues_endpoints ON repmeta.replicate_release_issue USING gin (endpoints);
CREATE UNIQUE INDEX uniq_rep_issue_vut ON repmeta.replicate_release_issue USING btree (version, url, text);


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
	CONSTRAINT unknown_field_pkey PRIMARY KEY (unknown_id)
);
CREATE INDEX ix_unknown_field_gin_value ON repmeta.unknown_field USING gin (value);
CREATE INDEX ix_unknown_field_run_entity ON repmeta.unknown_field USING btree (run_id, entity);


-- repmeta.customer_license_capabilities definition

-- Drop table

-- DROP TABLE repmeta.customer_license_capabilities;

CREATE TABLE repmeta.customer_license_capabilities (
	license_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	licensed_all_sources bool DEFAULT false NOT NULL,
	licensed_all_targets bool DEFAULT false NOT NULL,
	licensed_sources _text DEFAULT '{}'::text[] NOT NULL,
	licensed_targets _text DEFAULT '{}'::text[] NOT NULL,
	raw_line text NOT NULL,
	parsed_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT customer_license_capabilities_pkey PRIMARY KEY (license_id),
	CONSTRAINT customer_license_capabilities_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id)
);
CREATE INDEX ix_cust_license_latest ON repmeta.customer_license_capabilities USING btree (customer_id, parsed_at DESC);
CREATE INDEX ix_customer_license_latest ON repmeta.customer_license_capabilities USING btree (customer_id, parsed_at DESC);


-- repmeta.dim_server definition

-- Drop table

-- DROP TABLE repmeta.dim_server;

CREATE TABLE repmeta.dim_server (
	server_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_name text NOT NULL,
	environment text DEFAULT 'prod'::text NULL,
	CONSTRAINT dim_server_customer_id_server_name_key UNIQUE (customer_id, server_name),
	CONSTRAINT dim_server_pkey PRIMARY KEY (server_id),
	CONSTRAINT dim_server_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE
);


-- repmeta.dim_talend_tenant definition

-- Drop table

-- DROP TABLE repmeta.dim_talend_tenant;

CREATE TABLE repmeta.dim_talend_tenant (
	talend_tenant_key serial4 NOT NULL,
	customer_key int4 NOT NULL,
	tenant_display_name text NOT NULL,
	talend_account_id text NOT NULL,
	talend_tenant_id text NOT NULL,
	environment text NULL,
	notes text NULL,
	created_at timestamptz DEFAULT now() NULL,
	updated_at timestamptz DEFAULT now() NULL,
	"source" text DEFAULT 'manual'::text NULL,
	is_configured bool DEFAULT false NOT NULL,
	CONSTRAINT dim_talend_tenant_customer_key_talend_tenant_id_key UNIQUE (customer_key, talend_tenant_id),
	CONSTRAINT dim_talend_tenant_pkey PRIMARY KEY (talend_tenant_key),
	CONSTRAINT dim_talend_tenant_customer_key_fkey FOREIGN KEY (customer_key) REFERENCES repmeta.dim_customer(customer_id)
);


-- repmeta.endpoint_alias definition

-- Drop table

-- DROP TABLE repmeta.endpoint_alias;

CREATE TABLE repmeta.endpoint_alias (
	alias_id bigserial NOT NULL,
	endpoint_id int8 NOT NULL,
	alias text NOT NULL,
	CONSTRAINT endpoint_alias_pkey PRIMARY KEY (alias_id),
	CONSTRAINT endpoint_alias_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.endpoint_catalog(endpoint_id) ON DELETE CASCADE
);
CREATE INDEX ix_endpoint_alias_alias ON repmeta.endpoint_alias USING btree (lower(alias));


-- repmeta.endpoint_alias_map definition

-- Drop table

-- DROP TABLE repmeta.endpoint_alias_map;

CREATE TABLE repmeta.endpoint_alias_map (
	alias_id bigserial NOT NULL,
	"role" text NOT NULL,
	alias_type text NOT NULL,
	alias_value text NOT NULL,
	family_id int8 NOT NULL,
	active bool DEFAULT true NOT NULL,
	CONSTRAINT endpoint_alias_map_alias_type_check CHECK ((alias_type = ANY (ARRAY['license_ticker'::text, 'component_type'::text, 'label'::text]))),
	CONSTRAINT endpoint_alias_map_pkey PRIMARY KEY (alias_id),
	CONSTRAINT endpoint_alias_map_role_alias_type_alias_value_key UNIQUE (role, alias_type, alias_value),
	CONSTRAINT endpoint_alias_map_role_check CHECK ((role = ANY (ARRAY['SOURCE'::text, 'TARGET'::text]))),
	CONSTRAINT endpoint_alias_map_family_id_fkey FOREIGN KEY (family_id) REFERENCES repmeta.endpoint_family(family_id) ON DELETE CASCADE
);
CREATE INDEX ix_alias_map_ct ON repmeta.endpoint_alias_map USING btree (alias_type, role, alias_value) WHERE active;


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


-- repmeta.fact_talend_run definition

-- Drop table

-- DROP TABLE repmeta.fact_talend_run;

CREATE TABLE repmeta.fact_talend_run (
	talend_run_id bigserial NOT NULL,
	talend_tenant_key int4 NOT NULL,
	started_at timestamptz DEFAULT now() NULL,
	finished_at timestamptz NULL,
	status repmeta."talend_run_status" DEFAULT 'pending'::repmeta.talend_run_status NOT NULL,
	qtcmt_path text NOT NULL,
	cseat_paths jsonb NOT NULL,
	tmp_folder text NOT NULL,
	database_path text NOT NULL,
	artifact_name text NOT NULL,
	config_path text NOT NULL,
	timeout_minutes int4 NOT NULL,
	return_code int4 NULL,
	stdout_text text NULL,
	stderr_text text NULL,
	error_message text NULL,
	CONSTRAINT fact_talend_run_pkey PRIMARY KEY (talend_run_id),
	CONSTRAINT fact_talend_run_talend_tenant_key_fkey FOREIGN KEY (talend_tenant_key) REFERENCES repmeta.dim_talend_tenant(talend_tenant_key)
);


-- repmeta.ingest_run definition

-- Drop table

-- DROP TABLE repmeta.ingest_run;

CREATE TABLE repmeta.ingest_run (
	run_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	filename text NULL,
	uploaded_by text NULL,
	created_at timestamptz DEFAULT now() NULL,
	replicate_version text NULL,
	CONSTRAINT ingest_run_pkey PRIMARY KEY (run_id),
	CONSTRAINT ingest_run_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE,
	CONSTRAINT ingest_run_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id) ON DELETE CASCADE
);


-- repmeta.license_snapshot definition

-- Drop table

-- DROP TABLE repmeta.license_snapshot;

CREATE TABLE repmeta.license_snapshot (
	license_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	extracted_at timestamptz DEFAULT now() NOT NULL,
	src_file_name text NULL,
	all_sources bool DEFAULT false NOT NULL,
	all_targets bool DEFAULT false NOT NULL,
	raw_excerpt text NULL,
	CONSTRAINT license_snapshot_pkey PRIMARY KEY (license_id),
	CONSTRAINT license_snapshot_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE
);


-- repmeta.license_snapshot_item definition

-- Drop table

-- DROP TABLE repmeta.license_snapshot_item;

CREATE TABLE repmeta.license_snapshot_item (
	license_item_id bigserial NOT NULL,
	license_id int8 NOT NULL,
	"role" text NOT NULL,
	alias_value text NOT NULL,
	family_id int8 NULL,
	CONSTRAINT license_snapshot_item_pkey PRIMARY KEY (license_item_id),
	CONSTRAINT license_snapshot_item_role_check CHECK ((role = ANY (ARRAY['SOURCE'::text, 'TARGET'::text]))),
	CONSTRAINT license_snapshot_item_family_id_fkey FOREIGN KEY (family_id) REFERENCES repmeta.endpoint_family(family_id) ON DELETE SET NULL,
	CONSTRAINT license_snapshot_item_license_id_fkey FOREIGN KEY (license_id) REFERENCES repmeta.license_snapshot(license_id) ON DELETE CASCADE
);


-- repmeta.qem_batch definition

-- Drop table

-- DROP TABLE repmeta.qem_batch;

CREATE TABLE repmeta.qem_batch (
	qem_batch_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	file_name text NULL,
	collected_at timestamp NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT qem_batch_pkey PRIMARY KEY (qem_batch_id),
	CONSTRAINT qem_batch_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id)
);


-- repmeta.qem_ingest_run definition

-- Drop table

-- DROP TABLE repmeta.qem_ingest_run;

CREATE TABLE repmeta.qem_ingest_run (
	qem_run_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	file_name text NULL,
	collected_at timestamp NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	qem_batch_id int8 NULL,
	CONSTRAINT qem_ingest_run_pkey PRIMARY KEY (qem_run_id),
	CONSTRAINT qem_ingest_run_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id),
	CONSTRAINT qem_ingest_run_qem_batch_id_fkey FOREIGN KEY (qem_batch_id) REFERENCES repmeta.qem_batch(qem_batch_id),
	CONSTRAINT qem_ingest_run_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id)
);
CREATE INDEX ix_qem_ingest_run_batch ON repmeta.qem_ingest_run USING btree (qem_batch_id);
CREATE INDEX ix_qem_run_created ON repmeta.qem_ingest_run USING btree (customer_id, server_id, created_at DESC);


-- repmeta.qem_server_map definition

-- Drop table

-- DROP TABLE repmeta.qem_server_map;

CREATE TABLE repmeta.qem_server_map (
	map_id bigserial NOT NULL,
	customer_id int8 NOT NULL,
	"name" text NOT NULL,
	host text NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT qem_server_map_customer_id_name_key UNIQUE (customer_id, name),
	CONSTRAINT qem_server_map_pkey PRIMARY KEY (map_id),
	CONSTRAINT qem_server_map_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE
);
CREATE INDEX qem_server_map_cust_host_ci ON repmeta.qem_server_map USING btree (customer_id, lower(host));
CREATE INDEX qem_server_map_cust_name_ci ON repmeta.qem_server_map USING btree (customer_id, lower(name));


-- repmeta.qem_task_perf definition

-- Drop table

-- DROP TABLE repmeta.qem_task_perf;

CREATE TABLE repmeta.qem_task_perf (
	qem_perf_id bigserial NOT NULL,
	qem_run_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	task_name text NOT NULL,
	task_id int8 NULL,
	state text NULL,
	stage text NULL,
	server_type text NULL,
	source_name text NULL,
	source_type text NULL,
	target_name text NULL,
	target_type text NULL,
	tables_with_error int4 NULL,
	memory_kb int8 NULL,
	disk_usage_kb int8 NULL,
	cpu_pct numeric(7, 3) NULL,
	fl_progress_pct numeric(7, 3) NULL,
	fl_load_duration interval NULL,
	fl_total_tables int4 NULL,
	fl_total_records int8 NULL,
	fl_target_throughput_rec_sec numeric(18, 3) NULL,
	cdc_incoming_changes int8 NULL,
	cdc_inserts int8 NULL,
	cdc_updates int8 NULL,
	cdc_deletes int8 NULL,
	cdc_applied_changes int8 NULL,
	cdc_commit_change_records int8 NULL,
	cdc_commit_change_volume int8 NULL,
	cdc_apply_throughput_rec_sec numeric(18, 3) NULL,
	cdc_source_latency interval NULL,
	cdc_apply_latency interval NULL,
	raw jsonb NULL,
	CONSTRAINT qem_task_perf_pkey PRIMARY KEY (qem_perf_id),
	CONSTRAINT qem_task_perf_qem_run_id_fkey FOREIGN KEY (qem_run_id) REFERENCES repmeta.qem_ingest_run(qem_run_id) ON DELETE CASCADE
);
CREATE INDEX ix_qem_task_perf_lookup ON repmeta.qem_task_perf USING btree (customer_id, server_id, lower(task_name));
CREATE UNIQUE INDEX uq_qem_task_perf_run_task ON repmeta.qem_task_perf USING btree (qem_run_id, task_name);


-- repmeta.rep_database definition

-- Drop table

-- DROP TABLE repmeta.rep_database;

CREATE TABLE repmeta.rep_database (
	database_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	server_id int8 NOT NULL,
	"name" text NOT NULL,
	"role" text NULL,
	type_id text NULL,
	is_licensed bool NULL,
	db_settings jsonb NULL,
	override_properties jsonb NULL,
	endpoint_id bigserial NOT NULL,
	db_settings_type text NULL,
	CONSTRAINT rep_database_endpoint_id_key UNIQUE (endpoint_id),
	CONSTRAINT rep_database_pkey PRIMARY KEY (database_id),
	CONSTRAINT rep_database_run_id_name_key UNIQUE (run_id, name),
	CONSTRAINT rep_database_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE,
	CONSTRAINT rep_database_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_database_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id) ON DELETE CASCADE
);
CREATE INDEX ix_rep_database_role_type ON repmeta.rep_database USING btree (role, type_id, customer_id);
CREATE INDEX rep_database_run_id_name_idx ON repmeta.rep_database USING btree (run_id, name);
CREATE UNIQUE INDEX rep_database_run_name_uidx ON repmeta.rep_database USING btree (run_id, name);


-- repmeta.rep_db_amazon_msk_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_amazon_msk_target;

CREATE TABLE repmeta.rep_db_amazon_msk_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	brokers text NULL,
	topic text NULL,
	partition_mapping text NULL,
	message_key text NULL,
	"compression" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_amazon_msk_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_amazon_msk_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_azure_adls_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_azure_adls_target;

CREATE TABLE repmeta.rep_db_azure_adls_target (
	endpoint_id int8 NOT NULL,
	storage_account text NULL,
	file_system text NULL,
	adls_folder text NULL,
	tenant_id text NULL,
	client_app_id text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_azure_adls_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_azure_adls_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_azure_synapse_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_azure_synapse_target;

CREATE TABLE repmeta.rep_db_azure_synapse_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	az_account_name text NULL,
	az_container_name text NULL,
	files_in_batch int4 NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_azure_synapse_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_azure_synapse_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_bigquery_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_bigquery_target;

CREATE TABLE repmeta.rep_db_bigquery_target (
	endpoint_id int8 NOT NULL,
	json_credentials text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_bigquery_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_bigquery_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_confluent_cloud_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_confluent_cloud_target;

CREATE TABLE repmeta.rep_db_confluent_cloud_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	brokers text NULL,
	topic text NULL,
	"compression" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_confluent_cloud_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_confluent_cloud_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_databricks_cloud_storage_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_databricks_cloud_storage_target;

CREATE TABLE repmeta.rep_db_databricks_cloud_storage_target (
	endpoint_id int8 NOT NULL,
	"server" text NULL,
	"database" text NULL,
	http_path text NULL,
	warehouse_type text NULL,
	s3_bucket_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_databricks_cloud_storage_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_databricks_cloud_storage_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_databricks_delta_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_databricks_delta_target;

CREATE TABLE repmeta.rep_db_databricks_delta_target (
	endpoint_id int8 NOT NULL,
	"server" text NULL,
	"database" text NULL,
	http_path text NULL,
	staging_dir text NULL,
	s3_bucket text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_databricks_delta_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_databricks_delta_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_db2_iseries_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_db2_iseries_source;

CREATE TABLE repmeta.rep_db_db2_iseries_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	database_alias text NULL,
	journal_library text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_db2_iseries_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_db2_iseries_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_db2_luw_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_db2_luw_source;

CREATE TABLE repmeta.rep_db_db2_luw_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	database_alias text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_db2_luw_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_db2_luw_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_db2_zos_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_db2_zos_source;

CREATE TABLE repmeta.rep_db_db2_zos_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	database_alias text NULL,
	ifi306_sp_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_db2_zos_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_db2_zos_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_db2_zos_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_db2_zos_target;

CREATE TABLE repmeta.rep_db_db2_zos_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	database_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_db2_zos_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_db2_zos_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_eventhubs_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_eventhubs_target;

CREATE TABLE repmeta.rep_db_eventhubs_target (
	endpoint_id int8 NOT NULL,
	"namespace" text NULL,
	topic text NULL,
	partition_mapping text NULL,
	message_format text NULL,
	publish_option text NULL,
	policy_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_eventhubs_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_eventhubs_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_file_channel_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_file_channel_target;

CREATE TABLE repmeta.rep_db_file_channel_target (
	endpoint_id int8 NOT NULL,
	"path" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_file_channel_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_file_channel_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_file_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_file_source;

CREATE TABLE repmeta.rep_db_file_source (
	endpoint_id int8 NOT NULL,
	csv_string_escape text NULL,
	quote_empty_string bool NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_file_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_file_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_file_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_file_target;

CREATE TABLE repmeta.rep_db_file_target (
	endpoint_id int8 NOT NULL,
	csv_string_escape text NULL,
	quote_empty_string bool NULL,
	data_path text NULL,
	"path" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_file_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_file_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_gcs_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_gcs_target;

CREATE TABLE repmeta.rep_db_gcs_target (
	endpoint_id int8 NOT NULL,
	bucket_name text NULL,
	bucket_folder text NULL,
	json_credentials text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_gcs_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_gcs_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_generic definition

-- Drop table

-- DROP TABLE repmeta.rep_db_generic;

CREATE TABLE repmeta.rep_db_generic (
	endpoint_id int8 NOT NULL,
	db_settings jsonb NULL,
	CONSTRAINT rep_db_generic_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_generic_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_db_generic_endpoint_id ON repmeta.rep_db_generic USING btree (endpoint_id);


-- repmeta.rep_db_hadoop_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_hadoop_target;

CREATE TABLE repmeta.rep_db_hadoop_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	webhdfs_host text NULL,
	hdfs_path text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_hadoop_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_hadoop_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_hana_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_hana_target;

CREATE TABLE repmeta.rep_db_hana_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_hana_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_hana_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_hdinsight_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_hdinsight_target;

CREATE TABLE repmeta.rep_db_hdinsight_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	hdfs_path text NULL,
	hive_odbc_host text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_hdinsight_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_hdinsight_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_ims_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_ims_source;

CREATE TABLE repmeta.rep_db_ims_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	source_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_ims_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_ims_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_informix_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_informix_source;

CREATE TABLE repmeta.rep_db_informix_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_informix_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_informix_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_kafka_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_kafka_target;

CREATE TABLE repmeta.rep_db_kafka_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	brokers text NULL,
	topic text NULL,
	"compression" text NULL,
	auth_type text NULL,
	message_format text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_kafka_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_kafka_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_logstream_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_logstream_target;

CREATE TABLE repmeta.rep_db_logstream_target (
	endpoint_id int8 NOT NULL,
	"path" text NULL,
	compression_level int4 NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_logstream_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_logstream_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_ms_fabric_dw_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_ms_fabric_dw_target;

CREATE TABLE repmeta.rep_db_ms_fabric_dw_target (
	endpoint_id int8 NOT NULL,
	"server" text NULL,
	"database" text NULL,
	tenant_id text NULL,
	client_id text NULL,
	storage_account text NULL,
	container text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_ms_fabric_dw_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_ms_fabric_dw_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_mysql_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_mysql_source;

CREATE TABLE repmeta.rep_db_mysql_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	ssl_root_cert text NULL,
	ssl_client_key text NULL,
	ssl_client_cert text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_mysql_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_mysql_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_mysql_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_mysql_target;

CREATE TABLE repmeta.rep_db_mysql_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	ssl_root_cert text NULL,
	ssl_client_key text NULL,
	ssl_client_cert text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_mysql_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_mysql_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_netezza_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_netezza_target;

CREATE TABLE repmeta.rep_db_netezza_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_netezza_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_netezza_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_odbc_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_odbc_source;

CREATE TABLE repmeta.rep_db_odbc_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	additional_connection_properties text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_odbc_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_odbc_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_odbc_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_odbc_target;

CREATE TABLE repmeta.rep_db_odbc_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	additional_connection_properties text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_odbc_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_odbc_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_oracle_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_oracle_source;

CREATE TABLE repmeta.rep_db_oracle_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	use_logminer bool NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_oracle_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_oracle_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_oracle_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_oracle_target;

CREATE TABLE repmeta.rep_db_oracle_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_oracle_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_oracle_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_pg_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_pg_source;

CREATE TABLE repmeta.rep_db_pg_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"password" text NULL,
	"server" text NULL,
	port int4 NULL,
	"database" text NULL,
	heartbeat_enable bool NULL,
	support_cdc_partitioned_tables bool NULL,
	ssh_tunnel_type text NULL,
	CONSTRAINT rep_db_pg_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_pg_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_db_pg_source_endpoint_id ON repmeta.rep_db_pg_source USING btree (endpoint_id);


-- repmeta.rep_db_postgresql_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_postgresql_source;

CREATE TABLE repmeta.rep_db_postgresql_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	host text NULL,
	port int4 NULL,
	"database" text NULL,
	heartbeat bool NULL,
	ssl_cert text NULL,
	ssl_key text NULL,
	ssl_root_cert text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_postgresql_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_postgresql_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_postgresql_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_postgresql_target;

CREATE TABLE repmeta.rep_db_postgresql_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	host text NULL,
	"database" text NULL,
	ssl_cert text NULL,
	ssl_key text NULL,
	ssl_root_cert text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_postgresql_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_postgresql_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_pubsub_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_pubsub_target;

CREATE TABLE repmeta.rep_db_pubsub_target (
	endpoint_id int8 NOT NULL,
	topic text NULL,
	project_id text NULL,
	region text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_pubsub_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_pubsub_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_redshift_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_redshift_target;

CREATE TABLE repmeta.rep_db_redshift_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	s3_bucket text NULL,
	s3_region text NULL,
	files_in_batch int4 NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_redshift_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_redshift_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_s3_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_s3_target;

CREATE TABLE repmeta.rep_db_s3_target (
	endpoint_id int8 NOT NULL,
	bucket_name text NULL,
	bucket_folder text NULL,
	s3_region text NULL,
	encryption_mode text NULL,
	access_type text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_s3_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_s3_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_settings_json definition

-- Drop table

-- DROP TABLE repmeta.rep_db_settings_json;

CREATE TABLE repmeta.rep_db_settings_json (
	endpoint_id int8 NOT NULL,
	"role" text NOT NULL,
	type_label text NOT NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_settings_json_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_settings_json_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_snowflake_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_snowflake_target;

CREATE TABLE repmeta.rep_db_snowflake_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	oauth_type text NULL,
	private_key_file text NULL,
	staging_type text NULL,
	files_in_batch int4 NULL,
	settings_json jsonb DEFAULT '{}'::jsonb NOT NULL,
	CONSTRAINT rep_db_snowflake_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_snowflake_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_db_snowflake_target_endpoint_id ON repmeta.rep_db_snowflake_target USING btree (endpoint_id);


-- repmeta.rep_db_sqlserver_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_sqlserver_source;

CREATE TABLE repmeta.rep_db_sqlserver_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"password" text NULL,
	"server" text NULL,
	"database" text NULL,
	safeguard_policy text NULL,
	suspend_table_with_computed_column bool NULL,
	custom_header_for_trx_id bool NULL,
	custom_header_for_ts bool NULL,
	trx_id_like_db_trx_id bool NULL,
	ssh_tunnel_type text NULL,
	suspend_computed bool NULL,
	settings_json jsonb NULL,
	CONSTRAINT rep_db_sqlserver_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_sqlserver_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_db_sqlserver_source_endpoint_id ON repmeta.rep_db_sqlserver_source USING btree (endpoint_id);


-- repmeta.rep_db_sqlserver_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_sqlserver_target;

CREATE TABLE repmeta.rep_db_sqlserver_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	safeguard_policy text NULL,
	suspend_computed bool NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_sqlserver_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_sqlserver_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_teradata_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_teradata_source;

CREATE TABLE repmeta.rep_db_teradata_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_teradata_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_teradata_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_teradata_target definition

-- Drop table

-- DROP TABLE repmeta.rep_db_teradata_target;

CREATE TABLE repmeta.rep_db_teradata_target (
	endpoint_id int8 NOT NULL,
	username text NULL,
	"server" text NULL,
	"database" text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_teradata_target_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_teradata_target_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_db_vsam_source definition

-- Drop table

-- DROP TABLE repmeta.rep_db_vsam_source;

CREATE TABLE repmeta.rep_db_vsam_source (
	endpoint_id int8 NOT NULL,
	username text NULL,
	source_name text NULL,
	settings_json jsonb NOT NULL,
	CONSTRAINT rep_db_vsam_source_pkey PRIMARY KEY (endpoint_id),
	CONSTRAINT rep_db_vsam_source_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE
);


-- repmeta.rep_disk_utilization definition

-- Drop table

-- DROP TABLE repmeta.rep_disk_utilization;

CREATE TABLE repmeta.rep_disk_utilization (
	run_id int8 NOT NULL,
	config jsonb NULL,
	CONSTRAINT rep_disk_utilization_pkey PRIMARY KEY (run_id),
	CONSTRAINT rep_disk_utilization_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_disk_utilization_run_id_idx ON repmeta.rep_disk_utilization USING btree (run_id);


-- repmeta.rep_endpoint_server definition

-- Drop table

-- DROP TABLE repmeta.rep_endpoint_server;

CREATE TABLE repmeta.rep_endpoint_server (
	endpoint_server_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	"name" text NULL,
	host text NULL,
	port int4 NULL,
	is_local bool NULL,
	is_secured bool NULL,
	description text NULL,
	local_data_folder text NULL,
	CONSTRAINT rep_endpoint_server_pkey PRIMARY KEY (endpoint_server_id),
	CONSTRAINT rep_endpoint_server_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_endpoint_server_run_id_idx ON repmeta.rep_endpoint_server USING btree (run_id);


-- repmeta.rep_environment definition

-- Drop table

-- DROP TABLE repmeta.rep_environment;

CREATE TABLE repmeta.rep_environment (
	run_id int8 NOT NULL,
	"name" text NULL,
	mail_settings jsonb NULL,
	task_user_context jsonb NULL,
	run_command_settings jsonb NULL,
	proxy_settings jsonb NULL,
	CONSTRAINT rep_environment_pkey PRIMARY KEY (run_id),
	CONSTRAINT rep_environment_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_environment_run_id_idx ON repmeta.rep_environment USING btree (run_id);


-- repmeta.rep_error_behavior definition

-- Drop table

-- DROP TABLE repmeta.rep_error_behavior;

CREATE TABLE repmeta.rep_error_behavior (
	run_id int8 NOT NULL,
	apply_error_behavior jsonb NULL,
	table_error_behavior jsonb NULL,
	data_error_behavior jsonb NULL,
	recoverable_error_behavior jsonb NULL,
	CONSTRAINT rep_error_behavior_pkey PRIMARY KEY (run_id),
	CONSTRAINT rep_error_behavior_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_error_behavior_run_id_idx ON repmeta.rep_error_behavior USING btree (run_id);


-- repmeta.rep_memory_utilization definition

-- Drop table

-- DROP TABLE repmeta.rep_memory_utilization;

CREATE TABLE repmeta.rep_memory_utilization (
	run_id int8 NOT NULL,
	config jsonb NULL,
	CONSTRAINT rep_memory_utilization_pkey PRIMARY KEY (run_id),
	CONSTRAINT rep_memory_utilization_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_memory_utilization_run_id_idx ON repmeta.rep_memory_utilization USING btree (run_id);


-- repmeta.rep_metrics_pair_total definition

-- Drop table

-- DROP TABLE repmeta.rep_metrics_pair_total;

CREATE TABLE repmeta.rep_metrics_pair_total (
	metrics_run_id int8 NOT NULL,
	source_family_id int4 NOT NULL,
	target_family_id int4 NOT NULL,
	load_rows_total int8 DEFAULT 0 NOT NULL,
	load_bytes_total int8 DEFAULT 0 NOT NULL,
	cdc_rows_total int8 DEFAULT 0 NOT NULL,
	cdc_bytes_total int8 DEFAULT 0 NOT NULL,
	events_count int8 DEFAULT 0 NOT NULL,
	first_ts timestamptz NULL,
	last_ts timestamptz NULL,
	CONSTRAINT rep_metrics_pair_total_pkey PRIMARY KEY (metrics_run_id, source_family_id, target_family_id),
	CONSTRAINT rep_metrics_pair_total_metrics_run_id_fkey FOREIGN KEY (metrics_run_id) REFERENCES repmeta.rep_metrics_run(metrics_run_id) ON DELETE CASCADE,
	CONSTRAINT rep_metrics_pair_total_source_family_id_fkey FOREIGN KEY (source_family_id) REFERENCES repmeta.endpoint_family(family_id),
	CONSTRAINT rep_metrics_pair_total_target_family_id_fkey FOREIGN KEY (target_family_id) REFERENCES repmeta.endpoint_family(family_id)
);


-- repmeta.rep_notifications definition

-- Drop table

-- DROP TABLE repmeta.rep_notifications;

CREATE TABLE repmeta.rep_notifications (
	run_id int8 NOT NULL,
	"data" jsonb NULL,
	CONSTRAINT rep_notifications_pkey PRIMARY KEY (run_id),
	CONSTRAINT rep_notifications_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_notifications_run_id_idx ON repmeta.rep_notifications USING btree (run_id);


-- repmeta.rep_scheduler_job definition

-- Drop table

-- DROP TABLE repmeta.rep_scheduler_job;

CREATE TABLE repmeta.rep_scheduler_job (
	job_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	"name" text NULL,
	command_id int8 NULL,
	schedule text NULL,
	command_requests jsonb NULL,
	CONSTRAINT rep_scheduler_job_pkey PRIMARY KEY (job_id),
	CONSTRAINT rep_scheduler_job_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX rep_scheduler_job_run_id_idx ON repmeta.rep_scheduler_job USING btree (run_id);


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
	task_uuid text NULL,
	description text NULL,
	target_names _text NULL,
	raw jsonb NULL,
	CONSTRAINT rep_task_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_run_id_task_name_key UNIQUE (run_id, task_name),
	CONSTRAINT rep_task_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES repmeta.dim_customer(customer_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_server_id_fkey FOREIGN KEY (server_id) REFERENCES repmeta.dim_server(server_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_task_taskname_server_run ON repmeta.rep_task USING btree (customer_id, server_id, task_name, run_id DESC);
CREATE INDEX rep_task_run_id_task_name_idx ON repmeta.rep_task USING btree (run_id, task_name);


-- repmeta.rep_task_configuration definition

-- Drop table

-- DROP TABLE repmeta.rep_task_configuration;

CREATE TABLE repmeta.rep_task_configuration (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	config_name text NOT NULL,
	CONSTRAINT rep_task_configuration_pkey PRIMARY KEY (task_id, config_name),
	CONSTRAINT rep_task_configuration_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_configuration_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_configuration_run_id_idx ON repmeta.rep_task_configuration USING btree (run_id);


-- repmeta.rep_task_configuration_item definition

-- Drop table

-- DROP TABLE repmeta.rep_task_configuration_item;

CREATE TABLE repmeta.rep_task_configuration_item (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	config_name text NOT NULL,
	"key" text NOT NULL,
	value text NULL,
	CONSTRAINT rep_task_configuration_item_pkey PRIMARY KEY (task_id, config_name, key),
	CONSTRAINT rep_task_configuration_item_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_configuration_item_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_configuration_item_run_id_idx ON repmeta.rep_task_configuration_item USING btree (run_id);


-- repmeta.rep_task_endpoint definition

-- Drop table

-- DROP TABLE repmeta.rep_task_endpoint;

CREATE TABLE repmeta.rep_task_endpoint (
	task_id int8 NOT NULL,
	"role" text NOT NULL,
	run_id int8 NOT NULL,
	database_name text NULL,
	endpoint_id int8 NOT NULL,
	CONSTRAINT rep_task_endpoint_role_check CHECK ((role = ANY (ARRAY['SOURCE'::text, 'TARGET'::text]))),
	CONSTRAINT rep_task_endpoint_endpoint_fk FOREIGN KEY (endpoint_id) REFERENCES repmeta.rep_database(endpoint_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT rep_task_endpoint_run_fk FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT rep_task_endpoint_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_endpoint_role_idx ON repmeta.rep_task_endpoint USING btree (role);
CREATE INDEX rep_task_endpoint_task_id_idx ON repmeta.rep_task_endpoint USING btree (task_id);
CREATE UNIQUE INDEX rep_task_endpoint_task_role_epid_uidx ON repmeta.rep_task_endpoint USING btree (task_id, role, endpoint_id);


-- repmeta.rep_task_feature_flag definition

-- Drop table

-- DROP TABLE repmeta.rep_task_feature_flag;

CREATE TABLE repmeta.rep_task_feature_flag (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	"name" text NOT NULL,
	array_values jsonb NULL,
	CONSTRAINT rep_task_feature_flag_pkey PRIMARY KEY (task_id, name),
	CONSTRAINT rep_task_feature_flag_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_feature_flag_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_feature_flag_run_id_idx ON repmeta.rep_task_feature_flag USING btree (run_id);


-- repmeta.rep_task_logger definition

-- Drop table

-- DROP TABLE repmeta.rep_task_logger;

CREATE TABLE repmeta.rep_task_logger (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	logger_name text NOT NULL,
	"level" text NULL,
	CONSTRAINT rep_task_logger_pkey PRIMARY KEY (task_id, run_id, logger_name),
	CONSTRAINT rep_task_logger_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id),
	CONSTRAINT rep_task_logger_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_pk_manip definition

-- Drop table

-- DROP TABLE repmeta.rep_task_pk_manip;

CREATE TABLE repmeta.rep_task_pk_manip (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	table_name text NOT NULL,
	pk_origin text NULL,
	CONSTRAINT rep_task_pk_manip_pkey PRIMARY KEY (task_id, table_name),
	CONSTRAINT rep_task_pk_manip_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_pk_manip_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_pk_manip_run_id_idx ON repmeta.rep_task_pk_manip USING btree (run_id);


-- repmeta.rep_task_pk_segment definition

-- Drop table

-- DROP TABLE repmeta.rep_task_pk_segment;

CREATE TABLE repmeta.rep_task_pk_segment (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	table_name text NOT NULL,
	seg_name text NOT NULL,
	"position" int4 NULL,
	seg_id text NULL,
	CONSTRAINT rep_task_pk_segment_pkey PRIMARY KEY (task_id, table_name, seg_name),
	CONSTRAINT rep_task_pk_segment_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_pk_segment_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_pk_segment_run_id_idx ON repmeta.rep_task_pk_segment USING btree (run_id);


-- repmeta.rep_task_settings_change_table definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_change_table;

CREATE TABLE repmeta.rep_task_settings_change_table (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	handle_ddl bool NULL,
	CONSTRAINT rep_task_settings_change_table_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_change_table_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_change_table_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_common definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_common;

CREATE TABLE repmeta.rep_task_settings_common (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	write_full_logging bool NULL,
	status_table_name text NULL,
	suspended_tables_table_name text NULL,
	exception_table_name text NULL,
	save_changes_enabled bool NULL,
	batch_apply_memory_limit int4 NULL,
	batch_apply_timeout int4 NULL,
	batch_apply_timeout_min int4 NULL,
	status_table_enabled bool NULL,
	suspended_tables_table_enabled bool NULL,
	history_table_enabled bool NULL,
	exception_table_enabled bool NULL,
	recovery_table_enabled bool NULL,
	ddl_history_table_enabled bool NULL,
	batch_apply_use_parallel_bulk bool NULL,
	parallel_bulk_max_num_threads int4 NULL,
	batch_optimize_by_merge bool NULL,
	use_inserts_for_status_table_updates bool NULL,
	task_uuid text NULL,
	CONSTRAINT rep_task_settings_common_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_common_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_common_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX idx_task_settings_common_uuid ON repmeta.rep_task_settings_common USING btree (task_uuid);


-- repmeta.rep_task_settings_kv definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_kv;

CREATE TABLE repmeta.rep_task_settings_kv (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	full_path text NOT NULL,
	val_type text NULL,
	val_bool bool NULL,
	val_num numeric NULL,
	val_text text NULL,
	val_json jsonb NULL,
	CONSTRAINT rep_task_settings_kv_pkey PRIMARY KEY (task_id, full_path),
	CONSTRAINT rep_task_settings_kv_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_kv_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_task_settings_kv_full_path ON repmeta.rep_task_settings_kv USING btree (full_path);


-- repmeta.rep_task_settings_section definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_section;

CREATE TABLE repmeta.rep_task_settings_section (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	section_path text NOT NULL,
	body jsonb NOT NULL,
	CONSTRAINT rep_task_settings_section_pkey PRIMARY KEY (task_id, section_path),
	CONSTRAINT rep_task_settings_section_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_section_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_target definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_target;

CREATE TABLE repmeta.rep_task_settings_target (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	create_pk_after_data_load bool NULL,
	artifacts_cleanup_enabled bool NULL,
	handle_truncate_ddl bool NULL,
	handle_drop_ddl bool NULL,
	max_transaction_size int4 NULL,
	ddl_handling_policy text NULL,
	ftm_settings jsonb NULL,
	CONSTRAINT rep_task_settings_target_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_target_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_target_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_target_key definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_target_key;

CREATE TABLE repmeta.rep_task_settings_target_key (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	key_record_container text NULL,
	CONSTRAINT rep_task_settings_target_key_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_target_key_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_target_key_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_settings_target_queue definition

-- Drop table

-- DROP TABLE repmeta.rep_task_settings_target_queue;

CREATE TABLE repmeta.rep_task_settings_target_queue (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	use_custom_message bool NULL,
	use_custom_key bool NULL,
	header_fields_flag int4 NULL,
	include_before_data bool NULL,
	message_shape_json jsonb NULL,
	key_shape_json jsonb NULL,
	CONSTRAINT rep_task_settings_target_queue_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_settings_target_queue_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_settings_target_queue_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_sorter_settings definition

-- Drop table

-- DROP TABLE repmeta.rep_task_sorter_settings;

CREATE TABLE repmeta.rep_task_sorter_settings (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	memory_keep_time int4 NULL,
	memory_limit_total int8 NULL,
	transaction_consistency_timeout int4 NULL,
	CONSTRAINT rep_task_sorter_settings_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_sorter_settings_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_sorter_settings_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_source definition

-- Drop table

-- DROP TABLE repmeta.rep_task_source;

CREATE TABLE repmeta.rep_task_source (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	source_name text NULL,
	database_name text NULL,
	CONSTRAINT rep_task_source_pkey PRIMARY KEY (task_id),
	CONSTRAINT rep_task_source_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_source_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);


-- repmeta.rep_task_source_table definition

-- Drop table

-- DROP TABLE repmeta.rep_task_source_table;

CREATE TABLE repmeta.rep_task_source_table (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_id int8 NOT NULL,
	"owner" text NULL,
	table_name text NULL,
	estimated_size int8 NULL,
	orig_db_id text NULL,
	extra jsonb NULL,
	CONSTRAINT rep_task_source_table_pkey PRIMARY KEY (id),
	CONSTRAINT rep_task_source_table_task_id_owner_table_name_key UNIQUE (task_id, owner, table_name),
	CONSTRAINT rep_task_source_table_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_source_table_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_source_table_run_id_task_id_idx ON repmeta.rep_task_source_table USING btree (run_id, task_id);


-- repmeta.rep_task_stats definition

-- Drop table

-- DROP TABLE repmeta.rep_task_stats;

CREATE TABLE repmeta.rep_task_stats (
	run_id int8 NOT NULL,
	task_id int8 NOT NULL,
	table_count int4 NULL,
	CONSTRAINT rep_task_stats_run_id_task_id_key UNIQUE (run_id, task_id),
	CONSTRAINT rep_task_stats_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX ix_rep_task_stats_run ON repmeta.rep_task_stats USING btree (run_id);


-- repmeta.rep_task_table definition

-- Drop table

-- DROP TABLE repmeta.rep_task_table;

CREATE TABLE repmeta.rep_task_table (
	task_table_id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	estimated_size int8 NULL,
	orig_db_id int8 NULL,
	CONSTRAINT rep_task_table_pkey PRIMARY KEY (task_table_id),
	CONSTRAINT rep_task_table_task_id_owner_table_name_key UNIQUE (task_id, owner, table_name),
	CONSTRAINT rep_task_table_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_table_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX idx_task_table__run_id ON repmeta.rep_task_table USING btree (run_id);
CREATE INDEX idx_task_table__task_id ON repmeta.rep_task_table USING btree (task_id);


-- repmeta.rep_task_table_manip definition

-- Drop table

-- DROP TABLE repmeta.rep_task_table_manip;

CREATE TABLE repmeta.rep_task_table_manip (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	fl_passthru_filter text NULL,
	CONSTRAINT rep_task_table_manip_pkey PRIMARY KEY (task_id, owner, table_name),
	CONSTRAINT rep_task_table_manip_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_table_manip_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_table_manip_run_id_idx ON repmeta.rep_task_table_manip USING btree (run_id);


-- repmeta.rep_task_target definition

-- Drop table

-- DROP TABLE repmeta.rep_task_target;

CREATE TABLE repmeta.rep_task_target (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_id int8 NOT NULL,
	target_name text NULL,
	target_state text NULL,
	database_name text NULL,
	CONSTRAINT rep_task_target_pkey PRIMARY KEY (id),
	CONSTRAINT rep_task_target_task_id_target_name_key UNIQUE (task_id, target_name),
	CONSTRAINT rep_task_target_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_target_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX rep_task_target_run_id_task_id_idx ON repmeta.rep_task_target USING btree (run_id, task_id);


-- repmeta.rep_task_unload_segments_entry definition

-- Drop table

-- DROP TABLE repmeta.rep_task_unload_segments_entry;

CREATE TABLE repmeta.rep_task_unload_segments_entry (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	entry_name text NOT NULL,
	CONSTRAINT rep_task_unload_segments_entry_pkey PRIMARY KEY (task_id, owner, table_name, entry_name),
	CONSTRAINT rep_task_unload_segments_entry_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_unload_segments_entry_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_task_unload_segments_entry_task_id ON repmeta.rep_task_unload_segments_entry USING btree (task_id);
CREATE INDEX rep_task_unload_segments_entry_run_id_idx ON repmeta.rep_task_unload_segments_entry USING btree (run_id);


-- repmeta.rep_task_unload_segments_range definition

-- Drop table

-- DROP TABLE repmeta.rep_task_unload_segments_range;

CREATE TABLE repmeta.rep_task_unload_segments_range (
	task_id int8 NOT NULL,
	run_id int8 NOT NULL,
	"owner" text NOT NULL,
	table_name text NOT NULL,
	range_json jsonb NULL,
	CONSTRAINT rep_task_unload_segments_range_pkey PRIMARY KEY (task_id, owner, table_name),
	CONSTRAINT rep_task_unload_segments_range_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE,
	CONSTRAINT rep_task_unload_segments_range_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE CASCADE
);
CREATE INDEX idx_rep_task_unload_segments_range_task_id ON repmeta.rep_task_unload_segments_range USING btree (task_id);
CREATE INDEX rep_task_unload_segments_range_run_id_idx ON repmeta.rep_task_unload_segments_range USING btree (run_id);


-- repmeta.source_table definition

-- Drop table

-- DROP TABLE repmeta.source_table;

CREATE TABLE repmeta.source_table (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NULL,
	"owner" text NULL,
	table_name text NULL,
	estimated_size int8 NULL,
	orig_db_id text NULL,
	CONSTRAINT source_table_pkey PRIMARY KEY (id),
	CONSTRAINT source_table_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX source_table_run_id_task_name_idx ON repmeta.source_table USING btree (run_id, task_name);


-- repmeta.task definition

-- Drop table

-- DROP TABLE repmeta.task;

CREATE TABLE repmeta.task (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	"name" text NULL,
	source_name text NULL,
	task_type text NULL,
	description text NULL,
	status_table text NULL,
	suspended_tables_table text NULL,
	task_uuid text NULL,
	raw jsonb NULL,
	CONSTRAINT task_pkey PRIMARY KEY (id),
	CONSTRAINT task_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX task_run_id_idx ON repmeta.task USING btree (run_id);


-- repmeta.task_target definition

-- Drop table

-- DROP TABLE repmeta.task_target;

CREATE TABLE repmeta.task_target (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NULL,
	target_name text NULL,
	target_state text NULL,
	database_name text NULL,
	CONSTRAINT task_target_pkey PRIMARY KEY (id),
	CONSTRAINT task_target_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX task_target_run_id_task_name_idx ON repmeta.task_target USING btree (run_id, task_name);


-- repmeta.feature_flag_value definition

-- Drop table

-- DROP TABLE repmeta.feature_flag_value;

CREATE TABLE repmeta.feature_flag_value (
	id bigserial NOT NULL,
	run_id int8 NOT NULL,
	task_name text NULL,
	"name" text NULL,
	value text NULL,
	CONSTRAINT feature_flag_value_pkey PRIMARY KEY (id),
	CONSTRAINT feature_flag_value_run_id_fkey FOREIGN KEY (run_id) REFERENCES repmeta.ingest_run(run_id) ON DELETE CASCADE
);
CREATE INDEX feature_flag_value_run_id_task_name_idx ON repmeta.feature_flag_value USING btree (run_id, task_name);


-- repmeta.rep_metrics_event definition

-- Drop table

-- DROP TABLE repmeta.rep_metrics_event;

CREATE TABLE repmeta.rep_metrics_event (
	event_id bigserial NOT NULL,
	metrics_run_id int8 NOT NULL,
	task_uuid text NOT NULL,
	task_id int8 NULL,
	source_type text NULL,
	target_type text NULL,
	source_family_id int4 NULL,
	target_family_id int4 NULL,
	start_ts timestamptz NULL,
	stop_ts timestamptz NULL,
	event_type text NULL,
	load_rows int8 NULL,
	load_bytes int8 NULL,
	cdc_rows int8 NULL,
	cdc_bytes int8 NULL,
	status text NULL,
	CONSTRAINT rep_metrics_event_pkey PRIMARY KEY (event_id),
	CONSTRAINT rep_metrics_event_metrics_run_id_fkey FOREIGN KEY (metrics_run_id) REFERENCES repmeta.rep_metrics_run(metrics_run_id) ON DELETE CASCADE,
	CONSTRAINT rep_metrics_event_source_family_id_fkey FOREIGN KEY (source_family_id) REFERENCES repmeta.endpoint_family(family_id),
	CONSTRAINT rep_metrics_event_target_family_id_fkey FOREIGN KEY (target_family_id) REFERENCES repmeta.endpoint_family(family_id),
	CONSTRAINT rep_metrics_event_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE SET NULL
);
CREATE INDEX idx_metrics_event_run_pair ON repmeta.rep_metrics_event USING btree (metrics_run_id, source_family_id, target_family_id);
CREATE INDEX idx_metrics_event_run_uuid ON repmeta.rep_metrics_event USING btree (metrics_run_id, task_uuid);
CREATE INDEX idx_metrics_event_uuid ON repmeta.rep_metrics_event USING btree (task_uuid);
CREATE INDEX ix_metrics_event_run ON repmeta.rep_metrics_event USING btree (metrics_run_id);
CREATE INDEX ix_metrics_event_type_status ON repmeta.rep_metrics_event USING btree (upper(COALESCE(event_type, ''::text)), upper(COALESCE(status, ''::text)));
CREATE INDEX ix_rep_metrics_event_run ON repmeta.rep_metrics_event USING btree (metrics_run_id);
CREATE INDEX ix_rep_metrics_event_sf ON repmeta.rep_metrics_event USING btree (source_family_id);
CREATE INDEX ix_rep_metrics_event_task ON repmeta.rep_metrics_event USING btree (task_id);
CREATE INDEX ix_rep_metrics_event_tf ON repmeta.rep_metrics_event USING btree (target_family_id);
CREATE INDEX ix_rep_metrics_event_uuid ON repmeta.rep_metrics_event USING btree (task_uuid);
CREATE INDEX rep_metrics_event_run_idx ON repmeta.rep_metrics_event USING btree (metrics_run_id);
CREATE INDEX rep_metrics_event_start_idx ON repmeta.rep_metrics_event USING btree (start_ts);
CREATE INDEX rep_metrics_event_stop_idx ON repmeta.rep_metrics_event USING btree (stop_ts);


-- repmeta.rep_metrics_task_total definition

-- Drop table

-- DROP TABLE repmeta.rep_metrics_task_total;

CREATE TABLE repmeta.rep_metrics_task_total (
	metrics_run_id int8 NOT NULL,
	task_uuid text NOT NULL,
	task_id int8 NULL,
	load_rows_total int8 DEFAULT 0 NOT NULL,
	load_bytes_total int8 DEFAULT 0 NOT NULL,
	cdc_rows_total int8 DEFAULT 0 NOT NULL,
	cdc_bytes_total int8 DEFAULT 0 NOT NULL,
	events_count int8 DEFAULT 0 NOT NULL,
	first_ts timestamptz NULL,
	last_ts timestamptz NULL,
	CONSTRAINT rep_metrics_task_total_pkey PRIMARY KEY (metrics_run_id, task_uuid),
	CONSTRAINT rep_metrics_task_total_metrics_run_id_fkey FOREIGN KEY (metrics_run_id) REFERENCES repmeta.rep_metrics_run(metrics_run_id) ON DELETE CASCADE,
	CONSTRAINT rep_metrics_task_total_task_id_fkey FOREIGN KEY (task_id) REFERENCES repmeta.rep_task(task_id) ON DELETE SET NULL
);


-- repmeta.v_current_endpoints source

CREATE OR REPLACE VIEW repmeta.v_current_endpoints
AS SELECT d.database_id,
    d.run_id,
    d.customer_id,
    d.server_id,
    d.name,
    d.role,
    d.type_id,
    d.is_licensed,
    d.db_settings,
    d.override_properties,
    d.endpoint_id,
    d.db_settings_type,
    c.customer_name,
    s.server_name
   FROM repmeta.rep_database d
     JOIN repmeta.v_latest_run vr ON vr.customer_id = d.customer_id AND vr.server_id = d.server_id AND vr.run_id = d.run_id
     JOIN repmeta.dim_customer c ON c.customer_id = d.customer_id
     JOIN repmeta.dim_server s ON s.server_id = d.server_id;


-- repmeta.v_current_task_endpoints source

CREATE OR REPLACE VIEW repmeta.v_current_task_endpoints
AS WITH latest AS (
         SELECT ir.customer_id,
            ir.server_id,
            max(ir.created_at) AS max_created_at
           FROM repmeta.ingest_run ir
          GROUP BY ir.customer_id, ir.server_id
        ), runs AS (
         SELECT ir.run_id,
            ir.customer_id,
            ir.server_id
           FROM repmeta.ingest_run ir
             JOIN latest l ON l.customer_id = ir.customer_id AND l.server_id = ir.server_id AND l.max_created_at = ir.created_at
        )
 SELECT te.task_id,
    t.task_name,
    t.task_type,
    te.role,
    te.endpoint_id,
    te.endpoint_id AS database_id,
    d.name AS endpoint_name,
    d.role AS endpoint_role,
    d.type_id,
    d.db_settings_type,
    t.run_id,
    runs.customer_id,
    runs.server_id
   FROM repmeta.rep_task_endpoint te
     JOIN repmeta.rep_task t ON t.task_id = te.task_id
     JOIN repmeta.rep_database d ON d.endpoint_id = te.endpoint_id AND d.run_id = t.run_id
     JOIN runs ON runs.run_id = t.run_id;


-- repmeta.v_current_task_props source

CREATE OR REPLACE VIEW repmeta.v_current_task_props
AS SELECT kv.task_id,
    kv.run_id,
    t.task_name,
    t.customer_id,
    t.server_id,
    c.customer_name,
    s.server_name,
    kv.full_path,
    kv.val_type,
    kv.val_bool,
    kv.val_num,
    kv.val_text,
    kv.val_json
   FROM repmeta.rep_task_settings_kv kv
     JOIN repmeta.v_latest_run vr ON vr.run_id = kv.run_id
     JOIN repmeta.rep_task t ON t.task_id = kv.task_id AND t.run_id = kv.run_id
     JOIN repmeta.dim_customer c ON c.customer_id = t.customer_id
     JOIN repmeta.dim_server s ON s.server_id = t.server_id;


-- repmeta.v_current_tasks source

CREATE OR REPLACE VIEW repmeta.v_current_tasks
AS SELECT t.task_id,
    t.run_id,
    t.customer_id,
    t.server_id,
    t.task_name,
    t.source_name,
    t.task_type,
    t.task_uuid,
    t.description,
    t.target_names,
    t.raw,
    c.customer_name,
    s.server_name
   FROM repmeta.rep_task t
     JOIN repmeta.v_latest_run vr ON vr.customer_id = t.customer_id AND vr.server_id = t.server_id AND vr.run_id = t.run_id
     JOIN repmeta.dim_customer c ON c.customer_id = t.customer_id
     JOIN repmeta.dim_server s ON s.server_id = t.server_id;


-- repmeta.v_customer_endpoint_families source

CREATE OR REPLACE VIEW repmeta.v_customer_endpoint_families
AS WITH dnorm AS (
         SELECT d.customer_id,
            d.type_id,
                CASE
                    WHEN d.role = 'SOURCE'::text THEN 'SOURCE'::text
                    ELSE 'TARGET'::text
                END AS ep_role
           FROM repmeta.rep_database d
        ), joined AS (
         SELECT dn.customer_id,
            dn.ep_role,
            fam.family_id
           FROM dnorm dn
             LEFT JOIN repmeta.endpoint_alias_map am ON am.alias_type = 'component_type'::text AND am.role = dn.ep_role AND am.alias_value = dn.type_id AND am.active
             LEFT JOIN repmeta.endpoint_family fam ON fam.family_id = am.family_id
        )
 SELECT joined.customer_id,
    joined.ep_role AS role,
    joined.family_id,
    count(*) AS endpoint_count
   FROM joined
  GROUP BY joined.customer_id, joined.ep_role, joined.family_id;


-- repmeta.v_endpoint_perf_t90 source

CREATE OR REPLACE VIEW repmeta.v_endpoint_perf_t90
AS WITH s AS (
         SELECT v_metrics_sessions_t90.customer_id,
            v_metrics_sessions_t90.server_id,
            v_metrics_sessions_t90.tkey,
            v_metrics_sessions_t90.source_family_id,
            v_metrics_sessions_t90.target_family_id,
            v_metrics_sessions_t90.session_start,
            v_metrics_sessions_t90.session_stop,
            v_metrics_sessions_t90.secs,
            v_metrics_sessions_t90.rows_moved,
            v_metrics_sessions_t90.status
           FROM repmeta.v_metrics_sessions_t90
        ), agg AS (
         SELECT s.customer_id,
            s.server_id,
            s.source_family_id AS family_id,
            'SOURCE'::text AS role,
            count(DISTINCT s.tkey) AS tasks,
            sum(s.rows_moved)::bigint AS rows_moved,
            sum(s.secs)::bigint AS uptime_sec,
            percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (s.rows_moved::double precision / NULLIF(s.secs, 0)::double precision)) AS median_rps,
            avg(
                CASE
                    WHEN s.status ~~* '%error%'::text THEN 1
                    ELSE 0
                END)::double precision AS err_stop_rate,
            percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (s.secs::double precision)) / 60.0::double precision AS median_session_minutes
           FROM s
          GROUP BY s.customer_id, s.server_id, s.source_family_id, 'SOURCE'::text
        UNION ALL
         SELECT s.customer_id,
            s.server_id,
            s.target_family_id AS family_id,
            'TARGET'::text AS role,
            count(DISTINCT s.tkey) AS tasks,
            sum(s.rows_moved)::bigint AS rows_moved,
            sum(s.secs)::bigint AS uptime_sec,
            percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (s.rows_moved::double precision / NULLIF(s.secs, 0)::double precision)) AS median_rps,
            avg(
                CASE
                    WHEN s.status ~~* '%error%'::text THEN 1
                    ELSE 0
                END)::double precision AS err_stop_rate,
            percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (s.secs::double precision)) / 60.0::double precision AS median_session_minutes
           FROM s
          GROUP BY s.customer_id, s.server_id, s.target_family_id, 'TARGET'::text
        ), win AS (
         SELECT v_metrics_t90_window.customer_id,
            v_metrics_t90_window.server_id,
            EXTRACT(epoch FROM v_metrics_t90_window.window_end - v_metrics_t90_window.window_start)::bigint AS window_sec
           FROM repmeta.v_metrics_t90_window
        )
 SELECT a.customer_id,
    a.server_id,
    a.role,
    a.family_id,
    a.tasks,
    a.rows_moved,
    a.uptime_sec,
    round(100.0 * a.uptime_sec::numeric / NULLIF(w.window_sec, 0)::numeric, 2) AS uptime_pct,
    a.median_rps,
    a.err_stop_rate,
    a.median_session_minutes
   FROM agg a
     JOIN win w USING (customer_id, server_id);

COMMENT ON VIEW repmeta.v_endpoint_perf_t90 IS 'Per-endpoint-family KPIs (SOURCE/TARGET) in each server’s dynamic 90-day window with uptime normalized to window.';


-- repmeta.v_latest_customer_license source

CREATE OR REPLACE VIEW repmeta.v_latest_customer_license
AS SELECT DISTINCT ON (customer_license_capabilities.customer_id) customer_license_capabilities.license_id,
    customer_license_capabilities.customer_id,
    customer_license_capabilities.licensed_all_sources,
    customer_license_capabilities.licensed_all_targets,
    customer_license_capabilities.licensed_sources,
    customer_license_capabilities.licensed_targets,
    customer_license_capabilities.raw_line,
    customer_license_capabilities.parsed_at
   FROM repmeta.customer_license_capabilities
  ORDER BY customer_license_capabilities.customer_id, customer_license_capabilities.parsed_at DESC;


-- repmeta.v_latest_run source

CREATE OR REPLACE VIEW repmeta.v_latest_run
AS SELECT r.customer_id,
    r.server_id,
    max(r.run_id) AS run_id
   FROM repmeta.ingest_run r
  GROUP BY r.customer_id, r.server_id;


-- repmeta.v_latest_run_by_name source

CREATE OR REPLACE VIEW repmeta.v_latest_run_by_name
AS SELECT c.customer_name,
    s.server_name,
    vr.run_id
   FROM repmeta.v_latest_run vr
     JOIN repmeta.dim_customer c ON c.customer_id = vr.customer_id
     JOIN repmeta.dim_server s ON s.server_id = vr.server_id;


-- repmeta.v_license_families source

CREATE OR REPLACE VIEW repmeta.v_license_families
AS WITH latest AS (
         SELECT v_license_latest.license_id,
            v_license_latest.customer_id,
            v_license_latest.extracted_at,
            v_license_latest.all_sources,
            v_license_latest.all_targets
           FROM repmeta.v_license_latest
        ), explicit AS (
         SELECT s.customer_id,
            i.role,
            COALESCE(i.family_id, am.family_id) AS family_id
           FROM repmeta.license_snapshot_item i
             JOIN repmeta.license_snapshot s ON s.license_id = i.license_id
             JOIN latest l ON l.license_id = s.license_id
             LEFT JOIN repmeta.endpoint_alias_map am ON am.role = i.role AND am.alias_type = 'license_ticker'::text AND lower(am.alias_value) = lower(i.alias_value) AND am.active
        ), wildcard AS (
         SELECT l.customer_id,
            f.role,
            f.family_id
           FROM latest l
             JOIN repmeta.endpoint_family f ON l.all_sources AND f.role = 'SOURCE'::text OR l.all_targets AND f.role = 'TARGET'::text
        )
 SELECT u.customer_id,
    u.role,
    u.family_id
   FROM ( SELECT explicit.customer_id,
            explicit.role,
            explicit.family_id
           FROM explicit
        UNION
         SELECT wildcard.customer_id,
            wildcard.role,
            wildcard.family_id
           FROM wildcard) u
  GROUP BY u.customer_id, u.role, u.family_id;


-- repmeta.v_license_latest source

CREATE OR REPLACE VIEW repmeta.v_license_latest
AS SELECT DISTINCT ON (license_snapshot.customer_id) license_snapshot.license_id,
    license_snapshot.customer_id,
    license_snapshot.extracted_at,
    license_snapshot.all_sources,
    license_snapshot.all_targets
   FROM repmeta.license_snapshot
  ORDER BY license_snapshot.customer_id, license_snapshot.extracted_at DESC;


-- repmeta.v_license_vs_usage source

CREATE OR REPLACE VIEW repmeta.v_license_vs_usage
AS SELECT lf.customer_id,
    lf.role AS ef_role,
    ef.family_id,
    ef.family_name,
    COALESCE(u.endpoint_count, 0::bigint) AS configured_count,
    true AS is_licensed
   FROM repmeta.v_license_families lf
     JOIN repmeta.endpoint_family ef ON ef.family_id = lf.family_id
     LEFT JOIN repmeta.v_customer_endpoint_families u ON u.customer_id = lf.customer_id AND u.role = lf.role AND u.family_id = lf.family_id
UNION ALL
 SELECT u.customer_id,
    u.role AS ef_role,
    ef.family_id,
    ef.family_name,
    u.endpoint_count AS configured_count,
    false AS is_licensed
   FROM repmeta.v_customer_endpoint_families u
     JOIN repmeta.endpoint_family ef ON ef.family_id = u.family_id
     LEFT JOIN repmeta.v_license_families lf ON lf.customer_id = u.customer_id AND lf.role = u.role AND lf.family_id = u.family_id
  WHERE lf.family_id IS NULL;


-- repmeta.v_metrics_events_clean source

CREATE OR REPLACE VIEW repmeta.v_metrics_events_clean
AS WITH latest_runs AS (
         SELECT DISTINCT ON (rep_metrics_run.customer_id, rep_metrics_run.server_id) rep_metrics_run.metrics_run_id,
            rep_metrics_run.customer_id,
            rep_metrics_run.server_id
           FROM repmeta.rep_metrics_run
          ORDER BY rep_metrics_run.customer_id, rep_metrics_run.server_id, rep_metrics_run.created_at DESC NULLS LAST, rep_metrics_run.metrics_run_id DESC
        )
 SELECT lr.customer_id,
    lr.server_id,
    e.task_id,
    e.task_uuid,
    COALESCE(e.stop_ts, e.start_ts) AS ts,
    COALESCE(e.load_rows, 0::bigint) AS load_rows,
    COALESCE(e.load_bytes, 0::bigint) AS load_bytes,
    COALESCE(e.cdc_rows, 0::bigint) AS cdc_rows,
    COALESCE(e.cdc_bytes, 0::bigint) AS cdc_bytes,
    e.source_family_id,
    e.target_family_id,
    e.source_type,
    e.target_type,
    e.status,
    e.event_type
   FROM repmeta.rep_metrics_event e
     JOIN latest_runs lr ON lr.metrics_run_id = e.metrics_run_id
  WHERE upper(COALESCE(e.event_type, ''::text)) = 'STOP'::text AND upper(COALESCE(e.status, 'OK'::text)) = 'OK'::text;


-- repmeta.v_metrics_pair_total_latest source

CREATE OR REPLACE VIEW repmeta.v_metrics_pair_total_latest
AS WITH latest AS (
         SELECT rep_metrics_run.customer_id,
            rep_metrics_run.server_id,
            max(rep_metrics_run.created_at) AS max_created
           FROM repmeta.rep_metrics_run
          GROUP BY rep_metrics_run.customer_id, rep_metrics_run.server_id
        ), pick AS (
         SELECT r.metrics_run_id,
            r.customer_id,
            r.server_id
           FROM repmeta.rep_metrics_run r
             JOIN latest l ON l.customer_id = r.customer_id AND l.server_id = r.server_id AND l.max_created = r.created_at
        )
 SELECT p.customer_id,
    p.server_id,
    pt.metrics_run_id,
    pt.source_family_id,
    sf.family_name AS source_family,
    pt.target_family_id,
    tf.family_name AS target_family,
    pt.load_rows_total,
    pt.load_bytes_total,
    pt.cdc_rows_total,
    pt.cdc_bytes_total,
    pt.events_count,
    pt.first_ts,
    pt.last_ts
   FROM pick p
     JOIN repmeta.rep_metrics_pair_total pt ON pt.metrics_run_id = p.metrics_run_id
     LEFT JOIN repmeta.endpoint_family sf ON sf.family_id = pt.source_family_id
     LEFT JOIN repmeta.endpoint_family tf ON tf.family_id = pt.target_family_id;


-- repmeta.v_metrics_sessions_t90 source

CREATE OR REPLACE VIEW repmeta.v_metrics_sessions_t90
AS WITH e AS (
         SELECT r.customer_id,
            r.server_id,
            COALESCE(ev.task_id::text, ev.task_uuid) AS tkey,
            ev.source_family_id,
            ev.target_family_id,
            ev.event_type,
            ev.status,
            ev.start_ts,
            ev.stop_ts,
            COALESCE(ev.load_rows, 0::bigint) AS load_rows,
            COALESCE(ev.cdc_rows, 0::bigint) AS cdc_rows,
            COALESCE(ev.stop_ts, ev.start_ts) AS event_ts
           FROM repmeta.rep_metrics_event ev
             JOIN repmeta.rep_metrics_run r ON r.metrics_run_id = ev.metrics_run_id
             JOIN repmeta.v_metrics_t90_window w ON w.customer_id = r.customer_id AND w.server_id = r.server_id
          WHERE COALESCE(ev.stop_ts, ev.start_ts) >= w.window_start AND COALESCE(ev.stop_ts, ev.start_ts) <= w.window_end
        ), runs AS (
         SELECT e.customer_id,
            e.server_id,
            e.tkey,
            e.source_family_id,
            e.target_family_id,
            e.start_ts AS session_start_raw,
            COALESCE(e.stop_ts, e.start_ts + '00:10:00'::interval) AS session_stop_raw,
            COALESCE(e.load_rows, 0::bigint) + COALESCE(e.cdc_rows, 0::bigint) AS rows_moved,
            e.status
           FROM e
          WHERE e.start_ts IS NOT NULL
        ), clipped AS (
         SELECT r.customer_id,
            r.server_id,
            r.tkey,
            r.source_family_id,
            r.target_family_id,
            GREATEST(r.session_start_raw, w.window_start) AS session_start,
            LEAST(r.session_stop_raw, w.window_end) AS session_stop,
            r.rows_moved,
            r.status
           FROM runs r
             JOIN repmeta.v_metrics_t90_window w USING (customer_id, server_id)
        ), secs_calc AS (
         SELECT c.customer_id,
            c.server_id,
            c.tkey,
            c.source_family_id,
            c.target_family_id,
            c.session_start,
            c.session_stop,
            GREATEST(0::numeric, EXTRACT(epoch FROM c.session_stop - c.session_start))::bigint AS secs,
            c.rows_moved,
            c.status
           FROM clipped c
        )
 SELECT secs_calc.customer_id,
    secs_calc.server_id,
    secs_calc.tkey,
    secs_calc.source_family_id,
    secs_calc.target_family_id,
    secs_calc.session_start,
    secs_calc.session_stop,
    secs_calc.secs,
    secs_calc.rows_moved,
    secs_calc.status
   FROM secs_calc;

COMMENT ON VIEW repmeta.v_metrics_sessions_t90 IS 'Rolling 90-day sessions per server using event_ts=COALESCE(stop_ts,start_ts) for windowing; session durations clipped to window.';


-- repmeta.v_metrics_t90_window source

CREATE OR REPLACE VIEW repmeta.v_metrics_t90_window
AS SELECT r.customer_id,
    r.server_id,
    max(COALESCE(ev.stop_ts, ev.start_ts)) AS window_end,
    max(COALESCE(ev.stop_ts, ev.start_ts)) - '90 days'::interval AS window_start
   FROM repmeta.rep_metrics_event ev
     JOIN repmeta.rep_metrics_run r ON r.metrics_run_id = ev.metrics_run_id
  GROUP BY r.customer_id, r.server_id;

COMMENT ON VIEW repmeta.v_metrics_t90_window IS 'Per (customer_id, server_id): window_end = max(event_ts=COALESCE(stop_ts,start_ts)); window_start = end - 90 days.';


-- repmeta.v_metrics_task_latest_event source

CREATE OR REPLACE VIEW repmeta.v_metrics_task_latest_event
AS WITH ranked AS (
         SELECT v_metrics_events_clean.customer_id,
            v_metrics_events_clean.server_id,
            v_metrics_events_clean.task_id,
            v_metrics_events_clean.task_uuid,
            v_metrics_events_clean.ts,
            v_metrics_events_clean.load_rows,
            v_metrics_events_clean.load_bytes,
            v_metrics_events_clean.cdc_rows,
            v_metrics_events_clean.cdc_bytes,
            v_metrics_events_clean.source_family_id,
            v_metrics_events_clean.target_family_id,
            v_metrics_events_clean.source_type,
            v_metrics_events_clean.target_type,
            v_metrics_events_clean.status,
            v_metrics_events_clean.event_type,
            row_number() OVER (PARTITION BY v_metrics_events_clean.customer_id, v_metrics_events_clean.server_id, (COALESCE(v_metrics_events_clean.task_id::text, v_metrics_events_clean.task_uuid)) ORDER BY v_metrics_events_clean.ts DESC) AS rn
           FROM repmeta.v_metrics_events_clean
        )
 SELECT ranked.customer_id,
    ranked.server_id,
    ranked.task_id,
    ranked.task_uuid,
    ranked.ts,
    ranked.load_rows,
    ranked.load_bytes,
    ranked.cdc_rows,
    ranked.cdc_bytes,
    ranked.source_family_id,
    ranked.target_family_id,
    ranked.source_type,
    ranked.target_type
   FROM ranked
  WHERE ranked.rn = 1;


-- repmeta.v_metrics_task_total_latest source

CREATE OR REPLACE VIEW repmeta.v_metrics_task_total_latest
AS WITH latest AS (
         SELECT rep_metrics_run.customer_id,
            rep_metrics_run.server_id,
            max(rep_metrics_run.created_at) AS max_created
           FROM repmeta.rep_metrics_run
          GROUP BY rep_metrics_run.customer_id, rep_metrics_run.server_id
        ), pick AS (
         SELECT r.metrics_run_id,
            r.customer_id,
            r.server_id
           FROM repmeta.rep_metrics_run r
             JOIN latest l ON l.customer_id = r.customer_id AND l.server_id = r.server_id AND l.max_created = r.created_at
        )
 SELECT p.customer_id,
    p.server_id,
    t.metrics_run_id,
    t.task_uuid,
    t.task_id,
    t.load_rows_total,
    t.load_bytes_total,
    t.cdc_rows_total,
    t.cdc_bytes_total,
    t.events_count,
    t.first_ts,
    t.last_ts
   FROM pick p
     JOIN repmeta.rep_metrics_task_total t ON t.metrics_run_id = p.metrics_run_id;


-- repmeta.v_rep_release_issue source

CREATE OR REPLACE VIEW repmeta.v_rep_release_issue
AS SELECT r.id,
    r.version,
    r.issue_date,
    r.title,
    r.url,
    r.jira,
    r.endpoints,
    r.buckets,
    r.text,
    r.created_at,
    COALESCE((regexp_match(r.version, '(\d{4})\.(\d{1,2})'::text))[1]::integer, NULLIF(regexp_replace(r.version, '.*(20\d{2}).*'::text, '\1'::text), ''::text)::integer) AS train_year,
    COALESCE((regexp_match(r.version, '\d{4}\.(\d{1,2})'::text))[1]::integer,
        CASE
            WHEN r.version ~~* '%May %'::text THEN 5
            WHEN r.version ~~* '%November %'::text THEN 11
            ELSE NULL::integer
        END) AS train_month_code
   FROM repmeta.replicate_release_issue r;


-- repmeta.v_task_health_t90 source

CREATE OR REPLACE VIEW repmeta.v_task_health_t90
AS WITH s AS (
         SELECT v_metrics_sessions_t90.customer_id,
            v_metrics_sessions_t90.server_id,
            v_metrics_sessions_t90.tkey,
            v_metrics_sessions_t90.source_family_id,
            v_metrics_sessions_t90.target_family_id,
            v_metrics_sessions_t90.session_start,
            v_metrics_sessions_t90.session_stop,
            v_metrics_sessions_t90.secs,
            v_metrics_sessions_t90.rows_moved,
            v_metrics_sessions_t90.status
           FROM repmeta.v_metrics_sessions_t90
        ), per_task AS (
         SELECT s.customer_id,
            s.server_id,
            s.tkey,
            min(s.session_start) AS first_seen,
            max(s.session_stop) AS last_seen,
            count(*) AS session_count,
            sum(s.secs)::bigint AS uptime_sec,
            sum(s.rows_moved)::bigint AS rows_moved,
            percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (s.secs::double precision)) / 60.0::double precision AS median_session_minutes,
            avg(
                CASE
                    WHEN s.status ~~* '%error%'::text THEN 1
                    ELSE 0
                END)::double precision AS error_stop_rate
           FROM s
          GROUP BY s.customer_id, s.server_id, s.tkey
        ), win AS (
         SELECT v_metrics_t90_window.customer_id,
            v_metrics_t90_window.server_id,
            v_metrics_t90_window.window_start,
            v_metrics_t90_window.window_end,
            EXTRACT(epoch FROM v_metrics_t90_window.window_end - v_metrics_t90_window.window_start)::bigint AS window_sec
           FROM repmeta.v_metrics_t90_window
        )
 SELECT p.customer_id,
    p.server_id,
    p.tkey,
    p.first_seen,
    p.last_seen,
    p.session_count,
    p.uptime_sec,
    p.rows_moved,
    p.median_session_minutes,
    p.error_stop_rate,
    GREATEST(0::bigint, w.window_sec - p.uptime_sec) AS downtime_sec,
    round(100.0 * p.uptime_sec::numeric / NULLIF(w.window_sec, 0)::numeric, 2) AS uptime_pct,
    p.session_count::double precision / NULLIF(GREATEST(1, LEAST(date_trunc('day'::text, p.last_seen)::date, w.window_end::date) - GREATEST(date_trunc('day'::text, p.first_seen)::date, w.window_start::date) + 1), 0)::double precision AS restarts_per_day,
    p.rows_moved::double precision / NULLIF(p.uptime_sec, 0)::double precision AS throughput_rps
   FROM per_task p
     JOIN win w USING (customer_id, server_id);

COMMENT ON VIEW repmeta.v_task_health_t90 IS 'Per-task health metrics in each server’s dynamic 90-day window (uptime/downtime clipped to window; restarts/day within window).';


-- repmeta.v_task_latest source

CREATE OR REPLACE VIEW repmeta.v_task_latest
AS WITH ranked AS (
         SELECT t.task_id,
            t.run_id,
            t.customer_id,
            t.server_id,
            t.task_name,
            t.task_type,
            t.source_name,
            t.target_names,
            r.created_at,
            row_number() OVER (PARTITION BY t.customer_id, t.server_id, t.task_name ORDER BY t.run_id DESC) AS rn
           FROM repmeta.rep_task t
             JOIN repmeta.ingest_run r USING (run_id)
        )
 SELECT ranked.task_id,
    ranked.run_id,
    ranked.customer_id,
    ranked.server_id,
    ranked.task_name,
    ranked.task_type,
    ranked.source_name,
    ranked.target_names,
    ranked.created_at,
    ranked.rn
   FROM ranked
  WHERE ranked.rn = 1;


-- repmeta.v_task_settings_kv_json source

CREATE OR REPLACE VIEW repmeta.v_task_settings_kv_json
AS SELECT s.task_id,
    jsonb_object_agg(s.full_path, s.val_jsonb) AS kv_json
   FROM ( SELECT rep_task_settings_kv.task_id,
            rep_task_settings_kv.full_path,
                CASE rep_task_settings_kv.val_type
                    WHEN 'bool'::text THEN to_jsonb(rep_task_settings_kv.val_bool)
                    WHEN 'num'::text THEN to_jsonb(rep_task_settings_kv.val_num)
                    WHEN 'text'::text THEN to_jsonb(rep_task_settings_kv.val_text)
                    WHEN 'json'::text THEN COALESCE(rep_task_settings_kv.val_json, 'null'::jsonb)
                    ELSE 'null'::jsonb
                END AS val_jsonb
           FROM repmeta.rep_task_settings_kv
          WHERE rep_task_settings_kv.full_path IS NOT NULL) s
  GROUP BY s.task_id;


-- repmeta.v_task_settings_overview source

CREATE OR REPLACE VIEW repmeta.v_task_settings_overview
AS SELECT tl.customer_id,
    tl.server_id,
    ds.server_name,
    dc.customer_name,
    tl.task_id,
    tl.run_id,
    tl.created_at,
    tl.task_name,
    tl.task_type,
    tl.source_name,
    tl.target_names,
    c.write_full_logging,
    c.batch_apply_memory_limit,
    c.batch_apply_timeout,
    c.batch_apply_timeout_min,
    c.status_table_enabled,
    c.suspended_tables_table_enabled,
    c.history_table_enabled,
    c.exception_table_enabled,
    c.recovery_table_enabled,
    c.ddl_history_table_enabled,
    c.batch_apply_use_parallel_bulk,
    c.parallel_bulk_max_num_threads,
    c.batch_optimize_by_merge,
    c.use_inserts_for_status_table_updates,
    c.task_uuid,
    ct.handle_ddl,
    tg.create_pk_after_data_load,
    tg.artifacts_cleanup_enabled,
    tg.handle_truncate_ddl,
    tg.handle_drop_ddl,
    tg.max_transaction_size,
    tg.ddl_handling_policy,
    tg.ftm_settings,
    ss.memory_keep_time,
    ss.memory_limit_total,
    ss.transaction_consistency_timeout,
    sj.sections_json,
    kj.kv_json
   FROM repmeta.v_task_latest tl
     LEFT JOIN repmeta.rep_task_settings_common c USING (task_id)
     LEFT JOIN repmeta.rep_task_settings_change_table ct USING (task_id)
     LEFT JOIN repmeta.rep_task_settings_target tg USING (task_id)
     LEFT JOIN repmeta.rep_task_sorter_settings ss USING (task_id)
     LEFT JOIN repmeta.v_task_settings_sections_json sj USING (task_id)
     LEFT JOIN repmeta.v_task_settings_kv_json kj USING (task_id)
     LEFT JOIN repmeta.dim_server ds ON ds.server_id = tl.server_id
     LEFT JOIN repmeta.dim_customer dc ON dc.customer_id = tl.customer_id;


-- repmeta.v_task_settings_sections_json source

CREATE OR REPLACE VIEW repmeta.v_task_settings_sections_json
AS SELECT rep_task_settings_section.task_id,
    jsonb_object_agg(split_part(rep_task_settings_section.section_path, '.'::text, 2), rep_task_settings_section.body) AS sections_json
   FROM repmeta.rep_task_settings_section
  GROUP BY rep_task_settings_section.task_id;


-- repmeta.v_tasks_debug_loggers source

CREATE OR REPLACE VIEW repmeta.v_tasks_debug_loggers
AS WITH latest AS (
         SELECT ingest_run.customer_id,
            ingest_run.server_id,
            max(ingest_run.created_at) AS last_ingest
           FROM repmeta.ingest_run
          GROUP BY ingest_run.customer_id, ingest_run.server_id
        ), runs AS (
         SELECT r_1.customer_id,
            r_1.server_id,
            r_1.run_id
           FROM latest l_1
             JOIN repmeta.ingest_run r_1 ON r_1.customer_id = l_1.customer_id AND r_1.server_id = l_1.server_id AND r_1.created_at = l_1.last_ingest
        )
 SELECT t.customer_id,
    t.server_id,
    t.task_id,
    t.task_name,
    bool_or(upper(l.level) = 'DEBUG'::text) AS has_debug,
    string_agg((l.logger_name || '='::text) || COALESCE(l.level, '?'::text), ', '::text ORDER BY l.logger_name) AS debug_loggers
   FROM repmeta.rep_task t
     JOIN repmeta.rep_task_logger l USING (task_id, run_id)
     JOIN runs r ON r.run_id = t.run_id
  GROUP BY t.customer_id, t.server_id, t.task_id, t.task_name;


-- repmeta.v_unknown_counts source

CREATE OR REPLACE VIEW repmeta.v_unknown_counts
AS SELECT unknown_field.run_id,
    unknown_field.entity,
    count(*) AS unknown_key_count
   FROM repmeta.unknown_field
  GROUP BY unknown_field.run_id, unknown_field.entity;


-- repmeta.v_unmapped_component_types source

CREATE OR REPLACE VIEW repmeta.v_unmapped_component_types
AS SELECT DISTINCT d.role,
    d.type_id
   FROM repmeta.rep_database d
     LEFT JOIN repmeta.endpoint_alias_map am ON am.alias_type = 'component_type'::text AND am.role = d.role AND am.alias_value = d.type_id
  WHERE am.alias_id IS NULL;


-- repmeta.v_unmapped_endpoints source

CREATE OR REPLACE VIEW repmeta.v_unmapped_endpoints
AS SELECT DISTINCT d.role,
    d.db_settings_type
   FROM repmeta.rep_database d
     LEFT JOIN repmeta.endpoint_alias_map a ON a.role = d.role AND lower(regexp_replace(d.db_settings_type, '[^a-z0-9]+'::text, ''::text, 'g'::text)) = lower(regexp_replace(a.alias_value, '[^a-z0-9]+'::text, ''::text, 'g'::text))
  WHERE a.alias_id IS NULL;


-- repmeta.v_unmapped_license_tickers source

CREATE OR REPLACE VIEW repmeta.v_unmapped_license_tickers
AS SELECT DISTINCT i.role,
    i.alias_value AS ticker
   FROM repmeta.license_snapshot_item i
     LEFT JOIN repmeta.endpoint_alias_map am ON am.alias_type = 'license_ticker'::text AND am.role = i.role AND lower(am.alias_value) = lower(i.alias_value)
  WHERE am.alias_id IS NULL;


-- repmeta.vw_server_latest_run source

CREATE OR REPLACE VIEW repmeta.vw_server_latest_run
AS SELECT DISTINCT ON (s.server_id) s.server_id,
    s.server_name,
    s.customer_id,
    ir.run_id,
    ir.created_at
   FROM repmeta.dim_server s
     JOIN repmeta.ingest_run ir ON ir.server_id = s.server_id
  ORDER BY s.server_id, ir.created_at DESC;


-- repmeta.vw_tasks_by_endpoint source

CREATE OR REPLACE VIEW repmeta.vw_tasks_by_endpoint
AS SELECT te.task_id,
    t.task_name,
    t.task_type,
    te.role,
    te.endpoint_id,
    te.endpoint_id AS database_id,
    d.name AS endpoint_name,
    d.role AS endpoint_role,
    d.type_id,
    d.db_settings_type,
    t.run_id,
    ir.customer_id,
    ir.server_id,
    ir.created_at
   FROM repmeta.rep_task_endpoint te
     JOIN repmeta.rep_task t ON t.task_id = te.task_id
     JOIN repmeta.rep_database d ON d.endpoint_id = te.endpoint_id AND d.run_id = t.run_id
     JOIN repmeta.ingest_run ir ON ir.run_id = t.run_id;



-- DROP FUNCTION repmeta."_t90_window_start"();

CREATE OR REPLACE FUNCTION repmeta._t90_window_start()
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT now() - interval '90 days'
$function$
;

-- DROP FUNCTION repmeta.canon_family(text, text);

CREATE OR REPLACE FUNCTION repmeta.canon_family(_db_settings_type text, _role text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
WITH src AS (
  SELECT coalesce(_db_settings_type, '') AS t
),
hit AS (
  SELECT m.family
  FROM repmeta.endpoint_family_map m, src
  WHERE m.active
    AND (_role IS NULL OR m.role IS NULL OR m.role = _role)
    AND src.t ~* m.pattern           -- case-insensitive regex
  ORDER BY m.priority ASC, length(m.pattern) DESC
  LIMIT 1
)
SELECT
  coalesce(
    (SELECT family FROM hit),
    -- heuristic fallback: remove trailing "Settings", split camel/snake, title-case
    initcap(
      regexp_replace(
        regexp_replace(coalesce(_db_settings_type,''), 'Settings$', '', 'i'),
        '([a-z])([A-Z])|[_\-]+', '\1 \2', 'g'
      )
    )
  );
$function$
;

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