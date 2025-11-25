-- DROP SCHEMA repmeta_qs;

CREATE SCHEMA repmeta_qs AUTHORIZATION postgres;

-- DROP SEQUENCE repmeta_qs.snapshot_snapshot_id_seq;

CREATE SEQUENCE repmeta_qs.snapshot_snapshot_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- repmeta_qs."snapshot" definition

-- Drop table

-- DROP TABLE repmeta_qs."snapshot";

CREATE TABLE repmeta_qs."snapshot" (
	snapshot_id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	notes text NULL,
	CONSTRAINT snapshot_pkey PRIMARY KEY (snapshot_id)
);
CREATE INDEX snapshot_customer_created_idx ON repmeta_qs.snapshot USING btree (customer_id, created_at DESC);


-- repmeta_qs.about definition

-- Drop table

-- DROP TABLE repmeta_qs.about;

CREATE TABLE repmeta_qs.about (
	snapshot_id int4 NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT about_pkey PRIMARY KEY (snapshot_id),
	CONSTRAINT about_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);


-- repmeta_qs.access_analyzer definition

-- Drop table

-- DROP TABLE repmeta_qs.access_analyzer;

CREATE TABLE repmeta_qs.access_analyzer (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT access_analyzer_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT access_analyzer_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX access_analyzer_id_idx ON repmeta_qs.access_analyzer USING btree (id);
CREATE INDEX access_anlz_data_gin ON repmeta_qs.access_analyzer USING gin (data);


-- repmeta_qs.access_analyzer_time definition

-- Drop table

-- DROP TABLE repmeta_qs.access_analyzer_time;

CREATE TABLE repmeta_qs.access_analyzer_time (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT access_analyzer_time_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT access_analyzer_time_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX access_analyzer_time_id_idx ON repmeta_qs.access_analyzer_time USING btree (id);


-- repmeta_qs.access_professional definition

-- Drop table

-- DROP TABLE repmeta_qs.access_professional;

CREATE TABLE repmeta_qs.access_professional (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT access_professional_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT access_professional_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX access_prof_data_gin ON repmeta_qs.access_professional USING gin (data);
CREATE INDEX access_professional_id_idx ON repmeta_qs.access_professional USING btree (id);


-- repmeta_qs.app definition

-- Drop table

-- DROP TABLE repmeta_qs.app;

CREATE TABLE repmeta_qs.app (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT app_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT app_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX app_data_gin ON repmeta_qs.app USING gin (data);
CREATE INDEX app_id_idx ON repmeta_qs.app USING btree (id);


-- repmeta_qs.app_object definition

-- Drop table

-- DROP TABLE repmeta_qs.app_object;

CREATE TABLE repmeta_qs.app_object (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	app_id text NULL,
	CONSTRAINT app_object_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT app_object_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX app_object_app_id_idx ON repmeta_qs.app_object USING btree (app_id);
CREATE INDEX app_object_data_gin ON repmeta_qs.app_object USING gin (data);
CREATE INDEX app_object_id_idx ON repmeta_qs.app_object USING btree (id);


-- repmeta_qs.app_objects definition

-- Drop table

-- DROP TABLE repmeta_qs.app_objects;

CREATE TABLE repmeta_qs.app_objects (
	snapshot_id int4 NOT NULL,
	object_id text NOT NULL,
	app_id text NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT app_objects_pkey PRIMARY KEY (snapshot_id, object_id),
	CONSTRAINT app_objects_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX app_objects_app_id_idx ON repmeta_qs.app_objects USING btree (app_id);
CREATE INDEX app_objects_data_gin ON repmeta_qs.app_objects USING gin (data);
CREATE INDEX app_objects_object_id_idx ON repmeta_qs.app_objects USING btree (object_id);


-- repmeta_qs.apps definition

-- Drop table

-- DROP TABLE repmeta_qs.apps;

CREATE TABLE repmeta_qs.apps (
	snapshot_id int4 NOT NULL,
	app_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT apps_pkey PRIMARY KEY (snapshot_id, app_id),
	CONSTRAINT apps_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX apps_app_id_idx ON repmeta_qs.apps USING btree (app_id);
CREATE INDEX apps_data_gin ON repmeta_qs.apps USING gin (data);


-- repmeta_qs."extension" definition

-- Drop table

-- DROP TABLE repmeta_qs."extension";

CREATE TABLE repmeta_qs."extension" (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT extension_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT extension_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX extension_data_gin ON repmeta_qs.extension USING gin (data);
CREATE INDEX extension_id_idx ON repmeta_qs.extension USING btree (id);


-- repmeta_qs.extensions definition

-- Drop table

-- DROP TABLE repmeta_qs.extensions;

CREATE TABLE repmeta_qs.extensions (
	snapshot_id int4 NOT NULL,
	extension_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT extensions_pkey PRIMARY KEY (snapshot_id, extension_id),
	CONSTRAINT extensions_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX extensions_data_gin ON repmeta_qs.extensions USING gin (data);
CREATE INDEX extensions_extension_id_idx ON repmeta_qs.extensions USING btree (extension_id);


-- repmeta_qs.license definition

-- Drop table

-- DROP TABLE repmeta_qs.license;

CREATE TABLE repmeta_qs.license (
	snapshot_id int4 NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT license_pkey PRIMARY KEY (snapshot_id),
	CONSTRAINT license_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);


-- repmeta_qs.reload_task definition

-- Drop table

-- DROP TABLE repmeta_qs.reload_task;

CREATE TABLE repmeta_qs.reload_task (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	app_id text NULL,
	CONSTRAINT reload_task_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT reload_task_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX reload_task_data_gin ON repmeta_qs.reload_task USING gin (data);
CREATE INDEX reload_task_id_idx ON repmeta_qs.reload_task USING btree (id);


-- repmeta_qs.reload_tasks definition

-- Drop table

-- DROP TABLE repmeta_qs.reload_tasks;

CREATE TABLE repmeta_qs.reload_tasks (
	snapshot_id int4 NOT NULL,
	task_id text NOT NULL,
	app_id text NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT reload_tasks_pkey PRIMARY KEY (snapshot_id, task_id),
	CONSTRAINT reload_tasks_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX reload_tasks_app_id_idx ON repmeta_qs.reload_tasks USING btree (app_id);
CREATE INDEX reload_tasks_data_gin ON repmeta_qs.reload_tasks USING gin (data);
CREATE INDEX reload_tasks_task_id_idx ON repmeta_qs.reload_tasks USING btree (task_id);


-- repmeta_qs.server_config definition

-- Drop table

-- DROP TABLE repmeta_qs.server_config;

CREATE TABLE repmeta_qs.server_config (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT server_config_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT server_config_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX server_config_data_gin ON repmeta_qs.server_config USING gin (data);
CREATE INDEX server_config_id_idx ON repmeta_qs.server_config USING btree (id);


-- repmeta_qs.servernode_config definition

-- Drop table

-- DROP TABLE repmeta_qs.servernode_config;

CREATE TABLE repmeta_qs.servernode_config (
	snapshot_id int4 NOT NULL,
	node_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT servernode_config_pkey PRIMARY KEY (snapshot_id, node_id),
	CONSTRAINT servernode_config_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX servernode_config_data_gin ON repmeta_qs.servernode_config USING gin (data);
CREATE INDEX servernode_config_node_id_idx ON repmeta_qs.servernode_config USING btree (node_id);


-- repmeta_qs.stream definition

-- Drop table

-- DROP TABLE repmeta_qs.stream;

CREATE TABLE repmeta_qs.stream (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT stream_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT stream_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX stream_data_gin ON repmeta_qs.stream USING gin (data);
CREATE INDEX stream_id_idx ON repmeta_qs.stream USING btree (id);


-- repmeta_qs.streams definition

-- Drop table

-- DROP TABLE repmeta_qs.streams;

CREATE TABLE repmeta_qs.streams (
	snapshot_id int4 NOT NULL,
	stream_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT streams_pkey PRIMARY KEY (snapshot_id, stream_id),
	CONSTRAINT streams_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX streams_data_gin ON repmeta_qs.streams USING gin (data);
CREATE INDEX streams_stream_id_idx ON repmeta_qs.streams USING btree (stream_id);


-- repmeta_qs.system_info definition

-- Drop table

-- DROP TABLE repmeta_qs.system_info;

CREATE TABLE repmeta_qs.system_info (
	snapshot_id int4 NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT system_info_pkey PRIMARY KEY (snapshot_id),
	CONSTRAINT system_info_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);


-- repmeta_qs.system_rule definition

-- Drop table

-- DROP TABLE repmeta_qs.system_rule;

CREATE TABLE repmeta_qs.system_rule (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT system_rule_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT system_rule_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX system_rule_data_gin ON repmeta_qs.system_rule USING gin (data);
CREATE INDEX system_rule_id_idx ON repmeta_qs.system_rule USING btree (id);


-- repmeta_qs.system_rules definition

-- Drop table

-- DROP TABLE repmeta_qs.system_rules;

CREATE TABLE repmeta_qs.system_rules (
	snapshot_id int4 NOT NULL,
	rule_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT system_rules_pkey PRIMARY KEY (snapshot_id, rule_id),
	CONSTRAINT system_rules_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX system_rules_data_gin ON repmeta_qs.system_rules USING gin (data);
CREATE INDEX system_rules_rule_id_idx ON repmeta_qs.system_rules USING btree (rule_id);


-- repmeta_qs."user" definition

-- Drop table

-- DROP TABLE repmeta_qs."user";

CREATE TABLE repmeta_qs."user" (
	snapshot_id int4 NOT NULL,
	id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT user_pkey PRIMARY KEY (snapshot_id, id),
	CONSTRAINT user_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX user_data_gin ON repmeta_qs."user" USING gin (data);
CREATE INDEX user_id_idx ON repmeta_qs."user" USING btree (id);


-- repmeta_qs.users definition

-- Drop table

-- DROP TABLE repmeta_qs.users;

CREATE TABLE repmeta_qs.users (
	snapshot_id int4 NOT NULL,
	user_id text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (snapshot_id, user_id),
	CONSTRAINT users_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES repmeta_qs."snapshot"(snapshot_id) ON DELETE CASCADE
);
CREATE INDEX users_data_gin ON repmeta_qs.users USING gin (data);
CREATE INDEX users_user_id_idx ON repmeta_qs.users USING btree (user_id);


-- repmeta_qs.servernode_configuration source

CREATE OR REPLACE VIEW repmeta_qs.servernode_configuration
AS SELECT server_config.snapshot_id,
    server_config.id,
    server_config.data
   FROM repmeta_qs.server_config;


-- repmeta_qs.snapshots source

CREATE OR REPLACE VIEW repmeta_qs.snapshots
AS SELECT snapshot.snapshot_id,
    snapshot.customer_id,
    snapshot.created_at,
    snapshot.notes
   FROM repmeta_qs.snapshot;


-- repmeta_qs.v_app_objects source

CREATE OR REPLACE VIEW repmeta_qs.v_app_objects
AS SELECT o.snapshot_id,
    o.object_id,
    o.app_id,
    o.data,
    COALESCE(o.data ->> 'name'::text, o.data ->> 'title'::text) AS object_name,
    lower(COALESCE(o.data ->> 'type'::text, o.data ->> 'qType'::text)) AS object_type,
    lower(COALESCE(o.data ->> 'published'::text, (o.data -> 'qMeta'::text) ->> 'published'::text)) = 'true'::text AS published
   FROM repmeta_qs.app_objects o;


-- repmeta_qs.v_app_objects_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_app_objects_summary
AS WITH base AS (
         SELECT v_app_objects.snapshot_id,
            v_app_objects.object_type,
            v_app_objects.published
           FROM repmeta_qs.v_app_objects
        )
 SELECT s.snapshot_id,
    count(*) FILTER (WHERE b.object_type = 'sheet'::text) AS total_sheets,
    count(*) FILTER (WHERE b.object_type = 'story'::text) AS total_stories,
    count(*) AS total_objects,
    count(*) FILTER (WHERE b.object_type = 'sheet'::text AND b.published) AS published_sheets,
    count(*) FILTER (WHERE b.object_type = 'story'::text AND b.published) AS published_stories
   FROM repmeta_qs.snapshot s
     LEFT JOIN base b ON b.snapshot_id = s.snapshot_id
  GROUP BY s.snapshot_id;


-- repmeta_qs.v_app_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_app_summary
AS WITH apps AS (
         SELECT v_apps.snapshot_id,
            v_apps.app_id,
            v_apps.published,
            v_apps.stream
           FROM repmeta_qs.v_apps
        )
 SELECT s.snapshot_id,
    ( SELECT count(*) AS count
           FROM apps a
          WHERE a.snapshot_id = s.snapshot_id) AS total_apps,
    ( SELECT count(*) AS count
           FROM apps a
          WHERE a.snapshot_id = s.snapshot_id AND a.published) AS published_apps,
    ( SELECT count(*) AS count
           FROM repmeta_qs.v_streams st
          WHERE st.snapshot_id = s.snapshot_id) AS streams,
    ( SELECT count(DISTINCT a.stream) AS count
           FROM apps a
          WHERE a.snapshot_id = s.snapshot_id AND a.stream IS NOT NULL) AS streams_with_apps
   FROM repmeta_qs.snapshot s;


-- repmeta_qs.v_apps source

CREATE OR REPLACE VIEW repmeta_qs.v_apps
AS SELECT a.snapshot_id,
    a.app_id,
    a.data,
    COALESCE(a.data ->> 'name'::text, a.data ->> 'appName'::text, a.data ->> 'title'::text) AS app_name,
    COALESCE(a.data ->> 'stream'::text, a.data ->> 'streamName'::text) AS stream,
    a.data ->> 'streamId'::text AS stream_id,
    lower(a.data ->> 'published'::text) = 'true'::text AS published
   FROM repmeta_qs.apps a;


-- repmeta_qs.v_counts_by_snapshot source

CREATE OR REPLACE VIEW repmeta_qs.v_counts_by_snapshot
AS SELECT s.snapshot_id,
    ( SELECT count(*) AS count
           FROM repmeta_qs.apps t
          WHERE t.snapshot_id = s.snapshot_id) AS apps,
    ( SELECT count(*) AS count
           FROM repmeta_qs.app_objects t
          WHERE t.snapshot_id = s.snapshot_id) AS app_objects,
    ( SELECT count(*) AS count
           FROM repmeta_qs.streams t
          WHERE t.snapshot_id = s.snapshot_id) AS streams,
    ( SELECT count(*) AS count
           FROM repmeta_qs.users t
          WHERE t.snapshot_id = s.snapshot_id) AS users,
    ( SELECT count(*) AS count
           FROM repmeta_qs.reload_tasks t
          WHERE t.snapshot_id = s.snapshot_id) AS reload_tasks,
    ( SELECT count(*) AS count
           FROM repmeta_qs.extensions t
          WHERE t.snapshot_id = s.snapshot_id) AS extensions
   FROM repmeta_qs.snapshot s;


-- repmeta_qs.v_customer source

CREATE OR REPLACE VIEW repmeta_qs.v_customer
AS SELECT s.snapshot_id,
    s.customer_id,
    COALESCE(to_jsonb(dc.*) ->> 'display_name'::text, to_jsonb(dc.*) ->> 'customer_name'::text, to_jsonb(dc.*) ->> 'name'::text, to_jsonb(dc.*) ->> 'company_name'::text, to_jsonb(dc.*) ->> 'legal_name'::text, to_jsonb(dc.*) ->> 'short_name'::text) AS customer_name
   FROM repmeta_qs.snapshot s
     LEFT JOIN repmeta.dim_customer dc ON dc.customer_id = s.customer_id;


-- repmeta_qs.v_environment_overview source

CREATE OR REPLACE VIEW repmeta_qs.v_environment_overview
AS SELECT s.snapshot_id,
    s.customer_id,
    s.created_at,
    s.notes,
    COALESCE(a.data ->> 'productName'::text, a.data ->> 'ProductName'::text, a.data ->> 'product'::text, a.data ->> 'Product'::text) AS product_name,
    COALESCE(a.data ->> 'productVersion'::text, a.data ->> 'ProductVersion'::text, a.data ->> 'version'::text, a.data ->> 'Version'::text) AS product_version,
    COALESCE(a.data ->> 'buildVersion'::text, a.data ->> 'BuildVersion'::text) AS build_version,
    COALESCE(a.data ->> 'buildDate'::text, a.data ->> 'BuildDate'::text) AS build_date,
    ( SELECT count(*) AS count
           FROM repmeta_qs.servernode_config t
          WHERE t.snapshot_id = s.snapshot_id) AS node_count,
    ( SELECT count(*) AS count
           FROM repmeta_qs.extensions t
          WHERE t.snapshot_id = s.snapshot_id) AS extension_count,
    ( SELECT count(*) AS count
           FROM repmeta_qs.streams t
          WHERE t.snapshot_id = s.snapshot_id) AS stream_count,
    ( SELECT count(*) AS count
           FROM repmeta_qs.apps t
          WHERE t.snapshot_id = s.snapshot_id) AS app_count,
    ( SELECT count(*) AS count
           FROM repmeta_qs.users t
          WHERE t.snapshot_id = s.snapshot_id) AS user_count,
    ( SELECT count(*) AS count
           FROM repmeta_qs.reload_tasks t
          WHERE t.snapshot_id = s.snapshot_id) AS reload_task_count,
    (( SELECT count(*) AS count
           FROM repmeta_qs.servernode_config t
          WHERE t.snapshot_id = s.snapshot_id)) = 1 AS single_node_only
   FROM repmeta_qs.snapshot s
     LEFT JOIN repmeta_qs.about a ON a.snapshot_id = s.snapshot_id
     LEFT JOIN repmeta_qs.system_info si ON si.snapshot_id = s.snapshot_id
     LEFT JOIN repmeta_qs.license l ON l.snapshot_id = s.snapshot_id;


-- repmeta_qs.v_environment_overview_enriched source

CREATE OR REPLACE VIEW repmeta_qs.v_environment_overview_enriched
AS SELECT e.snapshot_id,
    e.customer_id,
    e.created_at,
    e.notes,
    e.product_name,
    e.product_version,
    e.build_version,
    e.build_date,
    e.node_count,
    e.extension_count,
    e.stream_count,
    e.app_count,
    e.user_count,
    e.reload_task_count,
    e.single_node_only,
    v.customer_name
   FROM repmeta_qs.v_environment_overview e
     LEFT JOIN repmeta_qs.v_customer v USING (snapshot_id);


-- repmeta_qs.v_extension_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_extension_summary
AS SELECT s.snapshot_id,
    ( SELECT count(*) AS count
           FROM repmeta_qs.v_extensions e
          WHERE e.snapshot_id = s.snapshot_id) AS total_extensions,
    ( SELECT count(*) AS count
           FROM repmeta_qs.v_extensions e
          WHERE e.snapshot_id = s.snapshot_id AND e.is_bundled) AS bundled_extensions,
    ( SELECT count(*) AS count
           FROM repmeta_qs.v_extensions e
          WHERE e.snapshot_id = s.snapshot_id AND NOT e.is_bundled) AS custom_extensions
   FROM repmeta_qs.snapshot s;


-- repmeta_qs.v_extensions source

CREATE OR REPLACE VIEW repmeta_qs.v_extensions
AS SELECT e.snapshot_id,
    e.extension_id,
    e.data,
    COALESCE(e.data ->> 'name'::text, e.data ->> 'extensionName'::text) AS extension_name,
    lower(COALESCE(e.data ->> 'isBundled'::text, e.data ->> 'bundled'::text)) = 'true'::text AS is_bundled
   FROM repmeta_qs.extensions e;


-- repmeta_qs.v_governance_checks source

CREATE OR REPLACE VIEW repmeta_qs.v_governance_checks
AS WITH app_ids AS (
         SELECT a.snapshot_id,
            a.app_id
           FROM repmeta_qs.v_apps a
        ), task_by_app AS (
         SELECT t.snapshot_id,
            t.app_id,
                CASE
                    WHEN lower(t.data ->> 'enabled'::text) = 'true'::text THEN true
                    WHEN lower(t.data ->> 'enabled'::text) = 'false'::text THEN false
                    ELSE NULL::boolean
                END AS enabled
           FROM repmeta_qs.reload_tasks t
        )
 SELECT s.snapshot_id,
    ( SELECT count(*) AS count
           FROM app_ids a
             LEFT JOIN task_by_app tb ON tb.snapshot_id = a.snapshot_id AND tb.app_id = a.app_id
          WHERE a.snapshot_id = s.snapshot_id AND tb.app_id IS NULL) AS apps_without_tasks,
    ( SELECT count(*) AS count
           FROM task_by_app tb
          WHERE tb.snapshot_id = s.snapshot_id AND tb.enabled = false) AS disabled_tasks_count
   FROM repmeta_qs.snapshot s;


-- repmeta_qs.v_license_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_license_summary
AS SELECT s.snapshot_id,
    COALESCE(l.data ->> 'licenseNumber'::text, l.data ->> 'serial'::text, l.data ->> 'key'::text, l.data #>> '{license,number}'::text[]) AS license_number,
    COALESCE(l.data ->> 'controlNumber'::text, l.data #>> '{control,number}'::text[], l.data ->> 'control'::text) AS control_number,
    COALESCE(l.data ->> 'expiration'::text, l.data ->> 'expiryDate'::text, l.data #>> '{expiration,date}'::text[], l.data #>> '{license,expires}'::text[]) AS expiration,
    ( SELECT count(*) AS count
           FROM repmeta_qs.access_professional t
          WHERE t.snapshot_id = s.snapshot_id) AS professional_allocations,
    ( SELECT count(*) AS count
           FROM repmeta_qs.access_analyzer_time t
          WHERE t.snapshot_id = s.snapshot_id) AS analyzer_allocations
   FROM repmeta_qs.snapshot s
     LEFT JOIN repmeta_qs.license l ON l.snapshot_id = s.snapshot_id;


-- repmeta_qs.v_license_usage_30d source

CREATE OR REPLACE VIEW repmeta_qs.v_license_usage_30d
AS WITH allocs AS (
         SELECT access_professional.snapshot_id,
            'professional'::text AS kind,
            access_professional.data ->> 'id'::text AS alloc_id,
            COALESCE(NULLIF(access_professional.data ->> 'lastUsed'::text, ''::text), NULLIF(access_professional.data ->> 'lastAccess'::text, ''::text), NULLIF(access_professional.data ->> 'lastSeen'::text, ''::text))::timestamp with time zone AS last_used
           FROM repmeta_qs.access_professional
        UNION ALL
         SELECT access_analyzer_time.snapshot_id,
            'analyzer'::text AS kind,
            access_analyzer_time.data ->> 'id'::text,
            COALESCE(NULLIF(access_analyzer_time.data ->> 'lastUsed'::text, ''::text), NULLIF(access_analyzer_time.data ->> 'lastAccess'::text, ''::text), NULLIF(access_analyzer_time.data ->> 'lastSeen'::text, ''::text))::timestamp with time zone AS "coalesce"
           FROM repmeta_qs.access_analyzer_time
        ), bucketed AS (
         SELECT allocs.snapshot_id,
            allocs.kind,
                CASE
                    WHEN allocs.last_used IS NULL THEN 'never'::text
                    WHEN allocs.last_used >= (now() - '30 days'::interval) THEN 'used_30d'::text
                    ELSE 'not_used_30d'::text
                END AS bucket
           FROM allocs
        )
 SELECT s.snapshot_id,
    COALESCE(sum((b.bucket = 'used_30d'::text)::integer) FILTER (WHERE b.kind = 'analyzer'::text), 0::bigint) AS analyzer_used_30d,
    COALESCE(sum((b.bucket = 'not_used_30d'::text)::integer) FILTER (WHERE b.kind = 'analyzer'::text), 0::bigint) AS analyzer_not_used_30d,
    COALESCE(sum((b.bucket = 'never'::text)::integer) FILTER (WHERE b.kind = 'analyzer'::text), 0::bigint) AS analyzer_never_used,
    COALESCE(sum((b.bucket = 'used_30d'::text)::integer) FILTER (WHERE b.kind = 'professional'::text), 0::bigint) AS professional_used_30d,
    COALESCE(sum((b.bucket = 'not_used_30d'::text)::integer) FILTER (WHERE b.kind = 'professional'::text), 0::bigint) AS professional_not_used_30d,
    COALESCE(sum((b.bucket = 'never'::text)::integer) FILTER (WHERE b.kind = 'professional'::text), 0::bigint) AS professional_never_used
   FROM repmeta_qs.snapshot s
     LEFT JOIN bucketed b ON b.snapshot_id = s.snapshot_id
  GROUP BY s.snapshot_id;


-- repmeta_qs.v_nodes source

CREATE OR REPLACE VIEW repmeta_qs.v_nodes
AS SELECT n.snapshot_id,
    n.node_id,
    n.data
   FROM repmeta_qs.servernode_config n;


-- repmeta_qs.v_reload_activity_json source

CREATE OR REPLACE VIEW repmeta_qs.v_reload_activity_json
AS WITH raw AS (
         SELECT rt.snapshot_id,
            NULLIF((rt.data -> 'app'::text) ->> 'id'::text, ''::text)::uuid AS app_id,
            COALESCE(NULLIF(((rt.data -> 'operational'::text) -> 'lastExecutionResult'::text) ->> 'stopTime'::text, ''::text)::timestamp with time zone, NULLIF((rt.data -> 'operational'::text) ->> 'stopTime'::text, ''::text)::timestamp with time zone, NULLIF(rt.data ->> 'stopTime'::text, ''::text)::timestamp with time zone) AS stop_ts
           FROM repmeta_qs.reload_tasks rt
        ), last_by_app AS (
         SELECT raw.snapshot_id,
            raw.app_id,
            max(raw.stop_ts) AS ts
           FROM raw
          WHERE raw.stop_ts IS NOT NULL
          GROUP BY raw.snapshot_id, raw.app_id
        )
 SELECT last_by_app.snapshot_id,
    count(*) FILTER (WHERE last_by_app.ts >= (now() - '30 days'::interval)) AS apps_reloaded_30d,
    count(*) FILTER (WHERE last_by_app.ts >= (now() - '90 days'::interval)) AS apps_reloaded_90d
   FROM last_by_app
  GROUP BY last_by_app.snapshot_id;


-- repmeta_qs.v_reload_task_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_reload_task_summary
AS SELECT s.snapshot_id,
    count(*) FILTER (WHERE rt.task_id IS NOT NULL) AS total_tasks,
    count(*) FILTER (WHERE rt.duration_sec IS NOT NULL AND rt.duration_sec > (3 * 60 * 60)) AS over_3h,
    count(*) FILTER (WHERE rt.duration_sec IS NOT NULL AND rt.duration_sec <= (3 * 60 * 60)) AS under_3h,
    count(*) FILTER (WHERE rt.last_status = ANY (ARRAY['failed'::text, 'error'::text])) AS failed
   FROM repmeta_qs.snapshot s
     LEFT JOIN repmeta_qs.v_reload_tasks rt ON rt.snapshot_id = s.snapshot_id
  GROUP BY s.snapshot_id;


-- repmeta_qs.v_reload_tasks source

CREATE OR REPLACE VIEW repmeta_qs.v_reload_tasks
AS SELECT t.snapshot_id,
    t.task_id,
    t.app_id,
    t.data,
    COALESCE(t.data ->> 'name'::text, t.data ->> 'taskName'::text, t.data ->> 'appName'::text) AS task_name,
        CASE
            WHEN lower(t.data ->> 'enabled'::text) = 'true'::text THEN true
            WHEN lower(t.data ->> 'enabled'::text) = 'false'::text THEN false
            ELSE NULL::boolean
        END AS enabled,
    COALESCE(NULLIF(t.data #>> '{operational,lastExecution,durationSec}'::text[], ''::text)::integer, NULLIF(t.data ->> 'lastExecutionDurationSec'::text, ''::text)::integer, NULLIF(t.data ->> 'durationSec'::text, ''::text)::integer) AS duration_sec,
    COALESCE((t.data #>> '{operational,lastExecution,stopTime}'::text[])::timestamp with time zone, (t.data #>> '{lastExecution,stopTime}'::text[])::timestamp with time zone, (t.data ->> 'lastExecutionStopTime'::text)::timestamp with time zone) AS last_stop_time,
    lower(COALESCE(t.data #>> '{operational,lastExecution,status}'::text[], t.data ->> 'lastExecutionStatus'::text)) AS last_status
   FROM repmeta_qs.reload_tasks t;


-- repmeta_qs.v_security_rule_breakdown source

CREATE OR REPLACE VIEW repmeta_qs.v_security_rule_breakdown
AS WITH rules AS (
         SELECT system_rules.snapshot_id,
            COALESCE(NULLIF(lower(system_rules.data ->> 'disabled'::text), ''::text), 'false'::text) = ANY (ARRAY['true'::text, 't'::text, '1'::text, 'yes'::text, 'y'::text]) AS disabled,
            NULLIF(system_rules.data ->> 'seedId'::text, ''::text) AS seed1,
            NULLIF(system_rules.data ->> 'seedID'::text, ''::text) AS seed2,
            NULLIF((system_rules.data -> 'references'::text) ->> 'seedId'::text, ''::text) AS seed3,
            lower(COALESCE(system_rules.data ->> 'type'::text, ''::text)) AS ruletype
           FROM repmeta_qs.system_rules
        ), norm AS (
         SELECT rules.snapshot_id,
            rules.disabled,
                CASE
                    WHEN rules.ruletype = 'custom'::text THEN true
                    WHEN COALESCE(rules.seed1, rules.seed2, rules.seed3) IS NULL THEN true
                    WHEN COALESCE(rules.seed1, rules.seed2, rules.seed3) = '00000000-0000-0000-0000-000000000000'::text THEN true
                    ELSE false
                END AS is_custom
           FROM rules
        )
 SELECT norm.snapshot_id,
    count(*) AS total_rules,
    count(*) FILTER (WHERE norm.is_custom) AS custom_total,
    count(*) FILTER (WHERE norm.is_custom AND NOT norm.disabled) AS custom_enabled,
    count(*) FILTER (WHERE norm.is_custom AND norm.disabled) AS custom_disabled,
    count(*) FILTER (WHERE NOT norm.is_custom) AS default_total,
    count(*) FILTER (WHERE NOT norm.is_custom AND NOT norm.disabled) AS default_enabled,
    count(*) FILTER (WHERE NOT norm.is_custom AND norm.disabled) AS default_disabled
   FROM norm
  GROUP BY norm.snapshot_id;


-- repmeta_qs.v_security_rule_summary source

CREATE OR REPLACE VIEW repmeta_qs.v_security_rule_summary
AS SELECT s.snapshot_id,
    count(*) FILTER (WHERE r.rule_id IS NOT NULL) AS total_rules,
    count(*) FILTER (WHERE r.rule_id IS NOT NULL AND NOT r.is_default AND NOT r.is_readonly) AS custom_rules,
    count(*) FILTER (WHERE r.is_readonly) AS readonly_rules,
    count(*) FILTER (WHERE r.is_default) AS default_rules,
    count(*) FILTER (WHERE r.disabled) AS disabled_rules
   FROM repmeta_qs.snapshot s
     LEFT JOIN repmeta_qs.v_system_rules r ON r.snapshot_id = s.snapshot_id
  GROUP BY s.snapshot_id;


-- repmeta_qs.v_streams source

CREATE OR REPLACE VIEW repmeta_qs.v_streams
AS SELECT s.snapshot_id,
    s.stream_id,
    s.data,
    COALESCE(s.data ->> 'name'::text, s.data ->> 'streamName'::text, s.data ->> 'displayName'::text) AS stream_name
   FROM repmeta_qs.streams s;


-- repmeta_qs.v_system_rules source

CREATE OR REPLACE VIEW repmeta_qs.v_system_rules
AS SELECT r.snapshot_id,
    r.rule_id,
    r.data,
    COALESCE(r.data ->> 'name'::text, r.data ->> 'ruleName'::text) AS rule_name,
    lower(COALESCE(r.data ->> 'disabled'::text, r.data ->> 'isDisabled'::text)) = 'true'::text AS disabled,
    lower(COALESCE(r.data ->> 'isReadOnly'::text, r.data ->> 'readOnly'::text)) = 'true'::text AS is_readonly,
    lower(COALESCE(r.data ->> 'isDefault'::text, r.data ->> 'default'::text)) = 'true'::text AS is_default
   FROM repmeta_qs.system_rules r;


-- repmeta_qs.v_users source

CREATE OR REPLACE VIEW repmeta_qs.v_users
AS SELECT u.snapshot_id,
    u.user_id,
    u.data,
    COALESCE(u.data ->> 'name'::text, u.data ->> 'userName'::text, u.data ->> 'userId'::text) AS user_name,
    COALESCE(u.data ->> 'userDirectory'::text, u.data ->> 'directory'::text, u.data ->> 'user_directory'::text) AS user_directory
   FROM repmeta_qs.users u;



-- DROP FUNCTION repmeta_qs.rx_int(text, text);

CREATE OR REPLACE FUNCTION repmeta_qs.rx_int(src text, pat text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT NULLIF((regexp_match(src, pat))[1],'')::int
$function$
;

-- DROP FUNCTION repmeta_qs.rx_text(text, text);

CREATE OR REPLACE FUNCTION repmeta_qs.rx_text(src text, pat text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT (regexp_match(src, pat))[1]
$function$
;