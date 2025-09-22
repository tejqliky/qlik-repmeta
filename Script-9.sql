-- 00_schema_plus_core.sql
create schema if not exists repmeta;

create table if not exists repmeta.ingest_run (
  run_id        bigserial primary key,
  uploaded_at   timestamptz not null default now(),
  uploaded_by   text,
  file_name     text not null,
  file_size     integer
);

create table if not exists repmeta.task (
  id                         bigserial primary key,
  run_id                     bigint not null references repmeta.ingest_run(run_id) on delete cascade,
  name                       text not null,
  source_name                text,
  task_type                  text,
  description                text,
  status_table               text,
  suspended_tables_table     text,
  task_uuid                  text,
  raw                        jsonb not null,
  unique (run_id, name)
);

create table if not exists repmeta.task_target (
  id            bigserial primary key,
  run_id        bigint not null references repmeta.ingest_run(run_id) on delete cascade,
  task_name     text not null,
  target_name   text not null,
  target_state  text,
  database_name text
);

create table if not exists repmeta.source_table (
  id             bigserial primary key,
  run_id         bigint not null references repmeta.ingest_run(run_id) on delete cascade,
  task_name      text not null,
  owner          text not null,
  table_name     text not null,
  estimated_size bigint,
  orig_db_id     bigint
);

create table if not exists repmeta.feature_flag_value (
  id         bigserial primary key,
  run_id     bigint not null references repmeta.ingest_run(run_id) on delete cascade,
  task_name  text not null,
  name       text not null,
  value      text
);

-- If you already created unknown_field earlier, skip / adjust to match your version
create table if not exists repmeta.unknown_field (
  id        bigserial primary key,
  run_id    bigint not null references repmeta.ingest_run(run_id) on delete cascade,
  entity    text not null,
  json_path text not null,
  value     jsonb
);

-- Handy view for UI “unknown key” bubbles
create or replace view repmeta.v_unknown_counts as
select run_id, entity, count(*) as unknown_key_count
from repmeta.unknown_field
group by run_id, entity;



ALTER TABLE repmeta.ingest_run ADD COLUMN uploaded_by TEXT;