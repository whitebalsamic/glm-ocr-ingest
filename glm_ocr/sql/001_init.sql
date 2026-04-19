create schema if not exists __DB_SCHEMA__;

create table if not exists __DB_SCHEMA__.ocr_runs (
    run_id uuid primary key,
    status text not null,
    provider_name text not null,
    provider_version text not null,
    canonical_schema_version integer not null,
    artifact_manifest_version integer not null,
    provider_contract_version integer not null,
    started_at timestamptz not null,
    finished_at timestamptz not null,
    settings_json jsonb not null,
    execution_context_json jsonb not null,
    warnings_json jsonb not null default '[]'::jsonb,
    aggregate_counts_json jsonb not null default '{}'::jsonb
);

create table if not exists __DB_SCHEMA__.ocr_documents (
    document_id uuid primary key,
    sha256 text not null unique,
    display_name text not null,
    logical_source_id text not null,
    byte_size bigint not null,
    mime_type text,
    file_extension text,
    source_metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null
);

create table if not exists __DB_SCHEMA__.ocr_results (
    result_id uuid primary key,
    run_id uuid not null references __DB_SCHEMA__.ocr_runs(run_id),
    document_id uuid not null references __DB_SCHEMA__.ocr_documents(document_id),
    provider_name text not null,
    provider_version text not null,
    provider_metadata jsonb not null default '{}'::jsonb,
    settings_hash text not null,
    started_at timestamptz,
    finished_at timestamptz,
    canonical_result jsonb not null,
    raw_provider_payload jsonb not null,
    artifact_refs jsonb not null default '[]'::jsonb,
    provenance jsonb not null default '{}'::jsonb,
    warnings_json jsonb not null default '[]'::jsonb
);

create table if not exists __DB_SCHEMA__.ocr_pages (
    page_id uuid primary key,
    result_id uuid not null references __DB_SCHEMA__.ocr_results(result_id),
    page_index integer not null,
    region_count integer not null,
    page_json jsonb not null,
    unique (result_id, page_index)
);

create table if not exists __DB_SCHEMA__.ocr_regions (
    region_id uuid primary key,
    result_id uuid not null references __DB_SCHEMA__.ocr_results(result_id),
    page_id uuid not null references __DB_SCHEMA__.ocr_pages(page_id),
    page_index integer not null,
    region_index integer not null,
    label text not null,
    native_label text,
    content text not null,
    bbox_2d jsonb,
    polygon jsonb,
    region_json jsonb not null,
    unique (result_id, page_index, region_index)
);

create index if not exists idx_ocr_documents_sha256 on __DB_SCHEMA__.ocr_documents (sha256);
create index if not exists idx_ocr_results_run_id on __DB_SCHEMA__.ocr_results (run_id);
create index if not exists idx_ocr_results_document_id on __DB_SCHEMA__.ocr_results (document_id);
create index if not exists idx_ocr_results_grouping
    on __DB_SCHEMA__.ocr_results (document_id, provider_name, settings_hash);
