{#
  PLT merge model for the shared `orders` table.

  Sources are passed in at runtime via --vars:
    k1_db            Snowflake database where both temp schemas live
    k1_schema        Temp schema written by pipeline k1
    k1_prime_schema  Temp schema written by pipeline k1_prime
    k1_table         Table name (same in both schemas, e.g. ORDERS)

  What this model does (v3 macro engine):
    1. plt_resolve_schema   — introspects all source schemas via adapter +
                              INFORMATION_SCHEMA, computes unified LCA types
    2. plt_evolve_table     — DDL to align final table with unified schema
                              (soft-drop, column swap, NOT NULL management)
    3. plt_generate_union   — UNION ALL with TRY_CAST + NULL-pad
    4. MERGE (dbt built-in) — incremental merge handles INSERT/UPDATE

  To add a new source pipeline in the future, append one dict to plt_sources.
  The macros handle everything else dynamically.
#}

{{ config(
    materialized='incremental',
    unique_key=['id', '__hevo_source_pipeline'],
    incremental_strategy='merge'
) }}

{% set plt_sources = [
  {'database': var('k1_db'), 'schema': var('k1_schema'), 'table': var('k1_table'), 'label': 'k1'},
  {'database': var('k1_db'), 'schema': var('k1_prime_schema'), 'table': var('k1_table'), 'label': 'k1_prime'}
] %}

{# Step 1: Resolve unified schema across all sources #}
{% set result = plt_resolve_schema(plt_sources) %}
{% set unified = result['unified'] %}
{% set source_columns = result['source_columns'] %}

{# Guard: abort if no sources exist #}
{% if unified | length == 0 %}
  {{ exceptions.raise_compiler_error("PLT: No active sources found — cannot generate model.") }}
{% endif %}

{# Step 2: Evolve final table DDL (incremental only — first run creates from SELECT) #}
{% if is_incremental() %}
  {% do plt_evolve_table(this, unified) %}
{% endif %}

{# Step 3: Generate UNION ALL with TRY_CAST + NULL-pad #}
{{ plt_generate_union(plt_sources, unified, source_columns) }}
