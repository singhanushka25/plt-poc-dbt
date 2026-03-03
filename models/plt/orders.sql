{#
  PLT merge model for the shared `orders` table.

  Sources are passed in at runtime via --vars:
    k1_db            Snowflake database where both temp schemas live
    k1_schema        Temp schema written by pipeline k1
    k1_prime_schema  Temp schema written by pipeline k1_prime
    k1_table         Table name (same in both schemas, e.g. ORDERS)

  What this model does:
    1. resolve_column_schema  — inspects INFORMATION_SCHEMA on both temp tables
                                and computes the union of all columns.
    2. evolve_final_table     — for each column in the union that is missing
                                from PLT_FINAL.orders, issues ALTER TABLE ADD COLUMN.
    3. generate_source_select — builds a SELECT per source, NULL-padding any
                                column that the source does not have.
    4. UNION ALL              — combines the per-source SELECTs.
    5. MERGE (DBT built-in)   — DBT's incremental materialization wraps the
                                UNION ALL in a Snowflake MERGE INTO statement
                                keyed on (id, __hevo_source_pipeline).

  To add a new source pipeline in the future, append one dict to plt_sources.
  The macros handle everything else dynamically.
#}

{% set plt_sources = [
  {
    'database': var('k1_db'),
    'schema':   var('k1_schema'),
    'table':    var('k1_table'),
    'label':    'k1'
  },
  {
    'database': var('k1_db'),
    'schema':   var('k1_prime_schema'),
    'table':    var('k1_table'),
    'label':    'k1_prime'
  }
] %}

{{
  config(
    materialized         = 'incremental',
    unique_key           = ['id', '__hevo_source_pipeline'],
    incremental_strategy = 'merge'
  )
}}

{# Step 1: Resolve the unified column schema across all source temp tables #}
{% set unified_cols = resolve_column_schema(plt_sources) %}

{# Step 2: Evolve the final table — ADD COLUMN for anything new in the sources #}
{% do evolve_final_table(this, unified_cols) %}

{# Step 3 + 4: UNION ALL — each source SELECT is NULL-padded to the unified schema #}
{% for src in plt_sources %}
{{ generate_source_select(src, unified_cols) }}
{%- if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
