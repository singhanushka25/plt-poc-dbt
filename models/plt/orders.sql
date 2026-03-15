{#
  PLT merge model for the shared `orders` table.

  Sources are passed in at runtime via --vars:
    k1_db            Snowflake database where both temp schemas live
    k1_schema        Temp schema written by pipeline k1
    k1_prime_schema  Temp schema written by pipeline k1_prime
    k1_table         Table name (same in both schemas, e.g. ORDERS)

  What this model does:
    1. resolve_column_schema  — introspects source tables via dbt adapter,
                                computes the union of all columns (first type wins).
    2. evolve_final_table     — ADD COLUMN for anything new in the sources.
    3. generate_source_select — builds a SELECT per source, NULL-padding any
                                column that the source does not have.
    4. UNION ALL + MERGE      — dbt incremental merge keyed on (id, pipeline).
#}

{% set plt_sources = [
  {'database': var('k1_db'), 'schema': var('k1_schema'),       'table': var('k1_table'), 'label': 'k1'},
  {'database': var('k1_db'), 'schema': var('k1_prime_schema'), 'table': var('k1_table'), 'label': 'k1_prime'}
] %}

{{ config(
    materialized='incremental',
    unique_key=['id', '__hevo_source_pipeline'],
    incremental_strategy='merge'
) }}

{# Step 1: Resolve unified schema across all sources #}
{% set result = resolve_column_schema(plt_sources) %}
{% set unified_cols = result['unified'] %}
{% set source_cols = result['source_columns'] %}

{# Guard: no sources exist yet (parse phase or pre-load) #}
{% if execute and unified_cols | length == 0 %}
  {{ exceptions.raise_compiler_error("PLT: No active sources found — cannot generate model.") }}
{% endif %}

{# Step 2: Evolve final table — ADD COLUMN for new columns #}
{% if is_incremental() %}
  {% do evolve_final_table(this, unified_cols) %}
{% endif %}

{# Step 3 + 4: UNION ALL — each source SELECT is NULL-padded to unified schema #}
{% if unified_cols | length > 0 %}
  {% set ns = namespace(first=true) %}
  {% for src in plt_sources %}
    {% if src.label in source_cols %}
      {% if not ns.first %}
UNION ALL
      {% endif %}
      {% set ns.first = false %}
{{ generate_source_select(src, unified_cols, source_cols) }}
    {% endif %}
  {% endfor %}
{% else %}
SELECT NULL AS _plt_placeholder WHERE FALSE
{% endif %}
