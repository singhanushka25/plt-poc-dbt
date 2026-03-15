{#
  PLT merge model for the shared `orders` table.

  Sources are passed in at runtime via --vars:
    k1_db            Snowflake database where both temp schemas live
    k1_schema        Temp schema written by pipeline k1
    k1_prime_schema  Temp schema written by pipeline k1_prime
    k1_table         Table name (same in both schemas, e.g. ORDERS)

  Flow:
    1. resolve_column_schema  — introspects sources, builds unified + per-source column maps
    2. evolve_final_table     — ADD COLUMN for anything new in the sources
    3. Read final table schema — the source of truth for SELECT generation
    4. generate_source_select — SELECT based on final table columns, NULL-pad per source
    5. UNION ALL + MERGE      — dbt incremental merge keyed on (id, pipeline)
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

{# Step 3: Read final table schema (source of truth for SELECTs) #}
{# On first run the table doesn't exist yet — fall back to unified #}
{% if is_incremental() %}
  {% set final_rel = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% set final_columns = adapter.get_columns_in_relation(final_rel) %}
  {% set select_schema = {} %}
  {% for col in final_columns %}
    {% if col.name | upper != '__HEVO_SOURCE_PIPELINE' %}
      {% do select_schema.update({col.name | upper: col.dtype | upper}) %}
    {% endif %}
  {% endfor %}
{% else %}
  {% set select_schema = unified_cols %}
{% endif %}

{# Step 4: UNION ALL — each source SELECT is NULL-padded to final table schema #}
{% if select_schema | length > 0 %}
  {% set ns = namespace(first=true) %}
  {% for src in plt_sources %}
    {% if src.label in source_cols %}
      {% if not ns.first %}
UNION ALL
      {% endif %}
      {% set ns.first = false %}
{{ generate_source_select(src, select_schema, source_cols) }}
    {% endif %}
  {% endfor %}
{% else %}
SELECT NULL AS _plt_placeholder WHERE FALSE
{% endif %}
