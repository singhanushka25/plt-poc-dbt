{% macro plt_resolve_schema(sources) %}
  {#
    Introspects all source tables and computes a unified schema using
    pairwise LCA via get_widened_type. Also queries INFORMATION_SCHEMA
    for nullability (adapter doesn't provide it).

    Input:  list of {database, schema, table, label}
    Output: dict with two keys:
      - unified:        {COL_NAME: {full_type, is_nullable, sources}}
      - source_columns: {label: {COL_NAME: TYPE}}  (reused by plt_generate_union)
  #}
  {% set unified = {} %}
  {% set source_columns = {} %}
  {% set active_sources = [] %}

  {% for src in sources %}
    {% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}
    {% if rel is none %}
      {{ log("PLT resolve_schema: source " ~ src.label ~ " does not exist, skipping", info=True) }}
      {% continue %}
    {% endif %}
    {% do active_sources.append(src.label) %}

    {# ── Column types via dbt adapter ── #}
    {% set columns = adapter.get_columns_in_relation(rel) %}

    {# ── Nullability via INFORMATION_SCHEMA (one query per source) ── #}
    {% set nullable_query %}
      SELECT COLUMN_NAME, IS_NULLABLE
      FROM {{ src.database }}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = UPPER('{{ src.schema }}')
        AND TABLE_NAME   = UPPER('{{ src.table }}')
    {% endset %}
    {% set nullable_result = run_query(nullable_query) %}
    {% set nullable_map = {} %}
    {% if execute %}
      {% for row in nullable_result %}
        {% do nullable_map.update({row[0] | upper: (row[1] == 'YES')}) %}
      {% endfor %}
    {% endif %}

    {# ── Build per-source column map (reused by plt_generate_union) ── #}
    {% set src_col_map = {} %}
    {% for col in columns %}
      {% if col.name | upper != '__HEVO_SOURCE_PIPELINE' %}
        {% do src_col_map.update({col.name | upper: col.dtype | upper}) %}
      {% endif %}
    {% endfor %}
    {% do source_columns.update({src.label: src_col_map}) %}

    {# ── Merge each column into unified ── #}
    {% for col in columns %}
      {% set col_name = col.name | upper %}
      {# Skip __HEVO_SOURCE_PIPELINE — we synthesize it in plt_generate_union #}
      {% if col_name == '__HEVO_SOURCE_PIPELINE' %}
        {% continue %}
      {% endif %}
      {% set col_type = col.dtype | upper %}
      {% set is_nullable = nullable_map[col_name] if col_name in nullable_map else true %}

      {% if col_name not in unified %}
        {# First occurrence — seed #}
        {% do unified.update({col_name: {
          'full_type': col_type,
          'is_nullable': is_nullable,
          'sources': [src.label]
        }}) %}
      {% else %}
        {# Pairwise LCA #}
        {% set existing = unified[col_name] %}
        {% set widened = get_widened_type(existing['full_type'], col_type) %}
        {% do existing.update({
          'full_type': widened,
          'is_nullable': existing['is_nullable'] or is_nullable
        }) %}
        {% do existing['sources'].append(src.label) %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {# ── Post-processing: columns missing from some sources → force nullable ── #}
  {% for col_name, meta in unified.items() %}
    {% if meta['sources'] | length < active_sources | length %}
      {% do meta.update({'is_nullable': true}) %}
    {% endif %}
  {% endfor %}

  {{ return({'unified': unified, 'source_columns': source_columns}) }}
{% endmacro %}
