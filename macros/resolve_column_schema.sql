{% macro resolve_column_schema(sources) %}
  {#
    Introspects all source tables via dbt adapter and computes the unified
    column schema (union of all columns). First-seen type wins — no widening.

    Skips sources whose table does not exist yet (e.g. pipeline not loaded).

    Input:  list of {database, schema, table, label}
    Output: dict with two keys:
      - unified:        {COL_NAME: DATA_TYPE}
      - source_columns: {label: {COL_NAME: DATA_TYPE}}
  #}
  {% set unified = {} %}
  {% set source_columns = {} %}

  {% for src in sources %}
    {% set rel = adapter.get_relation(
      database=src.database, schema=src.schema, identifier=src.table
    ) %}
    {% if rel is none %}
      {{ log("PLT resolve_schema: source " ~ src.label ~ " does not exist, skipping", info=True) }}
      {% continue %}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(rel) %}

    {# Build per-source column map (reused by generate_source_select) #}
    {% set src_col_map = {} %}
    {% for col in columns %}
      {% if col.name | upper != '__HEVO_SOURCE_PIPELINE' %}
        {% do src_col_map.update({col.name | upper: col.dtype | upper}) %}
      {% endif %}
    {% endfor %}
    {% do source_columns.update({src.label: src_col_map}) %}

    {# Merge into unified — first-seen type wins #}
    {% for col_name, col_type in src_col_map.items() %}
      {% if col_name not in unified %}
        {% do unified.update({col_name: col_type}) %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {{ return({'unified': unified, 'source_columns': source_columns}) }}
{% endmacro %}
