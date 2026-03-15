{% macro generate_source_select(src, unified_columns, source_columns) %}
  {#
    Builds a SELECT from one source table aligned to the unified schema.

    - Columns present in this source:  selected as-is.
    - Columns missing from this source: emitted as NULL::TYPE.
    - Appends __hevo_source_pipeline to identify the originating pipeline.

    src:             {database, schema, table, label}
    unified_columns: {COL_NAME: DATA_TYPE} from resolve_column_schema
    source_columns:  {label: {COL_NAME: DATA_TYPE}} from resolve_column_schema
  #}
  {% set src_cols = source_columns[src.label] %}

  SELECT
  {% for col_name, col_type in unified_columns.items() %}
    {% if col_name in src_cols %}
    {{ col_name }},
    {% else %}
    NULL::{{ col_type }} AS {{ col_name }},
    {% endif %}
  {% endfor %}
    '{{ src.label }}' AS __hevo_source_pipeline
  FROM {{ src.database }}.{{ src.schema }}.{{ src.table }}
{% endmacro %}
