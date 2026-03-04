{% macro generate_source_select(src, unified_columns) %}
  {#
    Builds a SELECT from one source temp table aligned to the unified schema.

    - Columns present in this source:  selected as-is.
    - Columns missing from this source: emitted as NULL::TYPE.
    - Appends __hevo_source_pipeline label to identify the originating pipeline.

    src: {database, schema, table, label}
    unified_columns: dict of {COLUMN_NAME: DATA_TYPE} from resolve_column_schema
  #}
  {% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}
  {% set src_col_names = adapter.get_columns_in_relation(rel) | map(attribute='name') | map('upper') | list %}

  SELECT
  {% for col_name, col_type in unified_columns.items() %}
    {% if col_name | upper in src_col_names %}
    {{ col_name }},
    {% else %}
    NULL::{{ col_type }} AS {{ col_name }},
    {% endif %}
  {% endfor %}
    '{{ src.label }}' AS __hevo_source_pipeline
  FROM {{ src.database }}.{{ src.schema }}.{{ src.table }}
{% endmacro %}
