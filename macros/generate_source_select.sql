{% macro generate_source_select(src, unified_columns) %}
  {#
    Builds a SELECT from one source temp table aligned to the unified schema.

    - Columns missing from this source:              NULL::TYPE AS col
    - Columns present, type matches unified type:    col  (as-is)
    - Columns present, type differs from unified:    CAST(col AS unified_type) AS col

    The explicit CAST ensures that type-conflict columns (e.g. FLOAT resolved to
    VARCHAR(256)) land in the final table with the correct unified type rather than
    inheriting the source type via Snowflake's UNION ALL inference.

    src: {database, schema, table, label}
    unified_columns: dict of {COLUMN_NAME: DATA_TYPE} from resolve_column_schema
  #}
  {% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}

  {# Build a name→dtype map for columns actually in this source #}
  {% set src_col_map = {} %}
  {% for col in adapter.get_columns_in_relation(rel) %}
    {% do src_col_map.update({col.name | upper: col.dtype | upper | replace(' ', '')}) %}
  {% endfor %}

  SELECT
  {% for col_name, col_type in unified_columns.items() %}
    {% set col_upper = col_name | upper %}
    {% if col_upper in src_col_map %}
      {% set src_type = src_col_map[col_upper] %}
      {% set unified_norm = col_type | upper | replace(' ', '') %}
      {% if src_type != unified_norm %}
    CAST({{ col_name }} AS {{ col_type }}) AS {{ col_name }},
      {% else %}
    {{ col_name }},
      {% endif %}
    {% else %}
    NULL::{{ col_type }} AS {{ col_name }},
    {% endif %}
  {% endfor %}
    '{{ src.label }}' AS __hevo_source_pipeline
  FROM {{ src.database }}.{{ src.schema }}.{{ src.table }}
{% endmacro %}
