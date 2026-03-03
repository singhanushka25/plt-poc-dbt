{% macro get_source_columns(database, schema, table) %}
  {#
    Returns a list of {name, type} dicts for every column in the given
    Snowflake table, excluding the PLT-internal __hevo_source_pipeline column
    (which is added by this project, not by the loader).

    Called by: resolve_column_schema, generate_source_select
  #}
  {% set query %}
    SELECT COLUMN_NAME, DATA_TYPE
    FROM {{ database }}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER('{{ schema }}')
      AND TABLE_NAME   = UPPER('{{ table }}')
      AND COLUMN_NAME != UPPER('__HEVO_SOURCE_PIPELINE')
    ORDER BY ORDINAL_POSITION
  {% endset %}

  {% if execute %}
    {% set result = run_query(query) %}
    {% set columns = [] %}
    {% for row in result.rows %}
      {% do columns.append({'name': row[0], 'type': row[1]}) %}
    {% endfor %}
    {{ return(columns) }}
  {% endif %}

  {{ return([]) }}
{% endmacro %}
