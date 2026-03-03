{% macro resolve_column_schema(sources) %}
  {#
    Computes the union of columns across all source temp tables.
    Returns a dict of {COLUMN_NAME: DATA_TYPE}.

    When the same column appears in multiple sources, the first-seen type wins.
    Full type widening (via LCA on the Snowflake hierarchy) will replace this
    once get_widened_type is implemented.

    sources: list of {database, schema, table, label}
  #}
  {% set unified = {} %}

  {% for src in sources %}
    {% set cols = get_source_columns(src.database, src.schema, src.table) %}
    {% for col in cols %}
      {% if col.name | upper not in unified %}
        {% do unified.update({col.name | upper: col.type}) %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {{ return(unified) }}
{% endmacro %}
