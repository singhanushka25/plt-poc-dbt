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
    {% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}
    {% if rel %}
      {% for col in adapter.get_columns_in_relation(rel) %}
        {% if col.name | upper not in unified and col.name | upper != '__HEVO_SOURCE_PIPELINE' %}
          {% do unified.update({col.name | upper: col.dtype}) %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endfor %}

  {{ return(unified) }}
{% endmacro %}
