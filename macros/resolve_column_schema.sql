{% macro resolve_column_schema(sources) %}
  {#
    Computes the unified column schema across all source temp tables using
    pairwise LCA reduction via get_widened_type.

    For columns appearing in multiple sources, the type is resolved by calling
    get_widened_type(existing_type, new_type) iteratively. Because get_widened_type
    is associative, one left-to-right pass yields the full LCA result.

    Columns whose names start with '__HEVO__' are skipped (Hevo internal columns).
    The synthetic __HEVO_SOURCE_PIPELINE column is also excluded.

    sources: list of {database, schema, table, label}
    returns: dict of {COLUMN_NAME_UPPER: DATA_TYPE}
  #}
  {% set unified = {} %}

  {% for src in sources %}
    {% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}
    {% if rel %}
      {% for col in adapter.get_columns_in_relation(rel) %}
        {% set col_upper = col.name | upper %}

        {# Skip Hevo-internal columns #}
        {% if col_upper.startswith('__HEVO__') or col_upper == '__HEVO_SOURCE_PIPELINE' %}
          {% continue %}
        {% endif %}

        {% if col_upper in unified %}
          {# Pairwise LCA: resolve existing type vs incoming type #}
          {% set wider = get_widened_type(unified[col_upper], col.dtype) %}
          {% do unified.update({col_upper: wider}) %}
        {% else %}
          {% do unified.update({col_upper: col.dtype}) %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endfor %}

  {{ return(unified) }}
{% endmacro %}
