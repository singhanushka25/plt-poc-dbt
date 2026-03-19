{% macro plt_generate_union(sources, unified_schema, source_columns) %}
  {#
    Generates a UNION ALL SELECT across all sources, casting to unified types
    and NULL-padding missing columns. Uses TRY_CAST for resilient casting.

    sources:        list of {database, schema, table, label}
    unified_schema: {COL_NAME: {full_type, is_nullable, sources}} from plt_resolve_schema
    source_columns: {label: {COL_NAME: TYPE}} from plt_resolve_schema (avoids redundant introspection)
  #}
  {% set ns = namespace(first=true) %}

  {% for src in sources %}
    {# Skip sources that don't exist (not in source_columns) #}
    {% if src.label not in source_columns %}
      {% continue %}
    {% endif %}

    {% set src_cols = source_columns[src.label] %}

    {% if not ns.first %}
UNION ALL
    {% endif %}
    {% set ns.first = false %}

SELECT
    {% for col_name, meta in unified_schema.items() %}
      {% set col_upper = col_name | upper %}
      {% if col_upper in src_cols %}
        {% set src_type = src_cols[col_upper] | upper | replace(' ', '') %}
        {% set unified_norm = meta['full_type'] | upper | replace(' ', '') %}
        {% set tgt_base = unified_norm.split('(')[0] %}
        {% set src_base = src_type.split('(')[0] %}
        {% set string_bases = ['VARCHAR', 'TEXT', 'STRING', 'CHARACTERVARYING'] %}
        {% set is_tgt_string = tgt_base in string_bases %}
        {% set is_src_string = src_base in string_bases %}
        {% if src_type == unified_norm %}
  {{ col_name }},
        {% elif is_tgt_string and not is_src_string %}
  TO_VARCHAR({{ col_name }}) AS {{ col_name }},
        {% else %}
  TRY_CAST({{ col_name }} AS {{ meta['full_type'] }}) AS {{ col_name }},
        {% endif %}
      {% else %}
  NULL::{{ meta['full_type'] }} AS {{ col_name }},
      {% endif %}
    {% endfor %}
  '{{ src.label }}' AS __hevo_source_pipeline
FROM {{ src.database }}.{{ src.schema }}.{{ src.table }}
  {% endfor %}
{% endmacro %}
