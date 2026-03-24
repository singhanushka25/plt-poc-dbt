{% macro plt_datatype_change_union(table_name) %}
{% if execute %}
{% set sources = var('plt_sources') %}
{% set plt_db = var('plt_database') %}

{% set relations = [] %}
{% for src in sources %}
  {% do relations.append(
    api.Relation.create(database=plt_db, schema=src.schema, identifier=table_name)
  ) %}
{% endfor %}

{{ dbt_utils.union_relations(
    relations=relations,
    source_column_name='_dbt_source_relation'
) }}
{% else %}
SELECT NULL
{% endif %}
{% endmacro %}
