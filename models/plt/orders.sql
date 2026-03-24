{{
  config(
    materialized='incremental',
    unique_key=['id', '_dbt_source_relation'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

{# Build relation list from vars #}
{% if execute %}
{% set sources = var('plt_sources') %}
{% set plt_db = var('plt_database') %}
{% set plt_table = var('plt_table') %}

{% set relations = [] %}
{% for src in sources %}
  {% do relations.append(
    api.Relation.create(database=plt_db, schema=src.schema, identifier=plt_table)
  ) %}
{% endfor %}

{{ dbt_utils.union_relations(
    relations=relations,
    source_column_name='_dbt_source_relation'
) }}
{% else %}
SELECT NULL
{% endif %}
