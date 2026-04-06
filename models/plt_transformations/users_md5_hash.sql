{{
  config(
    materialized='incremental',
    unique_key=['id', '_dbt_source_relation'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

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

WITH unioned AS (
  {{ dbt_utils.union_relations(
      relations=relations,
      source_column_name='_dbt_source_relation'
  ) }}
)

SELECT
  id,
  name,
  email,
  {{ plt_md5_hash('email') }} AS email_hashed,
  _dbt_source_relation
FROM unioned
{% else %}
SELECT NULL
{% endif %}
