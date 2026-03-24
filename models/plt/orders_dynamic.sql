{{
  config(
    materialized='incremental',
    unique_key=['id', '_dbt_source_relation'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

{% if execute %}
{# Discover all schemas that have an ORDERS table #}
{% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern='%_STAGING',
    table_pattern='ORDERS',
    database=var('plt_database')
) %}

{% if relations | length == 0 %}
  {{ exceptions.raise_compiler_error("PLT: No source schemas found matching pattern") }}
{% endif %}

{{ dbt_utils.union_relations(
    relations=relations,
    source_column_name='_dbt_source_relation'
) }}
{% else %}
SELECT NULL
{% endif %}
