{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
  )
}}

{% if execute %}
{% set plt_db = var('plt_database') %}
{% set plt_table = var('plt_table') %}
{% set src_schema = var('plt_sources')[0].schema %}

SELECT
  id,
  name,
  email,
  {{ plt_md5_hash('email') }} AS email_hashed
FROM {{ api.Relation.create(database=plt_db, schema=src_schema, identifier=plt_table) }}
{% else %}
SELECT NULL
{% endif %}
