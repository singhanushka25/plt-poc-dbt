{{
  config(
    materialized='incremental',
    unique_key=['id', '_dbt_source_relation'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

{{ plt_datatype_change_union('TB_TIME_TO_INT') }}
