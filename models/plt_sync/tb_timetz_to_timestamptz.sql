{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

{{ plt_sync_select('tb_TIMETZ_to_TIMESTAMPTZ') }}
