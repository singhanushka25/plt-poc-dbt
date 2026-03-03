{% macro evolve_final_table(target, unified_columns) %}
  {#
    Schema evolution for the PLT final table.

    Compares unified_columns (resolved across all sources) against what currently
    exists in the final table. For every column that is in unified_columns but
    missing from the final table, issues ALTER TABLE ... ADD COLUMN.

    Only runs in incremental mode — on first run DBT creates the table from the
    SELECT schema, so all columns from the unified schema are already included.

    Narrowing (a resolved type is narrower than the current final column type)
    is NOT handled here — that requires a CONFLICT alert and is out of scope
    for the POC.

    target:          DBT relation (this) pointing at the final table
    unified_columns: dict of {COLUMN_NAME: DATA_TYPE} from resolve_column_schema
  #}
  {% if is_incremental() %}
    {% for col_name, col_type in unified_columns.items() %}
      {% set check_query %}
        SELECT COUNT(*) AS cnt
        FROM {{ target.database }}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = UPPER('{{ target.schema }}')
          AND TABLE_NAME   = UPPER('{{ target.identifier }}')
          AND COLUMN_NAME  = UPPER('{{ col_name }}')
      {% endset %}

      {% set result = run_query(check_query) %}

      {% if execute and result.columns[0].values()[0] == 0 %}
        {% do run_query(
          "ALTER TABLE " ~ target ~ " ADD COLUMN " ~ col_name ~ " " ~ col_type
        ) %}
        {{ log(
          "PLT evolve_final_table: added " ~ col_name ~ " (" ~ col_type ~ ") to " ~ target,
          info=True
        ) }}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
