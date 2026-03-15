{% macro evolve_final_table(target, unified_columns) %}
  {#
    Schema evolution for the PLT final table.

    Compares unified_columns against the final table's current columns.
    Adds any column present in unified but missing from the final table.

    Only runs in incremental mode — first run creates the table from SELECT.

    target:          dbt relation (this) pointing at the final table
    unified_columns: dict of {COLUMN_NAME: DATA_TYPE} from resolve_column_schema
  #}
  {% if is_incremental() %}
    {% set final_columns = adapter.get_columns_in_relation(target) %}
    {% set final_col_names = final_columns | map(attribute='name') | map('upper') | list %}

    {% for col_name, col_type in unified_columns.items() %}
      {% if col_name | upper not in final_col_names %}
        {% do run_query(
          "ALTER TABLE " ~ target ~ " ADD COLUMN " ~ col_name ~ " " ~ col_type
        ) %}
        {{ log(
          "PLT evolve_final_table: added " ~ col_name ~ " (" ~ col_type ~ ")",
          info=True
        ) }}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
