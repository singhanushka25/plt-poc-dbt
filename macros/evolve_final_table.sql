{% macro evolve_final_table(target, unified_columns) %}
  {#
    Schema evolution for the PLT final table.

    In incremental mode, compares unified_columns against what currently
    exists in the final table and:
      1. ADDs columns that are missing from the final table.
      2. ALTERs columns whose unified type is wider than the current type.
      3. Skips (logs) columns whose unified type would be a narrowing — never narrows.

    Only runs in incremental mode. On the first run DBT creates the table from
    the SELECT, so all columns from the unified schema are already included.

    target:          DBT relation (this) pointing at the final table
    unified_columns: dict of {COLUMN_NAME: DATA_TYPE} from resolve_column_schema
  #}
  {% if is_incremental() %}

    {# ── Fetch current column types from INFORMATION_SCHEMA ──────── #}
    {% set schema_query %}
      SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
      FROM {{ target.database }}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = UPPER('{{ target.schema }}')
        AND TABLE_NAME   = UPPER('{{ target.identifier }}')
    {% endset %}

    {% set schema_result = run_query(schema_query) %}
    {% set current_cols = {} %}
    {% if execute %}
      {% for row in schema_result %}
        {# Reconstruct a type string comparable to what get_widened_type returns #}
        {% set col_name = row[0] | upper %}
        {% set base     = row[1] | upper %}
        {% if base in ('NUMBER', 'NUMERIC', 'DECIMAL') and row[2] is not none %}
          {% set type_str = base ~ '(' ~ row[2] ~ ',' ~ (row[3] if row[3] is not none else 0) ~ ')' %}
        {% elif base in ('TEXT', 'VARCHAR', 'CHARACTER VARYING') and row[4] is not none %}
          {% set type_str = 'VARCHAR(' ~ row[4] ~ ')' %}
        {% else %}
          {% set type_str = base %}
        {% endif %}
        {% do current_cols.update({col_name: type_str}) %}
      {% endfor %}
    {% endif %}

    {# ── Evolve: add missing columns, widen types, skip narrowing ── #}
    {% for col_name, col_type in unified_columns.items() %}
      {% set col_upper = col_name | upper %}

      {% if col_upper not in current_cols %}
        {# Column missing from final table — ADD it #}
        {% do run_query(
          "ALTER TABLE " ~ target ~ " ADD COLUMN " ~ col_name ~ " " ~ col_type
        ) %}
        {{ log(
          "PLT evolve_final_table: added " ~ col_name ~ " (" ~ col_type ~ ") to " ~ target,
          info=True
        ) }}

      {% else %}
        {# Column exists — check if unified type is wider #}
        {% set current_type = current_cols[col_upper] %}
        {% set wider = get_widened_type(current_type, col_type) %}

        {% if wider | upper != current_type | upper %}
          {# Unified type is wider — ALTER the column #}
          {% do run_query(
            "ALTER TABLE " ~ target
            ~ " ALTER COLUMN " ~ col_name
            ~ " SET DATA TYPE " ~ wider
          ) %}
          {{ log(
            "PLT evolve_final_table: widened " ~ col_name
            ~ " from " ~ current_type ~ " to " ~ wider ~ " in " ~ target,
            info=True
          ) }}
        {% elif get_widened_type(col_type, current_type) | upper != col_type | upper %}
          {# current_type is already wider than unified — narrowing detected, skip #}
          {{ log(
            "PLT evolve_final_table: skipping narrowing of " ~ col_name
            ~ " (current=" ~ current_type ~ ", unified=" ~ col_type ~ ")",
            info=True
          ) }}
        {# else: types are equal — nothing to do #}
        {% endif %}

      {% endif %}
    {% endfor %}

  {% endif %}
{% endmacro %}
