{% macro plt_evolve_table(target, unified_schema) %}
  {#
    Compares unified "desired" schema against the current final table.
    Executes DDL to align. Only meaningful when called in incremental mode.

    Operation ordering (mirrors catalog service):
      1. DROP NOT NULL
      2. ALTER TYPE (same-family) / Column swap (cross-family)
      3. ADD COLUMN
      4. SET NOT NULL (skip for newly added columns)

    target:         dbt relation (this) pointing at the final table
    unified_schema: {COL_NAME: {full_type, is_nullable, sources}} from plt_resolve_schema
  #}

  {# ══════════════════════════════════════════════════════════════════
     Step 0: Clean up stale __PLT_MIGRATE columns from prior failed runs.
     Snowflake DDL auto-commits — each ALTER commits independently.
     If a prior column swap failed mid-way, orphaned temp columns may exist.
     ══════════════════════════════════════════════════════════════════ #}
  {% set target_columns = adapter.get_columns_in_relation(target) %}
  {% set final_col_names = [] %}
  {% for col in target_columns %}
    {% do final_col_names.append(col.name | upper) %}
  {% endfor %}

  {% for col in target_columns %}
    {% set col_upper = col.name | upper %}
    {% if col_upper.endswith('__PLT_MIGRATE') %}
      {% set original_col = col_upper.replace('__PLT_MIGRATE', '') %}
      {% if original_col in final_col_names %}
        {# Original still exists — drop the orphaned temp column #}
        {% do run_query("ALTER TABLE " ~ target ~ " DROP COLUMN " ~ col_upper) %}
        {{ log("PLT evolve_table: cleaned up stale migration column " ~ col_upper, info=True) }}
      {% else %}
        {# Original was already dropped — rename temp to original #}
        {% do run_query("ALTER TABLE " ~ target ~ " RENAME COLUMN " ~ col_upper ~ " TO " ~ original_col) %}
        {{ log("PLT evolve_table: recovered " ~ original_col ~ " from stale " ~ col_upper, info=True) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# ══════════════════════════════════════════════════════════════════
     Fetch final table state (after cleanup)
     ══════════════════════════════════════════════════════════════════ #}
  {% set final_cols = {} %}
  {% for col in adapter.get_columns_in_relation(target) %}
    {% do final_cols.update({col.name | upper: col.dtype | upper}) %}
  {% endfor %}

  {# Nullability via INFORMATION_SCHEMA #}
  {% set nullable_query %}
    SELECT COLUMN_NAME, IS_NULLABLE
    FROM {{ target.database }}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER('{{ target.schema }}')
      AND TABLE_NAME   = UPPER('{{ target.identifier }}')
  {% endset %}
  {% set nullable_result = run_query(nullable_query) %}
  {% set final_nullable_map = {} %}
  {% if execute %}
    {% for row in nullable_result %}
      {% do final_nullable_map.update({row[0] | upper: (row[1] == 'YES')}) %}
    {% endfor %}
  {% endif %}

  {# ══════════════════════════════════════════════════════════════════
     Build operation lists (compute then execute)
     ══════════════════════════════════════════════════════════════════ #}
  {% set drop_not_null_ops = [] %}
  {% set alter_type_ops = [] %}
  {% set add_column_ops = [] %}
  {% set set_not_null_ops = [] %}
  {% set newly_added_columns = [] %}

  {# ── Compare unified vs final ── #}
  {% for col_name, unified_meta in unified_schema.items() %}
    {% set col_upper = col_name | upper %}

    {% if col_upper not in final_cols %}
      {# ── NEW COLUMN ── #}
      {% do add_column_ops.append({'column': col_upper, 'type': unified_meta['full_type']}) %}
      {% do newly_added_columns.append(col_upper) %}
      {# NOTE: Do NOT add SET NOT NULL for newly added columns.
         Existing rows will have NULL — SET NOT NULL would fail. #}

    {% else %}
      {# ── EXISTING COLUMN ── #}
      {% set final_type = final_cols[col_upper] %}

      {# Normalize via get_widened_type to handle Snowflake aliases
         (TEXT vs VARCHAR, INT vs NUMBER(38,0), etc.) #}
      {% set lca = get_widened_type(unified_meta['full_type'], final_type) %}
      {% set lca_norm = lca | upper | replace(' ', '') %}
      {% set final_norm = final_type | upper | replace(' ', '') %}

      {% if lca_norm != final_norm %}
        {# Determine strategy: same base family → alter, different → swap #}
        {% set lca_base = lca_norm.split('(')[0] %}
        {% set final_base = final_norm.split('(')[0] %}

        {# Normalize type aliases to families #}
        {% set numeric_types = ['NUMBER', 'NUMERIC', 'DECIMAL', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'] %}
        {% set varchar_types = ['VARCHAR', 'TEXT', 'STRING', 'CHARACTERVARYING'] %}
        {% set float_types = ['FLOAT', 'DOUBLE', 'DOUBLEPRECISION', 'REAL'] %}
        {% set lca_family = 'NUMBER' if lca_base in numeric_types else ('VARCHAR' if lca_base in varchar_types else ('FLOAT' if lca_base in float_types else lca_base)) %}
        {% set final_family = 'NUMBER' if final_base in numeric_types else ('VARCHAR' if final_base in varchar_types else ('FLOAT' if final_base in float_types else final_base)) %}

        {% if lca_family == final_family %}
          {% set strategy = 'alter' %}
        {% else %}
          {% set strategy = 'swap' %}
        {% endif %}

        {% do alter_type_ops.append({
          'column': col_upper,
          'old_type': final_type,
          'new_type': lca,
          'strategy': strategy
        }) %}
      {% endif %}
      {# else: lca == final_type → final already at or wider. No DDL. #}

      {# ── Nullability change? ── #}
      {% set final_is_nullable = final_nullable_map[col_upper] if col_upper in final_nullable_map else true %}
      {% if not final_is_nullable and unified_meta['is_nullable'] %}
        {% do drop_not_null_ops.append({'column': col_upper}) %}
      {% elif final_is_nullable and not unified_meta['is_nullable'] %}
        {% do set_not_null_ops.append({'column': col_upper}) %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# ── SOFT-DROP: columns in final but missing from ALL sources ──
     Never DROP the column. Just ensure it's nullable so future rows get NULL.
     Skip __HEVO_SOURCE_PIPELINE — it's synthesized by plt_generate_union, not a source column. #}
  {% for col_name, col_type in final_cols.items() %}
    {% if col_name not in unified_schema and col_name != '__HEVO_SOURCE_PIPELINE' %}
      {% set final_is_nullable = final_nullable_map[col_name] if col_name in final_nullable_map else true %}
      {% if not final_is_nullable %}
        {% do drop_not_null_ops.append({'column': col_name}) %}
        {{ log("PLT evolve_table: soft-drop — will make " ~ col_name ~ " nullable", info=True) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# ══════════════════════════════════════════════════════════════════
     Execute in order: DROP NOT NULL → ALTER TYPE → ADD COLUMN → SET NOT NULL
     ══════════════════════════════════════════════════════════════════ #}

  {# Order 1: DROP NOT NULL #}
  {% for op in drop_not_null_ops %}
    {% do run_query("ALTER TABLE " ~ target ~ " MODIFY COLUMN " ~ op['column'] ~ " DROP NOT NULL") %}
    {{ log("PLT evolve_table: dropped NOT NULL on " ~ op['column'], info=True) }}
  {% endfor %}

  {# Order 2: CHANGE DATA TYPE #}
  {% for op in alter_type_ops %}
    {% if op['strategy'] == 'alter' %}
      {% do run_query("ALTER TABLE " ~ target ~ " MODIFY COLUMN " ~ op['column'] ~ " SET DATA TYPE " ~ op['new_type']) %}
      {{ log("PLT evolve_table: widened " ~ op['column'] ~ ": " ~ op['old_type'] ~ " -> " ~ op['new_type'] ~ " (alter)", info=True) }}

    {% elif op['strategy'] == 'swap' %}
      {# Column swap for cross-family type changes.
         NOTE: Snowflake DDL auto-commits. Each ALTER commits independently.
         If this fails mid-way, Step 0 cleanup recovers on next run. #}
      {% set tmp_col = op['column'] ~ '__PLT_MIGRATE' %}
      {% do run_query("ALTER TABLE " ~ target ~ " ADD COLUMN IF NOT EXISTS " ~ tmp_col ~ " " ~ op['new_type']) %}
      {% do run_query("UPDATE " ~ target ~ " SET " ~ tmp_col ~ " = TRY_CAST(" ~ op['column'] ~ " AS " ~ op['new_type'] ~ ")") %}
      {% do run_query("ALTER TABLE " ~ target ~ " DROP COLUMN " ~ op['column']) %}
      {% do run_query("ALTER TABLE " ~ target ~ " RENAME COLUMN " ~ tmp_col ~ " TO " ~ op['column']) %}
      {{ log("PLT evolve_table: promoted " ~ op['column'] ~ ": " ~ op['old_type'] ~ " -> " ~ op['new_type'] ~ " (column swap)", info=True) }}
    {% endif %}
  {% endfor %}

  {# Order 3: ADD COLUMNS #}
  {% for op in add_column_ops %}
    {% do run_query("ALTER TABLE " ~ target ~ " ADD COLUMN IF NOT EXISTS " ~ op['column'] ~ " " ~ op['type']) %}
    {{ log("PLT evolve_table: added column " ~ op['column'] ~ " (" ~ op['type'] ~ ")", info=True) }}
  {% endfor %}

  {# Order 4: SET NOT NULL (only for pre-existing columns, never for newly added) #}
  {% for op in set_not_null_ops %}
    {% if op['column'] not in newly_added_columns %}
      {% do run_query("ALTER TABLE " ~ target ~ " MODIFY COLUMN " ~ op['column'] ~ " SET NOT NULL") %}
      {{ log("PLT evolve_table: set NOT NULL on " ~ op['column'], info=True) }}
    {% endif %}
  {% endfor %}

{% endmacro %}
