# Schema Drift Scenarios — PLT Relevant

Filtered from `sentinel-tests/functional/tests/schema_and_destinations/`.

**Filter criterion**: PLT only sees what landed in the destination staging schemas. Anything that is a pipeline policy outcome (allow_all/block_all, inaccessible columns, CDC publication exclusions, pipeline refresh flows) is excluded — those decisions are already resolved before DBT runs.

**Destinations in scope**: Snowflake, Redshift, BigQuery.

---

## 1. Column Presence

### 1.1 Add Column

- **ADD column (supported type, e.g. INT/DECIMAL/VARCHAR)**: A new column appears in one staging schema that wasn't there before. PLT must add the column to the final table and NULL-pad rows from schemas that don't have it.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (ADD_ACCESSIBLE_COL)

- **ADD column with DEFAULT value**: New column `bonus` (DECIMAL, default=5000) appears in staging. PLT adds to final table; DEFAULT is a source-side concern, destination column is nullable.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Change 9)

- **ADD multiple columns in one run**: Multiple new columns appear across staging schemas in the same DBT invocation. PLT must handle all of them in one `evolve_final_table` pass.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py`

- **ADD column to a newly-appeared staging table**: A brand-new staging schema/table appears (new source object loaded for the first time) and already has additional columns vs the final table. PLT must diff against the final table and add all missing columns.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Changes 4+5)

### 1.2 Drop Column

- **DROP column from one staging schema**: A column disappears from one staging schema (still present in others). PLT keeps the column in the final table; future rows from the schema that dropped it get NULL for that column.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Change 1)

- **DROP column from all staging schemas**: Column disappears from every staging schema simultaneously. PLT keeps the column in the final table (soft-drop semantics); all future rows get NULL. No ALTER DROP on the final table.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (REMOVE_ACCESSIBLE_COL)

- **DROP PK column from staging schema**: The primary key column is physically removed from a staging schema. Merge key for that source is now invalid. PLT must fall back gracefully (APPEND or skip merge for that source).
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (REMOVE_PK_COLUMN / OBJECT_PK_REMOVED)

- **DROP + re-add column (same type)**: Column dropped from staging, then re-added with the same type in a later run. PLT should handle idempotently — column already exists in final table, no ALTER needed.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_alter_table_columns.py`

### 1.3 Column Name Mismatch (Rename equivalent in PLT)

- **Same column, different name across schemas**: `k1.orders` has `mobile`, `k1_prime.orders` has `phone` (conceptually the same data, different name). PLT has no aliasing config — treats them as two distinct columns. Final table gets both; k1 rows get `phone = NULL`, k1_prime rows get `mobile = NULL`.
  - PLT framing: COLUMN_NAME_MISMATCH
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (RENAME_NON_PK_COLUMN — old col DELETED, new col ADDED in staging)

- **Rename + re-add original name**: After a rename, the original column name is re-added to the source. Final table ends up with both the original and the renamed column (both non-NULL for different rows).
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_alter_table_columns.py` (step 3)

### 1.4 Table-level

- **New staging table appears**: A new source object is loaded for the first time, creating a new staging schema. PLT must union it into the final table; `__hevo_source_pipeline` label distinguishes its rows.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Change 4)

- **Staging table disappears (source dropped)**: A staging schema/table no longer exists when DBT runs. PLT must handle gracefully — `adapter.get_relation()` returns None; skip that source.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/new_table_on_source/test_drop_table.py`

- **Staging table renamed** (appears as new name): Source table renamed means the old staging schema disappears and a new one appears. PLT sees it as old source gone + new source added. Rows from the old name already in final table are unaffected; new rows come in under the new label.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Change 3)

---

## 2. Type Evolution

### 2.1 Safe Widening — Snowflake (apply ALTER COLUMN)

Column has different types across staging schemas; PLT resolves to the wider type and ALTERs the final table column.

| Scenario | k1 type | k1_prime type | Resolved (Snowflake) |
|----------|---------|--------------|----------------------|
| Integer widening | `NUMBER(10,0)` | `NUMBER(15,0)` | `NUMBER(15,0)` |
| Precision + scale widening | `NUMBER(10,3)` | `NUMBER(15,6)` | `NUMBER(15,6)` |
| Date → timestamp | `DATE` | `TIMESTAMP_NTZ(3)` | `TIMESTAMP_NTZ(3)` |
| Date → timestamptz | `DATE` | `TIMESTAMP_TZ(3)` | `TIMESTAMP_TZ(3)` |
| NTZ → TZ | `TIMESTAMP_NTZ(3)` | `TIMESTAMP_TZ(3)` | `TIMESTAMP_TZ(3)` |
| Timestamp precision widening | `TIMESTAMP_NTZ(3)` | `TIMESTAMP_NTZ(6)` | `TIMESTAMP_NTZ(6)` |
| String widening | `VARCHAR(100)` | `VARCHAR(500)` | `VARCHAR(500)` |
| Bool → number | `BOOLEAN` | `NUMBER(10,3)` | `NUMBER(10,3)` |

Source: `destination_utils/evolution_matrices/snowflake_type_evolution_matrix.csv`

### 2.2 Unsafe / Cross-type Conflicts — Snowflake (fall back to VARCHAR)

Types are incompatible; resolved to `VARCHAR(256)` as the universal fallback.

| Scenario | k1 type | k1_prime type | Resolved (Snowflake) |
|----------|---------|--------------|----------------------|
| Float vs decimal | `FLOAT` | `NUMBER(10,3)` | `VARCHAR(256)` |
| Decimal vs float | `NUMBER(10,3)` | `FLOAT` | `VARCHAR(256)` |
| Date vs float | `DATE` | `FLOAT` | `VARCHAR(256)` |
| Date vs bool | `DATE` | `BOOLEAN` | `VARCHAR(256)` |
| Bool vs date | `BOOLEAN` | `DATE` | `VARCHAR(256)` |
| Time vs timestamp | `TIME(3)` | `TIMESTAMP_TZ(3)` | `VARCHAR(256)` |
| Number overridden by string | `NUMBER(10,3)` | `VARCHAR(500)` | `VARCHAR(500)` |

Source: `destination_utils/evolution_matrices/snowflake_type_evolution_matrix.csv`

### 2.3 Safe Widening — Redshift (apply ALTER COLUMN)

| Scenario | k1 type | k1_prime type | Resolved (Redshift) |
|----------|---------|--------------|---------------------|
| Smallint → bigint | `SMALLINT` | `BIGINT` | `BIGINT` |
| Integer → bigint | `INTEGER` | `BIGINT` | `BIGINT` |
| Smallint → integer | `SMALLINT` | `INTEGER` | `INTEGER` |
| Decimal precision widening | `DECIMAL(10,3)` | `DECIMAL(15,6)` | `DECIMAL(15,6)` |
| Decimal + bigint | `DECIMAL(10,3)` | `BIGINT` | `DECIMAL(22,3)` |
| Integer + decimal | `INTEGER` | `DECIMAL(10,3)` | `DECIMAL(13,3)` |
| Smallint + decimal | `SMALLINT` | `DECIMAL(10,3)` | `DECIMAL(10,3)` |
| Date → timestamptz | `DATE` | `TIMESTAMPTZ(3)` | `TIMESTAMPTZ` |
| Date → timestamp | `DATE` | `TIMESTAMP(3)` | `TIMESTAMP` |
| Timestamp → timestamptz | `TIMESTAMP(3)` | `TIMESTAMPTZ(3)` | `TIMESTAMPTZ` |
| String widening | `VARCHAR(100)` | `VARCHAR(500)` | `VARCHAR(500)` |
| Bool → bigint | `BOOLEAN` | `BIGINT` | `BIGINT` |

Source: `destination_utils/evolution_matrices/redshift_type_evolution_matrix.csv`

### 2.4 Unsafe / Cross-type Conflicts — Redshift (fall back to VARCHAR)

| Scenario | k1 type | k1_prime type | Resolved (Redshift) |
|----------|---------|--------------|---------------------|
| Double vs bigint | `DOUBLE PRECISION` | `BIGINT` | `VARCHAR(256)` |
| Real vs bigint | `REAL` | `BIGINT` | `VARCHAR(256)` |
| Date vs bool | `DATE` | `BOOLEAN` | `VARCHAR(256)` |
| Bigint vs date | `BIGINT` | `DATE` | `VARCHAR(256)` |
| TIMETZ vs anything | `TIMETZ(3)` | any | `VARCHAR(65535)` |
| SUPER vs anything | `SUPER` | any | `VARCHAR(65535)` |
| String + int | `VARCHAR(100)` | `BIGINT` | `VARCHAR(400)` |

Source: `destination_utils/evolution_matrices/redshift_type_evolution_matrix.csv`

### 2.5 Safe Widening — BigQuery (apply ALTER COLUMN)

| Scenario | k1 type | k1_prime type | Resolved (BigQuery) |
|----------|---------|--------------|---------------------|
| Int64 → numeric | `INT64` | `NUMERIC(10,3)` | `NUMERIC` |
| Int64 → bignumeric | `INT64` | `BIGNUMERIC(76,38)` | `BIGNUMERIC` |
| Numeric precision widening | `NUMERIC(10,3)` | `NUMERIC(15,6)` | `NUMERIC` |
| Bool → int64 | `BOOL` | `INT64` | `INT64` |
| Date → datetime | `DATE` | `DATETIME` | `DATETIME` |
| Date → timestamp | `DATE` | `TIMESTAMP(3)` | `TIMESTAMP` |
| Datetime → timestamp | `DATETIME` | `TIMESTAMP(3)` | `TIMESTAMP` |
| Timestamp precision widening | `TIMESTAMP(3)` | `TIMESTAMP(6)` | `TIMESTAMP` |

Source: `destination_utils/evolution_matrices/bigquery_type_evolution_matrix.csv`

### 2.6 Unsafe / Cross-type Conflicts — BigQuery (fall back to STRING)

| Scenario | k1 type | k1_prime type | Resolved (BigQuery) |
|----------|---------|--------------|---------------------|
| Float64 vs int64 | `FLOAT64` | `INT64` | `STRING` |
| Float64 vs numeric | `FLOAT64` | `NUMERIC(10,3)` | `STRING` |
| Int64 vs float64 | `INT64` | `FLOAT64` | `STRING` |
| Bytes vs int64 | `BYTES(100)` | `INT64` | `STRING` |
| TIME_TZ vs anything | `TIME_TZ(3)` | any | `STRING` |
| Date vs bool | `DATE` | `BOOL` | `STRING` |
| Timestamp + int | `TIMESTAMP_TZ(3)` | `INT64` | `STRING` |
| STRING always unparameterised | `STRING(100)` | anything | `STRING` |

Source: `destination_utils/evolution_matrices/bigquery_type_evolution_matrix.csv`

### 2.7 Inline Type Change on Staging Column

- **VARCHAR widened** (`col_to_widen` size 100 → 200): Staging column widens. PLT picks up wider size; resolves to VARCHAR(200) in final table.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (WIDEN_DATA_TYPE / COLUMN_DATA_TYPE_WIDENED)

- **VARCHAR narrowed** (`col_to_narrow` size 100 → 50): Staging column narrows. PLT detects narrowing → keeps final table at VARCHAR(100), does NOT alter to narrower type.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (NARROW_DATA_TYPE / COLUMN_DATA_TYPE_NARROWED)

- **INT → BIGINT**: Type promoted in staging. PLT applies wider type to final table.
  - Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py` (Schema Change 7)

- **INT → VARCHAR (incompatible type change)**: Cross-type conflict; falls back to VARCHAR in final table.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (CHANGE_DATA_TYPE / COLUMN_DATA_TYPE_CHANGED)

### 2.8 Parameterised Type Promotion — BigQuery specific

- **Parameterised → unparameterised**: Destination has `STRING(23)`, `BYTES(23)`, `NUMERIC(10,2)`, `BIGNUMERIC(38,9)`. BigQuery always promotes these to unparameterised `STRING`, `BYTES`, `NUMERIC`, `BIGNUMERIC` before loading. PLT must account for this when comparing staging column types to final table types.
  - Sentinel ref: `destinations/bigquery/test_parameterised_promotion.py`

### 2.9 Decimal with Scale = 0 — Snowflake specific

- **DECIMAL / BIGNUMERIC columns with scale 0**: Replicated to Snowflake as `NUMBER`. PLT must treat `NUMBER(x,0)` correctly in type resolution — not conflate with `NUMBER(x,y)` where y > 0.
  - Sentinel ref: `destinations/snowflake/snowflake_decimal_with_scale_0.py`

### 2.10 Promotion Interruption Recovery — Snowflake specific

- **Leftover `__HEVO__*__TEMP` columns**: Snowflake staging table may contain `__HEVO__COL__TEMP` columns from a previously interrupted type promotion. PLT's `adapter.get_columns_in_relation()` will see these. PLT must ignore or handle them gracefully — do not include in the unified schema or the final table.
  - Sentinel ref: `destinations/snowflake/datatype_promotion_interruption.py`

---

## 3. PK / Merge Key Drift

### 3.1 PK Added

- **PK added to previously unkeyed staging table**: A staging table that had no PK gains one. PLT was using APPEND for that source; must detect the new PK and switch to MERGE.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_source_pk_changes.py` (source_add_pk)

- **No-PK staging table mixed with PK staging table**: `k1.orders` has PK `id`; `k1_prime.orders` has no PK. PLT's merge key is `(id, __hevo_source_pipeline)` — works for k1 rows but k1_prime rows may duplicate. PLT must handle the mixed case.
  - Sentinel ref: `automatic_append_mode/test_create_mode_verification.py`

### 3.2 PK Dropped

- **PK dropped → auto-switch to APPEND**: Staging table loses its PK. PLT must detect this and fall back to APPEND for that source.
  - Sentinel ref: `automatic_append_mode/test_pk_drop_automatic_append.py`

### 3.3 PK Column Changed (Single → Different Single)

- **PK column swapped**: Merge key column changes. PLT's `unique_key` must reflect the new PK.
  - Sentinel ref: `schema_evolution/allow_all/append_mode/schema_evolution_runner.py` (CHANGE_TABLE_PK / OBJECT_PK_CHANGED)

### 3.4 Simple PK → Composite PK

- **Single PK → composite PK `(salary, address)`**: PLT's merge key expands to multi-column.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_source_pk_changes.py` (source_pk_to_composite_pk)

- **Single PK → composite PK `(mobile, salary)`**: Second variant; validates multi-column merge key works end-to-end.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/new_table_on_source/test_pk_to_cpk.py`

### 3.5 Composite PK → Different Composite PK

- **CPK `(id1, id2, id3)` → `(id1, id2)`**: One PK column dropped from the composite key. PLT merge key shrinks; prior rows with distinct `id3` values are now potential duplicates.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_source_pk_changes.py` (source_cpk_to_diff_cpk)

### 3.6 Mismatched PKs Across Staging Schemas

- **k1 PK: `[id]`, k1_prime PK: `[id, category]`**: Two staging schemas have different PK definitions for the same conceptual table. PLT merge key is `(id, __hevo_source_pipeline)` — `__hevo_source_pipeline` disambiguates, but the composite PK in k1_prime means rows can duplicate across k1_prime batches unless the full composite is used.
  - PLT framing: PK_MISMATCH_ACROSS_SOURCES

- **k1 PK column type: SMALLINT, k1_prime PK column type: DECIMAL**: Same PK column name, different types. PLT type resolution kicks in — must widen to DECIMAL before using as merge key.
  - PLT framing: PK_TYPE_MISMATCH
  - Sentinel ref: `destination_utils/evolution_matrices/redshift_type_evolution_matrix.csv` (SMALLINT→DECIMAL widening)

### 3.7 Distkey / Sortkey — Redshift specific

- **PK column type evolved INT → VARCHAR on Redshift**: PK constraint dropped, columns altered, PK re-added. After incremental: 3 PKs preserved in destination; distkey/sortkey constraints may change. PLT must not assume distkey/sortkey are stable across runs.
  - Sentinel ref: `schema_evolution/destination_constraints/redshift_pk_evolution.py`

- **Sortkey columns evolved INT → VARCHAR (distkey unchanged)**: Sortkey columns evolved separately from distkey. PLT must handle type changes on sort-key columns without invalidating the distkey.
  - Sentinel ref: `schema_evolution/destination_constraints/redshift_pk_evolution.py`

---

## 4. Constraint Drift

### 4.1 NOT NULL

- **NOT NULL dropped on source** (`salary` column): Staging column becomes nullable. PLT: final table column stays nullable, no change needed.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_nullable.py` (test_make_col_nullable)

- **NOT NULL added on source** (`mobile` column): Source adds NOT NULL. PLT behavior: destination column remains nullable. NOT NULL is never propagated to destination.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/test_nullable.py` (test_make_col_not_nullable)

- **NOT NULL mismatch across schemas**: `k1.mobile` is NOT NULL, `k1_prime.mobile` is nullable. PLT uses the permissive rule — final table column is nullable (NULL-padding requires it).

### 4.2 Destination-level NOT NULL rules (per destination)

- **Snowflake / BigQuery MERGE mode**: No NOT NULL constraints on destination columns, even if source has NOT NULL. PK columns exist in mapping but destination does not enforce NOT NULL on them.
  - Sentinel ref: `schema_evolution/destination_constraints/validate_not_null_constraints.py`

- **Redshift MERGE mode**: PK columns MUST have NOT NULL on destination. Non-PK columns must NOT have NOT NULL. PLT must enforce this when evolving the Redshift final table.
  - Sentinel ref: `schema_evolution/destination_constraints/validate_not_null_constraints.py`

- **APPEND mode (all destinations)**: No NOT NULL constraints on any destination column regardless of source.
  - Sentinel ref: `schema_evolution/destination_constraints/validate_not_null_constraints.py`

### 4.3 Index / UNIQUE (Not propagated)

- **Index added/dropped on source**: Never propagated to destination. PLT ignores entirely.
  - Sentinel ref: `schema_evolution/existing_table_load/source_table_changes/alter_unique_constraint_on_source.py`

---

## 5. Multi-Change Batch (CRO equivalent for PLT)

All of the following happen before a single DBT run — PLT must handle the full set in one invocation:

1. DROP column `department` (VARCHAR) from one staging schema
2. ADD column `salary` (DECIMAL) to that same schema
3. Table rename → old staging schema disappears, new one appears
4. New staging table added with its own column set
5. ADD column `salary` (DECIMAL) to the new staging table
6. COLUMN_NAME_MISMATCH (`hire_date` in old schema, `test_date` in new)
7. ALTER column type INT → BIGINT
8. DROP column `last_name` (NOT NULL VARCHAR) from new staging table
9. ADD column `bonus` (DECIMAL with default) to existing staging table
10. New staging table rename → old disappears, new appears

PLT must compute the correct union schema and apply all needed `evolve_final_table` ALTERs in one pre-hook pass.

- Sentinel ref: `schema_evolution/multiple_schema_changes_in_single_batch_cro/add_and_drop_multiple_objects_columns.py`

---

## 6. Existing Staging Table with Schema Mismatch vs Final Table

- **Staging table has more columns than final table**: PLT computes the diff; adds the missing columns to final table. Primary happy-path for PLT on every incremental run.
  - Sentinel ref: `existing_table_load/destination_changes.py` (test_existing_table_merge_adding_columns_between_batches)

- **Final table has columns that no staging schema has**: Final table retains the orphaned columns. All new rows get NULL for those columns. No DROP COLUMN on final table.
  - Sentinel ref: `existing_table_load/destination_changes.py` (test_existing_table_merge_remove_columns_destination)
