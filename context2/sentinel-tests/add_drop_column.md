# Add / Drop Column Tests

---

## 1. Common Scenarios (All Sources)

### `tests/db_sources/common_scenarios/add_field.py`

**Scenario**: Add a new supported-type column mid-pipeline.

**Flow**:
1. Wait for historical job completion
2. `ALTER TABLE` to add new column (via `SentinelColumn`)
3. Create incremental data (10 INSERTs)
4. Run incremental job, validate it reaches COMPLETED
5. Assert new column is **ACTIVE** in the catalog (ALLOW_ALL mode)
6. Verify column replicated to destination with correct type

**Variants tested**:
- Add VARCHAR column
- Add INT / NUMERIC column
- Add column with DEFAULT value

**ALLOW_ALL** → column added automatically in destination
**BLOCK_ALL** → column stays INACTIVE, not replicated

---

### `tests/db_sources/common_scenarios/drop_field.py`

**Scenario**: Drop an existing column mid-pipeline.

**Key assertions**:
- Column moves to INACTIVE state in catalog after drop
- CDC offsets are still tracked correctly after drop
- Destination rows retain the column (it's not retroactively dropped from destination)
- On ALLOW_ALL: pipeline continues processing other columns cleanly
- On BLOCK_ALL: pipeline errors / pauses depending on config

**Variants**:
- Drop regular (supported type) column
- Drop unsupported-type column (e.g., Postgres DOMAIN, Oracle UROWID, SQL Server sysname)
  - Unsupported columns are typically pre-dropped before fetch even starts

---

### `tests/db_sources/common_scenarios/schema_changes.py` — Column-level cases

Selected test IDs covering column operations:

| Test method | Case ID | Scenario |
|-------------|---------|---------|
| `test_add_column_with_supported_type_before_historical` | C1465438 | Add column before historical load |
| `test_add_column_with_supported_type_before_incremental` | C1465439 | Add column between historical and first incremental |
| `test_add_column_with_unsupported_type_before_historical` | C1465441 | Add unsupported-type column pre-fetch |
| `test_add_column_with_unsupported_type_before_incremental` | C1465442 | Add unsupported-type column pre-incremental |
| `test_drop_column_with_supported_type_before_historical` | C1465448 | Drop column before historical |
| `test_drop_column_with_supported_type_before_incremental` | C1465450 | Drop column before incremental |
| `test_drop_column_with_unsupported_type_before_historical` | C1465453 | Drop unsupported-type column pre-fetch |
| `test_drop_column_with_unsupported_type_before_incremental` | C1465454 | Drop unsupported-type column pre-incremental |
| `test_drop_column_and_re_add_same_type_before_historical` | C1465466 | Drop + re-add (same type) |
| `test_drop_column_and_re_add_different_type_before_incremental` | C1465468 | Drop + re-add (different type) |
| `test_toggle_active_inactive_before_historical` | C1465467 | Toggle column active ↔ inactive |
| `test_toggle_active_inactive_before_incremental` | C1465470 | Toggle active ↔ inactive mid-incremental |

---

## 2. Platform — Historical & Incremental in Parallel

### `tests/platform/historical_and_incremental_ingestion_in_parallel/add_new_column.py`

Tests adding a column while historical load and incremental ingestion run **in parallel**.
Ensures no race condition causes data loss or schema confusion.

### `tests/platform/historical_and_incremental_ingestion_in_parallel/delete_column.py`

Same parallelism context but for column deletion.
Validates that mid-historical column removal is handled gracefully.

---

## 3. SQL Server CDC — Column Addition / Deletion

### `tests/db_sources/sql_server_ct/sql_server_cdc/test_column_addition.py`

SQL Server Change Tracking specific.
Tests how column additions are captured via CDC log, not a schema snapshot.

### `tests/db_sources/sql_server_ct/sql_server_cdc/test_column_drop.py`

Column drop via CDC log events.

### `tests/db_sources/sql_server_ct/sql_server_cdc/test_column_drop_re_add.py`

Column dropped then re-added (same or different type).
CDC logs contain both events; tests that the final state in destination is correct.

---

## 4. Existing Table Load — Source Table Changes

**Suite**: `suites/schema_and_destinations/schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/`

### `test_alter_table_columns.py` (C1424767)

Tests alter operations on tables that were already loaded historically:
- Add new column → verify it propagates in next incremental
- Drop column → verify INACTIVE state, destination unchanged
- Rename column → depends on evolution strategy

---

## 5. Multiple Schema Changes in One Batch (CRO)

**Suite**: `suites/schema_and_destinations/schema_evolution/multiple_schema_changes_in_single_batch_cro/`

### `add_and_drop_multiple_objects_columns.py`

Tests adding AND dropping multiple columns in the same CDC batch.
Validates ordering of operations and final destination state.

### `inactive_column_schema_changes.py`

Schema changes that target columns which are already INACTIVE.
Ensures these don't cause unexpected errors.

---

## 6. Postgres DDL Column Tests

**Location**: `tests/db_sources/postgres/ddl_changes/ddl_column_changes.py`

Postgres-specific DDL (Logical Replication) capturing:
- DDL events emitted during ALTER TABLE ... ADD COLUMN / DROP COLUMN
- Validates that DDL events are parsed and applied correctly without a full resync

---

## 7. Refresh Schema — Column Operations

**Location**: `tests/platform/pipeline_flows/edit_pipeline/refresh_schema/`

| Test file | Scenario |
|-----------|---------|
| `test_add_column_allow_all.py` (C196634) | Add column + trigger refresh schema, ALLOW_ALL mode |
| `test_add_column_block_all_add.py` | Add column + refresh schema, BLOCK_ALL but user picks "add" |
| `test_add_column_block_all_dont_add.py` | Add column + refresh schema, BLOCK_ALL user picks "ignore" |
| `test_remove_nonpk_column_allow_all_append.py` | Remove non-PK column, ALLOW_ALL append mode |
| `test_rename_column_allow_all.py` | Rename column, ALLOW_ALL |
| `test_rename_column_block_all_add.py` | Rename column, BLOCK_ALL |
| `test_rename_nonpk_column_allow_all_merge.py` | Rename non-PK column, ALLOW_ALL merge |
| `test_with_no_change.py` | Refresh schema with no actual change → no-op |

---

## 8. Pipeline-Level Column Ops

| File | Scenario |
|------|---------|
| `tests/platform/pipeline_flows/run_pipeline/column_deletion.py` | Column deletion during active sync/run |
| `tests/platform/pipeline_flows/cancel_job/cancel_with_schema_change.py` | Cancel a job mid-way through schema change batch |
| `tests/platform/pipeline_flows/disable_enable_pipeline/disable_schema_data_changes_enable.py` | Schema change + data change during disable/enable cycle |

---

## PLT Relevance

| Sentinel scenario | PLT must handle |
|-------------------|----------------|
| Add column (ALLOW_ALL) | `evolve_final_table` → ALTER TABLE ADD COLUMN |
| Drop column (ALLOW_ALL) | No-op on final table; old rows keep the value |
| Drop + re-add same type | Idempotent ALTER (column already exists) |
| Drop + re-add different type | Type evolution check before ALTER |
| Multiple adds/drops in one batch | `resolve_column_schema` computes union correctly |
