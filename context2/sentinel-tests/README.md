# Sentinel Schema-Change Test Reference

> Reference documentation for the PLT POC — covers every schema-change-related test
> found in `sentinel-tests/functional/`. Use this when deciding which scenarios to cover
> and how to model them as PLT test cases.

---

## Directory Layout

| File | Contents |
|------|----------|
| `add_drop_column.md` | Add / drop column tests across all sources |
| `type_evolution.md` | Type widening, narrowing, and promotion tests |
| `constraints.md` | PK, NOT NULL, UNIQUE, DEFAULT constraint tests |
| `schema_evolution_strategies.md` | ALLOW_ALL / ALLOW_COLUMN_LEVEL / BLOCK_ALL / RESET_MAPPING strategy tests |
| `patterns.md` | Common patterns, APIs, and data structures used across tests |

---

## Quick Category Map

### 1. Add / Drop Column
- Common scenarios: `tests/db_sources/common_scenarios/add_field.py`, `drop_field.py`
- Platform parallel: `tests/platform/historical_and_incremental_ingestion_in_parallel/add_new_column.py`, `delete_column.py`
- SQL Server CDC: `tests/db_sources/sql_server_ct/sql_server_cdc/test_column_addition.py`, `test_column_drop.py`
- Drop & re-add: `tests/db_sources/sql_server_ct/sql_server_cdc/test_column_drop_re_add.py`

### 2. Type Evolution
- Widening (precision/scale increase): `tests/db_sources/postgres/compatible_precision_increase.py`
- Narrowing: `tests/db_sources/common_scenarios/datatype_narrowing.py`
- Inline type change: `tests/platform/pipeline_flows/edit_pipeline/refresh_schema/test_update_column_datatype.py`
- Migration narrowing suite: `tests/platform/migration/narrowing/`
- Destination-specific: `suites/schema_and_destinations/destinations/snowflake/datatype_promotion_interruption.yml`

### 3. Constraints
- PK add/drop/change: `tests/db_sources/common_scenarios/schema_changes.py`, `suites/.../alter_table_on_source/`
- NOT NULL: `suites/.../test_nullable.yml`, `tests/schema_and_destinations/schema_evolution/destination_constraints/`
- UNIQUE / multi-constraint: `tests/db_sources/common_scenarios/fetch_schema_column_constraints.py`
- Migration constraint preservation: `tests/platform/migration/migration_constraints_preservation.py`

### 4. Schema Evolution Strategies
- ALLOW_ALL append/merge: `suites/schema_and_destinations/schema_evolution/allow_all/`
- ALLOW_COLUMN_LEVEL: `suites/schema_and_destinations/schema_evolution/allow_column/`
- BLOCK_ALL: `suites/schema_and_destinations/schema_evolution/block_all/`
- RESET_MAPPING: `suites/schema_and_destinations/schema_evolution/reset_mapping/`
- Multiple changes in one batch (CRO): `suites/schema_and_destinations/schema_evolution/multiple_schema_changes_in_single_batch_cro/`

### 5. Refresh Schema
- Full test battery: `tests/platform/pipeline_flows/edit_pipeline/refresh_schema/` (40+ tests)
  - `test_add_column_allow_all.py`, `test_rename_column_allow_all.py`, `test_update_column_datatype.py`, etc.

### 6. Source-Specific
| Source | Suite |
|--------|-------|
| Postgres | `suites/db_sources/postgres/schema_changes/schema_changes.yml` (40+ parameterised tests) |
| MySQL | `suites/db_sources/mysql/schema_changes/` |
| Oracle | `suites/db_sources/oracle/schema_changes/` |
| SQL Server | `suites/db_sources/sql_server_ct/schema_changes/` |

### 7. Migration + Schema Changes
- `tests/platform/migration/migration_add_table_and_column.py`
- `tests/platform/migration/narrowing/` (full type evolution matrix)
- `tests/platform/migration/migration_constraints_preservation.py`

---

## Summary Statistics

| Dimension | Count |
|-----------|-------|
| Schema-related test files | 200+ |
| Test methods | 300+ |
| Sources covered | Postgres, MySQL, Oracle, SQL Server |
| Destinations covered | Snowflake, BigQuery, Redshift |
| Schema evolution strategies | ALLOW_ALL, ALLOW_COLUMN_LEVEL, BLOCK_ALL, RESET_MAPPING |
| Load modes | APPEND, MERGE, TNL |

---

## Our PLT Test (For Reference)

| File | Path |
|------|------|
| Suite YAML | `functional/suites/post_load_transformations/plt_add_column.yml` |
| Test Python | `functional/tests/post_load_transformations/test_plt_add_column.py` |
| SQL data | `functional/sql_data_files/plt_add_column/plt_add_column.sql` |
| DBT model | `PLT/plt-poc-dbt/models/plt/orders.sql` |
| Run orchestrator | `PLT/plt-poc-dbt/run_poc.py` |
