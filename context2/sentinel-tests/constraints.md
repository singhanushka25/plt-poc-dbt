# Constraint Change Tests

Covers: PRIMARY KEY (add, drop, change, composite), NOT NULL, UNIQUE, DEFAULT, multi-constraint scenarios.

---

## 1. PRIMARY KEY Changes

### Suite: `suites/schema_and_destinations/schema_evolution/existing_table_load/source_table_changes/alter_table_on_source/alter_table_on_source.yml`

All tests below live under `tests/db_sources/common_scenarios/schema_changes.py` unless noted.

| Test method | Case ID | Scenario |
|-------------|---------|---------|
| `test_source_pk_changes` — add PK | C1424773 | Non-PK table gains a PK column |
| `test_source_pk_changes` — drop PK | C1424774 | Existing PK dropped from table |
| `test_source_pk_changes` — simple→composite | C1424775 | Single PK → Composite PK (add column to key) |
| `test_source_pk_changes` — composite→different composite | C1424776 | Composite PK key columns swapped/changed |
| `test_rename_pk_column_historical` | C1465480 | PK column renamed (before historical) |
| `test_rename_pk_column_incremental` | C1465482 | PK column renamed (before incremental) |
| `test_drop_pk_column_historical` | C1465486 | PK column dropped (before historical) |
| `test_drop_pk_column_incremental` | C1465488 | PK column dropped (before incremental) |
| `test_change_pk_before_historical` | C1465477 | PK column changed before fetch |
| `test_change_pk_before_incremental` | C1465478 | PK column changed mid-pipeline |

---

### MySQL Specific: PK Position Change

**File**: `tests/db_sources/mysql/schema_changes/schema_changes.py`

| Test method | Case ID | Scenario |
|-------------|---------|---------|
| `test_change_pk_position_historical` | C1465463 | Reorder PK column positions (before historical) |
| `test_change_pk_position_incremental` | C1465465 | Reorder PK column positions (before incremental) |

MySQL's DDL ALTER TABLE MODIFY can change the order of PK columns.
Hevo must re-evaluate the composite key definition and update the merge key.

---

### Composite PK with Nullable Column

| Test method | Case ID | Scenario |
|-------------|---------|---------|
| `test_composite_pk_with_null_column_historical` | C1465479 | PK defined on a nullable column |
| `test_composite_pk_with_null_column_incremental` | C1465491 | PK nullable column mid-incremental |

**Relevance for PLT**: If source PK includes a nullable column, PLT merge key logic must use `IS NOT DISTINCT FROM` instead of `=`.

---

### PK Tests in Existing Table Load

**File**: `suites/.../alter_table_on_source/`

- `test_pk_to_cpk.py` — Simple PK transitioning to composite PK
- `test_composite_pk_handling.py` — Various composite PK permutations
- `test_source_table_rename.py` — Table renamed; PK tracking preserved

---

### DDL PK Change Tests (Postgres)

**File**: `tests/db_sources/postgres/ddl_changes/ddl_primary_key_change.py`

Postgres Logical Replication DDL events:
- `ALTER TABLE ... DROP CONSTRAINT pk_name`
- `ALTER TABLE ... ADD PRIMARY KEY (col1, col2)`

Validates DDL events are captured and the catalog updates the key definition.

---

## 2. NOT NULL Constraint Changes

### Suite: `suites/.../alter_table_on_source/test_nullable.yml`

| Test method | Case ID | Scenario |
|-------------|---------|---------|
| `TestNullable.test_make_col_nullable` | C1424777 | Column with NOT NULL becomes nullable |
| `TestNullable.test_make_col_not_nullable` | C1424778 | Nullable column gets NOT NULL constraint |

**Expectations**:
- NOT NULL addition in source: Hevo **downgrades** the destination column to nullable (safe approach)
- NOT NULL removal in source: no change to destination (already nullable or kept nullable)

---

### Destination NOT NULL Validation

**File**: `tests/schema_and_destinations/schema_evolution/destination_constraints/validate_not_null_constraints.py`

Tests NOT NULL constraint replication and behavior on Snowflake/BigQuery/Redshift:
- Validates that destination column is correctly nullable even if source is NOT NULL
- Tests PK → non-PK transition (removes NOT NULL from destination when PK dropped)
- Validates constraint metadata stored in catalog correctly

---

## 3. UNIQUE Constraint Changes

### `tests/db_sources/common_scenarios/fetch_schema_column_constraints.py`

Tests UNIQUE constraint detection in the catalog:
- Column declared UNIQUE in source → marked in catalog metadata
- Length, precision, scale captured correctly for UNIQUE columns
- `DEFAULT "CURRENT_TIMESTAMP"` preservation

**Structure**: Validates catalog API `get_catalog_mappings` returns correct constraint info.

---

### `suites/.../alter_unique_constraint_on_source.yml`

Adds/removes UNIQUE constraint from an already-loaded table.
Tests that Hevo updates the catalog and does not break subsequent incremental loads.

---

## 4. Multiple Constraints (Composite Scenarios)

### `tests/db_sources/common_scenarios/multiple_constraint_tnl.py`

Handles multiple constraint types in TNL (Table Name Level) operations:
- Foreign key + UNIQUE + NOT NULL on same table
- Tests that TNL offset tracking works correctly when constraints change

### `tests/db_sources/common_scenarios/tables_with_constraints.py`

Oracle-specific:
- PK, FK (Foreign Key), UNIQUE key detection and replication
- Validates that Hevo correctly identifies and handles all three constraint types
- Tests read-side constraint extraction

---

## 5. Migration — Constraint Preservation

### `tests/platform/migration/migration_constraints_preservation.py`

During Hevo → Hevo migration (moving pipeline to new destination):

**Dataclasses used**:
```python
@dataclass
class PrimaryKeyInfo:
    columns: List[str]
    constraint_name: str

@dataclass
class ForeignKeyInfo:
    columns: List[str]
    referenced_table: str
    referenced_columns: List[str]

@dataclass
class UniqueConstraintInfo:
    columns: List[str]
    constraint_name: str

@dataclass
class NotNullConstraintInfo:
    column: str
```

**Assertions**:
- All PK constraints preserved after migration
- FK constraints preserved (or explicitly noted as not replicated to destination)
- UNIQUE constraints preserved in catalog
- NOT NULL state preserved per column

---

## 6. Refresh Schema — Constraint-Related Tests

| Test file | Scenario |
|-----------|---------|
| `test_add_nonpk_object_allow_all.py` | Add new non-PK table → refresh schema → ALLOW_ALL |
| `test_add_nonpk_object_allow_all_merge.py` | Same but merge mode |
| `test_add_pk_object_allow_all.py` | Add new PK-bearing table → refresh schema |
| `test_update_publication_key.py` | Postgres publication key (logical replication slot) updated |

---

## PLT Relevance Summary

| Sentinel scenario | PLT must handle |
|-------------------|----------------|
| Add PK | Merge key expands; IS NOT DISTINCT FROM for nullable PKs |
| Drop PK | Re-evaluate merge key; if no PK, PLT may use all-column MERGE or APPEND |
| Simple → Composite PK | Merge key becomes multi-column |
| Composite → Different Composite | Merge key updated; old dedup key invalidated |
| NOT NULL added to source | PLT ignores — destination stays nullable |
| NOT NULL removed from source | No-op for PLT |
| UNIQUE constraint added | PLT catalog-level; no ALTER needed on destination |
| PK column renamed | Merge key column name must update |

### Merge Key Decision Tree (PLT)

```
Has PK? ──YES──→ Use PK columns as merge key
   │
   NO
   │
Has UNIQUE (NOT NULL)?──YES──→ Use UNIQUE columns
   │
   NO
   │
Fallback: APPEND only (no MERGE possible)
```

For PLT, the merge key is `(id, __hevo_source_pipeline)`.
When source adds a new PK column, PLT's `unique_key` DBT config must be updated.
