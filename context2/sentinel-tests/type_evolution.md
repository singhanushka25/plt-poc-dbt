# Type Evolution Tests

Covers: type widening, type narrowing, precision/scale changes, incompatible type changes.

---

## 1. Type Widening (Safe / Compatible Promotions)

### `tests/db_sources/postgres/compatible_precision_increase.py`

**Scenario**: Source column type widens (precision or scale increases).

Examples:
- `INT` → `BIGINT`
- `NUMERIC(10, 2)` → `NUMERIC(20, 4)`
- `VARCHAR(50)` → `VARCHAR(255)` → `TEXT`

**Expectation**:
- In ALLOW_ALL mode: destination column type is updated to the wider type
- No data loss; existing rows still readable
- Pipeline continues incremental load without resync

---

### `suites/schema_and_destinations/destinations/snowflake/datatype_promotion_interruption.yml`

**Scenario**: Type promotion (widening) is interrupted mid-migration (e.g., Hevo restarts, network drop).

**Tests**:
- Promotion started → process killed → promotion resumed
- Validates final destination type matches the wider type
- Validates no partial writes or corruption

Relevant for PLT because `evolve_final_table` must be idempotent — re-running ALTER COLUMN type must not fail if column is already widened.

---

### `tests/platform/pipeline_flows/edit_pipeline/refresh_schema/test_update_column_datatype.py`

**Scenario**: Column type changed at source; user triggers "Refresh Schema".

**Variants**:
- `VARCHAR → TEXT` (compatible, widening) → automatically applied
- `VARCHAR → BOOL` (non-compatible) → requires DROP + re-add of the column
- `NUMERIC → FLOAT` (widening in precision) → applied with ALTER COLUMN

---

### `suites/schema_and_destinations/destinations/snowflake/snowflake_decimal_with_scale_0.yml`

**Scenario**: Decimal columns with scale 0 (e.g., `DECIMAL(18, 0)`) — Snowflake treats these specially.

- Tests that scale-0 decimals don't get double-cast or lose precision
- Relevant if PLT is widening NUMBER to NUMBER(x, 0)

---

### `tests/schema_and_destinations/destinations/bigquery/test_parameterised_promotion.py`

**Scenario**: Full type promotion matrix for BigQuery.

Parameterised over:
- Source type → destination type pairs
- All ALLOW_ALL / ALLOW_SAFE modes

---

## 2. Type Narrowing (Unsafe / Incompatible Changes)

### `tests/db_sources/common_scenarios/datatype_narrowing.py`

**Scenario**: Source column type narrows (becomes less precise or less permissive).

Examples:
- `TIMESTAMP` → `DATE` (narrowing: loses time component)
- `FLOAT` → `INT` (narrowing: loses decimal places)
- `VARCHAR(255)` → `VARCHAR(10)` (narrowing: truncation risk)

**Modes tested**:
- **ALLOW_ALL**: pipeline continues, destination keeps the wider type (no ALTER to narrower type)
- **BLOCK_ALL**: pipeline pauses with an error/alert; user must resolve

**Key assertion**: With ALLOW_ALL, the destination column is NEVER altered to a narrower type. Source data is cast/truncated at read time but destination schema remains wide.

---

### `tests/platform/migration/narrowing/narrowing_type_evolution_test.py`

**Full type evolution matrix** across multiple destinations (Snowflake, BigQuery, Redshift).

Structure:
- Matrix of `(source_type, target_type)` pairs classified as safe / unsafe
- Each pair tested with ALLOW_ALL, ALLOW_SAFE, BLOCK_ALL
- Result: APPLIED / BLOCKED / ALERT depending on mode

This is the reference for building PLT's **LCA (Lowest Common Ancestor)** type widening logic.

---

### `tests/platform/migration/narrowing/narrowing_migration_test.py`

**Scenario**: Type narrowing happens during a migration (not steady-state incremental).

Tests:
- Source type narrows after migration job starts
- Validates migration completes without corrupting destination
- Validates alert is raised in BLOCK modes

---

### `tests/platform/migration/narrowing/narrowing_edge_and_refresh_test.py`

**Edge cases**:
- Timestamp narrowing (specific to timezone handling)
- Refresh Schema triggered during narrowing scenario
- Pipeline restarted after a narrowing alert — resumes correctly

---

### `tests/platform/migration/narrowing/narrowing_evolution_matrix.py`

Matrix definitions used by the above tests. Contains:
- All `(from_type, to_type)` combinations
- Expected behavior per destination per evolution strategy

Useful reference for PLT's `get_widened_type` macro (LCA on Snowflake type hierarchy).

---

## 3. Inline Type Changes (Non-compatible)

### `tests/platform/pipeline_flows/edit_pipeline/refresh_schema/test_update_column_datatype.py`

**VARCHAR → BOOL** (incompatible):
1. Column is dropped from pipeline
2. Column re-added with new BOOL type
3. Historical data in destination retains old type column
4. New column in destination is created fresh

**PLT relevance**: If a column type change is non-compatible, PLT cannot simply ALTER COLUMN — must decide: create new column, or BLOCK?

---

## 4. Precision & Scale Changes

### `suites/schema_and_destinations/destinations/redshift/redshift_pk_evolution.py`

**Redshift-specific**: Redshift doesn't support ALTER COLUMN type easily.
- Tests how Hevo handles type evolution on Redshift (typically involves DROP + recreate)
- Not directly applicable to Snowflake but good reference for "destination can't do ALTER COLUMN" scenarios

---

## 5. Source-Specific Type Promotion Tests

### MySQL — `tests/db_sources/mysql/schema_changes/datatype_promotion_mysql.py`

MySQL type promotions:
- `TINYINT` → `INT` → `BIGINT`
- `FLOAT` → `DOUBLE`
- `CHAR` → `VARCHAR`
- Tests CDC log parsing for ALTER TABLE MODIFY COLUMN events

### Oracle — `tests/db_sources/oracle/schema_changes/datatype_promotion.py`

Oracle type promotions:
- `NUMBER(p, s)` widening
- `VARCHAR2` → `CLOB`
- Tests REDO log parsing for ALTER TABLE MODIFY

---

## PLT Relevance Summary

| Sentinel scenario | PLT must handle |
|-------------------|----------------|
| Type widening (INT→BIGINT) | `get_widened_type` LCA + `evolve_final_table` ALTER COLUMN type |
| Type narrowing (TIMESTAMP→DATE) | Detect as unsafe; raise alert; BLOCK alter |
| Non-compatible change (VARCHAR→BOOL) | BLOCK; do not alter; raise structured alert |
| Precision/scale increase | Widening path — same as type widen |
| Precision/scale decrease | Narrowing path — same as type narrow |
| Interrupted promotion | `evolve_final_table` must be idempotent |

### Type Hierarchy for LCA (Snowflake)

```
NUMBER → FLOAT → VARCHAR (STRING)
TIMESTAMP_NTZ → VARCHAR
BOOLEAN → NUMBER → VARCHAR
DATE → TIMESTAMP_NTZ → VARCHAR
```

Widening always moves up the hierarchy toward VARCHAR.
Narrowing is the reverse and should be BLOCKED.
