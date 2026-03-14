# PLT Macro Engine v3 — Design Spec

> Date: 2026-03-14
> Status: Draft — pending approval
> Scope: Phase 1 (schema evolution, NOT NULL constraints, soft-drop)
> Supersedes: Nothing — exists alongside macro-architecture-v2-design.md as a separate approach
> Prior art: macro-architecture-v2-design.md, senior's dbt-poc, dbt_full_picture.md

---

## 1. Motivation & Approach

### Context

The PLT (Pipeline-Level Transform) merges data from multiple source pipelines (e.g., k1, k1_prime) into a shared final table. Source schemas can diverge — different columns, different types, different nullability. The macro engine must resolve these differences automatically.

### Design Directive

Maximize use of dbt built-ins and primitives. Only write custom logic where dbt provably cannot handle the scenario. This document records what dbt can and cannot do, and draws a defensible boundary.

### What dbt Handles Natively

| Mechanism | What It Does | Used For |
|-----------|-------------|----------|
| `adapter.get_columns_in_relation()` | Returns column name, dtype, precision, scale, char_size | Schema introspection |
| `run_query()` | Executes arbitrary SQL | INFORMATION_SCHEMA queries + DDL |
| TRY_CAST (Snowflake native) | Returns NULL on cast failure instead of error | Resilient type casting in SELECT |
| Incremental merge strategy | MERGE INTO with unique_key | Data merge (INSERT/UPDATE) |

### What dbt Cannot Handle (Proven Gaps)

| Scenario | Why dbt Can't | Evidence |
|----------|--------------|----------|
| Cross-family type resolution (NUMBER + FLOAT = VARCHAR) | No concept of type hierarchy or LCA | No dbt package implements this |
| Soft-drop (column removed from sources, kept in final) | `sync_all_columns` hard-drops, `append_new_columns` never drops | dbt docs: only two column removal strategies |
| NOT NULL constraint management | `adapter.get_columns_in_relation()` doesn't return nullability | dbt docs confirm; requires INFORMATION_SCHEMA |
| Multi-step type promotion (cross-family ALTER) | `expand_target_column_types` only handles same-family | dbt-adapters issue #319: type-only changes fail |
| Narrowing prevention with logging | `expand_target_column_types` silently skips — no guard or log | No configuration to enable detection |
| Precision/scale expansion (NUMBER(10,3) + NUMBER(15,6) → NUMBER(15,6)) | `expand_target_column_types` may handle, undocumented | Docs say "string and numeric" without precision math details |
| Dynamic type-aware UNION ALL | `union_relations` uses first relation's types (bug #957) | Order-dependent type resolution confirmed |

### What dbt Built-ins We Evaluated and Chose NOT to Use

| Mechanism | Why Not |
|-----------|---------|
| `on_schema_change: 'append_new_columns'` | Redundant — our `plt_evolve_table` handles ADD COLUMN. Double-handling risks type conflicts. |
| `on_schema_change: 'sync_all_columns'` | Hard-drops columns (we need soft-drop). Buggy with type-only changes (dbt-adapters #319). |
| `dbt_utils.union_relations` | Type resolution is order-dependent (dbt-utils #957). Requires pre-computed `column_override` (so we do the hard work anyway). Can't add custom source labels. |
| `expand_target_column_types` | Runs automatically during incremental — harmless, but we don't depend on it. Our DDL runs first during Jinja compilation. |

---

## 2. Architecture Overview

3 custom macros + 1 existing helper, inline in model SQL.

```
┌──────────────────────────────────────────────────────────────────┐
│  orders.sql (model orchestrator)                                  │
│                                                                    │
│  1. plt_resolve_schema(sources)                                   │
│     Uses: adapter.get_columns_in_relation() + INFORMATION_SCHEMA  │
│     Uses: get_widened_type() for pairwise LCA                     │
│     Output: unified_schema dict                                    │
│                                                                    │
│  2. plt_evolve_table(this, unified_schema)  [if incremental]      │
│     Compares unified vs final table                                │
│     Same-family: ALTER COLUMN SET DATA TYPE                        │
│     Cross-family: Column swap (ADD tmp -> UPDATE TRY_CAST ->      │
│                   DROP old -> RENAME) inside transaction            │
│     New columns: ALTER TABLE ADD COLUMN                            │
│     Soft-drop: DROP NOT NULL on orphaned columns                   │
│     NOT NULL: SET/DROP NOT NULL based on source agreement          │
│                                                                    │
│  3. plt_generate_union(sources, unified_schema)                   │
│     Per source: TRY_CAST to unified types + NULL-pad missing      │
│     UNION ALL across all sources                                   │
│     Adds source label column                                       │
│                                                                    │
│  4. dbt incremental merge handles the INSERT/UPDATE               │
│                                                                    │
└──────────────────────────────────────────────────────────────────┘
```

### Model SQL

```sql
{{ config(
    materialized='incremental',
    unique_key=['id', '__hevo_source_pipeline'],
    incremental_strategy='merge'
) }}

{% set plt_sources = [
  {'database': var('k1_db'), 'schema': var('k1_schema'), 'table': var('k1_table'), 'label': 'k1'},
  {'database': var('k1_db'), 'schema': var('k1_prime_schema'), 'table': var('k1_table'), 'label': 'k1_prime'}
] %}

{# Step 1: Resolve unified schema across all sources #}
{% set result = plt_resolve_schema(plt_sources) %}
{% set unified = result['unified'] %}
{% set source_columns = result['source_columns'] %}

{# Step 2: Evolve final table DDL (runs during Jinja compilation, before SELECT) #}
{% if is_incremental() %}
  {% do plt_evolve_table(this, unified) %}
{% endif %}

{# Step 3: Generate UNION ALL with TRY_CAST + NULL-pad #}
{{ plt_generate_union(plt_sources, unified, source_columns) }}
```

### Execution Order

```
1. Jinja compilation begins
2. plt_resolve_schema() runs:
   - adapter.get_columns_in_relation() per source
   - run_query() for INFORMATION_SCHEMA nullability per source
   - Pairwise LCA via get_widened_type()
   - Returns unified_schema dict
3. plt_evolve_table() runs (if incremental):
   - Reads final table schema via adapter + INFORMATION_SCHEMA
   - Computes diff
   - Executes DDL via run_query() (ALTER TABLE ADD/MODIFY/swap)
4. plt_generate_union() runs:
   - Generates SELECT per source with TRY_CAST + NULL-pad
   - Returns SQL string (UNION ALL)
5. Jinja compilation complete — resulting SQL is a UNION ALL SELECT
6. dbt wraps it in MERGE INTO (incremental strategy)
7. dbt executes the MERGE
```

---

## 3. Macro: `get_widened_type(type_a, type_b)` — LCA Kernel

**File:** `macros/get_widened_type.sql` (existing, **needs updates** — see below)

**Purpose:** Given two Snowflake column types, find their Lowest Common Ancestor in the type hierarchy.

**Input:** Two type strings, e.g., `'NUMBER(10,3)'`, `'VARCHAR(100)'`, `'FLOAT'`

**Output:** The wider type string, e.g., `'NUMBER(15,6)'`, `'VARCHAR(16777216)'`

### Type Resolution Rules

**Same base type — expand precision/scale/length:**

| Base Type | Expansion Rule |
|-----------|---------------|
| NUMBER/NUMERIC/DECIMAL | Correct integral-digit-preserving math (see below) |
| VARCHAR/TEXT/STRING | `max(char_size)` |
| TIMESTAMP_NTZ/TZ/LTZ | `max(precision)` |
| TIME | `max(precision)` |
| BOOLEAN, DATE, FLOAT, VARIANT, ARRAY | Fixed — no expansion |

**NUMBER precision math (corrected from current implementation):**

The current macro does `max(precision), max(scale)` which is wrong. Correct algorithm:
```
new_scale = max(s1, s2)
new_int_digits = max(p1 - s1, p2 - s2)
new_precision = new_int_digits + new_scale
```
Example: `NUMBER(10,8)` vs `NUMBER(15,3)`:
- Current (wrong): `NUMBER(15,8)` — only 7 integral digits, loses data from second type's 12
- Correct: `scale=8, int_digits=max(2,12)=12, precision=20` -> `NUMBER(20,8)`

**Different base types — DAG-based LCA:**

| Type A | Type B | LCA | Rationale |
|--------|--------|-----|-----------|
| DATE | TIMESTAMP_NTZ | TIMESTAMP_NTZ | child -> parent |
| DATE | TIMESTAMP_TZ | TIMESTAMP_TZ | child -> grandparent |
| DATE | TIMESTAMP_LTZ | TIMESTAMP_LTZ | child -> parent (missing from current macro) |
| TIMESTAMP_NTZ | TIMESTAMP_TZ | TIMESTAMP_TZ | child -> parent |
| TIMESTAMP_NTZ | TIMESTAMP_LTZ | TIMESTAMP_LTZ | sibling -> needs handling (missing) |
| TIMESTAMP_LTZ | TIMESTAMP_TZ | TIMESTAMP_TZ | child -> parent (missing) |
| BOOLEAN | NUMBER/INT/BIGINT | NUMBER | child -> parent |
| BOOLEAN | FLOAT | VARCHAR | not direct parent — goes through NUMBER, then siblings |
| VARCHAR | anything | VARCHAR | root absorbs all |
| NUMBER | FLOAT | VARCHAR | siblings, LCA = STRING |
| DATE | NUMBER | VARCHAR | unrelated families |
| FLOAT | TIMESTAMP | VARCHAR | unrelated families |
| All other cross-family | VARCHAR(16777216) | fallback to max VARCHAR |

### Required Updates to `get_widened_type.sql`

1. **Cross-family fallback**: Change `VARCHAR(256)` to `VARCHAR(16777216)` (max). The current `VARCHAR(256)` risks silent truncation for VARIANT/ARRAY serialized to string.
2. **NUMBER precision math**: Replace `max(p1,p2), max(s1,s2)` with integral-digit-preserving formula.
3. **TIMESTAMP_LTZ handling**: Add paths for `TIMESTAMP_NTZ + TIMESTAMP_LTZ`, `TIMESTAMP_LTZ + TIMESTAMP_TZ`, `DATE + TIMESTAMP_LTZ`.
4. **BOOLEAN + FLOAT**: Current macro resolves to FLOAT (direct path). Should resolve to VARCHAR since BOOLEAN's parent is NUMBER, and NUMBER + FLOAT = VARCHAR (siblings).

### Complexity

O(1) per call — dict lookup + max() operations.

---

## 4. Macro: `plt_resolve_schema(sources)` — Introspect + Cross-Source LCA

**File:** `macros/plt_resolve_schema.sql` (new)

**Purpose:** Read all source table schemas. Compute one unified "desired schema" using pairwise LCA across all sources.

### Input

```python
[
  {'database': 'DB', 'schema': 'STAGING_K1', 'table': 'ORDERS', 'label': 'k1'},
  {'database': 'DB', 'schema': 'STAGING_K1_PRIME', 'table': 'ORDERS', 'label': 'k1_prime'}
]
```

### Output

Returns a dict with two keys: `unified` (the resolved schema) and `source_columns` (per-source column maps, reused by `plt_generate_union` to avoid redundant introspection).

```python
{
  'unified': {
    'ID': {
      'full_type': 'NUMBER(38,0)',
      'is_nullable': false,
      'sources': ['k1', 'k1_prime']
    },
    'SALARY': {
      'full_type': 'NUMBER(15,6)',
      'is_nullable': true,
      'sources': ['k1', 'k1_prime']
    },
    'PHONE': {
      'full_type': 'VARCHAR(256)',
      'is_nullable': true,          # forced: missing from k1
      'sources': ['k1_prime']
    }
  },
  'source_columns': {
    'k1':       {'ID': 'NUMBER(38,0)', 'NAME': 'VARCHAR(100)', 'SALARY': 'NUMBER(10,3)'},
    'k1_prime': {'ID': 'NUMBER(38,0)', 'NAME': 'VARCHAR(500)', 'SALARY': 'FLOAT', 'PHONE': 'VARCHAR(256)'}
  }
}
```

### Algorithm

```
plt_resolve_schema(sources):

  unified = {}
  source_columns = {}    # {label: {COL_NAME: TYPE}} — reused by plt_generate_union
  active_sources = []

  for each src in sources:
    rel = adapter.get_relation(src.database, src.schema, src.table)
    if rel is none:
      log("PLT: source {src.label} does not exist, skipping")
      continue
    active_sources.append(src.label)

    # ── Column types via dbt adapter ──
    columns = adapter.get_columns_in_relation(rel)

    # ── Nullability via INFORMATION_SCHEMA (one query per source) ──
    nullable_query = "
      SELECT COLUMN_NAME, IS_NULLABLE
      FROM {src.database}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = UPPER('{src.schema}')
        AND TABLE_NAME = UPPER('{src.table}')
    "
    nullable_result = run_query(nullable_query)
    nullable_map = {}  # {COL_NAME: true/false}
    for row in nullable_result:
      nullable_map[row[0] | upper] = (row[1] == 'YES')

    # ── Build per-source column map (reused by plt_generate_union) ──
    src_col_map = {}
    for each col in columns:
      src_col_map[col.name | upper] = col.dtype | upper
    source_columns[src.label] = src_col_map

    # ── Merge each column into unified ──
    for each col in columns:
      col_name = col.name | upper
      col_type = col.dtype | upper   # e.g., 'NUMBER(10,3)'
      is_nullable = nullable_map.get(col_name, true)

      if col_name not in unified:
        # First occurrence — seed
        unified[col_name] = {
          'full_type': col_type,
          'is_nullable': is_nullable,
          'sources': [src.label]
        }
      else:
        # Pairwise LCA
        existing = unified[col_name]
        widened = get_widened_type(existing['full_type'], col_type)
        existing['full_type'] = widened
        existing['is_nullable'] = existing['is_nullable'] OR is_nullable
        existing['sources'].append(src.label)

  # ── Post-processing: missing columns → force nullable ──
  for each (col_name, meta) in unified:
    if len(meta['sources']) < len(active_sources):
      meta['is_nullable'] = true

  return {'unified': unified, 'source_columns': source_columns}
```

### Queries Per Source

| Query | What | Why |
|-------|------|-----|
| `adapter.get_columns_in_relation(rel)` | Column names + full type strings (with precision/scale/length) | dbt-native, gives normalized dtype strings ready for `get_widened_type` |
| INFORMATION_SCHEMA query | Nullability per column | `adapter.get_columns_in_relation()` does not return `IS_NULLABLE` |

### Source Existence Guard

If a source table doesn't exist (e.g., pipeline hasn't loaded yet), it's silently skipped. The unified schema is computed from whichever sources exist. This allows PLT to start running even if not all sources have data yet.

### Key Rule: Missing Column = Force Nullable

If k1 has `PHONE` but k1_prime doesn't, then k1_prime rows will produce `NULL AS PHONE`. The column MUST be nullable in the final table regardless of k1's NOT NULL constraint.

### Complexity

O(N x C) where N = number of sources, C = max columns per source. Each `get_widened_type` call is O(1).

---

## 5. Macro: `plt_evolve_table(target, unified_schema)` — DDL Executor

**File:** `macros/plt_evolve_table.sql` (new)

**Purpose:** Compare unified "desired" schema against current final table. Execute DDL to align. Only runs in incremental mode.

### Design Principles

1. **Compute then execute** — build full operation list before running any DDL
2. **Operation ordering mirrors catalog service** — DROP NOT NULL -> ALTER TYPE -> ADD COLUMN -> SET NOT NULL
3. **Soft-drop** — never DROP columns from final table; just ensure they're nullable
4. **Narrowing prevention** — if final type is already wider than unified, no change (log and skip)
5. **Idempotent** — safe to run multiple times; no-ops if already aligned
6. **Stale migration cleanup** — detect and recover `__plt_migrate` columns from prior failed runs
7. **Skip SET NOT NULL on newly added columns** — existing rows have NULLs, constraint would fail

### Algorithm

```
plt_evolve_table(target, unified_schema):

  # ── Step 0: Clean up stale __plt_migrate columns from prior failed runs ──
  # Snowflake DDL is auto-committed — BEGIN/COMMIT does NOT provide atomicity
  # for ALTER TABLE. Each ALTER commits independently. If a prior column swap
  # failed mid-way, leftover __plt_migrate columns may exist.
  for each col in adapter.get_columns_in_relation(target):
    col_upper = col.name | upper
    if col_upper.endswith('__PLT_MIGRATE'):
      original_col = col_upper.replace('__PLT_MIGRATE', '')
      if original_col in final_cols:
        # Original still exists — drop the orphaned temp column
        run_query("ALTER TABLE {target} DROP COLUMN {col_upper}")
        log("PLT: cleaned up stale migration column {col_upper}")
      else:
        # Original was already dropped — rename temp to original
        run_query("ALTER TABLE {target} RENAME COLUMN {col_upper} TO {original_col}")
        log("PLT: recovered {original_col} from stale {col_upper}")

  # ── Fetch final table state (after cleanup) ──
  final_cols = {}
  for each col in adapter.get_columns_in_relation(target):
    final_cols[col.name | upper] = col.dtype | upper

  final_nullable_query = "
    SELECT COLUMN_NAME, IS_NULLABLE
    FROM {target.database}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = UPPER('{target.schema}')
      AND TABLE_NAME = UPPER('{target.identifier}')
  "
  final_nullable = run_query(final_nullable_query)
  # → {COL_NAME: true/false}

  # ── Build operation lists ──
  drop_not_null_ops = []
  alter_type_ops = []
  add_column_ops = []
  set_not_null_ops = []
  newly_added_columns = set()   # track columns added this run

  # ── Compare unified vs final ──
  for each (col_name, unified_meta) in unified_schema:

    if col_name NOT in final_cols:
      # ── NEW COLUMN ──
      add_column_ops.append({
        column: col_name,
        type: unified_meta['full_type']
      })
      newly_added_columns.add(col_name)
      # NOTE: Do NOT add SET NOT NULL for newly added columns.
      # Existing rows will have NULL in this column after ADD COLUMN.
      # SET NOT NULL would fail with "column contains null values".

    else:
      # ── EXISTING COLUMN ──
      final_type = final_cols[col_name]

      # Normalize base types via get_widened_type to handle Snowflake aliases
      # (TEXT vs VARCHAR, INT vs NUMBER(38,0), etc.)
      lca = get_widened_type(unified_meta['full_type'], final_type)

      # Type change needed?
      if lca | upper != final_type | upper:
        # Determine strategy: same base type → ALTER, different → swap
        lca_base = get_widened_type(lca, lca).split('(')[0]  # normalize
        final_base = get_widened_type(final_type, final_type).split('(')[0]  # normalize

        if lca_base == final_base:
          strategy = 'alter'    # same family, ALTER COLUMN SET DATA TYPE
        else:
          strategy = 'swap'     # cross-family, column swap

        alter_type_ops.append({
          column: col_name,
          old_type: final_type,
          new_type: lca,
          strategy: strategy
        })
      # else: lca == final_type → final already at or wider. No DDL.

      # Nullability change?
      final_is_nullable = final_nullable_map[col_name]
      if NOT final_is_nullable AND unified_meta['is_nullable']:
        drop_not_null_ops.append({column: col_name})
      elif final_is_nullable AND NOT unified_meta['is_nullable']:
        set_not_null_ops.append({column: col_name})

  # ── SOFT-DROP: columns in final but missing from ALL sources ──
  for each col_name in final_cols:
    if col_name NOT in unified_schema:
      if NOT final_nullable_map.get(col_name, true):
        drop_not_null_ops.append({column: col_name})
        # Column stays. Future rows get NULL. Must be nullable.

  # ── Execute in order ──

  # Order 1: DROP NOT NULL
  for each op in drop_not_null_ops:
    run_query("ALTER TABLE {target} MODIFY COLUMN {op.column} DROP NOT NULL")
    log("PLT: dropped NOT NULL on {op.column}")

  # Order 2: CHANGE DATA TYPE
  for each op in alter_type_ops:
    if op.strategy == 'alter':
      run_query("ALTER TABLE {target} MODIFY COLUMN {op.column} SET DATA TYPE {op.new_type}")
      log("PLT: widened {op.column}: {op.old_type} -> {op.new_type} (alter)")

    elif op.strategy == 'swap':
      # NOTE: Snowflake DDL is auto-committed. Each ALTER commits independently.
      # BEGIN/COMMIT does NOT make this atomic. If it fails mid-way, the cleanup
      # logic in Step 0 (above) will recover on the next run.
      tmp_col = op.column ~ '__plt_migrate'
      run_query("ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {tmp_col} {op.new_type}")
      run_query("UPDATE {target} SET {tmp_col} = TRY_CAST({op.column} AS {op.new_type})")
      run_query("ALTER TABLE {target} DROP COLUMN {op.column}")
      run_query("ALTER TABLE {target} RENAME COLUMN {tmp_col} TO {op.column}")
      log("PLT: promoted {op.column}: {op.old_type} -> {op.new_type} (column swap)")

  # Order 3: ADD COLUMNS
  for each op in add_column_ops:
    run_query("ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {op.column} {op.type}")
    log("PLT: added column {op.column} ({op.type})")

  # Order 4: SET NOT NULL (only for pre-existing columns, never for newly added)
  for each op in set_not_null_ops:
    if op.column NOT in newly_added_columns:
      run_query("ALTER TABLE {target} MODIFY COLUMN {op.column} SET NOT NULL")
      log("PLT: set NOT NULL on {op.column}")
```

### Operation Ordering

| Order | Operation | SQL | When |
|-------|-----------|-----|------|
| 1 | DROP NOT NULL | `ALTER TABLE MODIFY COLUMN col DROP NOT NULL` | Column becoming nullable (source disagrees, soft-drop) |
| 2 | CHANGE DATA TYPE (same-family) | `ALTER TABLE MODIFY COLUMN col SET DATA TYPE new_type` | Same base type, wider precision/scale/length |
| 2 | CHANGE DATA TYPE (cross-family) | Column swap: ADD tmp -> UPDATE TRY_CAST -> DROP old -> RENAME | Different base types |
| 3 | ADD COLUMN | `ALTER TABLE ADD COLUMN IF NOT EXISTS col type` | New column from sources |
| 4 | SET NOT NULL | `ALTER TABLE MODIFY COLUMN col SET NOT NULL` | All sources agree NOT NULL |

Why this order: DROP NOT NULL must happen before ALTER TYPE (widening might need nullable column). ADD COLUMN before SET NOT NULL (can't constrain a column that doesn't exist yet). This matches the catalog service's `DestinationOperationType` ordering.

### Column Swap Detail (Cross-Family)

When the final table has `SALARY NUMBER(10,3)` and the unified type is `VARCHAR(16777216)` (because one source had FLOAT, LCA of NUMBER + FLOAT = VARCHAR):

```sql
ALTER TABLE final ADD COLUMN IF NOT EXISTS SALARY__plt_migrate VARCHAR(16777216);
UPDATE final SET SALARY__plt_migrate = TRY_CAST(SALARY AS VARCHAR(16777216));
ALTER TABLE final DROP COLUMN SALARY;
ALTER TABLE final RENAME COLUMN SALARY__plt_migrate TO SALARY;
```

**No BEGIN/COMMIT** — Snowflake DDL is auto-committed. Each ALTER commits independently regardless of transaction blocks. If the swap fails mid-way (e.g., after DROP but before RENAME), the cleanup logic in Step 0 of `plt_evolve_table` will detect the orphaned `__plt_migrate` column on the next run and recover automatically.

**Note on column ordering**: After a column swap, the column moves to the end of the table (since it was renamed from a newly added column). This does not affect SQL correctness (columns are referenced by name), but may surprise users inspecting the table schema.

TRY_CAST ensures incompatible values become NULL rather than failing the migration.

### Narrowing Prevention

If the final table already has `VARCHAR(500)` and unified says `NUMBER(10,3)` (because all current sources happen to be numeric, but a previous source had string), `get_widened_type('NUMBER(10,3)', 'VARCHAR(500)')` returns `VARCHAR(500)`. LCA == final type → no DDL. The final table is never narrowed.

### Idempotency

| Operation | Idempotent? | Notes |
|-----------|-------------|-------|
| DROP NOT NULL | Yes | No-op on already-nullable column |
| ALTER SET DATA TYPE (same type) | Yes | No-op |
| ADD COLUMN IF NOT EXISTS | Yes | Explicit IF NOT EXISTS |
| SET NOT NULL | No | Fails if column has NULL values. Skipped for newly added columns (which always have NULLs in existing rows). |
| Column swap | No | Partial state on failure. Step 0 cleanup recovers on next run. |

### Error Handling

- **Fail fast**: any DDL failure aborts the run
- **Auto-recovery**: Step 0 detects and cleans up orphaned `__plt_migrate` columns from prior failed column swaps. Two recovery paths: (a) if original column still exists, drop the temp; (b) if original was already dropped, rename temp to original.
- **Logging**: every DDL statement is logged. On failure, the log shows exactly which step failed.

---

## 6. Macro: `plt_generate_union(sources, unified_schema)` — SELECT Generator

**File:** `macros/plt_generate_union.sql` (new)

**Purpose:** For each source, generate a SELECT that casts all columns to unified types and NULL-pads missing columns. UNION ALL them together.

### Algorithm

```
plt_generate_union(sources, unified_schema, source_columns):
  # source_columns is passed from plt_resolve_schema to avoid redundant
  # adapter.get_columns_in_relation() calls (saves N metadata queries)

  first = true

  for each src in sources:
    if src.label NOT in source_columns: continue  # source doesn't exist, skip

    # Reuse column map from plt_resolve_schema
    src_cols = source_columns[src.label]

    if not first:
      emit "UNION ALL"
    first = false

    emit "SELECT"

    for each (col_name, meta) in unified_schema:
      if col_name in src_cols:
        src_type = src_cols[col_name]
        if src_type == meta['full_type']:
          emit "  {col_name},"                                          # types match
        else:
          emit "  TRY_CAST({col_name} AS {meta['full_type']}) AS {col_name},"  # cast to unified
      else:
        emit "  NULL::{meta['full_type']} AS {col_name},"               # NULL-pad

    emit "  '{src.label}' AS __hevo_source_pipeline"
    emit "FROM {src.database}.{src.schema}.{src.table}"
```

### Generated SQL Example

Given:
- k1: `ID NUMBER(38,0)`, `NAME VARCHAR(100)`, `SALARY NUMBER(10,3)`
- k1_prime: `ID NUMBER(38,0)`, `NAME VARCHAR(500)`, `SALARY FLOAT`, `PHONE VARCHAR(256)`

Unified schema: `ID NUMBER(38,0)`, `NAME VARCHAR(500)`, `SALARY VARCHAR(16777216)`, `PHONE VARCHAR(256)`

```sql
SELECT
  ID,
  TRY_CAST(NAME AS VARCHAR(500)) AS NAME,
  TRY_CAST(SALARY AS VARCHAR(16777216)) AS SALARY,
  NULL::VARCHAR(256) AS PHONE,
  'k1' AS __hevo_source_pipeline
FROM DB.STAGING_K1.ORDERS

UNION ALL

SELECT
  ID,
  NAME,
  TRY_CAST(SALARY AS VARCHAR(16777216)) AS SALARY,
  PHONE,
  'k1_prime' AS __hevo_source_pipeline
FROM DB.STAGING_K1_PRIME.ORDERS
```

### Key Decisions

| Decision | Rationale |
|----------|-----------|
| **TRY_CAST** (not CAST) | Bad data becomes NULL, never fails the run. Borrowed from senior's POC. |
| **Iterate over unified_schema** (not source columns) | Ensures every source SELECT has the same column list in the same order (UNION ALL requirement). |
| **Source label as literal string** | `'k1'` not the full relation path. Matches existing `__hevo_source_pipeline` convention. |
| **Skip non-existent sources** | If a source table doesn't exist, omit it from UNION ALL entirely. No error. |

---

## 7. File Layout

```
macros/
  get_widened_type.sql            # existing — LCA kernel, no changes
  plt_resolve_schema.sql          # NEW — introspect + cross-source LCA
  plt_evolve_table.sql            # NEW — DDL executor
  plt_generate_union.sql          # NEW — SELECT generator with TRY_CAST

models/plt/
  orders.sql                      # orchestrator — calls macros 1-3 in sequence
```

Old macros to retire: `resolve_column_schema.sql`, `evolve_final_table.sql`, `generate_source_select.sql`

---

## 8. Borrowing from Prior Art

| Source | What We Borrowed | Where It Lands |
|--------|-----------------|----------------|
| **macro-architecture-v2-design.md** | 5-stage pipeline concept, operation ordering (4/7/9/11), LCA against final table (not just comparison) | `plt_evolve_table` operation ordering, `get_widened_type` LCA logic |
| **Senior's dbt-poc** | TRY_CAST everywhere, soft-drop (never drop columns), `adapter.get_columns_in_relation()` for introspection, separate DDL + merge concerns | `plt_generate_union` uses TRY_CAST, `plt_evolve_table` soft-drop behavior, all macros use adapter for introspection |
| **dbt_full_picture.md** | Two-macro split (SELECT-side + DDL-side), column swap with transaction for cross-family, idempotent schema checks | `plt_evolve_table` column swap pattern with BEGIN/COMMIT, `plt_resolve_schema` + `plt_generate_union` as SELECT-side |
| **Existing get_widened_type.sql** | Correct DAG-based LCA with precision/scale math | Unchanged — used by both `plt_resolve_schema` and `plt_evolve_table` |

---

## 9. Test Scenario Coverage

### Fully Covered by This Design

| ID | Test | How |
|----|------|-----|
| T01 | Type matrix (29 pairs) | `get_widened_type` LCA + `plt_generate_union` TRY_CAST + `plt_evolve_table` DDL |
| S01 | Column add (one source) | `plt_resolve_schema` unions columns, `plt_generate_union` NULL-pads, `plt_evolve_table` ADD COLUMN |
| S02 | Column name mismatch | Same as S01 — different names are different columns, both appear |
| S03 | Multiple extra columns | Same as S01 — scales to N columns |
| S04 | Drop column all (soft-drop) | `plt_evolve_table` soft-drop: column stays, DROP NOT NULL |
| E01 | One source drops column | `plt_evolve_table` soft-drop on final, `plt_generate_union` NULL-pads |
| E02 | Both sources add column | `plt_evolve_table` ADD COLUMN |
| E03 | Same-family type widen | `plt_evolve_table` ALTER COLUMN SET DATA TYPE |
| E04 | Type narrowing blocked | `get_widened_type` LCA ensures final never narrows |
| E05 | Asymmetric evolution | `plt_resolve_schema` cross-source LCA handles divergent schemas |
| E06 | Drop+recreate type evolution | Cross-family column swap handles type changes across runs |
| C01 | NOT NULL mismatch | `plt_resolve_schema` resolves nullable (OR across sources), `plt_evolve_table` DROP NOT NULL |
| C02 | NOT NULL all agree | `plt_resolve_schema` preserves NOT NULL, `plt_evolve_table` SET NOT NULL |
| C03 | NOT NULL on new column | Missing from source -> force nullable, `plt_evolve_table` skips SET NOT NULL |
| I01 | Idempotent rerun | dbt merge strategy + idempotent DDL |

### Phase 2 (Not Covered)

| ID | Test | Why Deferred |
|----|------|-------------|
| T02 | Redshift type matrix | Redshift-specific type hierarchy needed |
| P01-P06 | PK / merge key | Dynamic merge key resolution from warehouse metadata |
| C04 | Redshift composite PK | Redshift-specific NOT NULL + PK interaction |
| C05 | NOT NULL PK promotion | PK-aware constraint management |

---

## 10. Complexity & Performance

### Queries Per Run

| Query | Count | Source |
|-------|-------|--------|
| `adapter.get_columns_in_relation()` | N (sources in resolve_schema) + 1 (final in evolve_table) | source_columns reused by generate_union — no redundant calls |
| INFORMATION_SCHEMA for nullability | N (sources) + 1 (final) | One per table |
| DDL statements | Variable — 0 if no changes | Only on schema drift |

Where N = number of source pipelines (typically 2-3).

### Total per run: ~2N + 2 metadata queries + variable DDL

For N=2: 6 metadata queries minimum. Acceptable for a schema evolution macro that runs once per dbt invocation.

---

## 11. Resolved Questions (from spec review)

1. **Transaction support on Snowflake for column swap**: **RESOLVED — NOT atomic.** Snowflake DDL is auto-committed. Each ALTER TABLE commits independently regardless of BEGIN/COMMIT blocks. The design now uses Step 0 cleanup (detect and recover `__plt_migrate` columns) instead of relying on transactions.

2. **`adapter.get_columns_in_relation()` caching**: **RESOLVED — not a concern.** `plt_generate_union` now receives `source_columns` from `plt_resolve_schema` instead of re-querying. It never reads the final table, so post-DDL cache staleness doesn't apply.

3. **Column ordering**: **RESOLVED — guaranteed.** Jinja dicts are backed by Python dicts, which preserve insertion order in Python 3.7+ (all dbt versions >= 1.0). UNION ALL column order is consistent.

## 12. Design Decisions & Rationale

1. **`on_schema_change: 'ignore'` (default)** — We explicitly do NOT set `append_new_columns` or `sync_all_columns`. Our `plt_evolve_table` owns all DDL. If `plt_evolve_table` succeeds, the final table already has all needed columns before the MERGE runs. If it fails partially, `append_new_columns` would add columns with dbt-inferred types that might conflict with our LCA-computed types.

2. **Zero active sources guard** — If ALL sources are missing (none exist), `plt_resolve_schema` returns an empty unified schema. `plt_generate_union` would emit no SQL. The model should detect this and abort gracefully rather than letting dbt try to MERGE an empty SELECT.

3. **TRY_CAST replaces v2's `conversion_expr` concept** — The v2 design had `get_widened_type` returning type-specific conversion expressions (e.g., `TO_VARCHAR()` vs `CAST()`). This v3 design uses `TRY_CAST` universally. TRY_CAST handles all safe conversions on Snowflake, and returns NULL for incompatible values. This is a deliberate simplification — fewer code paths, same correctness.
