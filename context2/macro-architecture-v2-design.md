# PLT Macro Architecture v2 — Design Spec

> Date: 2026-03-11
> Status: Draft — pending approval
> Scope: Phase 1 (schema evolution only, PK/merge key deferred)

---

## 1. Motivation

The current 4-macro architecture (`resolve_column_schema`, `get_widened_type`, `evolve_final_table`, `generate_source_select`) has three problems:

1. **Too tightly coupled** — `resolve_column_schema` returns `{COL: TYPE}`, a flat dict with no room for nullability, source provenance, or promotion strategy. Downstream macros must re-derive information.
2. **Doesn't generalize** — Adding PK resolution, NOT NULL enforcement, or multi-step promotion requires hacking into existing macros rather than extending the pipeline.
3. **Hard to test/debug** — `evolve_final_table` both computes the diff AND executes DDL. When it fails, you can't tell which part went wrong.

## 2. Design Principles

1. **Two-pass schema inference** — first across all sources, then against the final table. PLT never assumes what the destination schema is.
2. **Compute then execute** — pure computation (diff) is separated from side effects (DDL). The diff is testable in isolation.
3. **Rich metadata flows through the pipeline** — each column carries type, precision/scale, nullability, source provenance, and promotion strategy.
4. **Operation ordering mirrors the catalog service** — `DestinationOperationType` execution order is battle-tested in production.
5. **Stateless per run** — no stored schema state. Every run re-derives everything from current table metadata.

---

## 3. Pipeline Overview

```
┌──────────────────┐     ┌───────────────────────┐     ┌─────────────────────┐
│ Stage 1           │     │ Stage 2                │     │ Stage 3              │
│ gather_source_    │────▶│ resolve_unified_       │────▶│ compute_schema_      │
│ schemas()         │     │ schema()               │     │ diff()               │
│                   │     │                        │     │                      │
│ Read each source  │     │ Pairwise LCA across    │     │ LCA(unified, final)  │
│ table's columns,  │     │ all sources. Fold into │     │ per column. Emit     │
│ types, nullability│     │ one unified dict with  │     │ ordered operation     │
│                   │     │ rich column metadata   │     │ list                 │
│ Output: list of   │     │                        │     │                      │
│ source_schema     │     │ Output: unified_schema │     │ Output: operations[] │
│ dicts             │     │ dict                   │     │                      │
└──────────────────┘     └───────────────────────┘     └─────────────────────┘
                                                              │
                                                              ▼
┌──────────────────┐     ┌───────────────────────┐     ┌─────────────────────┐
│ Stage 5           │     │ Re-fetch final table   │     │ Stage 4              │
│ generate_source_  │◀────│ schema (post-DDL)      │◀────│ execute_schema_      │
│ select()          │     │                        │     │ operations()         │
│                   │     │                        │     │                      │
│ Per source: CAST  │     │                        │     │ Walk operation list  │
│ to final types +  │     │                        │     │ Execute DDL in order │
│ NULL-pad missing  │     │                        │     │ (side effects)       │
└──────────────────┘     └───────────────────────┘     └─────────────────────┘
```

### Model SQL (`orders.sql`)

```sql
{% set plt_sources = [
  {'database': var('k1_db'), 'schema': var('k1_schema'), 'table': var('k1_table'), 'label': 'k1'},
  {'database': var('k1_db'), 'schema': var('k1_prime_schema'), 'table': var('k1_table'), 'label': 'k1_prime'}
] %}

{{ config(materialized='incremental', unique_key=['id', '__hevo_source_pipeline'], incremental_strategy='merge') }}

{# Stage 1: Inspect all source schemas #}
{% set source_schemas = gather_source_schemas(plt_sources) %}

{# Stage 2: Resolve unified "desired" schema across all sources #}
{% set unified = resolve_unified_schema(source_schemas) %}

{# Stage 3: Diff unified schema against current final table #}
{% set diff_result = compute_schema_diff(unified, this) %}

{# Stage 4: Execute DDL in correct order (only in incremental mode) #}
{% if is_incremental() %}
  {% do execute_schema_operations(this, diff_result.operations) %}
{% endif %}

{# Stage 5: Generate SELECT per source, casting to post-DDL final schema #}
{% set ns = namespace(first=true) %}
{% for src in plt_sources %}
  {% set select_sql = generate_source_select(src, this, unified) %}
  {% if select_sql %}
    {% if not ns.first %} UNION ALL {% endif %}
    {{ select_sql }}
    {% set ns.first = false %}
  {% endif %}
{% endfor %}
```

---

## 4. Supporting Macro: `get_widened_type(type_a, type_b)`

### Purpose

Given two Snowflake column types, find their Lowest Common Ancestor (LCA) in the type hierarchy. Returns the resolved type, promotion strategy, and conversion expression.

### Implementation Strategy: Hybrid Hardcoded LCA + Runtime Expansion Math

**Step 1: Normalize base types.** Strip precision/scale/length to get the base family:
- `NUMBER(10,3)` → `NUMBER`
- `VARCHAR(500)` → `VARCHAR`
- `TIMESTAMP_NTZ(6)` → `TIMESTAMP_NTZ`

**Step 2: Hardcoded base-type LCA lookup.** 11 Snowflake base types → 66 unique symmetric pairs. Stored as a Jinja dict:

```
Base Type LCA Map (symmetric — only showing unique pairs):

NUMBER + NUMBER       → NUMBER       (same family, expand precision/scale)
NUMBER + BOOLEAN      → NUMBER       (child→parent)
NUMBER + FLOAT        → VARCHAR      (siblings, LCA=STRING)
NUMBER + VARCHAR      → VARCHAR      (child→root)
NUMBER + DATE         → VARCHAR      (cousins, LCA=STRING)
NUMBER + TIMESTAMP_NTZ→ VARCHAR
NUMBER + TIMESTAMP_TZ → VARCHAR
NUMBER + TIME         → VARCHAR
NUMBER + BINARY       → VARCHAR
NUMBER + VARIANT      → VARCHAR
NUMBER + ARRAY        → VARCHAR

BOOLEAN + BOOLEAN     → BOOLEAN
BOOLEAN + FLOAT       → VARCHAR      (BOOLEAN→NUMBER→STRING, FLOAT→STRING)
BOOLEAN + VARCHAR     → VARCHAR
BOOLEAN + DATE        → VARCHAR
BOOLEAN + ...         → VARCHAR      (all other cross-family → VARCHAR)

FLOAT + FLOAT         → FLOAT
FLOAT + VARCHAR       → VARCHAR
FLOAT + ...           → VARCHAR

VARCHAR + VARCHAR     → VARCHAR      (same family, expand length)
VARCHAR + *           → VARCHAR      (VARCHAR is root, always wins)

DATE + DATE           → DATE
DATE + TIMESTAMP_NTZ  → TIMESTAMP_NTZ (child→parent)
DATE + TIMESTAMP_TZ   → TIMESTAMP_TZ  (child→grandparent)
DATE + TIME           → VARCHAR

TIMESTAMP_NTZ + TIMESTAMP_NTZ → TIMESTAMP_NTZ (expand precision)
TIMESTAMP_NTZ + TIMESTAMP_TZ  → TIMESTAMP_TZ  (child→parent)
TIMESTAMP_NTZ + TIME          → VARCHAR

TIMESTAMP_TZ + TIMESTAMP_TZ → TIMESTAMP_TZ (expand precision)
TIMESTAMP_TZ + TIME         → VARCHAR

TIME + TIME           → TIME (expand precision)

VARIANT + VARIANT     → VARIANT
VARIANT + ARRAY       → VARIANT (parent wins)

ARRAY + ARRAY         → ARRAY

BINARY + BINARY       → BINARY (expand length)
```

**Step 3: Compute expansion level at the LCA.** Runtime math based on the LCA type:

| LCA Type | Expansion Math | Source |
|----------|---------------|--------|
| NUMBER | `new_scale = max(s1, s2); new_int = max(p1-s1, p2-s2); new_precision = new_int + new_scale` | `PrecisionScaleExpansionScheme` |
| VARCHAR | `new_length = max(len1, len2)` — clamped to [1, 16777216] | `LengthBasedExpansionScheme` |
| TIMESTAMP_NTZ/TZ | `new_precision = max(prec1, prec2)` — clamped to [0, 9] | `PrecisionBasedExpansionScheme` |
| TIME | `new_precision = max(prec1, prec2)` — clamped to [0, 9] | `PrecisionBasedExpansionScheme` |
| BINARY | `new_length = max(len1, len2)` | `LengthBasedExpansionScheme` |
| BOOLEAN, DATE, FLOAT, VARIANT, ARRAY | Fixed size — no expansion math needed | N/A |

**When LCA is VARCHAR from cross-family widening**: use `superTypeSize` (the string length needed to represent the child type's maximum value). For Phase 1, default to `VARCHAR(16777216)` (max) to be safe. Optimization can tighten this later.

**Step 4: Determine promotion strategy:**

| Condition | Strategy | Notes |
|-----------|----------|-------|
| Same base type, LCA == same type | `single_step` | ALTER COLUMN SET DATA TYPE |
| Different base types, LCA == parent or grandparent | `multi_step` | Temp col → UPDATE CAST → DROP → RENAME |
| Same types, same precision/scale | `none` | No change needed |

**Step 5: Return conversion expression:**

| From → To | Expression |
|-----------|-----------|
| NUMBER → VARCHAR | `TO_VARCHAR(%s)` |
| BOOLEAN → NUMBER | `%s::NUMBER` |
| BOOLEAN → VARCHAR | `TO_VARCHAR(%s)` |
| DATE → TIMESTAMP_NTZ | `%s::TIMESTAMP_NTZ` |
| DATE → TIMESTAMP_TZ | `%s::TIMESTAMP_TZ` |
| TIMESTAMP_NTZ → TIMESTAMP_TZ | `%s::TIMESTAMP_TZ` |
| FLOAT → VARCHAR | `TO_VARCHAR(%s)` |
| NUMBER(p1,s1) → NUMBER(p2,s2) | `CAST(%s AS NUMBER(p2,s2))` |
| VARCHAR(x) → VARCHAR(y) | `%s` (Snowflake auto-widens) |
| Same type, same params | `%s` (no cast) |
| Any → VARCHAR (cross-family) | `TO_VARCHAR(%s)` |

### Return Value

```python
{
    'resolved_type': 'NUMBER',           # base type after LCA
    'full_type': 'NUMBER(15,6)',         # with precision/scale/length
    'precision': 15,                      # for NUMBER types
    'scale': 6,                           # for NUMBER types
    'char_size': None,                    # for VARCHAR types
    'strategy': 'single_step',           # single_step | multi_step | none
    'conversion_expr': 'CAST(%s AS NUMBER(15,6))',  # SQL expression template
}
```

### Complexity

- Base type lookup: O(1) — dict access
- Expansion math: O(1) — max() operations
- Total per call: **O(1)**

---

## 5. Stage 1: `gather_source_schemas(plt_sources)`

### Purpose

Read column metadata from each source (staging) table. No resolution — just raw collection.

### Per Source, Per Column — What to Collect

| Field | Source | Notes |
|-------|--------|-------|
| `column_name` | `adapter.get_columns_in_relation()` → `.name` | Upper-cased for consistency |
| `data_type` | `.dtype` | Base type string (e.g., `NUMBER`, `VARCHAR`) |
| `char_size` | `.char_size` | For VARCHAR/BINARY |
| `numeric_precision` | `.numeric_precision` | For NUMBER |
| `numeric_scale` | `.numeric_scale` | For NUMBER |
| `is_nullable` | `INFORMATION_SCHEMA.COLUMNS.IS_NULLABLE` | Not available from adapter — requires separate query |

### Column Filtering

- **Filter OUT**: columns matching `__HEVO__%__TEMP` pattern (loader temp artifacts during multi-step promotion)
- **Retain**: internal columns like `_HEVO_DATABASE_NAME`, `_HEVO_INGESTED_AT` — these are legitimate columns that should propagate to the final table
- **Retain**: `__HEVO_SOURCE_PIPELINE` — handled separately by the model (appended in SELECT as a literal)

### Queries Per Source

Two queries per source table:
1. `adapter.get_columns_in_relation(rel)` → column names, base types, precision/scale/size
2. `run_query("SELECT COLUMN_NAME, IS_NULLABLE FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")` → nullability

These can potentially be combined into a single INFORMATION_SCHEMA query that returns everything.

### Source Existence Guard

```python
{% set rel = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.table) %}
{% if rel is none %}
  → source_schema.exists = False
  → Skip this source entirely (no error, just log)
{% endif %}
```

### Output

```python
[
  {
    'label': 'k1',
    'database': 'DB', 'schema': 'STAGING_K1', 'table': 'ORDERS',
    'exists': True,
    'columns': {
      'ID':     {'data_type': 'NUMBER', 'precision': 38, 'scale': 0, 'char_size': None, 'is_nullable': False},
      'NAME':   {'data_type': 'VARCHAR', 'precision': None, 'scale': None, 'char_size': 100, 'is_nullable': True},
      'SALARY': {'data_type': 'NUMBER', 'precision': 10, 'scale': 3, 'char_size': None, 'is_nullable': True},
    }
  },
  {
    'label': 'k1_prime',
    'database': 'DB', 'schema': 'STAGING_K1_PRIME', 'table': 'ORDERS',
    'exists': True,
    'columns': {
      'ID':     {'data_type': 'NUMBER', 'precision': 38, 'scale': 0, 'char_size': None, 'is_nullable': False},
      'NAME':   {'data_type': 'VARCHAR', 'precision': None, 'scale': None, 'char_size': 500, 'is_nullable': True},
      'SALARY': {'data_type': 'NUMBER', 'precision': 15, 'scale': 6, 'char_size': None, 'is_nullable': True},
      'PHONE':  {'data_type': 'VARCHAR', 'precision': None, 'scale': None, 'char_size': 256, 'is_nullable': True},
    }
  }
]
```

---

## 6. Stage 2: `resolve_unified_schema(source_schemas)`

### Purpose

Merge all source schemas into a single "desired schema" — the schema that represents what all sources collectively want. Uses pairwise LCA via `get_widened_type`.

### Algorithm

```
unified = {}

for each source_schema in source_schemas:
  if not source_schema.exists: continue

  for each (col_name, col_meta) in source_schema.columns:
    if col_name not in unified:
      # First occurrence — seed from this source
      unified[col_name] = {
        'resolved_type': col_meta.data_type,
        'precision': col_meta.precision,
        'scale': col_meta.scale,
        'char_size': col_meta.char_size,
        'is_nullable': col_meta.is_nullable,
        'sources': [source_schema.label],
        'original_types': {source_schema.label: full_type_string(col_meta)},
        'strategy': 'none',
        'conversion_expr': '%s',
      }
    else:
      # Column seen before — resolve via LCA
      existing = unified[col_name]
      widened = get_widened_type(
        type_a = {existing.resolved_type, existing.precision, existing.scale, existing.char_size},
        type_b = {col_meta.data_type, col_meta.precision, col_meta.scale, col_meta.char_size}
      )
      existing.resolved_type = widened.resolved_type
      existing.precision = widened.precision
      existing.scale = widened.scale
      existing.char_size = widened.char_size
      existing.strategy = widened.strategy  # most recent widening strategy
      existing.conversion_expr = widened.conversion_expr

      # Nullability: nullable if ANY source says nullable
      existing.is_nullable = existing.is_nullable OR col_meta.is_nullable

      # Track provenance
      existing.sources.append(source_schema.label)
      existing.original_types[source_schema.label] = full_type_string(col_meta)

# Post-processing: columns missing from some sources are force-nullable
for each (col_name, col_meta) in unified:
  if len(col_meta.sources) < len(active_sources):
    col_meta.is_nullable = True  # NULL-padded from missing sources
```

### Key Rule: Missing Column = Force Nullable

If source k1 has `PHONE` but k1_prime doesn't, then k1_prime rows will produce `NULL AS PHONE`. The column MUST be nullable in the final table regardless of k1's NOT NULL constraint.

### Complexity

- O(N × C) where N = number of sources, C = max columns per source
- Each `get_widened_type` call is O(1)
- Total: **O(N × C)** — linear scan, no nested loops

### Output

```python
{
  'ID': {
    'resolved_type': 'NUMBER', 'full_type': 'NUMBER(38,0)',
    'precision': 38, 'scale': 0, 'char_size': None,
    'is_nullable': False,
    'sources': ['k1', 'k1_prime'],
    'original_types': {'k1': 'NUMBER(38,0)', 'k1_prime': 'NUMBER(38,0)'},
    'strategy': 'none', 'conversion_expr': '%s'
  },
  'SALARY': {
    'resolved_type': 'NUMBER', 'full_type': 'NUMBER(15,6)',
    'precision': 15, 'scale': 6, 'char_size': None,
    'is_nullable': True,
    'sources': ['k1', 'k1_prime'],
    'original_types': {'k1': 'NUMBER(10,3)', 'k1_prime': 'NUMBER(15,6)'},
    'strategy': 'single_step', 'conversion_expr': 'CAST(%s AS NUMBER(15,6))'
  },
  'PHONE': {
    'resolved_type': 'VARCHAR', 'full_type': 'VARCHAR(256)',
    'precision': None, 'scale': None, 'char_size': 256,
    'is_nullable': True,  # forced: missing from k1
    'sources': ['k1_prime'],
    'original_types': {'k1_prime': 'VARCHAR(256)'},
    'strategy': 'none', 'conversion_expr': '%s'
  }
}
```

---

## 7. Stage 3: `compute_schema_diff(unified_schema, final_relation)`

### Purpose

Compare the unified "desired" schema against the current final table schema. Produce an ordered list of DDL operations. **Pure computation — no side effects.**

### Key Insight: LCA Against Final Table (Not Just Comparison)

The catalog's `DestinationSchemaChangeDiffService` runs `findCommonEvaluableAncestor(mapping_field, existing_field)` — it doesn't just compare "is unified wider?". It finds the LCA of unified type vs final type to determine the evolved type.

This handles edge cases correctly:

| Unified Type | Final Type | LCA | Action |
|---|---|---|---|
| NUMBER(15,6) | NUMBER(10,3) | NUMBER(15,6) | CHANGE_DATA_TYPE (single_step) — widen |
| NUMBER(10,3) | NUMBER(20,6) | NUMBER(20,6) | No change — final already wider |
| NUMBER(10,3) | FLOAT | VARCHAR(max) | CHANGE_DATA_TYPE (multi_step) — cross-family |
| VARCHAR(100) | VARCHAR(500) | VARCHAR(500) | No change — final already wider |
| NUMBER(10,3) | VARCHAR(500) | VARCHAR(500) | No change — final already wider (VARCHAR absorbs all) |

**When LCA == final_type**: no DDL needed. But `generate_source_select` will still CAST source values to match the final type. The casting happens in the SELECT, not via ALTER.

### Algorithm

```
Input: unified_schema (from Stage 2), final_relation (dbt Relation or None)

1. If final_relation is None or not is_incremental():
   → Return empty operations (dbt CREATE handles first run)

2. Fetch final table columns:
   → Query INFORMATION_SCHEMA.COLUMNS for final_relation
   → Build: final_columns = {COL_NAME: {data_type, precision, scale, char_size, is_nullable}}

3. Initialize operation buckets:
   → remove_not_null_ops = []
   → change_data_type_ops = []
   → add_fields_ops = []
   → add_not_null_ops = []
   → metadata = {soft_dropped: [], narrowing_blocked: []}

4. For each (col_name, unified_meta) in unified_schema:

   a. Column NOT in final_columns → NEW COLUMN
      → add_fields_ops.append({column: col_name, type: unified_meta.full_type, nullable: True})
      → If NOT unified_meta.is_nullable (all sources NOT NULL):
          add_not_null_ops.append({column: col_name})

   b. Column IN final_columns → COMPARE
      → Run get_widened_type(unified_type, final_type) to find LCA

      → If LCA type == final type AND same precision/scale/size:
          # Final is already at or wider than unified — no DDL
          pass

      → Elif LCA type != final type (need to widen final):
          # Determine what we're changing TO — it's the LCA, not unified
          change_data_type_ops.append({
            column: col_name,
            old_type: final_meta.full_type,
            new_type: lca.full_type,
            strategy: lca.strategy,
            conversion_expr: lca.conversion_expr,
          })

      → Check nullability:
          If final is NOT NULL but unified says nullable:
            remove_not_null_ops.append({column: col_name})
          If final is nullable but unified says NOT NULL:
            add_not_null_ops.append({column: col_name})

5. For each (col_name, final_meta) in final_columns NOT in unified_schema:
   → Destination-only column (soft-drop — no DDL, column stays)
   → metadata.soft_dropped.append(col_name)
   → If final_meta is NOT NULL:
       remove_not_null_ops.append({column: col_name})
       (must make nullable so future INSERTs with NULL don't fail)

6. Assemble ordered operation list:
   operations = (
     [{...op, 'order': 4} for op in remove_not_null_ops] +
     [{...op, 'order': 7} for op in change_data_type_ops] +
     [{...op, 'order': 9} for op in add_fields_ops] +
     [{...op, 'order': 11} for op in add_not_null_ops]
   )
   → Already in correct execution order by construction

7. Return:
   {
     'operations': operations,
     'soft_dropped_columns': metadata.soft_dropped,
     'narrowing_blocked': metadata.narrowing_blocked,
   }
```

### Phase 1 Operation Types (Snowflake only)

| # | Operation | SQL Template | When |
|---|-----------|-------------|------|
| 4 | REMOVE_NOT_NULL_CONSTRAINT | `ALTER TABLE {t} MODIFY COLUMN {c} DROP NOT NULL` | Column becoming nullable |
| 7 | CHANGE_DATA_TYPE (single_step) | `ALTER TABLE {t} MODIFY COLUMN {c} SET DATA TYPE {new_type}` | Same-family widening |
| 7 | CHANGE_DATA_TYPE (multi_step) | 4-step: ADD tmp → UPDATE CAST → DROP old → RENAME | Cross-family widening |
| 9 | ADD_FIELDS | `ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} {type}` | New column |
| 11 | ADD_NOT_NULL_CONSTRAINT | `ALTER TABLE {t} MODIFY COLUMN {c} SET NOT NULL` | All sources agree NOT NULL |

### Deferred to Phase 2

- DROP_PRIMARY_KEY (3) — PK management deferred
- DROP_FIELDS (5) — PLT uses soft-drop only
- RENAME_FIELDS (6) — treated as drop + add
- ADD_PRIMARY_KEY (12) — PK management deferred
- Redshift-specific: DROP_AND_RECREATE_TABLE (1), DROP_SORT_KEY (2), ADD_SORT_KEY (13)
- Redshift-specific: NOT NULL required for PK fields (requiresNotNullForPK pattern from catalog)

---

## 8. Stage 4: `execute_schema_operations(final_relation, operations)`

### Purpose

Walk the operation list and execute DDL. **This is the only macro with side effects.**

### Algorithm

```
Input: final_relation (dbt Relation), operations (ordered list from Stage 3)

For each op in operations:

  Case op.type == 'REMOVE_NOT_NULL_CONSTRAINT':
    run_query("ALTER TABLE {final} MODIFY COLUMN {op.column} DROP NOT NULL")
    log("Dropped NOT NULL on {op.column}")

  Case op.type == 'CHANGE_DATA_TYPE' AND op.strategy == 'single_step':
    run_query("ALTER TABLE {final} MODIFY COLUMN {op.column} SET DATA TYPE {op.new_type}")
    log("Widened {op.column}: {op.old_type} → {op.new_type} (single-step)")

  Case op.type == 'CHANGE_DATA_TYPE' AND op.strategy == 'multi_step':
    {% set tmp_col = '__plt_tmp_' ~ op.column %}
    run_query("ALTER TABLE {final} ADD COLUMN {tmp_col} {op.new_type}")
    run_query("UPDATE {final} SET {tmp_col} = {op.conversion_expr.format(op.column)}")
    run_query("ALTER TABLE {final} DROP COLUMN {op.column}")
    run_query("ALTER TABLE {final} RENAME COLUMN {tmp_col} TO {op.column}")
    log("Promoted {op.column}: {op.old_type} → {op.new_type} (multi-step)")

  Case op.type == 'ADD_FIELDS':
    run_query("ALTER TABLE {final} ADD COLUMN IF NOT EXISTS {op.column} {op.full_type}")
    log("Added column {op.column} {op.full_type}")

  Case op.type == 'ADD_NOT_NULL_CONSTRAINT':
    run_query("ALTER TABLE {final} MODIFY COLUMN {op.column} SET NOT NULL")
    log("Added NOT NULL on {op.column}")
```

### Error Handling (Phase 1)

- **Fail fast**: Any DDL failure aborts the entire PLT run
- **No auto-recovery**: Multi-step promotion leaves temp columns on failure. Manual cleanup required.
- **Snowflake DDL is auto-committed**: No transaction rollback possible. Each ALTER is permanent immediately.
- **Logging**: Every DDL statement is logged for debugging. On failure, the log shows exactly which step failed and the state of the operation list.

### Idempotency

| Operation | Idempotent? | Notes |
|-----------|-------------|-------|
| DROP NOT NULL | Yes | No-op on already-nullable column |
| SET DATA TYPE (same type) | Yes | No-op |
| ADD COLUMN IF NOT EXISTS | Yes | Explicit IF NOT EXISTS |
| SET NOT NULL | No | Fails if column has NULL values |
| Multi-step promotion | No | Partial state on failure |

---

## 9. Stage 5: `generate_source_select(source, final_relation, unified_schema)`

### Purpose

Generate a SELECT statement for one source's contribution to the MERGE. Casts columns to match the **post-DDL final table schema** and NULL-pads missing columns.

### Why Re-fetch Final Schema

After Stage 4 executes DDL, the final table schema may have changed. `generate_source_select` re-fetches the final table columns to ensure CASTs target the actual current types. This is the stateless principle: each stage reads current state, never assumes.

### Algorithm

```
Input: source (one source dict), final_relation, unified_schema

1. Guard: if source table doesn't exist, return None
   {% set rel = adapter.get_relation(...) %}
   {% if rel is none %} {{ return(none) }} {% endif %}

2. Read source columns:
   {% set src_cols = {} %}
   {% for col in adapter.get_columns_in_relation(rel) %}
     {% do src_cols.update({col.name|upper: col.dtype|upper}) %}
   {% endfor %}

3. Read post-DDL final table columns:
   {% set final_cols = {} %}
   {% for col in adapter.get_columns_in_relation(final_relation) %}
     {% do final_cols.update({col.name|upper: col.dtype|upper}) %}
   {% endfor %}

4. Generate SELECT:
   For each (col_name, final_type) in final_cols:
     Skip if col_name == '__HEVO_SOURCE_PIPELINE' (appended separately)
     Skip if col_name matches __HEVO__%__TEMP pattern

     If col_name in src_cols:
       If src_cols[col_name] == final_type:
         → emit: col_name                    (no cast needed)
       Else:
         → look up conversion_expr from unified_schema or compute via get_widened_type
         → emit: {conversion_expr}(col_name) AS col_name
     Else:
       → emit: NULL::{final_type} AS col_name   (NULL-pad missing column)

   Append: '{source.label}' AS __hevo_source_pipeline

5. Return: complete SELECT statement
```

### Cast Expression Resolution

For each source column that differs from the final type, we need the right CAST expression. Two options:

**Option A**: Re-call `get_widened_type(source_type, final_type)` for each column. O(1) per call, correct by construction.

**Option B**: Look up the `conversion_expr` stored in `unified_schema` from Stage 2. Faster but may not match the post-DDL final type if the diff/execute stages changed the type further.

**Decision**: Use **Option A** — re-derive from `get_widened_type(source_type, final_type)`. It's O(1) per column and guarantees correctness against the actual post-DDL state. The unified_schema's conversion_expr was computed source-vs-source, not source-vs-final.

---

## 10. Internal Hevo Column Handling

### Column Categories

| Pattern | Example | Action |
|---------|---------|--------|
| `__HEVO__%__TEMP` | `__HEVO__SALARY__TEMP` | **Filter out** — loader temp artifact during multi-step promotion |
| `_HEVO_*` (single underscore prefix) | `_HEVO_DATABASE_NAME`, `_HEVO_INGESTED_AT` | **Retain** — legitimate internal columns, propagate to final table as-is |
| `__HEVO_SOURCE_PIPELINE` | | **Special** — not read from source. Appended as literal in generate_source_select. Only present when `merge_tables = false` (Phase 2 config). |

### Filtering Logic (in `gather_source_schemas`)

```python
{% for col in adapter.get_columns_in_relation(rel) %}
  {% set col_upper = col.name | upper %}
  {# Filter loader temp columns: __HEVO__*__TEMP #}
  {% if col_upper.startswith('__HEVO__') and col_upper.endswith('__TEMP') %}
    {% continue %}
  {% endif %}
  {# Skip __HEVO_SOURCE_PIPELINE — handled separately #}
  {% if col_upper == '__HEVO_SOURCE_PIPELINE' %}
    {% continue %}
  {% endif %}
  {# Everything else (including _HEVO_* internal cols) is included #}
  ...
{% endfor %}
```

---

## 11. Phase 2 Scope (Deferred)

These items are explicitly out of scope for Phase 1. Document for future reference.

### PK / Merge Key
- Dynamic merge key resolution from warehouse metadata (`INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE`)
- New `resolve_merge_key(sources)` macro
- Union of source PKs + optional `__hevo_source_pipeline`
- IS NOT DISTINCT FROM for nullable merge key columns in MERGE ON
- PK drop handling: merge key shrink, re-keying historical rows
- PK drop → append mode switching (decision: auto-detect vs external config)
- NULL-safe MERGE syntax validation on Snowflake

### merge_tables Flag
- `merge_tables = true`: rows with same PK across sources merge into one record (last writer wins)
- `merge_tables = false`: `__hevo_source_pipeline` added to merge key, rows coexist
- Config-driven behavior in model SQL

### Dedup / Watermark
- Dedup-in-SELECT (ROW_NUMBER) for sources that dropped PKs
- `_hevo_ingested_at` watermark-based incremental filtering
- Watermark state management between PLT runs

### Redshift-Specific
- NOT NULL required for PK fields (`requiresNotNullForPK` pattern)
- Distribution key changes → DROP_AND_RECREATE_TABLE
- Sort key management (DROP_SORT_KEY → ADD_SORT_KEY)

### Advanced Schema Operations
- DROP_FIELDS with hard-delete policy config
- RENAME_FIELDS detection (currently treated as drop + add)
- Multi-step promotion auto-recovery (detect and clean stale `__plt_tmp_*` columns)

### Multi-Step Promotion Failure Recovery
- Detect leftover `__plt_tmp_*` columns from failed runs
- Auto-cleanup or guided recovery
- Transaction-like guarantees (best-effort on Snowflake)

---

## 12. Macro File Layout

```
plt-poc-dbt/macros/
├── get_widened_type.sql           — Pure function: LCA of two Snowflake types
├── gather_source_schemas.sql      — Stage 1: read source table metadata
├── resolve_unified_schema.sql     — Stage 2: merge sources into desired schema
├── compute_schema_diff.sql        — Stage 3: diff unified vs final → operation list
├── execute_schema_operations.sql  — Stage 4: execute DDL operations
└── generate_source_select.sql     — Stage 5: per-source SELECT with CASTs

plt-poc-dbt/models/plt/
└── orders.sql                     — Orchestrator: calls stages 1-5 in sequence
```

---

## 13. Testing Strategy

| Macro | Test Approach |
|-------|-------------|
| `get_widened_type` | Unit test: pass type pairs, assert return value. Can be tested via dbt `run-operation` or Jinja compilation tests. |
| `gather_source_schemas` | E2E only: needs real Snowflake tables. Tested implicitly by all sentinel tests. |
| `resolve_unified_schema` | Unit-testable: mock source_schemas list, assert unified output. |
| `compute_schema_diff` | Unit-testable: mock unified_schema + mock final_columns, assert operations list. |
| `execute_schema_operations` | E2E only: needs real Snowflake. Tested by sentinel tests that verify INFORMATION_SCHEMA after dbt run. |
| `generate_source_select` | E2E + SQL inspection: verify generated SQL contains correct CASTs and NULL pads. |

The 45-test sentinel suite (see `context/test-cases/implementation_plan.md`) exercises all macros end-to-end across the full spectrum of schema evolution scenarios.
