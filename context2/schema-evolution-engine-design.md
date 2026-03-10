# PLT Macro-Based Schema Evolution Engine — Design

---

## 1. System Setup

The loader's responsibility ends when it writes data to **temp schemas** on the destination:

```
Loader writes to:
  __HEVO_TEMP_K1.k0        ← all rows from k1.k0, with _hevo_source_pipeline = 'k1'
  __HEVO_TEMP_K1_PRIME.k0  ← all rows from k1'.k0, with _hevo_source_pipeline = 'k1_prime'

Macro engine reads from temp tables → manages SHARED_SCHEMA.k0 schema → merges data into it
```

The `_hevo_source_pipeline` column is added by the loader to every row in the temp table. It is the discriminator that tells PLT which source produced each row.

### Why no database-level PRIMARY KEY on the final table?

The PLT merge key = union of all source PKs + `__hevo_source_pipeline`. Some of those union PK columns may be absent from certain sources. When k1' adds `category` as PK but k1 has no `category` column, k1 rows in the final table will have `category = NULL`. Database-level PKs require NOT NULL on all key columns — structurally impossible here. DBT's `unique_key` drives MERGE logic only.

### Why are user columns nullable in the final table?

Same reason: if source k1' adds `phone VARCHAR NOT NULL`, k1 rows will always null-fill `phone`. NOT NULL on the final table column is only enforceable when ALL sources guarantee non-null for that column (handled in ADD_NOT_NULL_CONSTRAINT logic).

---

## 2. PLT Merge Key

### Definition

The PLT merge key = **union of all source loader dedup PKs** + `__hevo_source_pipeline`.

**It is computed dynamically at each PLT run** by reading the current PKs from each source's temp table metadata. It is not a static value registered once — it expands when sources add PK columns and re-evaluates when sources drop them.

### Why union, not intersection?

**Intersection (wrong):** k1.k0 PK = `(id, name)`, k1'.k0 PK = `(id)` → intersection = `(id)`.
The loader deduplicates k1's temp table on `(id, name)`, so k1's temp table can have:
- `(id=1, name='Alice', ...)`
- `(id=1, name='Bob', ...)`

Both rows have `id=1`. A MERGE on just `(id, 'k1')` sees two k1 source rows matching the same target key. **Snowflake MERGE fails.**

**Union (correct):** PLT merge key = `(id, name, __hevo_source_pipeline)`.
- k1 rows: unique on `(id, name)` per loader dedup → unique on PLT merge key.
- k1' rows: loader deduplicates on `(id)` → at most one row per `id` in k1' temp. Still unique on `(id, name, __hevo_source_pipeline)`.

### NULLs in the merge key — NULL-safe MERGE

When k1' adds `category` as PK but k1 has no `category` column, k1 rows will have `category = NULL` in the final table. Standard SQL equality (`T.category = S.category`) evaluates NULL = NULL as false — the MERGE would never find a match for k1 rows and would re-insert them every run, creating duplicates.

**Fix:** Use `IS NOT DISTINCT FROM` for nullable merge key columns in the MERGE ON clause:

```sql
MERGE INTO SHARED_SCHEMA.k0 AS T
USING (source UNION ALL) AS S
ON T.id = S.id
   AND T.category IS NOT DISTINCT FROM S.category   -- NULL IS NOT DISTINCT FROM NULL → TRUE
   AND T.__hevo_source_pipeline = S.__hevo_source_pipeline
```

**Why NULLs are still safe:** k1's loader deduplicates on its own PK `(id)`. So k1's temp table has at most one row per `id`. That one row produces `(id=X, category=NULL, 'k1')` in the final table — unique per PLT run window.

> **Open:** Confirm Snowflake MERGE supports `IS NOT DISTINCT FROM` in the ON clause. Fallback: `COALESCE(category, '__HEVO_NULL_SENTINEL__')`.

### What `__hevo_source_pipeline` does (`merge_key_includes_source=true` — default)

Rows from different sources with the same business PK coexist as separate rows in the final table.

```
k1.k0 row:   id=1, name='Alice', __hevo_source_pipeline='k1'
k1'.k0 row:  id=1, name='Bob',   __hevo_source_pipeline='k1_prime'
```

Both exist. Neither overwrites the other. Correct when k1 and k1' are genuinely different sources sharing a table name.

### `merge_key_includes_source=false` — replicated sources

Used when k1 and k1' are **read replicas of the same underlying database** — they contain identical data and represent the same source semantically.

- Merge key excludes `__hevo_source_pipeline`: merge key = union of source PKs only.
- If k1 and k1' both produce `(id=1, name='Alice')`, they compete for the same row in the final table.
- **Last-writer-wins:** whichever source's row has the later `_hevo_ingested_at` overwrites the previous. MERGE ON matches regardless of source.
- Result: one row per business PK, not two.

Requires explicit PLT job config. Default: `merge_key_includes_source=true`.

---

## 3. The Four Macros

### 3.1 `get_widened_type(type_a, type_b)`

**Purpose:** Given two types from different sources for the same column, return the resolved (lowest common ancestor) type, the promotion strategy, and the SQL conversion expression.

**Algorithm:** Find the Lowest Common Ancestor (LCA) in the Snowflake type hierarchy tree. Mirrors the catalog service's `AdaptiveSchemaEvolutionEngine`.

**The hierarchy is symmetric.** `get_widened_type(A, B)` and `get_widened_type(B, A)` return the same resolved type — LCA is commutative. Argument order does not affect the result.

### Snowflake Type Hierarchy (from `SnowflakeSchemaTypeHierarchy.java`)

```
STRING / VARCHAR  ← root super type (widest)
├── NUMBER (expandable: precision + scale)
│     └── BOOLEAN
├── FLOAT   (fixed)
├── BINARY  (expandable: length)
├── TIME    (expandable: precision)
├── TIMESTAMP_TZ  (expandable: precision)
│     └── TIMESTAMP_NTZ  (expandable: precision)
│               └── DATE  (fixed)
└── VARIANT
      └── ARRAY
```

Key points:
- `STRING` (VARCHAR) is the root, not `VARIANT`. VARIANT is a child of STRING.
- `FLOAT` and `NUMBER` are **siblings** — both children of STRING. They share no numeric common ancestor; their LCA is STRING.
- `BOOLEAN` is a child of `NUMBER`. Widening BOOLEAN with NUMBER resolves to NUMBER, not VARCHAR.
- `INT`, `BIGINT`, `SMALLINT` are Snowflake aliases for `NUMBER(38,0)` — same type in the hierarchy. Precision/scale expansion applies within NUMBER.

### Widening Table (LCA lookup)

| Type A | Type B | Resolved | Strategy | Notes |
|--------|--------|----------|----------|-------|
| NUMBER(p1,s1) | NUMBER(p2,s2) | NUMBER(max_int+max_scale, max_scale) | single_step | Precision/scale expansion within same family |
| NUMBER | BOOLEAN | NUMBER | single_step | BOOLEAN is child of NUMBER |
| NUMBER | FLOAT | STRING (VARCHAR) | multi_step | Siblings — no shared numeric ancestor |
| NUMBER | VARCHAR | STRING (VARCHAR) | multi_step | NUMBER parent chain reaches STRING |
| FLOAT | BOOLEAN | STRING (VARCHAR) | multi_step | FLOAT is sibling of NUMBER |
| FLOAT | VARCHAR | STRING (VARCHAR) | none | FLOAT parent is STRING |
| DATE | TIMESTAMP_NTZ | TIMESTAMP_NTZ | single_step | DATE is child of TIMESTAMP_NTZ |
| DATE | TIMESTAMP_TZ | TIMESTAMP_TZ | single_step | DATE → TIMESTAMP_NTZ → TIMESTAMP_TZ |
| TIMESTAMP_NTZ | TIMESTAMP_TZ | TIMESTAMP_TZ | single_step | TIMESTAMP_NTZ is child of TIMESTAMP_TZ |
| TIMESTAMP_NTZ | VARCHAR | STRING (VARCHAR) | multi_step | Both reach STRING |
| VARCHAR(x) | VARCHAR(y), y > x | VARCHAR(y) | single_step | Length expansion |
| BINARY | VARCHAR | STRING (VARCHAR) | multi_step | Siblings under STRING |

**On CONFLICT:** There are no intrinsic type-level conflicts — STRING is the universal common ancestor. CONFLICT arises at `evolve_final_table` time: if the resolved schema demands a type **narrower** than what the final table currently holds (all sources narrowed a column), the macro BLOCKS and alerts. The final table column is never narrowed; incoming data is cast to match the existing wider type.

Returns: `{resolved_type, strategy, conversion_expr}` e.g. `TO_VARCHAR(%s)`, `CAST(%s AS TIMESTAMP_NTZ)`, or `%s` (no cast needed).

---

### 3.2 `resolve_column_schema(temp_relations)`

**Purpose:** Inspect the schemas of all temp tables and produce the desired unified schema.

**What this macro does NOT do:** it does not inspect the final table. It only looks across temp table schemas. The comparison against the final table is `evolve_final_table`'s responsibility.

**Stateless — re-derives every PLT run.** No stored schema state. Source schemas can change between runs; we read current state fresh each time.

**Algorithm:**
1. `adapter.get_columns_in_relation(relation)` for each temp table → `(col_name, dtype)` per source
2. Build map: `{col_name: {resolved_type, strategy, conversion_expr, sources: [...]}}`
3. For each column across all temp tables:
   - First occurrence: record type as-is
   - Same type in another source: no change, add source to list
   - Different type: `get_widened_type(existing, new)` → update to resolved type
4. Read PKs from each temp table's metadata → union of PKs = current PLT merge key
5. Track which sources have dropped their PK (for dedup-in-SELECT in step 5)

Output: `{col_name: {resolved_type, ...}, plt_merge_key: [pk_cols], sources_with_dropped_pk: [...]}`

**Example:**
- `__HEVO_TEMP_K1.k0`: `(id NUMBER, name VARCHAR, email VARCHAR)`, PK=(id, name)
- `__HEVO_TEMP_K1_PRIME.k0`: `(id VARCHAR, name VARCHAR, phone VARCHAR)`, PK=(id)
- Result: `{id: {VARCHAR, multi_step, TO_VARCHAR(%s)}, name: {VARCHAR, -}, email: {VARCHAR, -}, phone: {VARCHAR, -}}`, PLT merge key = `(id, name)`

---

### 3.3 `evolve_final_table(final_relation, resolved_schema)`

**Purpose:** Pre-hook DDL. Compare `resolved_schema` (derived from temp tables) against the current final table schema and run the minimum DDL to bring the final table in sync.

Always re-fetches final table schema at run time.

**ADD COLUMN** (column in resolved_schema not in final table):
```sql
ALTER TABLE final ADD COLUMN IF NOT EXISTS phone VARCHAR
```
`IF NOT EXISTS` makes it idempotent in case of concurrent PLT runs.

**SINGLE_STEP promotion** (same type family, precision/length widening):
```sql
ALTER TABLE final MODIFY COLUMN score NUMBER(15,3)   -- NUMBER(5,2) → NUMBER(15,3)
```

**MULTI_STEP promotion** (cross-family — Snowflake does not support in-place cross-family ALTER):
```sql
ALTER TABLE final ADD COLUMN __plt_tmp_id VARCHAR        -- Step 1: add temp col
UPDATE final SET __plt_tmp_id = TO_VARCHAR(id)           -- Step 2: cast data in
ALTER TABLE final DROP COLUMN id                         -- Step 3: drop old col
ALTER TABLE final RENAME COLUMN __plt_tmp_id TO id       -- Step 4: rename to original
```
Mirrors the loader's multi-step field promotion exactly (`SnowflakeFieldPromotionUtils`).

**DROP NOT NULL** (before null-fill operations):
```sql
ALTER TABLE final MODIFY COLUMN email DROP NOT NULL
```
Must happen before any UPDATE that sets the column to NULL.

**BLOCK on narrowing:** If resolved_schema requires a type narrower than what the final table currently has, do NOT alter. Alert. Final table column stays wider; source SELECT casts incoming data to match.

**What it does NOT do:** drop columns. DROP_FIELDS is handled separately with eager null backfill.

---

### 3.4 `generate_source_select(temp_relation, pipeline_label, final_schema)`

**Purpose:** SELECT clause for one source's contribution to the MERGE. Cast target = final table schema re-fetched post-evolution.

**Why re-fetch final schema:** ensures casts target the actual current column types after DDL. Handles incremental type changes across runs correctly. Each run is fully stateless.

**Per column in final_schema:**
- Column in temp, same type as final → `col_name`
- Column in temp, different type → `{conversion_expr}(col_name) AS col_name`
- Column not in temp → `NULL AS col_name`

Example (final.id = VARCHAR, k1 has id NUMBER, k1' has id VARCHAR):
```sql
-- k1 SELECT:
SELECT TO_VARCHAR(id) AS id, name, email, NULL AS phone, 'k1' AS __hevo_source_pipeline
FROM __HEVO_TEMP_K1.k0
WHERE _hevo_ingested_at > '{{ last_watermark_k1 }}'
  AND _hevo_ingested_at <= '{{ snapshot_watermark_k1 }}'

-- k1' SELECT:
SELECT id, name, NULL AS email, phone, 'k1_prime' AS __hevo_source_pipeline
FROM __HEVO_TEMP_K1_PRIME.k0
WHERE _hevo_ingested_at > '{{ last_watermark_k1p }}'
  AND _hevo_ingested_at <= '{{ snapshot_watermark_k1p }}'
```

**Dedup wrapper (applied only after DROP_PRIMARY_KEY):**
When a source drops its PK, the loader stops deduplicating on that column. The temp table can then have duplicate rows on the PLT merge key. Snowflake MERGE fails when multiple source rows match the same target. Fix:
```sql
WITH k1_deduped AS (
  SELECT * EXCLUDE rn FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY id, name      -- PLT merge key cols (without __hevo_source_pipeline)
        ORDER BY _hevo_ingested_at DESC  -- latest row wins
      ) AS rn
    FROM __HEVO_TEMP_K1.k0
    WHERE ...watermark filter...
  ) WHERE rn = 1
)
```
Only applied to sources that have dropped their PK. Sources with intact PKs: loader already guarantees uniqueness.

---

## 4. Operation-by-Operation Responsibilities

### ADD_FIELDS(9)
Column added to one or more sources.

**Action:** ADD COLUMN as nullable on final table. NOT NULL is always downgraded.

**Why nullable:** Other sources don't have this column → NULL-fill from them → NOT NULL impossible on final table.

*Example:* k1'.k0 adds `phone VARCHAR NOT NULL` → final gets `phone VARCHAR`. k1 rows → NULL.

---

### ADD_NOT_NULL_FIELDS(10)
New column added with NOT NULL at source.

**Action:** Same as ADD_FIELDS (add as nullable). NOT NULL downgraded.

**OPEN QUESTION:** Should NOT NULL be enforced if ALL sources add the same column as NOT NULL simultaneously? Parked.

---

### ADD_NOT_NULL_CONSTRAINT(11)
Existing column gains NOT NULL at source.

**Action:** Add NOT NULL to final table column ONLY IF every source has this column AND has it as NOT NULL.

**Why the "all sources" condition:** If k1 adds NOT NULL to `email` but k1' doesn't have `email`, k1' rows always produce `NULL AS email`. Adding NOT NULL to the final table would be immediately violated.

If condition not met: log, no-op.

---

### REMOVE_NOT_NULL_CONSTRAINT(4)
NOT NULL removed from a source column.

**Action:** Always drop NOT NULL from the final table column.

**Why unconditional:** If any source no longer guarantees non-null, the final table cannot enforce it.

---

### DROP_FIELDS(5)
Column dropped from one or more sources.

**Problem 1 — What to do with the column in final table:**
- Column still in at least one other source: keep it. Source that dropped it NULL-fills going forward.
- All sources dropped it: soft-delete by default (column kept, all values NULL over time). Hard-drop via policy config `DROP_FIELDS_POLICY: hard`.

**Problem 2 — NOT NULL constraint blocks null-fill:**
If column had NOT NULL on the final table, the following sequence is mandatory:
```
Step 1: ALTER TABLE final MODIFY COLUMN email DROP NOT NULL   ← MUST be first
Step 2: UPDATE final SET email = NULL
        WHERE __hevo_source_pipeline = 'k1'                  ← eager backfill
Step 3: generate_source_select for k1 produces NULL AS email ← future runs
```
Step 1 must precede Step 2. Without it, the UPDATE violates the constraint.

Eager backfill (Step 2) is used because the lazy approach leaves stale non-null values in rows never re-touched by future MERGE runs — the column was dropped at source but old rows still show values.

**Alert + block if dropped column is in PLT merge key.**

---

### RENAME_FIELDS(6)
Column renamed in a source table.

**PLT limitation:** PLT has no event stream. It can only see current temp table schemas. A rename is indistinguishable from a coincidental column drop + new column add. PLT treats all renames as: **drop old column + add new column**.

**Data loss is explicit and documented:** Historical data under the old column name for that source remains in the final table (soft-deleted, values become NULL going forward). Historical data is NOT copied to the new column. Users must handle this manually if needed.

---

### CHANGE_DATA_TYPE(7) / CHANGE_DATA_TYPE_WITH_NOT_NULL(8)

**Sub-case A: Change in one source, no conflict with others.**
*Example:* k1 changes `score NUMBER(5,0) → NUMBER(10,0)`. k1' still has `score NUMBER(5,0)`.
- `get_widened_type(NUMBER(10,0), NUMBER(5,0))` = NUMBER(10,0), single_step
- `ALTER TABLE final MODIFY COLUMN score NUMBER(10,0)`
- k1 SELECT: `score` (already NUMBER(10,0) in temp)
- k1' SELECT: `CAST(score AS NUMBER(10,0))`

**Sub-case B: Cross-family widening across sources.**
*Example:* k1.id = NUMBER, k1'.id = VARCHAR.
- `get_widened_type(NUMBER, VARCHAR)` = VARCHAR, multi_step, `TO_VARCHAR(%s)`
- Multi-step ALTER on final table (add `__plt_tmp_id VARCHAR` → UPDATE CAST → DROP `id` → RENAME)
- Re-fetch final schema → id is now VARCHAR
- k1 SELECT: `TO_VARCHAR(id) AS id`
- k1' SELECT: `id` (already VARCHAR)

**Sub-case C: All sources narrow below current final table type (blocked).**
*Example:* Final table has `label VARCHAR`. All sources now report `label NUMBER`.
- `resolve_column_schema` → label: NUMBER
- `evolve_final_table` detects final is wider (VARCHAR > NUMBER) → BLOCK. Do not narrow. Alert.
- Final table stays VARCHAR. Incoming NUMBER values cast to VARCHAR in source SELECT.

**CHANGE_DATA_TYPE_WITH_NOT_NULL(8):** identical to above for the type change. NOT NULL part routes to ADD/REMOVE_NOT_NULL_CONSTRAINT logic independently.

---

### DROP_PRIMARY_KEY(3)
Source removes PK constraint. Column still exists.

**Effect:** Loader stops deduplicating that source's temp table on the dropped PK column. Temp table can have duplicate PLT merge key values.

**Action:**
1. Apply dedup-in-SELECT (ROW_NUMBER) for the affected source. Latest row per PLT merge key wins.
2. Re-evaluate PLT merge key: if the dropped PK column is no longer a PK in any source, it exits the merge key. If another source still has it as PK, it stays.

---

### ADD_PRIMARY_KEY(12)
Column becomes PK in one source.

**Action: NOT a no-op.** The PLT merge key expands.

**What changes:**
1. New PK column enters the union → PLT merge key now includes it.
2. Sources that do NOT have this column as PK get `NULL AS col_name` in their SELECT → those rows have NULL for this key column in the final table.
3. MERGE ON clause must use `IS NOT DISTINCT FROM` for this nullable key column.
4. The source that gained the PK is now deduped by the loader on the new composite key — no dedup-in-SELECT needed for it.

**Correctness check:** k1 PK = (id). k1' adds PK = (id, category). New PLT merge key = (id, category).
- k1 temp: deduped on (id) → at most one row per id. → (id=1, category=NULL) is unique. Safe.
- k1' temp: deduped on (id, category) → (id=1, category='A') and (id=1, category='B') can both exist. Both unique on PLT merge key. Safe.
- IS NOT DISTINCT FROM handles k1's category=NULL rows correctly in MERGE.

---

### DROP_SORT_KEY(2) / ADD_SORT_KEY(13)
**Action:** No-op. Deferred. Redshift-specific, no correctness impact on Snowflake.

---

## 5. Full Responsibility Matrix

| Operation | Macro Action | Notes |
|-----------|-------------|-------|
| ADD_FIELDS | ADD COLUMN (nullable) on final | NULL-fill for sources without it |
| ADD_NOT_NULL_FIELDS | ADD COLUMN (nullable) on final | NOT NULL downgraded; OPEN QUESTION |
| ADD_NOT_NULL_CONSTRAINT | ADD NOT NULL only if all sources have it as NOT NULL | Log if cannot enforce |
| REMOVE_NOT_NULL_CONSTRAINT | DROP NOT NULL on final column | Unconditional |
| DROP_FIELDS | DROP NOT NULL → eager null backfill → soft-delete column | Alert+block if merge key column |
| RENAME_FIELDS | ADD new col + soft-delete old col. No data copy. Data loss explicit. | POC limitation |
| CHANGE_DATA_TYPE | Single/multi-step ALTER → re-fetch final schema → CAST in SELECT | Block on narrowing |
| CHANGE_DATA_TYPE_WITH_NOT_NULL | Same + NOT NULL via ADD/REMOVE_NOT_NULL path | |
| DROP_PRIMARY_KEY | Dedup-in-SELECT for source + re-evaluate PLT merge key | |
| ADD_PRIMARY_KEY | PLT merge key expands + IS NOT DISTINCT FROM for new nullable key col | Sources without new PK get NULL; safe |
| DROP_SORT_KEY | No-op (deferred) | Redshift only |
| ADD_SORT_KEY | No-op (deferred) | Redshift only |

---

## 6. Full PLT Run Flow

```
Step 1: Capture snapshot watermarks (per source temp table)
        snapshot_k1  = MAX(_hevo_ingested_at) FROM __HEVO_TEMP_K1.k0
        snapshot_k1p = MAX(_hevo_ingested_at) FROM __HEVO_TEMP_K1_PRIME.k0
        Write to plt_watermarks before merge starts

Step 2: resolve_column_schema([__HEVO_TEMP_K1.k0, __HEVO_TEMP_K1_PRIME.k0])
        → Inspect current schemas of TEMP TABLES ONLY (not final table)
        → Compute union schema with widened types via get_widened_type
        → Compute current PLT merge key = union of source PKs
        → Identify sources that have dropped their PK (need dedup-in-SELECT)

Step 3: evolve_final_table(SHARED_SCHEMA.k0, resolved_schema)
        → Re-fetch current final table schema
        → Diff vs resolved_schema
        → Run ADD COLUMN / single-step ALTER / multi-step ALTER / DROP NOT NULL as needed
        → BLOCK if resolved schema is narrower than current final schema
        → All DDL committed immediately (Snowflake: DDL outside transactions)

Step 4: Re-fetch final table schema (post-evolution). This is the CAST target.

Step 5: MERGE
        Source = UNION ALL of generate_source_select() for each temp table
        Each source SELECT:
          - Casts columns to final table types using conversion_expr
          - NULL-fills columns absent from that source
          - Appends __hevo_source_pipeline label
          - Applies watermark filter: _hevo_ingested_at in (last_watermark, snapshot]
          - Applies dedup wrapper (ROW_NUMBER) if source has dropped its PK

        MERGE INTO SHARED_SCHEMA.k0 AS T
        USING (above UNION ALL) AS S
        ON T.id IS NOT DISTINCT FROM S.id
           AND T.name IS NOT DISTINCT FROM S.name
           AND T.__hevo_source_pipeline = S.__hevo_source_pipeline
        WHEN MATCHED THEN UPDATE SET T.col = S.col, ...
        WHEN NOT MATCHED THEN INSERT (col, ...) VALUES (S.col, ...)

Step 6: Success → advance watermarks, write SUCCESS to plt_run_status.
        Failure → watermarks unchanged, write FAILED + error. Next run retries same window.
```

---

## 7. Open Questions

- **ADD_NOT_NULL_FIELDS:** downgrade to nullable or enforce when all sources agree? Unresolved.
- **`_hevo_ingested_at` semantics:** Hevo-system-assigned (monotonic) vs source-derived?
- **Historical backfill:** rows below current watermark → missed by incremental PLT. Fix: watermark reset + idempotent retry.
- **DROP_FIELDS hard-delete policy:** configurable flag. Default: soft-delete.
- **Append mode (no PK tables):** out of scope for POC.
- **BigQuery / Redshift:** deferred. Type hierarchies and ALTER patterns differ per warehouse.
- **PLT merge key history:** when merge key expands (ADD_PRIMARY_KEY), historical rows in the final table have NULL for the new key column. Document implications for pre-expansion data.
- **IS NOT DISTINCT FROM in MERGE ON:** confirm Snowflake MERGE supports this syntax. Fallback: `COALESCE(col, '__HEVO_NULL_SENTINEL__')` for nullable merge key columns.
