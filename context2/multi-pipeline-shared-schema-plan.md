# Multi-Pipeline Shared Schema — Deep Dive Design Doc

> Approach chosen: **Approach 2 — PLT via Megatron + DBT (internal orchestrator)**
> Basic case: `k1.k0` + `k1'.k0` (Postgres) → `K2.SHARED_SCHEMA.K0` (Snowflake)

---

## Basic Case

```
Source (Postgres k2):
  k1.k0   →  (id, name, email)
  k1'.k0  →  (id, name, phone)

Destination (Snowflake):
  K2.SHARED_SCHEMA.K0  →  (id, name, email, phone, __hevo_source_pipeline)
```

---

## Why Approaches 1 & 3 are out

**Approach 1 (Direct Load + Lock):**
- Lock held for entire load duration → cascading pipeline delays
- No rollback on partial failure → data corruption risk
- Loader must own multi-pipeline schema evolution → wrong ownership

**Approach 3 (Airflow):**
- Same correctness as Approach 2
- Extra infrastructure overhead for a POC
- Defer to production if needed

---

## Full PLT Flow (Basic Case)

```
T0  Pipeline p1 (k1.k0)  → loads to staging_k1.k0     → marked LOADED
T1  Pipeline p2 (k1'.k0) → loads to staging_k1_prime.k0 → marked LOADED
T2  Orchestrator triggers PLT for SHARED_SCHEMA.K0
T3  PLT acquires schema-level lock  (on plt_locks table, TTL = 1hr)
T4  Capture snapshot watermarks:
      snapshot_k1  = SELECT MAX(_hevo_ingested_at) FROM staging_k1.k0
      snapshot_k1p = SELECT MAX(_hevo_ingested_at) FROM staging_k1_prime.k0
      → Write both to plt_watermarks.snapshot_watermark (BEFORE merge starts)
T5  CATALOG pre-run:
      Fetch schema(staging_k1.k0), schema(staging_k1_prime.k0), schema(SHARED_SCHEMA.K0)
      Call CATALOG evolution engine → get EvolutionResult (change decisions + DDL list)
      If BLOCK / INCONSISTENT → abort, raise alert
      Else → execute DDL on SHARED_SCHEMA.K0 (e.g., ADD COLUMN phone VARCHAR)
T6  DBT incremental MERGE (k0.sql):
      Source: UNION ALL of k1 + k1' staging, filtered by watermark window
      Unique key: (id, __hevo_source_pipeline)
      Vars passed by Megatron: last_watermark_k1, snapshot_watermark_k1, etc.
T7  On success: UPDATE plt_watermarks SET last_merged = snapshot, snapshot = NULL
T8  Release lock. Write to plt_run_status (SUCCESS + rows_merged + schema_changes).
T9  On failure: watermarks unchanged → next PLT retries same window
```

---

## PK Collision — Composite Key (Confirmed)

`unique_key = ['id', '__hevo_source_pipeline']`

- `k1.k0 id=1` → stored as `(id=1, pipeline='k1')`
- `k1'.k0 id=1` → stored as `(id=1, pipeline='k1_prime')`
- Both rows coexist independently. No overwrite.
- Future config: `merge_key_includes_source: false` → last-writer-wins for replicated sources.

---

## Locks — NOT in the Loader

The Loader writes to **isolated staging tables only** — no contention, no lock needed there.

The lock lives at the **PLT step**, because two concurrent PLT runs writing to the same `SHARED_SCHEMA.K0` would cause conflicts.

**Lock scope:** per `(schema_name, table_name)` pair.

**POC implementation:** MySQL advisory lock or `plt_locks` table with TTL:
```sql
CREATE TABLE plt_locks (
  schema_name  VARCHAR(255) NOT NULL,
  table_name   VARCHAR(255) NOT NULL,
  locked_by    VARCHAR(255),   -- PLT run ID
  locked_at    TIMESTAMP(6),
  expires_at   TIMESTAMP(6),   -- mandatory TTL for crash safety
  PRIMARY KEY  (schema_name, table_name)
);
```

**Production:** Temporal workflow ID = `plt_{schema}_{table}` → single active workflow guaranteed.

**Megatron concurrency note:** DWS thread pool (10 max threads) allows concurrent PLT runs for *different* tables. But for the *same* table, the lock serializes them. Second PLT trigger coalesces: it wakes up after first finishes, captures a fresh snapshot, and picks up all accumulated delta in one run.

---

## DBT UNION Model — Design + Capability

### The model (`models/shared/k0.sql`)

```sql
{{ config(
    materialized='incremental',
    unique_key=['id', '__hevo_source_pipeline'],
    on_schema_change='ignore'   -- CATALOG handles all DDL in pre-run; DBT just merges
) }}

WITH k1_data AS (
  SELECT *, 'k1' AS __hevo_source_pipeline
  FROM {{ source('staging_k1', 'k0') }}
  {% if is_incremental() %}
  WHERE _hevo_ingested_at >  '{{ var("last_watermark_k1") }}'
    AND _hevo_ingested_at <= '{{ var("snapshot_watermark_k1") }}'
  {% endif %}
),
k1_prime_data AS (
  SELECT *, 'k1_prime' AS __hevo_source_pipeline
  FROM {{ source('staging_k1_prime', 'k0') }}
  {% if is_incremental() %}
  WHERE _hevo_ingested_at >  '{{ var("last_watermark_k1p") }}'
    AND _hevo_ingested_at <= '{{ var("snapshot_watermark_k1p") }}'
  {% endif %}
),
unioned AS (
  SELECT * FROM k1_data
  UNION ALL
  SELECT * FROM k1_prime_data
)
SELECT * FROM unioned
```

Watermarks are injected by Megatron at trigger time:
```bash
dbt run --select shared.k0 --vars '{
  "last_watermark_k1": "2024-01-15T10:00:00",
  "snapshot_watermark_k1": "2024-01-15T10:05:00",
  "last_watermark_k1p": "2024-01-15T09:55:00",
  "snapshot_watermark_k1p": "2024-01-15T10:04:30"
}'
```

### What this model handles ✅

| Scenario | How |
|----------|-----|
| Identical schemas | Simple UNION. No DDL. |
| Additive column in one source | CATALOG adds column in pre-run. DBT merges with NULL for missing side. |
| Column reordering | Column-name-based SELECT. Safe. |
| Same PK from different pipelines | Composite key. Both rows coexist. |
| Incremental merge | Watermark filter per pipeline. Only new/changed rows since last PLT. |
| New pipeline added | New CTE + UNION leg. One-time change. |
| Backfill (watermark-aware) | If rows are above last_watermark: normal. If below (historical backfill): needs full-refresh trigger. |
| PLT failure / idempotent retry | Watermarks not advanced → same window reprocessed on retry. |

### What this model CANNOT handle alone ❌ (CATALOG owns it)

| Scenario | Problem |
|----------|---------|
| Type conflict (INT vs VARCHAR, same column) | DBT has no type hierarchy awareness |
| Type narrowing | DBT won't demote types |
| Column removal from final table | DBT won't DROP; needs soft-delete policy |
| PK structural changes | Requires full-refresh + redeploy |
| No-PK table (append mode) | Needs surrogate key + INSERT-only model config |
| Schema inconsistency across pipelines | Needs BLOCK decision from CATALOG |

### Other DBT materializations — why not used

| Type | Why not |
|------|---------|
| `table` | Full re-scan of staging every run — too expensive |
| `view` | No data landing; queries hit staging live; no freshness guarantee |
| `snapshot` (SCD2) | Adds historical tracking overhead; useful for audit but not basic merge |
| `incremental` ✅ | Correct: only new/changed rows, supports MERGE with unique_key |

---

## Watermark — Everything

### Which timestamp: `_hevo_ingested_at`

- Standard Hevo system column, added to every row by the platform (not the source)
- Represents when the record was captured — monotonically increasing within a pipeline
- **NOT** the source DB timestamp (which could be arbitrary/out of order)

**Edge case:** Historical backfill brings rows with `_hevo_ingested_at` *below* the current `last_merged_watermark`. These will be missed by incremental PLT. Fix: reset `last_merged_watermark = epoch` for that pipeline to force a full-refresh pass.

### What is the "snapshot"?

The snapshot is `MAX(_hevo_ingested_at)` captured **once, atomically, BEFORE the merge starts**.

```
BEFORE MERGE:
  snapshot_k1  = MAX(_hevo_ingested_at) FROM staging_k1.k0      →  frozen at T4
  snapshot_k1p = MAX(_hevo_ingested_at) FROM staging_k1_prime.k0 → frozen at T4

DURING MERGE (T4 → T6):
  Pipeline p2 loads 500 more rows to staging_k1_prime.k0 (above snapshot_k1p)
  → These rows are INVISIBLE to current PLT (above the frozen snapshot)
  → DBT MERGE filter: _hevo_ingested_at <= snapshot_k1p  excludes them

AFTER MERGE SUCCESS:
  last_merged_watermark_k1  = snapshot_k1
  last_merged_watermark_k1p = snapshot_k1p
  → Next PLT trigger captures new snapshot, picks up p2's rows
```

**Why not compute MAX() inline in the query?**
If the query computes `MAX()` at run time, rows arriving mid-merge can be partially included depending on timing. Capturing the snapshot before the merge guarantees a consistent, deterministic window.

### Watermark storage — new `plt_watermarks` table in Megatron

```sql
CREATE TABLE plt_watermarks (
  id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
  pipeline_id           BIGINT NOT NULL,
  source_schema         VARCHAR(255) NOT NULL,
  source_table          VARCHAR(255) NOT NULL,
  target_schema         VARCHAR(255) NOT NULL,
  target_table          VARCHAR(255) NOT NULL,
  last_merged_watermark TIMESTAMP(6),   -- NULL = never merged; do full load
  snapshot_watermark    TIMESTAMP(6),   -- set at PLT start; cleared after success
  updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk (pipeline_id, source_schema, source_table, target_schema, target_table)
);
```

**Lifecycle:**
```
PLT start:   snapshot_watermark = MAX(_hevo_ingested_at)  [written to DB]
PLT running: last_merged_watermark unchanged
PLT success: last_merged_watermark = snapshot_watermark; snapshot_watermark = NULL
PLT failure: both unchanged → retry uses same [last_merged, snapshot] window
PLT retry:   snapshot_watermark still set → reuse existing snapshot (no re-capture)
```

---

## Schema Resolution — CATALOG + DBT Split

### CATALOG's capabilities (from catalog-engine/README.md)

- **26+ change types**: COLUMN_ADDITION, COLUMN_REMOVAL, COLUMN_PK_ADDITION, type changes, etc.
- **Destination-specific type hierarchies** (common ancestor detection):
  - Snowflake: VARIANT → STRING → NUMBER → FLOAT/BOOLEAN
  - BigQuery: STRING → JSON → FLOAT64 → INT64
  - Redshift: STRING → TEXT → FLOAT8 → INTEGER
- **Change decisions**: DO_NOTHING, REPLICATE_AND_ALLOW, BLOCK_SCHEMA_CHANGE, BLOCK_FIELD, BLOCK_OBJECT, SCHEMA_IS_INCONSISTENT, MARK_COLUMN_DELETED
- **User preference modes**: ALLOW_ALL, ALLOW_COLUMN_LEVEL, BLOCK_ALL
- **O(n+m) diff algorithm** with HashMap-based field lookup

### Why DBT cannot replicate this

DBT's `on_schema_change` is binary (add/fail/sync). It has no:
- Type hierarchy awareness
- Multi-source schema conflict detection
- User preference modes
- Destination-specific type promotion rules

### Architecture: CATALOG does analysis, pre-run executes DDL, DBT merges

```
pre-run:
  schema_a    = GET SCHEMA (staging_k1.k0)
  schema_b    = GET SCHEMA (staging_k1_prime.k0)
  schema_final = GET SCHEMA (SHARED_SCHEMA.K0)
  merged_src  = UNION schema_a + schema_b  (field-level merge)

  evolution_result = CATALOG.evolve(previous=schema_final, new=merged_src,
                                     prefs=user_prefs, dest=SNOWFLAKE)

  if decision ∈ {BLOCK_OBJECT, SCHEMA_IS_INCONSISTENT}:
    abort PLT, raise alert, update plt_run_status=FAILED
  else:
    execute DDL from evolution_result on SHARED_SCHEMA.K0

dbt run: on_schema_change='ignore'  ← CATALOG already handled all DDL
```

### Schema resolution case table

| Case | CATALOG Decision | Pre-run DDL | DBT |
|------|-----------------|-------------|-----|
| k1' adds `phone` | REPLICATE_AND_ALLOW | `ADD COLUMN phone VARCHAR` | MERGE; k1 rows get NULL for phone |
| INT vs VARCHAR (same col) | REPLICATE → widen to VARCHAR | `ALTER COLUMN id TO VARCHAR` | MERGE; k1 INT cast to VARCHAR |
| Type narrowing | BLOCK_SCHEMA_CHANGE | No DDL; that column blocked | PLT runs but blocked column excluded |
| Both pipelines add same col, different types | SCHEMA_IS_INCONSISTENT | Abort | PLT fails; human resolves |
| k1 drops column | MARK_COLUMN_DELETED | Keep column, set nullable | k1 rows → NULL for dropped column |

---

## New Tables Required in Megatron (V12 migration)

```sql
-- 1. Watermark tracking (per pipeline per table)
CREATE TABLE plt_watermarks ( ... );

-- 2. Lock management (per target schema.table)
CREATE TABLE plt_locks ( ... );

-- 3. PLT run status (observability / status page)
CREATE TABLE plt_run_status (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  pipeline_id    BIGINT NOT NULL,
  target_schema  VARCHAR(255),
  target_table   VARCHAR(255),
  status         ENUM('QUEUED','RUNNING','SUCCESS','FAILED'),
  watermark_from TIMESTAMP(6),
  watermark_to   TIMESTAMP(6),
  rows_merged    BIGINT,
  schema_changes TEXT,   -- JSON list of DDL applied
  error_message  TEXT,
  triggered_at   TIMESTAMP(6),
  completed_at   TIMESTAMP(6)
);
```

---

## POC Corner Cases (Full List)

### Schema Evolution (E-series)

| # | Case | CATALOG Decision | Outcome |
|---|------|-----------------|---------|
| E1 | Identical schemas | DO_NOTHING | Simple union |
| E2 | k1' has extra `phone` | ADD COLUMN | Final gets phone; k1 rows → NULL |
| E3 | k1 adds `age` at runtime | ADD COLUMN | pre-run ALTER; next PLT adds age for k1 |
| E4 | k1 drops `email` | MARK_COLUMN_DELETED | Column kept; k1 rows → NULL |
| E5 | Same col, INT vs VARCHAR | Common ancestor → VARCHAR | pre-run ALTER; k1 INT cast |
| E6 | Column reorder | DO_NOTHING | Name-based SELECT; safe |
| E7 | Both add same col, different types | SCHEMA_IS_INCONSISTENT | PLT blocked; alert |
| E8 | Type narrowing | BLOCK_SCHEMA_CHANGE | Column blocked; data flow continues |

### PK / Merge Key (P-series)

| # | Case | Behavior |
|---|------|----------|
| P1 | Same PK, different pipelines | Both rows coexist; composite key |
| P2 | Duplicate PK within one pipeline | Last-writer-wins per (id, pipeline) |
| P3 | No PK on source table | Append mode; surrogate key needed |
| P4 | Composite PK on one, single on other | Per-table merge key config |

### Concurrency / Timing (C-series)

| # | Case | Behavior |
|---|------|----------|
| C1 | Dual simultaneous PLT triggers | Lock serializes; coalescing on wake |
| C2 | New staging data mid-PLT | Above snapshot → invisible; next PLT picks up |
| C3 | PLT crash mid-merge | TTL expires; watermarks unchanged; retry |
| C4 | Different schedules (5min vs 30min) | Per-pipeline watermarks; no starvation |
| C5 | Backfill from one pipeline | Below watermark → full-refresh needed for that pipeline |
| C6 | Dormant pipeline activated | First PLT = full initial load; CATALOG detects new columns |

### Lock (L-series)

| # | Case | Behavior |
|---|------|----------|
| L1 | PLT triggered while lock held | Queue + coalesce (POC: retry with backoff) |
| L2 | Lock holder crash | TTL expires → next PLT acquires |
| L3 | Lock implementation | MySQL `GET_LOCK()` for POC; Temporal workflow ID for prod |

### Data Freshness (F-series)

| # | Case | Behavior |
|---|------|----------|
| F1 | Query between LOADED and PLT finish | Stale data; expected; PLT status is source of truth |
| F2 | k1 SUCCESS, k1' FAILED | k1' stuck at last watermark; k1 unaffected; alert for k1' |
| F3 | Clock skew on `_hevo_ingested_at` | Add small buffer (e.g., snapshot = MAX - 1s) or use Snowflake server timestamp |

---

## POC Sequence

1. Set up `k1.k0` and `k1'.k0` in Postgres (initially identical schema)
2. Run two Hevo pipelines → load to isolated Snowflake staging
3. Build DBT incremental UNION model (`models/shared/k0.sql`)
4. Manually trigger PLT via Megatron job API with watermark vars
5. **E1**: verify both pipelines' rows in final table with `__hevo_source_pipeline`
6. **E2**: add `phone` to k1'. Re-run PLT. Verify ADD COLUMN + NULL fill.
7. **C2**: load new k1' data mid-PLT. Verify watermark isolation.
8. **C3**: simulate PLT crash. Verify watermarks unchanged, retry succeeds.
9. **E5**: change k1'.id type. Verify CATALOG type widening + DBT cast.
10. **E7**: both pipelines add same column with conflicting types. Verify BLOCK + alert.
