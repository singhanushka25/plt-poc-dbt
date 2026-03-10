# PLT Phase 2 — Deferred Scope

> Items explicitly deferred from Phase 1 macro architecture design.
> Each item includes context on WHY it was deferred and WHAT needs to happen.

---

## 1. Dynamic Merge Key Resolution

**What**: New `resolve_merge_key(sources)` macro that reads PK metadata from `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` and computes PLT merge key = union of all source PKs.

**Why deferred**: Merge key design is independent of schema evolution. The type widening, column add/drop, and constraint handling work regardless of what the merge key is. Phase 1 uses hardcoded `unique_key=['id', '__hevo_source_pipeline']`.

**Open design questions**:
- PK drop → merge key shrinks → historical rows may have multiple matches on new key → Snowflake MERGE fails. How to handle?
- No PK at all → append mode? Who decides? Auto-detect or config?
- IS NOT DISTINCT FROM for nullable merge key columns — needs Snowflake MERGE syntax validation
- When merge key expands (ADD_PRIMARY_KEY), historical rows have NULL for new key column. Impact on pre-expansion data?

---

## 2. `merge_tables` Flag

**What**: Config flag that controls whether `__hevo_source_pipeline` is part of the merge key.

- `merge_tables = false` (default): `__hevo_source_pipeline` in merge key. Rows from different sources with same business PK coexist as separate rows.
- `merge_tables = true`: `__hevo_source_pipeline` excluded from merge key. Same business PK across sources → last writer wins. One row per business PK.

**Why deferred**: Requires merge key resolution (item 1) to be designed first. Also needs decision on conflict resolution when `merge_tables = true` (timestamp-based? source priority?).

---

## 3. Dedup-in-SELECT

**What**: When a source drops its PK, the loader stops deduplicating that source's staging table. The staging table can have duplicate PLT merge key values. Snowflake MERGE fails on multiple source matches.

**Fix**: Wrap the source's SELECT in a ROW_NUMBER() dedup:
```sql
WITH k1_deduped AS (
  SELECT * EXCLUDE rn FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY id, name  -- PLT merge key cols
      ORDER BY _hevo_ingested_at DESC
    ) AS rn
    FROM staging_k1.orders
  ) WHERE rn = 1
)
```

**Why deferred**: Only needed after PK drop. PLT is stateless, so detecting "PK was dropped" (vs "never had PK") requires either:
- Comparing current PK set against a stored historical PK set (state)
- External signal from catalog/config

Both require design beyond Phase 1.

---

## 4. Watermark Tracking

**What**: Use `_hevo_ingested_at` for incremental processing — only process rows newer than the last successful PLT run.

**Why deferred**: Phase 1 processes all rows in staging tables on every run. Watermark tracking requires:
- State storage (plt_watermarks table)
- Per-source watermark management
- Failure/retry semantics (watermark not advanced on failure)

---

## 5. Redshift-Specific Handling

**What**: Several schema evolution behaviors differ on Redshift:

| Item | Redshift Behavior | Snowflake Behavior |
|------|-------------------|-------------------|
| PK fields | Require NOT NULL constraint (`requiresNotNullForPK = true`) | PK can be nullable |
| Distribution key change | DROP_AND_RECREATE_TABLE (terminal operation) | N/A |
| Sort keys | DROP_SORT_KEY → field changes → ADD_SORT_KEY | N/A |
| NOT NULL on PK fields | Must add NOT NULL before ADD_PRIMARY_KEY | Not required |

**Why deferred**: Phase 1 is Snowflake-only. Redshift support requires:
- Separate type hierarchy (RedshiftSchemaTypeHierarchy)
- Different expansion schemes and promotion strategies
- Additional operation types in `execute_schema_operations`
- The `requiresNotNullForPK` pattern from the catalog's `NormalFieldChangeHandler`

---

## 6. Advanced Schema Operations

### DROP_FIELDS with Hard-Delete Policy
- Current: soft-drop only (column stays, new rows get NULL)
- Phase 2: optional `DROP_FIELDS_POLICY: hard` config that actually drops columns when all sources have dropped them
- Requires: eager null-backfill before drop (UPDATE SET col = NULL WHERE __hevo_source_pipeline = ...)

### RENAME_FIELDS Detection
- Current: rename is indistinguishable from drop + add (PLT has no event stream)
- Phase 2: could accept rename hints via config or detect by column position/type similarity
- Data loss on rename is explicit and documented

### Multi-Step Promotion Failure Recovery
- Current: fail fast, manual cleanup of `__plt_tmp_*` columns
- Phase 2: detect stale temp columns at start of PLT run and clean up
- Pattern: `gather_source_schemas` checks for `__PLT_TMP_*` columns in final table → auto-cleanup or alert

---

## 7. Three+ Source Support

**What**: Current model hardcodes two sources (k1, k1_prime). Phase 2 supports N sources.

**Changes needed**:
- `plt_sources` list becomes dynamic (from config or auto-discovery via `dbt_utils.get_relations_by_pattern`)
- `resolve_unified_schema` already handles N sources (fold pattern)
- Model SQL loop already iterates `plt_sources`
- Tests: F07 (plt_three_sources) validates this

---

## 8. Test Cases Deferred to Phase 2

See `context/test-cases/implementation_plan.md` for full details.

| Branch | Tests | Count |
|--------|-------|-------|
| A (type evolution matrix) | A08-A14 | 7 |
| B (evolution variants) | B06-B11 | 6 |
| C (multi-change variant) | C02 | 1 |
| D (PK lifecycle) | D03-D05 | 3 |
| E (constraints) | E01-E03 | 3 |
| F (edge cases) | F02-F04, F07-F10 | 8 |
| G (Snowflake-specific) | G01-G02 | 2 |
| **Total** | | **30** |
