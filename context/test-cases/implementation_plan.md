# PLT POC — Test Case Implementation Plan

Each test case consists of: SQL data file(s), suite YAML, sentinel test Python, DBT macro changes, and assertions on the final table.

---

## What's Already Implemented (8 tests)

All shared infra + 8 tests exist in sentinel (`feat/H2-31153/plt_poc` branch):

| plt_all.yml ID | Test | Branch | Sentinel Files |
|---|---|---|---|
| A1 | plt_add_column | A (cross-source) | sql, yml, py — all done |
| A2 | plt_column_name_mismatch | A (cross-source) | sql, yml, py — all done |
| A3 | plt_type_widen_safe | A (cross-source) | sql, yml, py — all done |
| A4 | plt_type_conflict | A (cross-source) | sql, yml, py — all done |
| B1 | plt_drop_column | B (evolution) | base.sql, alter.sql, yml, py — all done |
| B2 | plt_add_column_all | B (evolution) | base.sql, alter.sql, yml, py — all done |
| B3 | plt_type_widen_inline | B (evolution) | base.sql, alter.sql, yml, py — all done |
| B4 | plt_no_narrow | B (evolution) | base.sql, alter.sql, yml, py — all done |

**Shared infra done**: `plt_runner.py` with `run_dbt()` + `discover_schemas()`.

**Macros done**: `get_widened_type.sql`, `resolve_column_schema.sql`, `evolve_final_table.sql`, `generate_source_select.sql`.

---

## Full Test Suite: 45 Tests across 7 Branches

### Priority Summary
| Priority | Count | Done | Remaining |
|----------|-------|------|-----------|
| **P0** | 16 | 6 | **10 new** |
| **P1** | 15 | 2 | 13 new |
| **P2** | 14 | 0 | 14 new |

### Design Decisions (User-Confirmed)
1. **PK/Merge Key**: IN SCOPE — implement `resolve_merge_key` macro + model refactor
2. **NOT NULL**: Enforce when all sources agree — enhance macros
3. **POC Scope**: Phase 1 only (P0 + PK tests = 19 tests total, 11 new)

---

## BRANCH A: Cross-Source Drift (single dbt run, schemas differ from start)

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| A01 | plt_add_column | k1 missing `phone`, k1_prime has it | P0 | **DONE** |
| A02 | plt_column_name_mismatch | k1 has `mobile`, k1_prime has `phone` | P0 | **DONE** |
| A03 | plt_type_widen_safe | salary NUMERIC(10,3) vs NUMERIC(15,6) | P0 | **DONE** |
| A04 | plt_type_conflict | salary FLOAT vs NUMERIC(10,3) → VARCHAR | P0 | **DONE** |
| A05 | plt_type_widen_varchar | name VARCHAR(100) vs VARCHAR(500) | P0 | NEW |
| A06 | plt_add_multiple_columns | k1_prime has 3 extra columns | P0 | NEW |
| A07 | plt_add_column_plus_type_drift | k1_prime adds phone + wider salary | P0 | NEW |
| A08 | plt_type_widen_date_to_timestamp | DATE vs TIMESTAMP_NTZ(6) | P1 | deferred |
| A09 | plt_type_widen_bool_to_number | BOOLEAN vs NUMBER(10,0) | P1 | deferred |
| A10 | plt_type_widen_ntz_to_tz | TIMESTAMP_NTZ(3) vs TIMESTAMP_TZ(3) | P1 | deferred |
| A11 | plt_type_conflict_date_vs_bool | DATE vs BOOLEAN → VARCHAR | P1 | deferred |
| A12 | plt_type_number_overridden_by_string | NUMBER(10,3) vs VARCHAR(500) | P1 | deferred |
| A13 | plt_type_widen_timestamp_precision | TIMESTAMP_NTZ(3) vs TIMESTAMP_NTZ(6) | P2 | deferred |
| A14 | plt_type_decimal_scale_zero | NUMBER(10,0) vs NUMBER(15,3) | P2 | deferred |

---

## BRANCH B: Consistent Evolution (two dbt runs, sources change between runs)

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| B01 | plt_drop_column | Both have phone → k1_prime drops → soft-drop | P0 | **DONE** |
| B02 | plt_add_column_all | No phone → Both add phone | P0 | **DONE** |
| B03 | plt_type_widen_inline | salary INTEGER → Both BIGINT | P0 | **DONE** |
| B04 | plt_no_narrow | salary NUMERIC(20,6) → Both NUMERIC(10,3) → blocked | P0 | **DONE** |
| B05 | plt_drop_column_all | Both have phone → Both drop → soft-drop, new rows NULL | P0 | NEW |
| B06 | plt_varchar_widen_inline | VARCHAR(100) → Both VARCHAR(200) | P1 | deferred |
| B07 | plt_varchar_no_narrow_inline | VARCHAR(200) → Both VARCHAR(50) → blocked | P1 | deferred |
| B08 | plt_type_conflict_inline | INTEGER → Both VARCHAR(100) | P1 | deferred |
| B09 | plt_drop_readd_same_type | phone → drop → re-add same type | P2 | deferred |
| B10 | plt_drop_readd_different_type | phone VARCHAR → drop → re-add NUMBER | P2 | deferred |
| B11 | plt_asymmetric_evolution | k1 adds phone, k1_prime drops email | P1 | deferred |

---

## BRANCH C: Multi-Change Batch (many operations in one dbt run)

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| C01 | plt_multi_change_10 | 7 simultaneous diffs in one run | P0 | NEW |
| C02 | plt_multi_add_and_widen | 3 new cols + 1 type widen between runs | P1 | deferred |

---

## BRANCH D: PK / Merge Key Scenarios

> Requires `resolve_merge_key` macro + `orders.sql` refactor (currently hardcoded `unique_key=['id', '__hevo_source_pipeline']`).

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| D01 | plt_pk_mismatch_across_sources | k1 PK=[id], k1_prime PK=[id,category] → union merge key | P1 | NEW |
| D02 | plt_no_pk_mixed_with_pk | k1 has PK, k1_prime has NO PK → mixed mode | P1 | NEW |
| D03 | plt_pk_added_to_unkeyed | Both no-PK → both gain PK between runs | P2 | deferred |
| D04 | plt_pk_dropped | Both have PK → both drop → APPEND fallback | P2 | deferred |
| D05 | plt_pk_simple_to_composite | PK [id] → PK [id, category] between runs | P2 | deferred |
| D06 | plt_pk_type_mismatch | k1 id=SMALLINT PK, k1_prime id=DECIMAL PK | P1 | NEW |

---

## BRANCH E: Constraint Scenarios

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| E01 | plt_not_null_mismatch | k1 mobile NOT NULL, k1_prime nullable → final nullable | P1 | deferred |
| E02 | plt_not_null_source_adds | k1 adds NOT NULL between runs → NOT propagated | P2 | deferred |
| E03 | plt_not_null_all_agree | Both NOT NULL → enforce on final | P2 | deferred |

---

## BRANCH F: Edge Cases

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| F01 | plt_hevo_temp_columns_filtered | `__HEVO__*__TEMP` cols in staging filtered out | P0 | NEW |
| F02 | plt_source_table_disappears | k1_prime staging dropped between runs (**reveals bug**) | P0 | deferred (needs bug fix first) |
| F03 | plt_empty_source_table | k1_prime has 0 rows, valid schema | P1 | deferred |
| F04 | plt_empty_source_with_extra_column | k1_prime empty but has extra column | P1 | deferred |
| F05 | plt_duplicate_ids_across_sources | k1 and k1_prime both have id=1,2,3 | P0 | NEW |
| F06 | plt_idempotent_rerun | dbt run twice on same data → no duplicates | P0 | NEW |
| F07 | plt_three_sources | Third source k1_double_prime | P2 | deferred |
| F08 | plt_large_column_count | 50+ columns across sources | P2 | deferred |
| F09 | plt_case_sensitivity | Mixed-case column names | P1 | deferred |
| F10 | plt_special_characters_in_data | NaN, Infinity in FLOAT → VARCHAR | P2 | deferred |

### Known Bug: F02 — `generate_source_select` has no None guard
If `adapter.get_relation()` returns `None` (table dropped), `adapter.get_columns_in_relation(None)` will error. Also `orders.sql` always emits `UNION ALL` for all sources regardless. Both need None guards before F02 can be tested.

---

## BRANCH G: Snowflake-Specific

| ID | Name | Scenario | Priority | Status |
|----|------|----------|----------|--------|
| G01 | plt_snowflake_varchar_16mb | VARCHAR(16777216) max length | P2 | deferred |
| G02 | plt_snowflake_number_38_0 | NUMBER(38,0) identity — no unnecessary ALTER | P2 | deferred |

---

## Phase 1 Implementation Plan (11 new tests + macro work)

### Step 1: New Branch A Tests (3 tests, no macro changes needed)

#### TEST A05 — plt_type_widen_varchar
**Branch**: A — cross-source drift
**Scenario**: k1 `name = VARCHAR(100)`, k1_prime `name = VARCHAR(500)`. PLT resolves to VARCHAR(500).

**Source schemas**:
| Schema | Columns |
|--------|---------|
| k1 | id PK, name VARCHAR(100), salary NUMERIC(15,2) |
| k1_prime | id PK, name VARCHAR(500), salary NUMERIC(15,2) |

k1_prime should have a row with a 300+ char name to verify no truncation.

**Asserts**:
- NAME column CHARACTER_MAXIMUM_LENGTH >= 500 in INFORMATION_SCHEMA
- k1_prime 300-char name preserved in final table
- Count = 6

**Files**: `sql_data_files/plt_type_widen_varchar/plt_type_widen_varchar.sql`, `suites/.../plt_type_widen_varchar.yml`, `tests/.../test_plt_type_widen_varchar.py`

---

#### TEST A06 — plt_add_multiple_columns
**Branch**: A — cross-source drift
**Scenario**: k1_prime has 3 extra columns (phone, address, city).

**Source schemas**:
| Schema | Columns |
|--------|---------|
| k1 | id PK, name, salary |
| k1_prime | id PK, name, salary, **phone**, **address**, **city** |

**Asserts**:
- All 3 extra columns exist in INFORMATION_SCHEMA
- k1 rows: all 3 are NULL
- k1_prime rows: all 3 are NOT NULL
- Count = 6

---

#### TEST A07 — plt_add_column_plus_type_drift
**Branch**: A — cross-source drift (compound scenario)
**Scenario**: k1_prime adds phone AND has wider salary NUMERIC(15,6) vs k1's NUMERIC(10,3).

**Source schemas**:
| Schema | Columns |
|--------|---------|
| k1 | id PK, name, salary NUMERIC(10,3) |
| k1_prime | id PK, name, salary NUMERIC(15,6), **phone** |

**Asserts**:
- PHONE column added, k1 rows NULL
- SALARY widened to NUMBER(15,6)
- k1_prime high-precision value preserved
- Count = 6

---

### Step 2: New Branch B Test (1 test)

#### TEST B05 — plt_drop_column_all
**Branch**: B — consistent evolution
**Scenario**: Both sources have phone in base state. Between runs, both drop phone. PLT soft-drop: column stays, new rows get NULL.

**Source schemas**:
| State | k1 | k1_prime |
|-------|-----|---------|
| base.sql | id, name, salary, phone | id, name, salary, phone |
| alter.sql | INSERT new row + DROP phone | INSERT new row + DROP phone |

**Test flow**:
1. Historical load → COMPLETED
2. DBT run 1 → all rows have phone
3. Assert: all rows PHONE IS NOT NULL
4. Run alter.sql → both drop phone + insert new rows
5. Incremental job → COMPLETED
6. DBT run 2 → phone column kept (soft-drop)
7. Assert: PHONE column still exists, run-1 rows retain phone, run-2 rows PHONE IS NULL

---

### Step 3: Multi-Change Integration (1 test)

#### TEST C01 — plt_multi_change_10
**Branch**: C — multi-change batch
**Scenario**: 7 simultaneous schema diffs between k1 and k1_prime in a single dbt run.

**Source schemas**:
| Column | k1 | k1_prime | Evolution |
|--------|-----|---------|-----------|
| id | PK | PK | — |
| name | VARCHAR(255) | VARCHAR(255) | — |
| salary | NUMERIC(15,2) | NUMERIC(15,2) | — |
| department | VARCHAR(100) | _(absent)_ | drop equivalent |
| bonus | DECIMAL(15,2) | DECIMAL(15,2) | same — no change |
| hire_date | DATE | _(absent)_ | name mismatch with test_date |
| test_date | _(absent)_ | DATE | name mismatch with hire_date |
| code | INTEGER | BIGINT | type widen |
| city | VARCHAR(100) | VARCHAR(500) | VARCHAR widen |
| amount | FLOAT | NUMERIC(10,3) | conflict → VARCHAR |
| region | _(absent)_ | VARCHAR(100) | add equivalent |

**Asserts**:
- DEPARTMENT exists (from k1), k1_prime rows NULL
- BONUS exists, both non-NULL
- HIRE_DATE and TEST_DATE both exist (name mismatch = two columns)
- CODE widened to NUMBER (BIGINT equivalent)
- CITY is VARCHAR(500+)
- AMOUNT is VARCHAR/TEXT (conflict fallback)
- REGION exists (from k1_prime), k1 rows NULL
- Count = 6

---

### Step 4: PK / Merge Key (3 tests + macro work)

#### Macro: `resolve_merge_key(sources)`
**Path**: `macros/resolve_merge_key.sql`
- Reads PK metadata from each source via `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE`
- Returns union of all source PKs + `__hevo_source_pipeline`
- Handles: no PK → append mode, mismatched PKs → union, IS NOT DISTINCT FROM for nullable key cols

#### Model refactor: `orders.sql`
- Replace hardcoded `unique_key=['id', '__hevo_source_pipeline']` with dynamic `resolve_merge_key` output
- Add IS NOT DISTINCT FROM in MERGE ON clause for nullable key cols

#### TEST D01 — plt_pk_mismatch_across_sources
**Scenario**: k1 PK=[id], k1_prime PK=[id, category]. Union merge key = [id, category, __hevo_source_pipeline].

**Source schemas**:
| Schema | Columns | PK |
|--------|---------|-----|
| k1 | id, name, category, salary | id |
| k1_prime | id, name, category, salary | (id, category) |

k1_prime has rows with same `id` but different `category`. k1 rows have category values but category is NOT in k1's PK.

**Asserts**:
- k1 rows: category non-NULL but merge key uses IS NOT DISTINCT FROM for nullable PKs
- All rows preserved (no merge collisions)
- Count = 6+ (depending on k1_prime row design)

#### TEST D02 — plt_no_pk_mixed_with_pk
**Scenario**: k1 has PK `id`, k1_prime has NO PK.

**Source schemas**:
| Schema | Columns | PK |
|--------|---------|-----|
| k1 | id PK, name, salary | id |
| k1_prime | id (no PK), name, salary | none |

k1_prime may have duplicate ids.

**Asserts**: PLT handles mixed mode — k1 uses MERGE, k1_prime uses dedup-in-SELECT or APPEND fallback. No Snowflake MERGE errors.

#### TEST D06 — plt_pk_type_mismatch
**Scenario**: k1 `id = SMALLINT PK`, k1_prime `id = DECIMAL(15,0) PK`.

**Source schemas**:
| Schema | Columns | PK |
|--------|---------|-----|
| k1 | id SMALLINT PK, name, salary | id |
| k1_prime | id DECIMAL(15,0) PK, name, salary | id |

**Asserts**: ID type widened to NUMBER(15,0) or wider. MERGE works. Count = 6.

---

### Step 5: Edge Cases (3 tests + bug fix)

#### Bug Fix: `generate_source_select.sql` None guard
Add check: if `adapter.get_relation()` returns None, skip that source. Also update `orders.sql` UNION ALL loop to skip None relations.

#### TEST F01 — plt_hevo_temp_columns_filtered
**Scenario**: Staging table has leftover `__HEVO__SALARY__TEMP` column from interrupted promotion.

**Note**: The Hevo loader wouldn't create such columns in staging — they only appear from interrupted type promotions on the destination. Two options:
1. INSERT directly into Snowflake staging post-load to add the column (requires destination DDL in test)
2. Test `resolve_column_schema` filtering as a dbt unit test

**Asserts**: `__HEVO__SALARY__TEMP` does NOT appear in final table.

#### TEST F05 — plt_duplicate_ids_across_sources
**Scenario**: k1 has id=1,2,3 and k1_prime also has id=1,2,3 with different names.

**Source schemas**:
| Schema | Rows |
|--------|------|
| k1 | (1, Alice), (2, Bob), (3, Carol) |
| k1_prime | (1, Charlie), (2, Diana), (3, Eve) |

**Asserts**: 6 rows in final (not 3). Composite key `[id, __hevo_source_pipeline]` keeps both sources' rows.

#### TEST F06 — plt_idempotent_rerun
**Scenario**: Run dbt twice on same data, no changes between runs.

Can reuse A01's SQL data. Just call `run_dbt()` twice.

**Asserts**: After run 2: count still = 6, no duplicates, no schema changes. `evolve_final_table` is a no-op on second run.

---

### Step 6: NOT NULL Enhancement (macro changes)

**Macro changes**:
- `resolve_column_schema.sql`: Track `is_nullable` per column across all sources via `IS_NULLABLE` from INFORMATION_SCHEMA
- `evolve_final_table.sql`: When adding a new column, if ALL sources have it as NOT NULL, add with NOT NULL constraint. Otherwise add as nullable.
- Dedicated tests (E01-E03) deferred to Phase 2.

---

## Key Files to Modify/Create

| File | Action |
|------|--------|
| `sentinel-tests/.../sql_data_files/plt_*/` | Create 8 new SQL data dirs |
| `sentinel-tests/.../suites/.../plt_*.yml` | Create 8 new suite YAMLs |
| `sentinel-tests/.../tests/.../test_plt_*.py` | Create 8 new test files |
| `sentinel-tests/.../suites/.../plt_all.yml` | Add 11 new entries |
| `plt-poc-dbt/macros/resolve_merge_key.sql` | **NEW** — dynamic PK resolution |
| `plt-poc-dbt/macros/generate_source_select.sql` | **FIX** — None guard for disappeared tables |
| `plt-poc-dbt/macros/resolve_column_schema.sql` | **ENHANCE** — nullability tracking |
| `plt-poc-dbt/macros/evolve_final_table.sql` | **ENHANCE** — NOT NULL when all agree |
| `plt-poc-dbt/models/plt/orders.sql` | **REFACTOR** — dynamic unique_key + None guard |

---

## Implementation Order

| Step | Task | Dependency |
|------|------|------------|
| 1 | A05: plt_type_widen_varchar | — |
| 2 | A06: plt_add_multiple_columns | — |
| 3 | A07: plt_add_column_plus_type_drift | — |
| 4 | B05: plt_drop_column_all | — |
| 5 | C01: plt_multi_change_10 | — |
| 6 | `resolve_merge_key.sql` macro | — |
| 7 | `orders.sql` refactor (dynamic unique_key) | resolve_merge_key |
| 8 | D01: plt_pk_mismatch_across_sources | resolve_merge_key + model refactor |
| 9 | D02: plt_no_pk_mixed_with_pk | resolve_merge_key + model refactor |
| 10 | D06: plt_pk_type_mismatch | resolve_merge_key + model refactor |
| 11 | Bug fix: generate_source_select None guard | — |
| 12 | F01: plt_hevo_temp_columns_filtered | — |
| 13 | F05: plt_duplicate_ids_across_sources | — |
| 14 | F06: plt_idempotent_rerun | — |
| 15 | NOT NULL enhancement (resolve_column_schema + evolve_final_table) | — |

---

## Deferred to Phase 2

- A08-A14 (type evolution matrix: DATE→TIMESTAMP, BOOL→NUMBER, NTZ→TZ, etc.)
- B06-B11 (VARCHAR widen/narrow inline, drop+readd, type conflict inline, asymmetric)
- C02 (multi-change variant)
- D03-D05 (PK lifecycle: added, dropped, simple→composite)
- E01-E03 (dedicated NOT NULL constraint tests)
- F02-F04, F07-F10 (source disappears, empty sources, 3 sources, wide tables, case sensitivity)
- G01-G02 (Snowflake-specific)
