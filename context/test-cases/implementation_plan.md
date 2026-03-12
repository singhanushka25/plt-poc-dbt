# PLT POC — Test Case Implementation Plan (v2)

> Replaces the old Branch A-G classification with a cleaner category system.
> Covers Snowflake (Phase 1) and Redshift (Phase 1 expansion — schema evolution + PK only).

---

## Test Categories

| Category | Code | What it tests |
|----------|------|---------------|
| Type Evolution Matrix | **T** | Every type pair LCA resolution across sources |
| Structural Changes | **S** | Column add, drop, mismatch |
| Evolution (Temporal) | **E** | Schema changes between dbt runs |
| PK / Merge Key | **P** | PK mismatch, no PK, composite keys, PK type drift |
| Constraints | **C** | NOT NULL resolution, PK constraint ordering |
| Integrity | **I** | Idempotency |

---

## Complete Test Suite — Presentation Summary

| ID | Test Name | Category | Pattern | Destination | Status | What it proves |
|----|-----------|----------|---------|-------------|--------|----------------|
| T01 | plt_type_matrix_snowflake | Type Matrix | Cross-source drift | Snowflake | NEW | All type pair LCA resolutions (29 column pairs) |
| T02 | plt_type_matrix_redshift | Type Matrix | Cross-source drift | Redshift | NEW | Redshift-specific type hierarchy LCA |
| S01 | plt_add_column | Structural | Cross-source drift | Snowflake | DONE | Column present in one source, absent in another |
| S02 | plt_column_name_mismatch | Structural | Cross-source drift | Snowflake | DONE | Different column names → both columns appear |
| S03 | plt_add_multiple_columns | Structural | Cross-source drift | Snowflake | NEW | 3 extra columns in one source → all added, NULL-padded |
| S04 | plt_drop_column_all | Structural | Temporal (2 runs) | Snowflake | NEW | Both sources drop column → soft-drop preserved |
| E01 | plt_drop_column | Evolution | Temporal (2 runs) | Snowflake | DONE | One source drops column → soft-drop |
| E02 | plt_add_column_all | Evolution | Temporal (2 runs) | Snowflake | DONE | Both sources add column between runs |
| E03 | plt_type_widen_inline | Evolution | Temporal (2 runs) | Snowflake | DONE | Same-family type widen between runs |
| E04 | plt_no_narrow | Evolution | Temporal (2 runs) | Snowflake | DONE | Type narrowing blocked between runs |
| E05 | plt_asymmetric_evolution | Evolution | Temporal (2 runs) | Snowflake | NEW | k1 adds col + k1_prime drops col simultaneously |
| E06 | plt_type_widen_temporal | Evolution | Temporal (2 runs) | Snowflake | NEW | Drop+recreate type evolution (competitor parity pattern) |
| P01 | plt_pk_mismatch | PK / Merge Key | Cross-source drift | Snowflake | NEW | Different PKs across sources → union merge key |
| P02 | plt_no_pk_mixed | PK / Merge Key | Cross-source drift | Snowflake | NEW | One source has PK, other doesn't → design TBD |
| P03 | plt_pk_type_mismatch | PK / Merge Key | Cross-source drift | Snowflake | NEW | PK column type differs → type widening on PK |
| P04 | plt_duplicate_ids | PK / Merge Key | Cross-source drift | Snowflake | NEW | Same IDs across sources → composite key preserves both |
| P05 | plt_pk_composite_mismatch | PK / Merge Key | Cross-source drift | Snowflake | NEW | Composite vs simple PK → union includes all PK cols |
| P06 | plt_pk_dropped_between_runs | PK / Merge Key | Temporal (2 runs) | Snowflake | NEW | PK dropped between runs → merge key behavior |
| C01 | plt_not_null_mismatch | Constraints | Cross-source drift | Snowflake | NEW | One source NOT NULL, other nullable → final nullable |
| C02 | plt_not_null_all_agree | Constraints | Cross-source drift | Snowflake | NEW | Both NOT NULL → final enforces NOT NULL |
| C03 | plt_not_null_on_new_column | Constraints | Cross-source drift | Snowflake | NEW | NOT NULL col missing in other source → final nullable |
| C04 | plt_redshift_composite_pk | Constraints | Cross-source drift | Redshift | NEW | Composite PK union + NOT NULL enforcement + pg_catalog validation |
| C05 | plt_not_null_pk_promotion | Constraints | Temporal (2 runs) | Both | NEW | PK type widen → DROP NOT NULL → promote → re-ADD |
| I01 | plt_idempotent_rerun | Integrity | Double run | Snowflake | NEW | dbt run twice → no duplicates, no schema changes |

---

## What's Already Implemented (8 tests)

All shared infra + 8 tests exist in sentinel (`feat/H2-31153/plt_poc` branch):

| New ID | Old ID | Test | Category | Status |
|--------|--------|------|----------|--------|
| S01 | A01 | plt_add_column | S | **DONE** |
| S02 | A02 | plt_column_name_mismatch | S | **DONE** |
| T-legacy-1 | A03 | plt_type_widen_safe | T | **DONE** (superseded by T01 matrix) |
| T-legacy-2 | A04 | plt_type_conflict | T | **DONE** (superseded by T01 matrix) |
| E01 | B01 | plt_drop_column | E | **DONE** |
| E02 | B02 | plt_add_column_all | E | **DONE** |
| E03 | B03 | plt_type_widen_inline | E | **DONE** |
| E04 | B04 | plt_no_narrow | E | **DONE** |

**Shared infra done**: `plt_runner.py` with `run_dbt()` + `discover_schemas()`.

---

## Full Test Suite

### Summary

| Category | Total | Done | New |
|----------|-------|------|-----|
| **T: Type Matrix** | 2 | 0 | 2 (Snowflake + Redshift) |
| **S: Structural** | 4 | 2 | 2 |
| **E: Evolution** | 6 | 4 | 2 |
| **P: PK / Merge Key** | 6 | 0 | 6 |
| **C: Constraints** | 5 | 0 | 5 |
| **I: Integrity** | 1 | 0 | 1 |
| **Total** | **24** | **6** | **18 new** |

---

## T: Type Evolution Matrix

Uses the competitor-parity pattern: one table with many columns, each column tests a specific type pair via cross-source drift. k1 has column of type A, k1_prime has same column name but type B. Single dbt run resolves all pairs.

### T01 — plt_type_matrix_snowflake

**Pattern**: Cross-source drift (single dbt run)
**Destination**: Snowflake

**SQL file**: `sql_data_files/plt_type_matrix_snowflake/plt_type_matrix_snowflake.sql`

Creates one table (`type_matrix`) in both k1 and k1_prime with many columns. Each column name encodes the type pair: `col_{FROM}_{TO}`. k1 has the FROM type, k1_prime has the TO type.

```sql
-- k1: all columns use the "FROM" type
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.type_matrix (
    id              BIGSERIAL PRIMARY KEY,

    -- Numeric family (same-family widen)
    col_int_bigint          INTEGER,
    col_smallint_int        SMALLINT,
    col_num10_num15         NUMERIC(10,3),
    col_num10_num76         NUMERIC(10,3),

    -- Numeric → Float (sibling, LCA = STRING)
    col_int_float           INTEGER,
    col_num_float           NUMERIC(10,3),
    col_float_num           DOUBLE PRECISION,

    -- Numeric → String (child → root)
    col_int_varchar         INTEGER,
    col_num_varchar         NUMERIC(10,3),
    col_float_varchar       DOUBLE PRECISION,
    col_bool_varchar        BOOLEAN,

    -- Boolean family
    col_bool_int            BOOLEAN,
    col_bool_num            BOOLEAN,
    col_bool_bigint         BOOLEAN,

    -- Temporal family (linear chain: DATE → NTZ → TZ)
    col_date_ntz            DATE,
    col_date_tz             DATE,
    col_ntz_tz              TIMESTAMP WITHOUT TIME ZONE,
    col_ntz3_ntz6           TIMESTAMP(3) WITHOUT TIME ZONE,

    -- Temporal → String
    col_date_varchar        DATE,
    col_ts_varchar          TIMESTAMP WITHOUT TIME ZONE,

    -- String family (length widen)
    col_vc100_vc500         VARCHAR(100),

    -- Cross-family conflicts (distant cousins → STRING)
    col_date_bool           DATE,
    col_date_int            DATE,
    col_time_int            TIME,
    col_bool_date           BOOLEAN,

    -- Array/Variant
    col_array_varchar       INT[],

    -- Same type (no-op)
    col_int_int             INTEGER,
    col_vc_vc               VARCHAR(100),
    col_date_date           DATE
);

INSERT INTO k1.type_matrix (
    id,
    col_int_bigint, col_smallint_int, col_num10_num15, col_num10_num76,
    col_int_float, col_num_float, col_float_num,
    col_int_varchar, col_num_varchar, col_float_varchar, col_bool_varchar,
    col_bool_int, col_bool_num, col_bool_bigint,
    col_date_ntz, col_date_tz, col_ntz_tz, col_ntz3_ntz6,
    col_date_varchar, col_ts_varchar,
    col_vc100_vc500,
    col_date_bool, col_date_int, col_time_int, col_bool_date,
    col_array_varchar,
    col_int_int, col_vc_vc, col_date_date
) VALUES
(1, 42, 100, 1234567.123, 1234567.123,
   42, 1234567.123, 3.14159,
   42, 1234567.123, 3.14159, true,
   true, true, true,
   '2024-01-15', '2024-01-15', '2024-01-15 10:30:00', '2024-01-15 10:30:00.123',
   '2024-01-15', '2024-01-15 10:30:00',
   'short_text',
   '2024-01-15', '2024-01-15', '10:30:00', true,
   ARRAY[1,2,3],
   42, 'hello', '2024-01-15'),
(2, 100, 200, 9999999.999, 9999999.999,
   100, 9999999.999, 2.71828,
   100, 9999999.999, 2.71828, false,
   false, false, false,
   '2024-06-30', '2024-06-30', '2024-06-30 23:59:59', '2024-06-30 23:59:59.456',
   '2024-06-30', '2024-06-30 23:59:59',
   'another_text',
   '2024-06-30', '2024-06-30', '23:59:59', false,
   ARRAY[4,5,6],
   100, 'world', '2024-06-30'),
(3, 999, 300, 5555555.555, 5555555.555,
   999, 5555555.555, 1.41421,
   999, 5555555.555, 1.41421, true,
   true, true, true,
   '2024-12-25', '2024-12-25', '2024-12-25 12:00:00', '2024-12-25 12:00:00.789',
   '2024-12-25', '2024-12-25 12:00:00',
   'third_text',
   '2024-12-25', '2024-12-25', '12:00:00', true,
   ARRAY[7,8,9],
   999, 'test', '2024-12-25');

-- k1_prime: same column NAMES but "TO" types
CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.type_matrix (
    id              BIGSERIAL PRIMARY KEY,

    -- Numeric widen
    col_int_bigint          BIGINT,
    col_smallint_int        INTEGER,
    col_num10_num15         NUMERIC(15,6),
    col_num10_num76         NUMERIC(76,38),

    -- Numeric → Float (sibling → LCA=STRING on Snowflake)
    col_int_float           DOUBLE PRECISION,
    col_num_float           DOUBLE PRECISION,
    col_float_num           NUMERIC(10,3),

    -- Numeric → String
    col_int_varchar         VARCHAR(100),
    col_num_varchar         VARCHAR(100),
    col_float_varchar       VARCHAR(100),
    col_bool_varchar        VARCHAR(100),

    -- Boolean → Numeric (parent)
    col_bool_int            INTEGER,
    col_bool_num            NUMERIC(10,3),
    col_bool_bigint         BIGINT,

    -- Temporal chain
    col_date_ntz            TIMESTAMP WITHOUT TIME ZONE,
    col_date_tz             TIMESTAMP WITH TIME ZONE,
    col_ntz_tz              TIMESTAMP WITH TIME ZONE,
    col_ntz3_ntz6           TIMESTAMP(6) WITHOUT TIME ZONE,

    -- Temporal → String
    col_date_varchar        VARCHAR(100),
    col_ts_varchar          VARCHAR(100),

    -- String widen
    col_vc100_vc500         VARCHAR(500),

    -- Cross-family (→ STRING)
    col_date_bool           BOOLEAN,
    col_date_int            INTEGER,
    col_time_int            INTEGER,
    col_bool_date           DATE,

    -- Array → String
    col_array_varchar       VARCHAR(100),

    -- Same type (no-op)
    col_int_int             INTEGER,
    col_vc_vc               VARCHAR(100),
    col_date_date           DATE
);

INSERT INTO k1_prime.type_matrix (
    id,
    col_int_bigint, col_smallint_int, col_num10_num15, col_num10_num76,
    col_int_float, col_num_float, col_float_num,
    col_int_varchar, col_num_varchar, col_float_varchar, col_bool_varchar,
    col_bool_int, col_bool_num, col_bool_bigint,
    col_date_ntz, col_date_tz, col_ntz_tz, col_ntz3_ntz6,
    col_date_varchar, col_ts_varchar,
    col_vc100_vc500,
    col_date_bool, col_date_int, col_time_int, col_bool_date,
    col_array_varchar,
    col_int_int, col_vc_vc, col_date_date
) VALUES
(4, 9223372036854775807, 42, 123456789.123456, 12345678901234567890.12345678901234567890,
   3.14, 3.14, 1234567.123,
   'text_val_1', 'text_val_1', 'text_val_1', 'true_as_text',
   1, 1.000, 1,
   '2024-03-20 15:30:00', '2024-03-20 15:30:00+05:30', '2024-03-20 15:30:00+05:30', '2024-03-20 15:30:00.123456',
   '2024-03-20', '2024-03-20 15:30:00',
   'this_is_a_longer_text_that_exceeds_100_chars_to_verify_varchar_widening_works_correctly_in_the_plt_macro_pipeline_end',
   true, 42, 42, '2024-03-20',
   'array_as_text',
   42, 'hello', '2024-03-20'),
(5, 1000000000000, 500, 999999999.999999, 99999999999999999999.99999999999999999999,
   2.71, 2.71, 9999999.999,
   'text_val_2', 'text_val_2', 'text_val_2', 'false_as_text',
   0, 0.000, 0,
   '2024-07-04 08:00:00', '2024-07-04 08:00:00+00:00', '2024-07-04 08:00:00+00:00', '2024-07-04 08:00:00.654321',
   '2024-07-04', '2024-07-04 08:00:00',
   'another_long_text_for_varchar_widening_test_over_one_hundred_characters_to_check_the_expansion_logic_works',
   false, 100, 100, '2024-07-04',
   'another_array',
   100, 'world', '2024-07-04'),
(6, 5000000000000, 700, 555555555.555555, 55555555555555555555.55555555555555555555,
   1.41, 1.41, 5555555.555,
   'text_val_3', 'text_val_3', 'text_val_3', 'maybe',
   1, 99.999, 999,
   '2024-11-11 00:00:00', '2024-11-11 00:00:00-08:00', '2024-11-11 00:00:00-08:00', '2024-11-11 00:00:00.000001',
   '2024-11-11', '2024-11-11 00:00:00',
   'third_long_text_entry_for_the_varchar_widening_type_matrix_test_to_ensure_all_rows_are_handled_properly_here',
   true, 999, 999, '2024-11-11',
   'third_array',
   999, 'test', '2024-11-11');
```

**Expected LCA resolutions on Snowflake** (what the test asserts):

| Column | k1 type (Postgres) | k1_prime type (Postgres) | Snowflake LCA | Notes |
|--------|-------------------|------------------------|---------------|-------|
| col_int_bigint | INTEGER | BIGINT | NUMBER (wider) | Same-family widen |
| col_smallint_int | SMALLINT | INTEGER | NUMBER (wider) | Same-family widen |
| col_num10_num15 | NUMERIC(10,3) | NUMERIC(15,6) | NUMBER(15,6) | Precision/scale merge |
| col_num10_num76 | NUMERIC(10,3) | NUMERIC(76,38) | NUMBER(38,37) or VARCHAR | May exceed Snowflake max → STRING |
| col_int_float | INTEGER | FLOAT | VARCHAR | Siblings under STRING |
| col_num_float | NUMERIC(10,3) | FLOAT | VARCHAR | Siblings under STRING |
| col_float_num | FLOAT | NUMERIC(10,3) | VARCHAR | Siblings under STRING |
| col_int_varchar | INTEGER | VARCHAR | VARCHAR | Child → root |
| col_num_varchar | NUMERIC | VARCHAR | VARCHAR | Child → root |
| col_float_varchar | FLOAT | VARCHAR | VARCHAR | Child → root |
| col_bool_varchar | BOOLEAN | VARCHAR | VARCHAR | Child → root |
| col_bool_int | BOOLEAN | INTEGER | NUMBER | BOOLEAN is child of NUMBER |
| col_bool_num | BOOLEAN | NUMERIC | NUMBER | BOOLEAN → parent NUMBER |
| col_bool_bigint | BOOLEAN | BIGINT | NUMBER | BOOLEAN → parent NUMBER |
| col_date_ntz | DATE | TIMESTAMP | TIMESTAMP_NTZ | DATE → parent TIMESTAMP_NTZ |
| col_date_tz | DATE | TIMESTAMPTZ | TIMESTAMP_TZ | DATE → grandparent |
| col_ntz_tz | TIMESTAMP | TIMESTAMPTZ | TIMESTAMP_TZ | NTZ → parent TZ |
| col_ntz3_ntz6 | TIMESTAMP(3) | TIMESTAMP(6) | TIMESTAMP_NTZ(6) | Precision merge |
| col_date_varchar | DATE | VARCHAR | VARCHAR | Child → root |
| col_ts_varchar | TIMESTAMP | VARCHAR | VARCHAR | Child → root |
| col_vc100_vc500 | VARCHAR(100) | VARCHAR(500) | VARCHAR(500) | Length widen |
| col_date_bool | DATE | BOOLEAN | VARCHAR | Distant cousins → STRING |
| col_date_int | DATE | INTEGER | VARCHAR | Distant cousins → STRING |
| col_time_int | TIME | INTEGER | VARCHAR | Distant cousins → STRING |
| col_bool_date | BOOLEAN | DATE | VARCHAR | Distant cousins → STRING |
| col_array_varchar | ARRAY | VARCHAR | VARCHAR | ARRAY → VARIANT → STRING |
| col_int_int | INTEGER | INTEGER | NUMBER | No-op (same type) |
| col_vc_vc | VARCHAR(100) | VARCHAR(100) | VARCHAR(100) | No-op (same type) |
| col_date_date | DATE | DATE | DATE | No-op (same type) |

**Asserts**:
- 6 total rows in PLT_FINAL.TYPE_MATRIX
- Each column's resolved type matches expected LCA (via INFORMATION_SCHEMA)
- k1 row values preserved (data not lost during CAST)
- k1_prime row values preserved
- No-op columns unchanged

**Files**:
- `sql_data_files/plt_type_matrix_snowflake/plt_type_matrix_snowflake.sql`
- `suites/post_load_transformations/plt_type_matrix_snowflake.yml`
- `tests/post_load_transformations/test_plt_type_matrix_snowflake.py`

---

### T02 — plt_type_matrix_redshift

**Pattern**: Cross-source drift (single dbt run)
**Destination**: Redshift

Same SQL data files as T01 (Postgres source is identical). Different suite YAML pointing to Redshift destination. Different expected LCA resolutions because Redshift has a different type hierarchy (`RedshiftSchemaTypeHierarchy`).

**Key Redshift differences**:
- FLOAT and NUMBER may resolve differently
- BOOLEAN handling may differ
- VARCHAR max length differs (65535 vs Snowflake's 16MB)
- TIMESTAMPTZ handling

**Files**:
- `sql_data_files/plt_type_matrix_redshift/` → symlink or copy of T01 SQL
- `suites/post_load_transformations/plt_type_matrix_redshift.yml` (destination: REDSHIFT)
- `tests/post_load_transformations/test_plt_type_matrix_redshift.py`

**Note**: Expected type resolutions TBD — need to map `RedshiftSchemaTypeHierarchy` first.

---

## S: Structural Changes

### S01 — plt_add_column (DONE)

k1 missing `phone`, k1_prime has it. Single dbt run.
**SQL**: Single file — k1.orders (5 cols), k1_prime.orders (6 cols with phone).

### S02 — plt_column_name_mismatch (DONE)

k1 has `mobile`, k1_prime has `phone`. Both appear in final (no rename detection).
**SQL**: Single file — different column names.

### S03 — plt_add_multiple_columns (NEW)

**Pattern**: Cross-source drift (single dbt run)

k1_prime has 3 extra columns (phone, address, city) that k1 doesn't have.

**SQL file**: `sql_data_files/plt_add_multiple_columns/plt_add_multiple_columns.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id       BIGSERIAL PRIMARY KEY,
    name     VARCHAR(255),
    salary   NUMERIC(15,2)
);
INSERT INTO k1.orders (name, salary) VALUES
('Alice', 75000), ('Bob', 80000), ('Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id       BIGSERIAL PRIMARY KEY,
    name     VARCHAR(255),
    salary   NUMERIC(15,2),
    phone    VARCHAR(50),
    address  VARCHAR(500),
    city     VARCHAR(100)
);
INSERT INTO k1_prime.orders (name, salary, phone, address, city) VALUES
('Charlie', 90000, '555-1234', '123 Main St', 'NYC'),
('Diana', 85000, '555-5678', '456 Oak Ave', 'LA'),
('Eve', 70000, '555-9012', '789 Pine Rd', 'Chicago');
```

**Asserts**:
- 6 rows total
- PHONE, ADDRESS, CITY all exist in INFORMATION_SCHEMA
- k1 rows: all 3 extra columns are NULL
- k1_prime rows: all 3 extra columns are NOT NULL

**Files**:
- `sql_data_files/plt_add_multiple_columns/plt_add_multiple_columns.sql`
- `suites/post_load_transformations/plt_add_multiple_columns.yml`
- `tests/post_load_transformations/test_plt_add_multiple_columns.py`

---

### S04 — plt_drop_column_all (NEW)

**Pattern**: Temporal evolution (two dbt runs)

Both sources have phone initially. Between runs, BOTH drop phone. Soft-drop: column stays in final, new rows get NULL.

**SQL files**:

`sql_data_files/plt_drop_column_all/base.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2), phone VARCHAR(50)
);
INSERT INTO k1.orders (name, salary, phone) VALUES
('Alice', 75000, '555-1001'), ('Bob', 80000, '555-1002'), ('Carol', 65000, '555-1003');

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2), phone VARCHAR(50)
);
INSERT INTO k1_prime.orders (name, salary, phone) VALUES
('Charlie', 90000, '555-2001'), ('Diana', 85000, '555-2002'), ('Eve', 70000, '555-2003');
```

`sql_data_files/plt_drop_column_all/alter.sql`:
```sql
-- New rows first (CDC trigger)
INSERT INTO k1.orders (name, salary, phone) VALUES ('Frank', 70000, '555-1004');
INSERT INTO k1_prime.orders (name, salary, phone) VALUES ('Grace', 88000, '555-2004');

-- Both drop phone
ALTER TABLE k1.orders DROP COLUMN phone;
ALTER TABLE k1_prime.orders DROP COLUMN phone;
```

**Asserts**:
- Run 1: 6 rows, all PHONE NOT NULL
- Run 2: 8 rows, PHONE column still exists (soft-drop), run-1 rows retain phone, run-2 rows PHONE IS NULL

**Files**:
- `sql_data_files/plt_drop_column_all/base.sql`
- `sql_data_files/plt_drop_column_all/alter.sql`
- `suites/post_load_transformations/plt_drop_column_all.yml`
- `tests/post_load_transformations/test_plt_drop_column_all.py`

---

## E: Evolution (Temporal)

### E01 — plt_drop_column (DONE)

k1_prime drops phone between runs. Soft-drop.

### E02 — plt_add_column_all (DONE)

Both gain phone between runs.

### E03 — plt_type_widen_inline (DONE)

Both widen INTEGER → BIGINT between runs.

### E04 — plt_no_narrow (DONE)

Both narrow NUMBER(20,6) → NUMBER(10,3) between runs. PLT blocks.

### E05 — plt_asymmetric_evolution (NEW)

**Pattern**: Temporal evolution (two dbt runs)

k1 adds phone between runs, k1_prime drops email between runs. Tests two different structural changes happening simultaneously.

**SQL files**:

`sql_data_files/plt_asymmetric_evolution/base.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, email, salary) VALUES
('Alice', 'alice@ex.com', 75000), ('Bob', 'bob@ex.com', 80000), ('Carol', 'carol@ex.com', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders (name, email, salary) VALUES
('Charlie', 'charlie@ex.com', 90000), ('Diana', 'diana@ex.com', 85000), ('Eve', 'eve@ex.com', 70000);
```

`sql_data_files/plt_asymmetric_evolution/alter.sql`:
```sql
INSERT INTO k1.orders (name, email, salary) VALUES ('Frank', 'frank@ex.com', 70000);
INSERT INTO k1_prime.orders (name, email, salary) VALUES ('Grace', 'grace@ex.com', 88000);

-- k1 adds phone
ALTER TABLE k1.orders ADD COLUMN phone VARCHAR(50);
UPDATE k1.orders SET phone = '555-' || id::text;

-- k1_prime drops email
ALTER TABLE k1_prime.orders DROP COLUMN email;
```

**Asserts**:
- Run 1: 6 rows, EMAIL present, no PHONE
- Run 2: 8 rows, EMAIL still exists (soft-drop from k1_prime), PHONE added (from k1)
- k1 run-2 rows: PHONE NOT NULL, EMAIL NOT NULL
- k1_prime run-2 rows: PHONE IS NULL, EMAIL IS NULL

**Files**:
- `sql_data_files/plt_asymmetric_evolution/base.sql`
- `sql_data_files/plt_asymmetric_evolution/alter.sql`
- `suites/post_load_transformations/plt_asymmetric_evolution.yml`
- `tests/post_load_transformations/test_plt_asymmetric_evolution.py`

---

### E06 — plt_type_widen_temporal (NEW)

**Pattern**: Temporal evolution (two dbt runs) — representative temporal type drift

k1 and k1_prime start with same type. Between runs, k1_prime evolves type (drop + recreate pattern from competitor parity).

**SQL files**:

`sql_data_files/plt_type_widen_temporal/base.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255),
    code INTEGER, amount NUMERIC(10,3), created DATE
);
INSERT INTO k1.orders (name, code, amount, created) VALUES
('Alice', 42, 1234.567, '2024-01-15'),
('Bob', 100, 9999.999, '2024-06-30'),
('Carol', 999, 5555.555, '2024-12-25');

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255),
    code INTEGER, amount NUMERIC(10,3), created DATE
);
INSERT INTO k1_prime.orders (name, code, amount, created) VALUES
('Charlie', 50, 8888.888, '2024-03-20'),
('Diana', 200, 7777.777, '2024-07-04'),
('Eve', 800, 6666.666, '2024-11-11');
```

`sql_data_files/plt_type_widen_temporal/alter.sql`:
```sql
-- Insert new row into k1 only (advances Hevo offset to trigger incremental run)
INSERT INTO k1.orders (name, code, amount, created) VALUES ('Frank', 150, 3333.333, '2024-09-01');

-- k1_prime evolves: drop + recreate with new types (competitor parity pattern)
-- code: INTEGER → BIGINT (same-family widen)
-- amount: NUMERIC(10,3) → FLOAT (cross-family → VARCHAR on Snowflake)
-- created: DATE → TIMESTAMP (temporal chain widen)
-- NOTE: drop destroys old rows, so we recreate with ALL data (original + new)
DROP TABLE k1_prime.orders;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255),
    code BIGINT, amount DOUBLE PRECISION, created TIMESTAMP WITHOUT TIME ZONE
);
INSERT INTO k1_prime.orders (name, code, amount, created) VALUES
('Charlie', 50, 8888.888, '2024-03-20 15:30:00'),
('Diana', 200, 7777.777, '2024-07-04 08:00:00'),
('Eve', 800, 6666.666, '2024-11-11 00:00:00'),
('Grace', 250, 4444.444, '2024-10-15 12:00:00');
```

**Asserts**:
- Run 1: 6 rows, code=NUMBER, amount=NUMBER, created=DATE
- Run 2: 8 rows, code widened to NUMBER(wider), amount→VARCHAR (cross-family), created→TIMESTAMP_NTZ
- k1 row values preserved through CASTs

---

## P: PK / Merge Key

> Requires new `resolve_merge_key` macro + model refactor.

### P01 — plt_pk_mismatch (NEW)

**Pattern**: Cross-source drift (single dbt run)

k1 PK=[id], k1_prime PK=[id, category]. Union merge key = [id, category, __hevo_source_pipeline].

**SQL file**: `sql_data_files/plt_pk_mismatch/plt_pk_mismatch.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id       BIGSERIAL PRIMARY KEY,
    name     VARCHAR(255),
    category VARCHAR(100),
    salary   NUMERIC(15,2)
);
INSERT INTO k1.orders (name, category, salary) VALUES
('Alice', 'engineering', 75000),
('Bob', 'sales', 80000),
('Carol', 'engineering', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id       BIGSERIAL,
    name     VARCHAR(255),
    category VARCHAR(100),
    salary   NUMERIC(15,2),
    PRIMARY KEY (id, category)
);
-- Same id, different category → should NOT collide
INSERT INTO k1_prime.orders (id, name, category, salary) VALUES
(1, 'Charlie', 'marketing', 90000),
(1, 'Diana', 'sales', 85000),
(2, 'Eve', 'engineering', 70000);
```

**Asserts**:
- All 6 rows preserved (no merge collisions due to composite key)
- k1_prime id=1 has 2 rows (different categories)
- Merge key includes category

---

### P02 — plt_no_pk_mixed (NEW)

k1 has PK `id`, k1_prime has NO PK. Tests mixed mode.

**SQL file**: `sql_data_files/plt_no_pk_mixed/plt_no_pk_mixed.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, salary) VALUES
('Alice', 75000), ('Bob', 80000), ('Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL,  -- NO PRIMARY KEY
    name VARCHAR(255), salary NUMERIC(15,2)
);
-- k1_prime may have duplicate ids (no PK enforcement)
INSERT INTO k1_prime.orders (id, name, salary) VALUES
(1, 'Charlie', 90000), (1, 'Diana', 85000), (2, 'Eve', 70000);
```

**Asserts** (depends on design decision — see callouts.md):
- **If MERGE mode**: PLT must dedup k1_prime rows before MERGE (dedup-in-SELECT on whatever key is available). No Snowflake MERGE errors. Merge key = k1's PK (id) + __hevo_source_pipeline. k1_prime's duplicate id=1 → latest wins after dedup.
- **If APPEND fallback for no-PK source**: k1_prime rows are INSERTed (no dedup), k1 rows MERGEd. k1_prime's duplicate id=1 both preserved.
- **If full APPEND**: No MERGE at all. All rows appended. Idempotency broken.
- **Design decision needed**: Which mode? Flagged in callouts.md.

---

### P03 — plt_pk_type_mismatch (NEW)

k1 `id = SMALLINT PK`, k1_prime `id = DECIMAL(15,0) PK`. PK column type must be widened.

**SQL file**: `sql_data_files/plt_pk_type_mismatch/plt_pk_type_mismatch.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id SMALLINT PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders VALUES (1, 'Alice', 75000), (2, 'Bob', 80000), (3, 'Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id DECIMAL(15,0) PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders VALUES (4, 'Charlie', 90000), (5, 'Diana', 85000), (6, 'Eve', 70000);
```

**Asserts**:
- ID type widened to NUMBER(15,0) or wider
- MERGE works correctly
- 6 rows, all IDs preserved

---

### P04 — plt_duplicate_ids_across_sources (NEW)

k1 and k1_prime both have id=1,2,3 with different data. Validates composite key [id, __hevo_source_pipeline] keeps both.

**SQL file**: `sql_data_files/plt_duplicate_ids/plt_duplicate_ids.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders VALUES (1, 'Alice', 75000), (2, 'Bob', 80000), (3, 'Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
-- SAME IDs, different names
INSERT INTO k1_prime.orders VALUES (1, 'Charlie', 90000), (2, 'Diana', 85000), (3, 'Eve', 70000);
```

**Asserts**:
- 6 rows (NOT 3) — composite key prevents merge
- Both Alice (k1, id=1) and Charlie (k1_prime, id=1) present
- __hevo_source_pipeline distinguishes them

---

### P05 — plt_pk_composite_mismatch (NEW)

k1 PK=[id, region], k1_prime PK=[id]. Tests PK shrink across sources.

**SQL file**: `sql_data_files/plt_pk_composite_mismatch/plt_pk_composite_mismatch.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id     BIGSERIAL,
    region VARCHAR(50),
    name   VARCHAR(255),
    salary NUMERIC(15,2),
    PRIMARY KEY (id, region)
);
INSERT INTO k1.orders (id, region, name, salary) VALUES
(1, 'US', 'Alice', 75000), (1, 'EU', 'Bob', 80000), (2, 'US', 'Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id     BIGSERIAL PRIMARY KEY,
    region VARCHAR(50),
    name   VARCHAR(255),
    salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders (id, region, name, salary) VALUES
(1, 'APAC', 'Charlie', 90000), (2, 'EU', 'Diana', 85000), (3, 'US', 'Eve', 70000);
```

**Asserts**:
- Union merge key includes region (from k1's PK)
- k1's (id=1, region=US) and (id=1, region=EU) are separate rows
- All 6 rows preserved

---

### P06 — plt_pk_dropped_between_runs (NEW)

**Pattern**: Temporal evolution (two dbt runs)

Both have PK initially. Between runs, k1_prime drops PK (recreated without PK constraint). Tests merge key behavior when PK disappears.

**SQL files**:

`sql_data_files/plt_pk_dropped/base.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, salary) VALUES ('Alice', 75000), ('Bob', 80000), ('Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders (name, salary) VALUES ('Charlie', 90000), ('Diana', 85000), ('Eve', 70000);
```

`sql_data_files/plt_pk_dropped/alter.sql`:
```sql
INSERT INTO k1.orders (name, salary) VALUES ('Frank', 70000);

-- k1_prime loses PK: drop + recreate without PK
INSERT INTO k1_prime.orders (name, salary) VALUES ('Grace', 88000);
-- Note: Postgres can't DROP PRIMARY KEY easily inline, so we use:
ALTER TABLE k1_prime.orders DROP CONSTRAINT k1_prime_orders_pkey;
-- Now k1_prime has no PK; k1 still has PK
-- Also insert duplicate id to prove no PK enforcement
INSERT INTO k1_prime.orders (id, name, salary) VALUES (4, 'Hank', 77000);
```

**Asserts**:
- Run 1: 6 rows, merge on [id, __hevo_source_pipeline]
- Run 2: PLT handles k1_prime having no PK (dedup-in-SELECT or append fallback)
- k1_prime's duplicate id=4 rows handled gracefully (no MERGE error)

---

## C: Constraints

### C01 — plt_not_null_mismatch (NEW)

**Pattern**: Cross-source drift (single dbt run)

k1 has `mobile NOT NULL`, k1_prime has `mobile` as nullable.

**SQL file**: `sql_data_files/plt_not_null_mismatch/plt_not_null_mismatch.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id     BIGSERIAL PRIMARY KEY,
    name   VARCHAR(255) NOT NULL,
    mobile VARCHAR(50) NOT NULL,
    salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, mobile, salary) VALUES
('Alice', '555-1001', 75000), ('Bob', '555-1002', 80000), ('Carol', '555-1003', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id     BIGSERIAL PRIMARY KEY,
    name   VARCHAR(255),
    mobile VARCHAR(50),         -- nullable
    salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders (name, mobile, salary) VALUES
('Charlie', '555-2001', 90000), ('Diana', NULL, 85000), ('Eve', '555-2003', 70000);
```

**Asserts**:
- Final table MOBILE is NULLABLE (one source allows NULL → final must be nullable)
- k1_prime Diana's NULL mobile preserved
- 6 rows total

---

### C02 — plt_not_null_all_agree (NEW)

**Pattern**: Cross-source drift (single dbt run)

Both sources have `mobile NOT NULL`. Final should enforce NOT NULL.

**SQL file**: `sql_data_files/plt_not_null_all_agree/plt_not_null_all_agree.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50) NOT NULL, salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, mobile, salary) VALUES
('Alice', '555-1001', 75000), ('Bob', '555-1002', 80000), ('Carol', '555-1003', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50) NOT NULL, salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders (name, mobile, salary) VALUES
('Charlie', '555-2001', 90000), ('Diana', '555-2002', 85000), ('Eve', '555-2003', 70000);
```

**Asserts**:
- Final table MOBILE is NOT NULL (both sources agree)
- 6 rows total
- IS_NULLABLE = 'NO' in INFORMATION_SCHEMA for MOBILE

---

### C03 — plt_not_null_on_new_column (NEW)

**Pattern**: Cross-source drift (single dbt run)

k1 has `mobile NOT NULL`, k1_prime doesn't have `mobile` at all. PLT adds the column — should it be nullable?

**SQL file**: `sql_data_files/plt_not_null_on_new_column/plt_not_null_on_new_column.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50) NOT NULL, salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, mobile, salary) VALUES
('Alice', '555-1001', 75000), ('Bob', '555-1002', 80000), ('Carol', '555-1003', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
    -- NO mobile column
);
INSERT INTO k1_prime.orders (name, salary) VALUES
('Charlie', 90000), ('Diana', 85000), ('Eve', 70000);
```

**Asserts**:
- MOBILE exists in final (added from k1)
- MOBILE is NULLABLE (k1_prime rows must have NULL for mobile)
- k1_prime rows: MOBILE IS NULL
- 6 rows total

---

### C04 — plt_redshift_composite_pk (NEW)

**Pattern**: Cross-source drift (single dbt run)
**Destination**: Redshift

Tests Redshift-specific behavior: (1) PK columns MUST have NOT NULL, (2) only one PK constraint per table, (3) composite PK from union of source PKs is correctly applied.

Scenario: k1 has PK=[id], k1_prime has PK=[id, name]. PLT must create final table with composite PK=(id, name) on Redshift, applying the 4-step approach:
1. DROP old PK constraint (if exists)
2. ADD NOT NULL on all PK columns (id, name) — Redshift requirement
3. ADD PRIMARY KEY (id, name)
4. Verify via pg_catalog

**SQL file**: `sql_data_files/plt_redshift_composite_pk/plt_redshift_composite_pk.sql`

```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, salary) VALUES
('Alice', 75000), ('Bob', 80000), ('Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id     BIGSERIAL,
    name   VARCHAR(255),
    salary NUMERIC(15,2),
    PRIMARY KEY (id, name)
);
INSERT INTO k1_prime.orders (id, name, salary) VALUES
(1, 'Charlie', 90000), (1, 'Diana', 85000), (2, 'Eve', 70000);
```

**Suite YAML**: `destination: template: REDSHIFT`

**Asserts**:
- Final table has composite PK (id, name) — verified via pg_catalog:
  ```sql
  SELECT a.attname
  FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
  WHERE i.indrelid = '"plt_final"."orders"'::regclass AND i.indisprimary
  ORDER BY array_position(i.indkey, a.attnum);
  ```
  Must return both `id` and `name`
- Both PK columns have NOT NULL constraint:
  ```sql
  SELECT column_name, is_nullable
  FROM information_schema.columns
  WHERE table_schema = 'plt_final' AND table_name = 'orders'
    AND column_name IN ('id', 'name');
  ```
  Both must show `is_nullable = 'NO'`
- MERGE works correctly — no "PK column contains NULL" errors
- All 6 rows preserved (composite key prevents collisions)

**Files**:
- `sql_data_files/plt_redshift_composite_pk/plt_redshift_composite_pk.sql`
- `suites/post_load_transformations/plt_redshift_composite_pk.yml`
- `tests/post_load_transformations/test_plt_redshift_composite_pk.py`

---

### C05 — plt_not_null_pk_promotion (NEW)

**Pattern**: Temporal evolution (two dbt runs)

PK column type widens between runs. On Redshift, must DROP NOT NULL before type promotion, then re-add NOT NULL after. On Snowflake, PKs can be nullable so this is less strict.

**SQL files**:

`sql_data_files/plt_not_null_pk_promotion/base.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS k1;
CREATE TABLE k1.orders (
    id SMALLINT PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders VALUES (1, 'Alice', 75000), (2, 'Bob', 80000), (3, 'Carol', 65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
CREATE TABLE k1_prime.orders (
    id SMALLINT PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders VALUES (4, 'Charlie', 90000), (5, 'Diana', 85000), (6, 'Eve', 70000);
```

`sql_data_files/plt_not_null_pk_promotion/alter.sql`:
```sql
INSERT INTO k1.orders VALUES (7, 'Frank', 70000);

-- k1_prime PK type widens: drop + recreate
DROP TABLE k1_prime.orders;
CREATE TABLE k1_prime.orders (
    id DECIMAL(15,0) PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1_prime.orders VALUES (4, 'Charlie', 90000), (5, 'Diana', 85000),
(6, 'Eve', 70000), (8, 'Grace', 88000);
```

**Asserts (Snowflake)**:
- Run 1: ID is SMALLINT/NUMBER, 6 rows
- Run 2: ID widened to NUMBER(15,0), 8 rows
- evolve_final_table handles the promotion correctly

**Asserts (Redshift variant)**:
- DROP NOT NULL on ID before promotion
- Re-ADD NOT NULL after promotion
- No constraint violation errors

**Files**:
- `sql_data_files/plt_not_null_pk_promotion/base.sql`
- `sql_data_files/plt_not_null_pk_promotion/alter.sql`
- `suites/post_load_transformations/plt_not_null_pk_promotion.yml` (Snowflake)
- `suites/post_load_transformations/plt_not_null_pk_promotion_redshift.yml` (Redshift)
- `tests/post_load_transformations/test_plt_not_null_pk_promotion.py`

---

## I: Integrity

### I01 — plt_idempotent_rerun (NEW)

**Pattern**: Single dbt run, then run AGAIN on same data

Reuses S01 (plt_add_column) SQL data. Calls `run_dbt()` twice with no source changes between runs.

**SQL file**: Reuses `sql_data_files/plt_add_column/plt_add_column.sql`

**Test flow**:
1. Historical load → COMPLETED
2. `run_dbt()` → 6 rows in PLT_FINAL
3. `run_dbt()` again (no source changes)
4. Assert: still 6 rows (no duplicates), no schema changes, no errors

**Files**:
- No new SQL (reuses S01)
- `suites/post_load_transformations/plt_idempotent_rerun.yml`
- `tests/post_load_transformations/test_plt_idempotent_rerun.py`

---

### ~~I02 — plt_hevo_temp_columns_filtered~~ (REMOVED)

Removed — not needed for POC scope.

---

## Implementation Order

| Step | Test | Dependencies | Macro Changes |
|------|------|-------------|---------------|
| 1 | T01: plt_type_matrix_snowflake | New dbt model for type_matrix table | Validate get_widened_type covers all LCA pairs |
| 2 | S03: plt_add_multiple_columns | None | None (existing macros handle N columns) |
| 3 | S04: plt_drop_column_all | None | None (soft-drop already works) |
| 4 | E05: plt_asymmetric_evolution | None | None |
| 5 | E06: plt_type_widen_temporal | None | Validate evolve_final_table handles multi-type changes |
| 6 | I01: plt_idempotent_rerun | None | None |
| 7 | **resolve_merge_key macro** | — | NEW macro |
| 8 | **orders.sql refactor** (dynamic unique_key) | resolve_merge_key | Refactor model |
| 9 | P01: plt_pk_mismatch | resolve_merge_key | — |
| 10 | P02: plt_no_pk_mixed | resolve_merge_key | Dedup-in-SELECT for no-PK sources |
| 11 | P03: plt_pk_type_mismatch | resolve_merge_key | — |
| 12 | P04: plt_duplicate_ids | resolve_merge_key | — |
| 13 | P05: plt_pk_composite_mismatch | resolve_merge_key | — |
| 14 | P06: plt_pk_dropped_between_runs | resolve_merge_key | PK drop detection |
| 15 | C01: plt_not_null_mismatch | None | Add nullability tracking to resolve_column_schema |
| 16 | C02: plt_not_null_all_agree | C01 | ADD NOT NULL in evolve_final_table |
| 17 | C03: plt_not_null_on_new_column | C01 | — |
| 18 | C05: plt_not_null_pk_promotion | C01 + resolve_merge_key | DROP NOT NULL before type promotion |
| 19 | T02: plt_type_matrix_redshift | T01 + Redshift type hierarchy | Redshift-specific get_widened_type |
| 20 | C04: plt_redshift_composite_pk | resolve_merge_key + Redshift | 4-step PK creation + NOT NULL + pg_catalog validation |

---

## Files Summary

### SQL Data File Directory Layout

```
sql_data_files/
├── type_matrix/
│   └── plt_type_matrix.sql              # T01 + T02 (shared SQL, different destinations)
├── structural/
│   ├── plt_add_column/                   # S01 (DONE)
│   ├── plt_column_name_mismatch/         # S02 (DONE)
│   ├── plt_add_multiple_columns/         # S03
│   │   └── plt_add_multiple_columns.sql
│   └── plt_drop_column_all/              # S04
│       ├── base.sql
│       └── alter.sql
├── evolution/
│   ├── plt_drop_column/                  # E01 (DONE)
│   ├── plt_add_column_all/               # E02 (DONE)
│   ├── plt_type_widen_inline/            # E03 (DONE)
│   ├── plt_no_narrow/                    # E04 (DONE)
│   ├── plt_asymmetric_evolution/         # E05
│   │   ├── base.sql
│   │   └── alter.sql
│   └── plt_type_widen_temporal/          # E06
│       ├── base.sql
│       └── alter.sql
├── pk_merge_key/
│   ├── plt_pk_mismatch/                  # P01
│   │   └── plt_pk_mismatch.sql
│   ├── plt_no_pk_mixed/                  # P02
│   │   └── plt_no_pk_mixed.sql
│   ├── plt_pk_type_mismatch/             # P03
│   │   └── plt_pk_type_mismatch.sql
│   ├── plt_duplicate_ids/                # P04
│   │   └── plt_duplicate_ids.sql
│   ├── plt_pk_composite_mismatch/        # P05
│   │   └── plt_pk_composite_mismatch.sql
│   └── plt_pk_dropped/                   # P06
│       ├── base.sql
│       └── alter.sql
├── constraints/
│   ├── plt_not_null_mismatch/            # C01
│   │   └── plt_not_null_mismatch.sql
│   ├── plt_not_null_all_agree/           # C02
│   │   └── plt_not_null_all_agree.sql
│   ├── plt_not_null_on_new_column/       # C03
│   │   └── plt_not_null_on_new_column.sql
│   ├── plt_redshift_composite_pk/        # C04
│   │   └── plt_redshift_composite_pk.sql
│   └── plt_not_null_pk_promotion/        # C05
│       ├── base.sql
│       └── alter.sql
└── integrity/
    └── plt_idempotent_rerun/             # I01 (reuses S01 SQL)
```
| `plt_not_null_pk_promotion/` | base.sql + alter.sql | Two-run |

### New Suite YAMLs (19)

One per test, following existing pattern. Redshift tests use `destination: config: template: REDSHIFT`.

### New Python Tests (17)

Reuse existing `plt_runner.py` infrastructure. Tests I01 and I02 reuse existing SQL data.

### Macro Changes Required

| Macro | Change |
|-------|--------|
| `resolve_merge_key.sql` | **NEW** — reads PK from INFORMATION_SCHEMA, computes union merge key |
| `resolve_column_schema.sql` | **ENHANCE** — nullability tracking, __HEVO__*__TEMP filter |
| `evolve_final_table.sql` | **ENHANCE** — NOT NULL management, DROP NOT NULL before type promotion |
| `get_widened_type.sql` | **VALIDATE** — ensure all LCA pairs from T01 are covered |
| `generate_source_select.sql` | **FIX** — None guard for disappeared tables |
| `orders.sql` | **REFACTOR** — dynamic unique_key from resolve_merge_key |

---

## Deferred (Phase 2+)

- B06-B08: VARCHAR widen/narrow inline, type conflict inline
- B09-B10: Drop + re-add same/different type
- D03-D05: PK lifecycle (added to unkeyed, dropped, simple→composite between runs)
- E01-E03 (old): Dedicated NOT NULL constraint evolution tests
- F02-F04: Source disappears, empty source, empty + extra column
- F07-F10: Three sources, 50+ columns, case sensitivity, special chars
- G01-G02: Snowflake-specific edge cases (VARCHAR 16MB, NUMBER(38,0))
- Full Redshift type hierarchy mapping
- BigQuery destination support
