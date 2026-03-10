# PLT POC — Test Case Implementation Plan

Each test case consists of: SQL data file(s), suite YAML, sentinel test Python, DBT macro changes, and assertions on the final table.

---

## Shared Infrastructure (implement first)

### `plt_runner.py`
**Path**: `sentinel-tests/functional/tests/post_load_transformations/plt_runner.py`
**Purpose**: Extracts DBT invocation out of `run_poc.py` so each test can call it directly.

```python
import json, subprocess, yaml
from pathlib import Path

FUNCTIONAL = Path(__file__).parent.parent.parent   # sentinel-tests/functional/
DBT_DIR    = Path("/Users/anushkasingh/Desktop/PLT/plt-poc-dbt")

def run_dbt(vars_dict: dict) -> None:
    sf      = yaml.safe_load((FUNCTIONAL / "config.yml").read_text())["local"]["test_dbs"]["snowflake"]
    account = sf["host"].removeprefix("https://").removesuffix(".snowflakecomputing.com")
    profiles = {"plt_poc": {"target": "dev", "outputs": {"dev": {
        "type": "snowflake", "account": account,
        "user": sf["root_username"], "password": sf["root_password"],
        "warehouse": sf["warehouse"], "database": vars_dict["k1_db"],
        "schema": "PLT_FINAL", "threads": 4
    }}}}
    (DBT_DIR / "profiles.yml").write_text(yaml.dump(profiles, default_flow_style=False))
    subprocess.run(["dbt", "run", "--vars", json.dumps(vars_dict)], cwd=DBT_DIR, check=True)
```

### Schema discovery helper (used in every test)
```python
def _discover_schemas(fi):
    object_to_mapping = fi.catalog_client.get_catalog_mappings(
        fi.id, fi.source.id, fi.destination.connection_type
    )
    k1_db = k1_schema = k1_prime_schema = k1_table = None
    for mapping in object_to_mapping.values():
        dst = mapping.destination_namespace
        k1_db, k1_table = dst.k2, dst.k0
        if "prime" in dst.k1.lower():
            k1_prime_schema = dst.k1
        else:
            k1_schema = dst.k1
    return dict(k1_db=k1_db, k1_schema=k1_schema,
                k1_prime_schema=k1_prime_schema, k1_table=k1_table)
```

### Macro changes needed before any type-evolution tests

| Macro | Change |
|-------|--------|
| `macros/get_widened_type.sql` | NEW — takes two type strings, returns wider type or VARCHAR(256) on conflict |
| `macros/resolve_column_schema.sql` | UPDATE — pairwise LCA reduction (call `get_widened_type` instead of first-seen-wins); filter `__HEVO__*` columns |
| `macros/evolve_final_table.sql` | UPDATE — add type ALTER (widen) + narrowing skip logic |

---

## TEST 1 — plt_add_column (existing, update only)

**Branch**: A — cross-source drift
**Scenario**: k1 has no `phone`, k1_prime has `phone`. PLT adds column to final table, NULL-pads k1 rows.
**Status**: SQL + suite YAML + test already exist. Just needs `run_dbt()` call + assertions added to test.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_add_column/plt_add_column.sql` | exists |
| `suites/post_load_transformations/plt_add_column.yml` | exists |
| `tests/post_load_transformations/test_plt_add_column.py` | UPDATE — add `run_dbt()` + assertions |

### Source schemas
| Schema | Columns |
|--------|---------|
| k1 | id, name, mobile, email, salary |
| k1_prime | id, name, mobile, email, salary, **phone** |

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)` — creates final table
4. Assert:
   - `COUNT(*) FROM PLT_FINAL.ORDERS` == 6
   - k1 rows: `PHONE IS NULL`
   - k1_prime rows: `PHONE IS NOT NULL`
   - `PHONE` column exists in `INFORMATION_SCHEMA.COLUMNS`

### Macros needed
None — existing macros handle ADD_COLUMN already.

---

## TEST 2 — plt_drop_column

**Branch**: A — cross-source drift
**Scenario**: k1 HAS `phone`, k1_prime does NOT. PLT keeps `phone` in final table (soft-drop); k1_prime rows get NULL.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_drop_column/plt_drop_column.sql` | NEW |
| `suites/post_load_transformations/plt_drop_column.yml` | NEW |
| `tests/post_load_transformations/test_plt_drop_column.py` | NEW |

### Source schemas
| Schema | Columns |
|--------|---------|
| k1 | id, name, mobile, email, salary, **phone** |
| k1_prime | id, name, mobile, email, salary |

### SQL (`plt_drop_column.sql`)
```sql
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50),
    email VARCHAR(255), salary NUMERIC(15,2), phone VARCHAR(50)
);
INSERT INTO k1.orders (name, mobile, email, salary, phone) VALUES
    ('Alice','555-1001','alice@example.com',75000,'555-AAA'),
    ('Bob','555-1002','bob@example.com',80000,'555-BBB'),
    ('Carol','555-1003','carol@example.com',65000,'555-CCC');

CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50),
    email VARCHAR(255), salary NUMERIC(15,2)
    -- no phone column
);
INSERT INTO k1_prime.orders (name, mobile, email, salary) VALUES
    ('Charlie','555-2001','charlie@example.com',90000),
    ('Diana','555-2002','diana@example.com',85000),
    ('Eve','555-2003','eve@example.com',92000);
```

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)`
4. Assert:
   - `COUNT(*) FROM PLT_FINAL.ORDERS` == 6
   - `COUNT(*) WHERE PHONE IS NOT NULL` == 3 (k1 rows)
   - `COUNT(*) WHERE PHONE IS NULL` == 3 (k1_prime rows)
   - `PHONE` column exists in `INFORMATION_SCHEMA.COLUMNS`

### Macros needed
None — `resolve_column_schema` picks up PHONE from k1; `generate_source_select` NULL-pads k1_prime.

---

## TEST 3 — plt_column_name_mismatch

**Branch**: A — cross-source drift
**Scenario**: k1 has `mobile` (no `phone`), k1_prime has `phone` (no `mobile`). PLT treats them as two separate columns. Final table has both; cross-NULL pattern.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_column_name_mismatch/plt_column_name_mismatch.sql` | NEW |
| `suites/post_load_transformations/plt_column_name_mismatch.yml` | NEW |
| `tests/post_load_transformations/test_plt_column_name_mismatch.py` | NEW |

### Source schemas
| Schema | Columns |
|--------|---------|
| k1 | id, name, **mobile**, email, salary |
| k1_prime | id, name, **phone**, email, salary |

### SQL (`plt_column_name_mismatch.sql`)
```sql
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), mobile VARCHAR(50),
    email VARCHAR(255), salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, mobile, email, salary) VALUES
    ('Alice','555-1001','alice@example.com',75000),
    ('Bob','555-1002','bob@example.com',80000),
    ('Carol','555-1003','carol@example.com',65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (
    id BIGSERIAL PRIMARY KEY, name VARCHAR(255), phone VARCHAR(50),
    email VARCHAR(255), salary NUMERIC(15,2)
    -- phone instead of mobile
);
INSERT INTO k1_prime.orders (name, phone, email, salary) VALUES
    ('Charlie','555-2001','charlie@example.com',90000),
    ('Diana','555-2002','diana@example.com',85000),
    ('Eve','555-2003','eve@example.com',92000);
```

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)`
4. Assert:
   - Both `MOBILE` and `PHONE` columns exist in `INFORMATION_SCHEMA.COLUMNS`
   - k1 rows: `MOBILE IS NOT NULL`, `PHONE IS NULL`
   - k1_prime rows: `PHONE IS NOT NULL`, `MOBILE IS NULL`
   - `COUNT(*) FROM PLT_FINAL.ORDERS` == 6

### Macros needed
None — unified schema = union of {MOBILE, PHONE, ...}; NULL-padding handles the rest.

---

## TEST 4 — plt_type_widen_safe

**Branch**: A — cross-source drift
**Scenario**: k1 `salary = NUMERIC(10,3)`, k1_prime `salary = NUMERIC(15,6)`. PLT resolves to NUMERIC(15,6) — safe widening. Final table column type is altered.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_type_widen_safe/plt_type_widen_safe.sql` | NEW |
| `suites/post_load_transformations/plt_type_widen_safe.yml` | NEW |
| `tests/post_load_transformations/test_plt_type_widen_safe.py` | NEW |

### Source schemas
| Schema | Columns |
|--------|---------|
| k1 | id, name, salary **NUMERIC(10,3)** |
| k1_prime | id, name, salary **NUMERIC(15,6)** |

### SQL (`plt_type_widen_safe.sql`)
```sql
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(10,3));
INSERT INTO k1.orders (name, salary) VALUES
    ('Alice',75000.123), ('Bob',80000.456), ('Carol',65000.789);

CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,6));
INSERT INTO k1_prime.orders (name, salary) VALUES
    ('Charlie',90000.123456), ('Diana',85000.654321), ('Eve',92000.000001);
```

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)`
4. Assert:
   - `SALARY` column type in `INFORMATION_SCHEMA.COLUMNS` is `NUMBER` with `NUMERIC_PRECISION=15`, `NUMERIC_SCALE=6`
   - All 6 rows have non-NULL salary
   - k1_prime row with `90000.123456` is not truncated

### Macros needed
- NEW `get_widened_type` — NUMBER(10,3) + NUMBER(15,6) → NUMBER(15,6)
- UPDATE `resolve_column_schema` — call `get_widened_type` on repeated column names

---

## TEST 5 — plt_type_conflict

**Branch**: A — cross-source drift
**Scenario**: k1 `salary = FLOAT`, k1_prime `salary = NUMERIC(10,3)`. Cross-family conflict → VARCHAR(256) fallback.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_type_conflict/plt_type_conflict.sql` | NEW |
| `suites/post_load_transformations/plt_type_conflict.yml` | NEW |
| `tests/post_load_transformations/test_plt_type_conflict.py` | NEW |

### Source schemas
| Schema | Columns |
|--------|---------|
| k1 | id, name, salary **FLOAT** |
| k1_prime | id, name, salary **NUMERIC(10,3)** |

### SQL (`plt_type_conflict.sql`)
```sql
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary FLOAT);
INSERT INTO k1.orders (name, salary) VALUES
    ('Alice',75000.5), ('Bob',80000.25), ('Carol',65000.75);

CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(10,3));
INSERT INTO k1_prime.orders (name, salary) VALUES
    ('Charlie',90000.123), ('Diana',85000.456), ('Eve',92000.789);
```

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)`
4. Assert:
   - `SALARY` column type in `INFORMATION_SCHEMA.COLUMNS` is `TEXT` or `VARCHAR`
   - All 6 rows have non-NULL salary as string values
   - e.g. `'75000.5'` and `'90000.123'` are readable as strings

### Macros needed
- `get_widened_type` — FLOAT + NUMBER → VARCHAR(256) (conflict path)
- UPDATE `resolve_column_schema` (same change as TEST 4)
- UPDATE `evolve_final_table` — ALTER COLUMN type when unified type differs from final table's current type

---

## TEST 6 — plt_add_column_all

**Branch**: B — consistent evolution
**Scenario**: Both k1 and k1_prime gain `phone` after the first DBT run. Final table was created without it. Second DBT run must ADD COLUMN and populate all rows.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_add_column_all/base.sql` | NEW |
| `sql_data_files/plt_add_column_all/alter.sql` | NEW |
| `suites/post_load_transformations/plt_add_column_all.yml` | NEW (init_sql_data_files → base.sql) |
| `tests/post_load_transformations/test_plt_add_column_all.py` | NEW |

### Source schemas
| State | k1 | k1_prime |
|-------|-----|---------|
| base.sql | id, name, salary | id, name, salary |
| alter.sql | + phone | + phone |

### SQL (`base.sql`)
```sql
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2));
INSERT INTO k1.orders (name, salary) VALUES
    ('Alice',75000), ('Bob',80000), ('Carol',65000);

CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(15,2));
INSERT INTO k1_prime.orders (name, salary) VALUES
    ('Charlie',90000), ('Diana',85000), ('Eve',92000);
```

### SQL (`alter.sql`)
```sql
ALTER TABLE k1.orders ADD COLUMN phone VARCHAR(50);
UPDATE k1.orders SET phone = '555-100' || id::text;

ALTER TABLE k1_prime.orders ADD COLUMN phone VARCHAR(50);
UPDATE k1_prime.orders SET phone = '555-200' || id::text;
```

### Test flow
1. Wait for historical load → COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)` — **DBT run 1**: final table created WITHOUT phone
4. Assert `PHONE` column does NOT exist in `INFORMATION_SCHEMA.COLUMNS` (pre-evolution baseline)
5. `fi.source.run_sql_data_file(["sql_data_files/plt_add_column_all/alter.sql"])` — evolves both Postgres schemas
6. Trigger incremental job + wait COMPLETED — staging schemas now have phone
7. `run_dbt(vars_dict)` — **DBT run 2**: `evolve_final_table` adds PHONE; all rows populated
8. Assert:
   - `PHONE` column now exists
   - `COUNT(*) WHERE PHONE IS NULL` == 0 (all rows have phone — both sources have it)
   - `COUNT(*) FROM PLT_FINAL.ORDERS` == 6

### Macros needed
None beyond the changes in TEST 4 (evolve_final_table ADD COLUMN path already works).

---

## TEST 7 — plt_type_widen_inline

**Branch**: B — consistent evolution
**Scenario**: Both k1 and k1_prime change `salary` from INTEGER to BIGINT. Final table has INTEGER. Second DBT run must ALTER COLUMN type to BIGINT.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_type_widen_inline/base.sql` | NEW |
| `sql_data_files/plt_type_widen_inline/alter.sql` | NEW |
| `suites/post_load_transformations/plt_type_widen_inline.yml` | NEW |
| `tests/post_load_transformations/test_plt_type_widen_inline.py` | NEW |

### Source schemas
| State | k1 salary | k1_prime salary |
|-------|-----------|----------------|
| base.sql | INTEGER | INTEGER |
| alter.sql | BIGINT | BIGINT |

### SQL (`alter.sql`)
```sql
ALTER TABLE k1.orders ALTER COLUMN salary TYPE BIGINT;
ALTER TABLE k1_prime.orders ALTER COLUMN salary TYPE BIGINT;
```

### Test flow
1. Historical + wait COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)` — **DBT run 1**: final table salary is NUMBER (INTEGER in Snowflake → NUMBER(38,0))
4. Record salary column precision/scale as baseline
5. `fi.source.run_sql_data_file(["sql_data_files/plt_type_widen_inline/alter.sql"])`
6. Incremental job + wait COMPLETED
7. `run_dbt(vars_dict)` — **DBT run 2**: `evolve_final_table` detects BIGINT > INTEGER → `ALTER COLUMN salary SET DATA TYPE BIGINT`
8. Assert:
   - salary column type is NUMBER(38,0) or BIGINT equivalent
   - All 6 rows have salary values (no data loss)

### Macros needed
- UPDATE `evolve_final_table` — type ALTER logic: `ALTER TABLE t ALTER COLUMN col SET DATA TYPE new_type`
- `get_widened_type` used in `resolve_column_schema` detects widening correctly

---

## TEST 8 — plt_no_narrow

**Branch**: B — consistent evolution (narrowing attempt, PLT must refuse)
**Scenario**: Both k1 and k1_prime change `salary` from NUMERIC(20,6) → NUMERIC(10,3). Final table has NUMERIC(20,6). PLT must detect narrowing and NOT alter the final table.

### Files
| File | Action |
|------|--------|
| `sql_data_files/plt_no_narrow/base.sql` | NEW |
| `sql_data_files/plt_no_narrow/alter.sql` | NEW |
| `suites/post_load_transformations/plt_no_narrow.yml` | NEW |
| `tests/post_load_transformations/test_plt_no_narrow.py` | NEW |

### Source schemas
| State | k1 salary | k1_prime salary |
|-------|-----------|----------------|
| base.sql | NUMERIC(20,6) | NUMERIC(20,6) |
| alter.sql | NUMERIC(10,3) | NUMERIC(10,3) |

### SQL (`alter.sql`)
```sql
-- Postgres requires DROP + ADD for narrowing
ALTER TABLE k1.orders DROP COLUMN salary;
ALTER TABLE k1.orders ADD COLUMN salary NUMERIC(10,3);
UPDATE k1.orders SET salary = 75000.123;

ALTER TABLE k1_prime.orders DROP COLUMN salary;
ALTER TABLE k1_prime.orders ADD COLUMN salary NUMERIC(10,3);
UPDATE k1_prime.orders SET salary = 90000.456;
```

### Test flow
1. Historical + wait COMPLETED
2. `_discover_schemas` → `vars_dict`
3. `run_dbt(vars_dict)` — **DBT run 1**: final table salary is NUMBER(20,6)
4. Record baseline: `NUMERIC_PRECISION=20`, `NUMERIC_SCALE=6`
5. `fi.source.run_sql_data_file(["sql_data_files/plt_no_narrow/alter.sql"])`
6. Incremental job + wait COMPLETED
7. `run_dbt(vars_dict)` — **DBT run 2**: `evolve_final_table` detects NUMERIC(10,3) < NUMERIC(20,6) → logs skip, no ALTER
8. Assert:
   - salary column type is STILL NUMBER(20,6) — `NUMERIC_PRECISION=20`, `NUMERIC_SCALE=6`
   - All 6 rows have salary values

### Macros needed
- `evolve_final_table` narrowing detection: compare `get_widened_type(unified_type, current_final_type)` — if result == current_final_type then unified is narrower → skip ALTER

---

## Implementation Order

| Step | Task | Dependency |
|------|------|------------|
| 1 | `plt_runner.py` shared utility | — |
| 2 | Update `test_plt_add_column.py` + assertions (TEST 1) | plt_runner.py |
| 3 | `get_widened_type.sql` macro | — |
| 4 | Update `resolve_column_schema.sql` | get_widened_type |
| 5 | TEST 2: plt_drop_column | resolve_column_schema update |
| 6 | TEST 3: plt_column_name_mismatch | resolve_column_schema update |
| 7 | TEST 4: plt_type_widen_safe | get_widened_type + resolve_column_schema |
| 8 | TEST 5: plt_type_conflict | get_widened_type + resolve_column_schema |
| 9 | Update `evolve_final_table.sql` — type ALTER + narrowing skip | get_widened_type |
| 10 | TEST 6: plt_add_column_all | evolve_final_table update |
| 11 | TEST 7: plt_type_widen_inline | evolve_final_table type ALTER |
| 12 | TEST 8: plt_no_narrow | evolve_final_table narrowing skip |
