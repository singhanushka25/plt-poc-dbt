# plt-poc-dbt

POC DBT project for PLT (Post-Load Transformation) schema evolution engine.

## What it does

Merges data from two Snowflake source schemas (simulating two Hevo pipeline outputs) into a
single shared final table — `PLT_FINAL.orders`. Demonstrates the ADD COLUMN schema evolution
case: `k1_prime` has a `PHONE` column that `k1` doesn't.

## Prerequisites

```bash
pip install dbt-snowflake
```

## Setup

1. Run the sentinel test to load k1 data and create the k1_prime schema:
   ```bash
   cd sentinel-tests
   pytest post_load_transformations/tests/test_plt_add_column.py -s
   ```
   Copy the `dbt run` command printed at the end of the test output.

2. Set the Snowflake database env var (from the test output):
   ```bash
   export SNOWFLAKE_DATABASE=<k1_db printed by test>
   ```

## Running

Paste and run the command printed by the sentinel test, e.g.:
```bash
dbt run --vars '{"k1_db": "MY_DB", "k1_schema": "PIPELINE_SCHEMA", "k1_prime_schema": "__PLT_K1_PRIME", "k1_table": "SAMPLE_DATA"}'
```

## What to verify

```sql
-- Run in Snowflake after dbt run:
SELECT __hevo_source_pipeline, ID, NAME, PHONE
FROM PLT_FINAL.orders
ORDER BY ID;
```

Expected:
- `k1` rows: `PHONE = NULL`
- `k1_prime` rows (IDs 101–103): `PHONE = '555-...'`

Run `dbt run` again (no new data) — row count stays the same (MERGE is idempotent).

## Project Structure

```
models/plt/orders.sql             — incremental MERGE model (UNION ALL from both sources)
models/plt/sources.yml            — source definitions (var-based schema names)
macros/add_column_if_not_exists.sql — pre-hook: ALTER TABLE ADD COLUMN if missing
```
