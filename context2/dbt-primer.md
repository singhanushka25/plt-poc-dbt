# DBT Primer for PLT

---

## 1. What DBT Actually Is

DBT is a **SQL transformation runner**. It does not move data — it only transforms data that's already in your warehouse. You write SELECT statements; DBT wraps them in CREATE TABLE / MERGE / INSERT depending on the materialization you choose.

The runtime looks like this:

```
your .sql files  →  dbt compiles Jinja  →  generates raw SQL  →  runs on Snowflake
```

Everything else (macros, hooks, packages) is just tooling around that core loop.

---

## 2. Key Concepts

### Models
A model = one `.sql` file = one table/view in Snowflake.

```
models/plt/orders.sql   →   PLT_FINAL.orders   (in Snowflake)
models/plt/customers.sql →  PLT_FINAL.customers
```

The schema (`PLT_FINAL`) comes from `profiles.yml`. The table name = the filename.

### Materializations
Configured per model via `config()` at the top of the file.

| Materialization | What it does |
|----------------|--------------|
| `table`        | DROP + CREATE TABLE AS SELECT every run |
| `view`         | CREATE OR REPLACE VIEW every run |
| `incremental`  | On first run: CREATE TABLE. On subsequent runs: MERGE/INSERT only new rows. |

We use `incremental` because we only want to process new data each PLT trigger, not re-merge everything.

### Macros
Macros are Jinja functions stored in `macros/*.sql`. They can:
- Return SQL strings (used inline in models)
- Run SQL statements as side effects (via `run_query()`)
- Be called from models, hooks, or other macros

```sql
-- calling a macro in a model
{{ my_macro(arg1, arg2) }}

-- calling a macro for side effects only (no return value emitted)
{% do my_macro(arg1, arg2) %}
```

### Hooks
SQL or macro calls that run before/after a model executes.

```python
config(pre_hook="{{ my_macro(this) }}")
```

`this` = the DBT relation (database.schema.table) for the current model.

### Sources
Declarations in `sources.yml` that tell DBT about external tables it reads from (not owned by DBT). Used with `{{ source('name', 'table') }}` in models.

---

## 3. Are We Rebuilding the Wheel?

### `get_source_columns` — YES, partially

DBT has a native adapter method that does the same thing more idiomatically:

```sql
{% set rel = adapter.get_relation(database=db, schema=schema, identifier=table) %}
{% set columns = adapter.get_columns_in_relation(rel) %}
{# columns is a list of Column objects: col.name, col.dtype #}
```

This is better than querying INFORMATION_SCHEMA directly. Worth updating `get_source_columns.sql` to use this.

### `generate_source_select` — PARTIALLY covered by dbt-utils

[`dbt_utils.union_relations()`](https://github.com/dbt-labs/dbt-utils#union_relations-source) does exactly UNION ALL with NULL-padding for missing columns. BUT it only works with static `ref()` or `source()` references, not dynamic database/schema/table strings from `--vars`. Since our source schemas change every sentinel test run, our custom macro is the right call here.

### `evolve_final_table` — CUSTOM, no package does this

No standard dbt package handles ALTER TABLE ADD COLUMN as a pre-merge step. This is genuinely PLT-specific logic. Keep it.

**Summary:** `get_source_columns` should use `adapter.get_columns_in_relation()`. Everything else is justified custom code.

---

## 4. Multiple Models in One Project

Yes. One project can have as many models as needed. They all share the same macros, profiles, and can reference each other.

```
models/plt/
├── orders.sql        →  PLT_FINAL.orders
├── customers.sql     →  PLT_FINAL.customers
└── inventory.sql     →  PLT_FINAL.inventory
```

Each model independently declares its sources and calls the PLT macros:

```sql
-- customers.sql
{% set plt_sources = [
  {'database': var('k1_db'), 'schema': var('k1_schema'),       'table': 'CUSTOMERS', 'label': 'k1'},
  {'database': var('k1_db'), 'schema': var('k1_prime_schema'), 'table': 'CUSTOMERS', 'label': 'k1_prime'},
] %}
{{ config(materialized='incremental', unique_key=['id', '__hevo_source_pipeline'], incremental_strategy='merge') }}
{% set unified_cols = resolve_column_schema(plt_sources) %}
{% do evolve_final_table(this, unified_cols) %}
{% for src in plt_sources %}
{{ generate_source_select(src, unified_cols) }}
{%- if not loop.last %}UNION ALL{% endif %}
{% endfor %}
```

### Running selectively

```bash
# run only one model
dbt run --select orders

# run all models in the plt/ folder
dbt run --select plt.*

# run everything
dbt run

# run only models tagged with 'plt'
dbt run --select tag:plt
```

Tag a model in its config:
```sql
{{ config(tags=['plt'], materialized='incremental', ...) }}
```

### Running macros on demand (without a model)

DBT has `run-operation` for calling macros directly, outside of a model run:

```bash
dbt run-operation evolve_final_table --args '{"target": "PLT_FINAL.orders", "unified_columns": {...}}'
```

Useful for one-off schema fixes or debugging without triggering a full MERGE.

---

## 5. How This Project's Flow Works End-to-End

```
run_poc.py
    │
    ├── 1. manage.py run_suite plt_add_column.yml
    │       └── TestPLTAddColumn.test_load
    │               ├── Postgres source: k1.orders (no phone) + k1_prime.orders (+ phone)
    │               ├── Hevo pipeline loads both → Snowflake temp schemas
    │               └── writes /tmp/plt_poc_vars.json
    │
    ├── 2. reads functional/config.yml → Snowflake creds
    │       writes profiles.yml
    │
    └── 3. dbt run --vars '{k1_db, k1_schema, k1_prime_schema, k1_table}'
                └── models/plt/orders.sql
                        ├── resolve_column_schema([k1, k1_prime])
                        │       └── get_source_columns() × 2  →  {ID, NAME, MOBILE, EMAIL, SALARY, PHONE}
                        ├── evolve_final_table(this, unified_cols)
                        │       └── ALTER TABLE PLT_FINAL.orders ADD COLUMN PHONE VARCHAR  (if missing)
                        ├── generate_source_select(k1, unified_cols)
                        │       └── SELECT ID, NAME, ..., NULL::TEXT AS PHONE, 'k1' AS __hevo_source_pipeline
                        ├── UNION ALL
                        ├── generate_source_select(k1_prime, unified_cols)
                        │       └── SELECT ID, NAME, ..., PHONE, 'k1_prime' AS __hevo_source_pipeline
                        └── DBT MERGE INTO PLT_FINAL.orders ON (id, __hevo_source_pipeline)
```

---

## 6. What Comes Next (Test Cases)

Scenarios to cover per the design doc:

| Scenario | What changes in sources | What PLT must do |
|----------|------------------------|------------------|
| `ADD_FIELDS` ✅ (done) | k1_prime gains `phone` | ADD COLUMN, NULL-pad k1 rows |
| `DROP_FIELDS` | k1_prime drops `phone` | no-op on final table; old rows keep value |
| `CHANGE_FIELD_TYPE` (widen) | k1 has `salary NUMBER`, k1_prime has `salary FLOAT` | LCA → STRING; ALTER COLUMN type |
| `CHANGE_FIELD_TYPE` (narrow) | resolved type < current final type | BLOCK + alert |
| `ADD_PRIMARY_KEY` | k1_prime adds `category` as PK | merge key expands; IS NOT DISTINCT FROM |
| `DROP_PRIMARY_KEY` | k1 drops its PK | re-evaluate merge key |
| `ADD_NOT_NULL` | k1_prime adds NOT NULL constraint | downgrade to nullable in final |
