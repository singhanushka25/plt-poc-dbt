# Common Patterns in Sentinel Schema-Change Tests

How sentinel tests are structured, what APIs they use, and how to model a new PLT test case.

---

## 1. Standard Test Flow

Every schema-change test follows this skeleton:

```python
class TestSomeSchemaChange:
    def test_<scenario>(
        self,
        fixture_integration: Integration,
        fixture_local_context: LocalContext,
    ) -> None:
        fi = fixture_integration

        # ── Phase 1: Historical load ──────────────────────────────────
        latest_job_id = fi.job_monitoring.wait_for_historical_job_to_start()
        fi.job_monitoring.wait_for_job_status_to_update(
            job_id=latest_job_id, job_status="COMPLETED"
        )

        # ── Phase 2: Schema change ────────────────────────────────────
        # (varies per test — see section 2 below)

        # ── Phase 3: Incremental data ─────────────────────────────────
        fi.source.create_incremental_data(
            10, operations=[DBOperations.OPERATION_INSERT]
        )

        # ── Phase 4: Run incremental job ──────────────────────────────
        Utils.run_incremental_job(fi, "COMPLETED")

        # ── Phase 5: Assert destination state ────────────────────────
        assert_<whatever>
```

---

## 2. How Schema Changes Are Applied

### Option A — Direct SQL (ALTER TABLE)

Used for simple column adds/drops that the dialect processor doesn't abstract:

```python
fi.source.run_query(
    "ALTER TABLE orders ADD COLUMN phone VARCHAR(50)"
)
```

### Option B — SentinelColumn / SentinelTable API

Used for cross-database abstraction:

```python
from sentinel.models.column import SentinelColumn
from sentinel.models.table import SentinelTable
from sentinel.models.data_type import DataType

table = fi.source.get_dialect_processor().get_table(table_identifier)
table.columns.append(
    SentinelColumn(name="PHONE", data_type=DataType.STRING, nullable=True)
)
fi.source.get_dialect_processor().alter_table(table)
```

### Option C — `init_sql_data_files` (our PLT approach)

SQL file run on source DB **before the pipeline starts**.
Defined in the suite YAML:

```yaml
data_profile:
  init_sql_data_files:
    - sql_data_files/plt_add_column/plt_add_column.sql
```

SQL file runs relative to the `functional/` directory (where `manage.py` runs).
This is what we use to create k1 and k1_prime schemas in Postgres.

---

## 3. Schema Evolution Timing Enum

```python
class SchemaEvolutionTiming(Enum):
    BEFORE_HISTORICAL   = "before_historical"
    BEFORE_INCREMENTAL  = "before_incremental"
```

Found in: `tests/db_sources/common_scenarios/schema_changes_helper.py`

Tests are parameterised over both timings:
- `BEFORE_HISTORICAL`: schema changed before pipeline fetches the initial schema snapshot
- `BEFORE_INCREMENTAL`: schema changed after historical completes, before first incremental batch

For PLT, both timings are relevant because the two source schemas (k1 / k1_prime) may diverge before OR after the pipeline loads them.

---

## 4. SchemaChangesHelper — Operation Enum

```python
class SchemaChangeOperation(Enum):
    ADD_COLUMN_WITH_SUPPORTED_TYPE
    ADD_COLUMN_WITH_UNSUPPORTED_TYPE
    DROP_COLUMN_WITH_SUPPORTED_TYPE
    DROP_COLUMN_WITH_UNSUPPORTED_TYPE_INACTIVE_COLUMN
    RENAME_REGULAR_COLUMN
    CHANGE_COLUMN_TYPE
    DROP_COLUMN_AND_RE_ADD_SAME_TYPE
    DROP_COLUMN_AND_RE_ADD_DIFFERENT_TYPE
    TOGGLE_ACTIVE_INACTIVE
    ADD_PK
    DROP_PK
    CHANGE_PK_COLUMN
    CHANGE_PK_POSITION
    COMPOSITE_KEY_WITH_NULL_COLUMN
    RENAME_PK_COLUMN
    DROP_PK_COLUMN
    # ... 40+ more
```

These map directly to our PLT schema-evolution plan. Each operation can be tested in our PLT suite by modelling the SQL data files (Postgres source schemas) to reflect the before/after state.

---

## 5. Catalog Mapping Discovery Pattern

How tests discover the Snowflake destination schema dynamically (our PLT test uses this):

```python
object_to_mapping = fi.catalog_client.get_catalog_mappings(
    fi.id,                          # integration (pipeline) ID
    fi.source.id,                   # source ID
    fi.destination.connection_type  # destination type (SNOWFLAKE)
)

for mapping in object_to_mapping.values():
    dst = mapping.destination_namespace
    db     = dst.k2   # Snowflake database
    schema = dst.k1   # Snowflake schema
    table  = dst.k0   # table name
```

**For PLT**: we distinguish k1 vs k1_prime by checking `"prime" in dst.k1.lower()`.

---

## 6. Suite YAML Structure

### Minimal schema-evolution suite:

```yaml
name: PLT > <Scenario Name>
owner: platform

tests:
  - name: <Human-readable test name>
    file: post_load_transformations/test_<scenario>.py
    config:
      skip_source_destroy: true        # keep Postgres source alive after test
      skip_destination_destroy: true   # keep Snowflake destination alive
      skip_pipeline_destroy: true      # keep pipeline alive (schemas persist)
      source:
        config:
          template: POSTGRES
        data_profile:
          init_sql_data_files:
            - sql_data_files/<scenario>/<scenario>.sql
      destination:
        config:
          template: SNOWFLAKE
      integration:
        config:
          load_mode: MERGE
          schema_evolution: ALLOW_ALL
        build_config:
          stage: INITIALIZE
```

### Key YAML flags

| Flag | Effect |
|------|--------|
| `skip_source_destroy: true` | Postgres source not torn down after test |
| `skip_destination_destroy: true` | Snowflake database not dropped after test |
| `skip_pipeline_destroy: true` | Hevo pipeline not deleted after test |
| `stage: INITIALIZE` | Build only the integration, don't wait for full sync to complete |
| `stage: WAIT_FOR_INITIAL_SYNC` | Build and wait for historical load to complete before test starts |
| `schema_evolution: ALLOW_ALL` | Pipeline auto-applies all schema changes |
| `load_mode: MERGE` | Use MERGE (dedup by PK) not APPEND |

---

## 7. SQL Data File Patterns

SQL files in `functional/sql_data_files/<test>/` run on the Postgres source.
They create the "before" state; the test Python code creates the "after" state by ALTERing.

**For PLT** we use the SQL file to create BOTH states (two schemas simultaneously), because PLT isn't about one table evolving — it's about merging two tables that have different schemas.

### Our SQL pattern (k1 / k1_prime):

```sql
-- Base schema (no phone)
CREATE SCHEMA IF NOT EXISTS k1;
DROP TABLE IF EXISTS k1.orders;
CREATE TABLE k1.orders (
    id     BIGSERIAL PRIMARY KEY,
    name   VARCHAR(255),
    mobile VARCHAR(50),
    email  VARCHAR(255),
    salary NUMERIC(15,2)
);
INSERT INTO k1.orders (name, mobile, email, salary)
VALUES ('Alice','555-1001','alice@example.com',75000), ...;

-- Extended schema (with phone)
CREATE SCHEMA IF NOT EXISTS k1_prime;
DROP TABLE IF EXISTS k1_prime.orders;
CREATE TABLE k1_prime.orders (
    id     BIGSERIAL PRIMARY KEY,
    name   VARCHAR(255),
    mobile VARCHAR(50),
    email  VARCHAR(255),
    salary NUMERIC(15,2),
    phone  VARCHAR(50)        -- extra column
);
INSERT INTO k1_prime.orders (name, mobile, email, salary, phone)
VALUES ('Charlie','555-2001','charlie@example.com',90000,'555-1234'), ...;
```

---

## 8. Incremental Data Creation

```python
# Insert 10 rows
fi.source.create_incremental_data(10, operations=[DBOperations.OPERATION_INSERT])

# Mixed DML
fi.source.create_incremental_data(
    10,
    operations=[DBOperations.OPERATION_INSERT, DBOperations.OPERATION_UPDATE]
)
```

---

## 9. Destination Query Pattern

```python
# Row count
count = fi.run_query_on_destination(
    f'SELECT COUNT(*) FROM "{db}"."{schema}"."{table}"'
).scalar()

# Column check
result = fi.run_query_on_destination(
    f'SELECT PHONE FROM "{db}"."{schema}"."{table}" LIMIT 1'
)

# Check column exists
cols = fi.run_query_on_destination(
    f"""SELECT COLUMN_NAME FROM "{db}".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"""
).fetchall()
```

---

## 10. Key Files for Reference

| File | What it contains |
|------|-----------------|
| `tests/db_sources/common_scenarios/schema_changes_helper.py` | `SchemaChangeOperation` enum (50+ ops), `SchemaEvolutionTiming`, helpers |
| `tests/db_sources/common_scenarios/add_field.py` | Reference add-column test |
| `tests/db_sources/common_scenarios/drop_field.py` | Reference drop-column test |
| `tests/db_sources/common_scenarios/datatype_narrowing.py` | Narrowing reference |
| `tests/schema_and_destinations/schema_evolution/allow_all/append_mode/schema_evolution_runner.py` | Full ALLOW_ALL test runner (most comprehensive) |
| `tests/platform/migration/narrowing/narrowing_evolution_matrix.py` | Type evolution LCA matrix |
| `tests/post_load_transformations/test_plt_add_column.py` | Our PLT test (reference) |

---

## 11. How to Model a New PLT Scenario

1. **SQL data file** (`sql_data_files/<scenario>/`):
   - Create k1 schema with base table
   - Create k1_prime schema with the diverging schema (the thing we're testing)
   - Insert rows into both

2. **Suite YAML** (`suites/post_load_transformations/<scenario>.yml`):
   - Use the standard structure above
   - `init_sql_data_files` points to your SQL file
   - `skip_*_destroy: true` to keep resources alive for DBT run

3. **Test Python** (`tests/post_load_transformations/test_<scenario>.py`):
   - Wait for historical load
   - Discover k1 / k1_prime schemas via `get_catalog_mappings`
   - Assert expected row counts
   - Write `/tmp/plt_poc_vars.json` for `run_poc.py`

4. **DBT model** (`models/plt/<table>.sql`):
   - `resolve_column_schema` → compute union
   - `evolve_final_table` → apply schema changes to destination
   - `generate_source_select` × N → NULL-pad missing columns
   - DBT MERGE

5. **DBT macros** — extend `evolve_final_table` to handle the new scenario's change type

---

## 12. PLT Scenario → SQL File Mapping (Planned)

| PLT scenario | k1 schema | k1_prime schema | What changes |
|--------------|-----------|----------------|--------------|
| ADD_FIELDS ✅ | id,name,mobile,email,salary | +phone | k1_prime adds phone |
| DROP_FIELDS | id,name,mobile,email,salary,phone | no phone | k1_prime drops phone |
| CHANGE_FIELD_TYPE (widen) | salary NUMBER | salary FLOAT | LCA → STRING |
| CHANGE_FIELD_TYPE (narrow) | salary FLOAT | salary INT | PLT must BLOCK |
| ADD_PRIMARY_KEY | no category col | +category as PK | merge key expands |
| DROP_PRIMARY_KEY | id as PK | no PK | re-evaluate merge key |
| ADD_NOT_NULL | nullable mobile | NOT NULL mobile | PLT ignores, stays nullable |
