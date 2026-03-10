# Loader Schema Evolution & Type Casting — Reference Doc

> Source: `/Users/anushkasingh/Desktop/hevo-2-repos/loader-base`
> Key files: `base-loader-snowflake/` module (SnowflakeClientV2, SnowflakeDDLUtils, SnowflakeDMLUtils, SnowflakeFieldPromotionUtils)
> Purpose: Reference for building equivalent DBT macro logic for PLT

---

## 1. Overall Schema Evolution Sequence

When a schema change is detected (new column, type change), the loader executes these steps in order:

```
1. createTable()          — initial table creation with PKs (PK is SET HERE ONLY)
2. addNewFields()         — ALTER TABLE ADD COLUMN for new columns
3. promoteFields()        — ALTER TABLE MODIFY COLUMN for type changes (single or multi-step)
4. dropNotNullConstraints() — DROP NOT NULL from PK columns if needed
5. MERGE / INSERT         — data load with CASTs applied inline in the query
```

**Snowflake-specific constraint:** DDL runs OUTSIDE transactions. Snowflake does not support DDL inside transactions — all DDL is auto-committed immediately. The loader comment at `SnowflakeClientV2.java:95` states:
> "Snowflake Transaction doesn't support DDL changes, so all the DDL changes are committed immediately."

---

## 2. PK Handling

**PK is immutable after table creation.** The loader sets the PRIMARY KEY only at `CREATE TABLE` time (`SnowflakeDDLUtils.java`, table creation block):

```java
sb.append(String.join(", ", tableFields));
if (!primaryKeys.isEmpty()) {
  sb.append(", PRIMARY KEY (");
  sb.append(
      primaryKeys.stream()
          .map(SnowflakeDDLUtils::safeQuoteIdentifier)
          .collect(Collectors.joining(", ")));
  sb.append(")");
}
```

**There is no `ALTER TABLE ADD PRIMARY KEY` or `ALTER TABLE DROP PRIMARY KEY` in the loader.** PK changes in the source are handled differently:

- When a PK column is promoted (PK column type changes), the loader first **drops the NOT NULL constraint** from that column before promoting its type.
- The actual PRIMARY KEY constraint structure is never modified post-creation.

### Drop NOT NULL on PK columns

`SnowflakeDDLUtils.java:612-637` — generates queries to drop NOT NULL from PK columns:

```java
public static List<String> getDropNotNullConstraintQueries(
    Namespace tableName, List<String> columns) {
  String quotedTableName = getQuotedFQDN(tableName);
  return columns.stream()
      .map(pk -> DROP_NOT_NULL_CONSTRAINT_QUERY.formatted(quotedTableName, safeQuoteIdentifier(pk)))
      .toList();
}
```

SQL template (`SnowflakeConstants.java:42-43`):
```sql
ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL
```

`SnowflakeClientV2.java:428-448` executes this:
```java
@Override
public void dropNotNullConstraints(AlterConstraintsContext context) throws LoaderFailureException {
  this.runWithStatement(Stage.DROP_NOT_NULL, statement -> {
    String tableName = getQuotedFQDN(context.namespace());
    for (DestinationField field : context.fieldsChanged()) {
      String fieldName = field.destinationFieldName();
      String quotedFieldName = SnowflakeDDLUtils.safeQuoteIdentifier(fieldName);
      String dropNotNullQuery =
          String.format(DROP_NOT_NULL_CONSTRAINT_QUERY, tableName, quotedFieldName);
      statement.execute(Stage.DROP_NOT_NULL.name(), dropNotNullQuery);
    }
  });
}
```

---

## 3. Deduplication Logic

The loader deduplicates the staging table BEFORE the final MERGE. This is important for PLT: **by the time PLT runs, each source's staging table is already deduped per its own PK.**

**Dedup query** (`SnowflakeDMLUtils.java:748-799`, `SnowflakeConstants.java:108`):

```sql
DELETE FROM {stage_table}
WHERE ({pk_columns}, {commit_id_column}) NOT IN (
  SELECT {pk_columns}, max({commit_id_column})
  FROM {stage_table}
  GROUP BY {pk_columns}
)
```

**Strategy:** For each unique PK value, keep only the row with the highest `_hevo_commit_id`. All older versions are deleted.

**NULL PK behavior:** Standard SQL `GROUP BY` semantics — NULLs are grouped together. A NULL PK row will be treated as one group, and only the latest-commit-id NULL-PK row survives. This is undefined behavior for practical purposes — NULL PKs should not exist in production data.

**Non-unique PKs in batch:** Resolved cleanly — only latest commit wins. No errors thrown.

---

## 4. Type Promotion — Single-Step vs Multi-Step

The loader supports two promotion strategies (`SnowflakeFieldPromotionUtils.java`):

### Single-Step Promotion
Direct `ALTER TABLE ... ALTER COLUMN TYPE`:

```sql
ALTER TABLE {table} ALTER ({col} TYPE {new_type}, {col2} TYPE {new_type2}, ...);
```

Used when the type change is directly supported in Snowflake (e.g., NUMBER(5) → NUMBER(10), VARCHAR(50) → VARCHAR(200)).

### Multi-Step Promotion
Used for complex type changes that Snowflake doesn't support in-place (e.g., NUMBER → VARCHAR). Steps:

```sql
-- Step 1: Add temporary column with new type
ALTER TABLE {table} ADD COLUMN {temp_col_name} {new_type};

-- Step 2: Copy data with CAST from old column to new
UPDATE {table} SET {temp_col_name} = {conversion_expression}({old_col});

-- Step 3: Drop old column
ALTER TABLE {table} DROP COLUMN {old_col};

-- Step 4: Rename temp column to original name
ALTER TABLE {table} RENAME COLUMN {temp_col_name} TO {old_col};
```

**`SnowflakeFieldPromotionUtils.java:172-241`** (key logic):
```java
switch (details.strategy()) {
  case SINGLE_STEP:
    singleStepQuery.append(String.format("%s TYPE %s, ", fromName, to.destinationDataType()));
    hasSingleStep = true;
    break;
  case MULTI_STEP, MULTI_STEP_WITH_FORMAT:
    String newFieldName = SnowflakeDDLUtils.safeQuoteIdentifier(
        String.format(TEMP_PROMOTION_COL_TEMPLATE, from.destinationFieldName()));
    // ... builds the 4-step query list ...
    List<String> multiStepQueries = List.of(
        String.format(ADD_COLUMN_FP_QUERY, tableName, newFieldName, to.destinationDataType()),
        String.format(UPDATE_QUERY, tableName, newFieldName, conversion),
        String.format(DROP_COLUMN_QUERY, tableName, fromName),
        String.format(RENAME_COLUMN_QUERY, tableName, newFieldName, fromName));
    groupedQueries.add(multiStepQueries);
    break;
}
```

---

## 5. CAST Expression Generation in MERGE Queries

When types differ between the staging table and the target table, the loader generates explicit CAST expressions in the MERGE statement.

**`SnowflakeDMLUtils.java:460-515` — `generateCastedStageField()`:**

```java
public static String generateCastedStageField(ColumnHolder columnHolder) {
  DestinationField targetField = columnHolder.targetField();
  SnowflakeType targetFieldType = SnowflakeType.fromLogicalType(targetField.logicalType());
  return switch (columnHolder.sourceValue().type()) {
    case COLUMN -> {
      DestinationField stageTableField = columnValue.stageTableField();
      SnowflakeType stageFieldType = SnowflakeType.fromLogicalType(stageTableField.logicalType());
      if (targetFieldType.equals(stageFieldType)) {
        // No cast needed — types match
        yield String.format("S.%s", safeQuoteIdentifier(stageTableField.destinationFieldName()));
      } else {
        // Get the CAST expression string from the promotion utils
        Optional<String> outputWithCast =
            SnowflakeFieldPromotionUtils.getDataConversionString(
                (SnowflakeField) stageTableField, (SnowflakeField) targetField);
        if (outputWithCast.isPresent()) {
          yield outputWithCast.get()
              .formatted("S." + safeQuoteIdentifier(stageTableField.destinationFieldName()));
        } else {
          // Unsupported cast → throws HevoFailureException (QUERY_FAILURE)
          throw new HevoFailureException(FailureType.QUERY_FAILURE,
              "Unsupported casting ... cannot cast from type %s to %s"
                  .formatted(stageFieldType, targetFieldType));
        }
      }
    }
  };
}
```

**Result — generated MERGE looks like:**
```sql
MERGE INTO TARGET_TABLE T
USING STAGE_TABLE S
ON T.id = CAST(S.id AS VARCHAR)          -- if id was promoted from NUMBER to VARCHAR
WHEN MATCHED THEN UPDATE SET
  T.col1 = CAST(S.col1 AS VARCHAR),
  T.col2 = S.col2                        -- no cast needed
WHEN NOT MATCHED THEN INSERT (id, col1, col2)
VALUES (CAST(S.id AS VARCHAR), CAST(S.col1 AS VARCHAR), S.col2)
```

**Unsupported cast:** If `getDataConversionString()` returns empty (no known conversion path), the loader throws a `QUERY_FAILURE` exception. The load is blocked.

---

## 6. NOT NULL and UNIQUE Constraint Handling

### NOT NULL
**Only applied to internal Hevo system fields.** User-data columns are NEVER given `NOT NULL` constraints in the destination.

From `SnowflakeDDLUtils.java:192-220`:
```java
private static String columnSQLDefFromSchema(...) {
  // NOT NULL and DEFAULT only for internal fields
  if (!SchemaUtils.isInternalField(field)) {
    return sb.toString();  // returns without NOT NULL
  }
  if (!isNull && defaultVal.isPresent()) {
    sb.append(" NOT NULL");
  }
  ...
}
```

**Implication for PLT:** User columns in the final shared table will never have NOT NULL constraints. A column that's PK in source (which Snowflake enforces as NOT NULL) gets its NOT NULL dropped during field promotion. The final table is fully nullable for user data.

### UNIQUE
**Not tracked or enforced anywhere in the loader.** No UNIQUE constraint code exists. Uniqueness is managed purely via the MERGE key logic (dedup + MERGE ON pk).

---

## 7. Schema Diff — How the Loader Decides What DDL to Run

The loader does **not have a dedicated SchemaDiff class**. Schema diff is computed externally by the loader orchestration layer and the results are passed into the loader operations. The loader executes:

| Operation | Method | Trigger |
|-----------|--------|---------|
| New table | `createTable(CreateTableContext)` | Table doesn't exist |
| New columns | `addNewFields(AddFieldsContext)` | Column in source not in destination |
| Type change | `promoteFields(PromoteFieldContext)` | Source type != destination type and widening is possible |
| Drop NOT NULL | `dropNotNullConstraints(AlterConstraintsContext)` | PK column type being promoted |

`SnowflakeClientV2.java` DDL operation for adding fields:
```java
public void addNewFields(AddFieldsContext context) throws LoaderFailureException {
  this.runStatementWithRetry(
      ADD_FIELDS,
      statement -> statement.execute(ADD_FIELDS.name(), SnowflakeDDLUtils.addFieldsQuery(context)));
}
```

`SnowflakeClientV2.java` for promoting fields:
```java
public void promoteFields(PromoteFieldContext context) throws LoaderFailureException {
  this.runWithStatement(PROMOTE_FIELDS, (hevoStatement -> {
    for (List<String> queries : SnowflakeDDLUtils.promoteFieldsQueries(context)) {
      for (String query : queries) hevoStatement.execute(PROMOTE_FIELDS.name(), query);
    }
  }));
}
```

---

## 8. Key Takeaways for DBT Macro Design

| Loader Behavior | DBT Macro Equivalent |
|-----------------|---------------------|
| `addNewFields()` → `ALTER TABLE ADD COLUMN` | Pre-hook macro: `run_query(alter table add column ...)` |
| `promoteFields()` single-step → `ALTER MODIFY COLUMN type` | Pre-hook macro: single-step ALTER |
| `promoteFields()` multi-step → add temp → UPDATE CAST → DROP → RENAME | Pre-hook macro: multi-step sequence |
| `dropNotNullConstraints()` → `ALTER COLUMN DROP NOT NULL` | Pre-hook macro if PKs are being altered (rare in PLT) |
| `generateCastedStageField()` → `CAST(S.col AS type)` in MERGE | In the UNION SELECT: `CAST(col AS resolved_type) AS col` |
| Unsupported cast → `QUERY_FAILURE` exception | Macro raises error: `{{ exceptions.raise_compiler_error(...) }}` |
| NO PK constraint changes post-creation | PLT merge key is static; PK changes = breaking event |
| NO UNIQUE constraints | No UNIQUE constraint on final table; uniqueness via MERGE key |
| NOT NULL only on internal fields | Final table user columns are all nullable |
| Dedup before MERGE: `DELETE ... WHERE NOT IN (SELECT pk, max(commit_id) GROUP BY pk)` | Staging is pre-deduped by loader before PLT runs. PLT does not re-dedup. |

---

## 9. Type Promotion Strategy — What's Single-Step vs Multi-Step on Snowflake

Based on the loader's `SnowflakeFieldPromotionUtils`:

| From → To | Strategy | Notes |
|-----------|----------|-------|
| NUMBER(x) → NUMBER(y) where y > x | Single-step | Direct MODIFY COLUMN |
| VARCHAR(x) → VARCHAR(y) where y > x | Single-step | Direct MODIFY COLUMN |
| NUMBER → VARCHAR/TEXT | **Multi-step** | No in-place cast; requires temp column |
| BOOLEAN → VARCHAR | **Multi-step** | Requires conversion expression |
| DATE → TIMESTAMP | **Multi-step** | Requires format-aware conversion |
| Any type → VARIANT | Single-step (Snowflake allows VARIANT as super-type) | Direct MODIFY |
| VARCHAR → NUMBER | **Blocked** (narrowing) | Would require data validation |
| TIMESTAMP → DATE | **Blocked** (narrowing) | Data loss |

**Implication for DBT macro:** The pre-hook macro must mirror this table. For multi-step promotions, the macro must execute all 4 steps in order. If any step fails, the whole PLT run should fail (not partial).

---

## 10. File Reference

| File | Location | Purpose |
|------|----------|---------|
| `SnowflakeClientV2.java` | `base-loader-snowflake/src/main/java/io/hevo/loader/basesnowflake/` | Main Snowflake destination client — all DDL + DML operations |
| `SnowflakeDDLUtils.java` | same | DDL query generators (CREATE, ALTER, DROP NOT NULL) |
| `SnowflakeDMLUtils.java` | same | DML query generators (MERGE, dedup, CAST generation) |
| `SnowflakeFieldPromotionUtils.java` | same | Type promotion strategy + CAST expression strings |
| `SnowflakeConstants.java` | same | SQL templates (DROP_NOT_NULL_CONSTRAINT_QUERY, DEDUP_STAGE_TABLE_QUERIES, etc.) |
| `SnowflakeType.java` | same | Snowflake type enum + fromLogicalType() mapping |
