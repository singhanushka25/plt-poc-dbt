# Catalog Service — Adaptive Schema Evolution Engine Analysis

> Source: `catalog-service/catalog-engine/` and `catalog-service/catalog-service/`
> Purpose: Reference for building equivalent dbt macro logic for PLT

---

## 1. Type Hierarchy — How the Tree is Represented

The Snowflake type hierarchy is a **static parent-pointer tree** defined in `SnowflakeSchemaTypeHierarchy.java`. Each node is an `EvolvableSchemaType` with a `parentType` pointer.

### Tree Structure (Snowflake)

```
STRING (VARCHAR) ← ROOT, isSuperType=true, expandable (LengthBasedExpansionScheme)
├── VARIANT (fixed size, parent=STRING)
│     └── ARRAY (fixed size, parent=VARIANT)
├── BINARY (expandable: LengthBasedExpansionScheme, parent=STRING)
├── FLOAT (fixed size, parent=STRING)
├── TIME (expandable: PrecisionBasedExpansionScheme, parent=STRING)
├── NUMBER (expandable: PrecisionScaleExpansionScheme, parent=STRING)
│     └── BOOLEAN (fixed size, parent=NUMBER)
├── TIMESTAMP_TZ (expandable: PrecisionBasedExpansionScheme, parent=STRING)
│     └── TIMESTAMP_NTZ (expandable: PrecisionBasedExpansionScheme, parent=TIMESTAMP_TZ)
│           └── DATE (fixed size, parent=TIMESTAMP_NTZ)
```

Key observations:
- **STRING is the root** (not VARIANT). VARIANT is a child of STRING.
- **FLOAT and NUMBER are siblings** — both children of STRING. Their LCA is STRING (VARCHAR).
- **BOOLEAN is a child of NUMBER** — BOOLEAN + NUMBER resolves to NUMBER.
- **DATE → TIMESTAMP_NTZ → TIMESTAMP_TZ** — linear chain under STRING.
- **ARRAY → VARIANT → STRING** — ARRAY is a grandchild of STRING.

### Node Types

1. **`EvolvableSchemaType`** (base) — fixed-size nodes (BOOLEAN, DATE, FLOAT, VARIANT, ARRAY)
   - Fields: `logicalTypeName`, `size`, `superTypeSize`, `parentType`, `isSuperType`
   - `parentType` is the pointer to the parent in the hierarchy
   - `superTypeSize` is used when this type needs to be represented in its parent's expansion level (e.g., NUMBER(1,0) for BOOLEAN when promoting to NUMBER)

2. **`ExpandableEvolvableSchemaType<LEVEL, METADATA, SCHEME>`** (extends base) — nodes with precision/scale/length
   - Adds: `expansionScheme` — handles within-type expansion (e.g., NUMBER(5,2) → NUMBER(10,3))
   - Expandable types: STRING, BINARY, TIME, NUMBER, TIMESTAMP_TZ, TIMESTAMP_NTZ

---

## 2. LCA Algorithm — `AdaptiveSchemaEvolutionEngine.findCommonEvaluableAncestor()`

### Algorithm

```
Input: destinationType, sourceSchemaEvaluable, targetSchemaEvaluable
Output: Optional<SchemaTypeEvaluable> — the evolved (widened) type

1. Look up sourceSchemaType and targetSchemaType in the hierarchy map
2. Collect ancestors of source: walk parentType chain → LinkedHashSet (preserves insertion order = closest first)
3. Collect ancestors of target: same walk
4. For each ancestor in sourceAncestors (closest first):
     If ancestor is also in targetAncestors:
       → This is the LCA candidate
       → Call determineEvolvedSchema(ancestor, sourcePair, targetPair)
       → If result is present: return it
       → If empty (expansion exceeded max): continue to next ancestor
5. If no ancestor produced a valid result: return Optional.empty()
```

### Time Complexity

- **Ancestor collection**: O(D) where D = max depth of tree (Snowflake tree depth = 4 max: DATE→TIMESTAMP_NTZ→TIMESTAMP_TZ→STRING)
- **LCA search**: O(D²) worst case (iterate source ancestors × check in target set), but D ≤ 4, so effectively O(1)
- **Expansion level calculation**: O(1) — just math comparisons
- **Total: O(1)** for practical purposes. The tree is tiny.

### `determineEvolvedSchema()` — What Happens at the LCA

Once the LCA is found, the engine determines the expansion level:

**Case 1: LCA is a super type (STRING)**
- Both types' sizes need to be projected into the super type's expansion scheme
- If source IS the super type: use source's expansion level directly
- If source is NOT the super type: use `new SizeExpansionLevel(source.getSuperTypeSize())` — the size this type would need as a string
- Same for target
- Then: `calculateNewExpansionLevel(adaptedSource, adaptedTarget)` on the LCA's expansion scheme
- Result: STRING with `max(sourceLength, targetLength)`

**Case 2: LCA is an expandable type (same family)**
- Both source and target expansion levels are adapted to the LCA's expansion level class
- `calculateNewExpansionLevel(level1, level2)` on the LCA's expansion scheme
- Result: the LCA type with the merged expansion level

**Case 3: LCA is a fixed-size type**
- `max(source.length, target.length)` must fit within `ancestor.getSize()`
- If it fits: return the ancestor type
- If not: return empty (escalate to next ancestor)

---

## 3. Expansion Schemes — Precision/Scale/Length Resolution

### `PrecisionScaleExpansionScheme` (for NUMBER)

```java
calculateNewExpansionLevel(level1, level2):
  newScale = max(scale1, scale2)
  newIntegerPart = max(precision1 - scale1, precision2 - scale2)
  newPrecision = newIntegerPart + newScale

  // Clamp to min bounds
  if newScale < minScale: newScale = minScale; recalculate
  if newPrecision < minPrecision: newPrecision = minPrecision

  // Check max bounds
  if newPrecision > maxPrecision OR newScale > maxScale: return empty

  return PrecisionScaleExpansionLevel(newPrecision, newScale)
```

**Example**: NUMBER(10,3) vs NUMBER(15,6)
- scale: max(3,6) = 6
- intPart: max(10-3, 15-6) = max(7, 9) = 9
- precision: 9 + 6 = 15
- Result: NUMBER(15,6)

**Example**: NUMBER(10,3) vs NUMBER(8,5)
- scale: max(3,5) = 5
- intPart: max(10-3, 8-5) = max(7, 3) = 7
- precision: 7 + 5 = 12
- Result: NUMBER(12,5)

**Snowflake bounds**: maxPrecision=38, minPrecision=1, maxScale=37, minScale=0

### `LengthBasedExpansionScheme` (for STRING/VARCHAR, BINARY)

```java
calculateNewExpansionLevel(level1, level2):
  parentLength = max(length1, length2)
  if parentLength < minLength: parentLength = minLength
  if parentLength > maxLength: return empty
  return SizeExpansionLevel(parentLength)
```

**Example**: VARCHAR(100) vs VARCHAR(500) → VARCHAR(500)

**Snowflake VARCHAR bounds**: min=1, max=16777216

### `PrecisionBasedExpansionScheme` (for TIME, TIMESTAMP_NTZ, TIMESTAMP_TZ)

Same as LengthBased — just `max(precision1, precision2)` within [min, max].

**TIMESTAMP bounds**: min=0, max=9

---

## 4. Cross-Type Widening — What Happens When Types Differ

When source and target are different types, the LCA is their nearest common ancestor in the tree.

| Source | Target | LCA | Strategy | Result |
|--------|--------|-----|----------|--------|
| NUMBER(10,3) | NUMBER(15,6) | NUMBER | same-family expansion | NUMBER(15,6) |
| VARCHAR(100) | VARCHAR(500) | STRING | same-family expansion | VARCHAR(500) |
| BOOLEAN | NUMBER(10,0) | NUMBER | child→parent, adapt BOOLEAN to SizeExpansionLevel | NUMBER(max(1, 10), max(0, 0)) = NUMBER(10,0) |
| DATE | TIMESTAMP_NTZ(6) | TIMESTAMP_NTZ | child→parent, adapt DATE to precision | TIMESTAMP_NTZ(max(datePrec, 6)) = TIMESTAMP_NTZ(6) |
| TIMESTAMP_NTZ(3) | TIMESTAMP_TZ(6) | TIMESTAMP_TZ | child→parent | TIMESTAMP_TZ(max(3,6)) = TIMESTAMP_TZ(6) |
| NUMBER(10,3) | FLOAT | STRING | siblings, LCA=STRING | VARCHAR(max(numStringLen, floatStringLen)) |
| NUMBER(10,3) | VARCHAR(500) | STRING | child→root | VARCHAR(max(numStringLen, 500)) |
| FLOAT | VARCHAR(100) | STRING | child→root | VARCHAR(max(floatStringLen, 100)) |
| DATE | BOOLEAN | STRING | distant cousins, LCA=STRING | VARCHAR(max(dateStringLen, boolStringLen)) |
| VARIANT | ARRAY | VARIANT | parent→child | VARIANT |

**`superTypeSize`**: When a non-STRING type needs to be represented as STRING, `superTypeSize` is used.
- NUMBER: `SnowflakeNumberField.STRING_LENGTH` (the string length needed to represent the max NUMBER)
- FLOAT: `SnowflakeFloatField.MAX_LENGTH`
- DATE: `SnowflakeDateField.STRING_LENGTH`
- BOOLEAN: `SnowflakeBooleanField.STRING_LENGTH`
- TIMESTAMP_TZ: `SnowflakeTimeStampTZField.STRING_LENGTH`

This determines the VARCHAR length when cross-family widening occurs.

---

## 5. Diff Service — `DestinationSchemaChangeDiffService`

### Overview

The diff service compares a **mapping** (source-derived schema) against a **destination object** (current destination table schema) and produces an ordered list of `DestinationOperation`s.

### Key Concepts

- **Mapping**: The desired schema — what the source wants the destination to look like
- **DestinationObject**: The current schema — what the destination table actually is
- **FieldOpsInfo**: Per-field comparison result containing:
  - `existingFieldInDestination` — the field as it currently is (empty if new)
  - `updatedFieldAfterEvolution` — the field after adaptive schema evolution (empty if destination-only)
  - `newOrDeleted` — field doesn't exist in destination
  - `dataTypeChanged` — raw types differ between existing and evolved
  - `destinationOnlyField` — field exists in destination but not in mapping

### Algorithm

```
1. Create lookup map: destinationFieldName → Field (from DestinationObject)

2. For each field in mapping (active + replicable):
   a. Look up existingField in destination map
   b. If field is new (not in destination or DELETED):
      → FieldOpsInfo(newOrDeleted=true)
   c. If field exists:
      → Run adaptive schema evolution: mutateFieldWithAdaptiveSchemaEvolution()
        - This calls AdaptiveSchemaEvolutionEngine.findCommonEvaluableAncestor()
          with (mappingField, existingDestinationField)
        - Returns the evolved field type (LCA of mapping type and destination type)
        - If no LCA found → field is INCONSISTENT → abort diff
      → Compare raw types: existing vs evolved
      → FieldOpsInfo(dataTypeChanged=true/false)

3. Process destination-only fields:
   - Fields in destination but NOT in mapping
   → FieldOpsInfo(destinationOnlyField=true)

4. Run handler chain (Strategy Pattern) in priority order:
   Priority 1: DistributionKeyChangeHandler (Redshift only)
   Priority 2: SortKeyChangeHandler (Redshift only)
   Priority 3: PrimaryKeyChangeHandler
   Priority 4: NormalFieldChangeHandler (fallback — handles all fields)
   Priority 5: NullabilityChangeHandler

   Each handler runs per-field:
   - handleKeyTypeRelatedChange(fieldOpsInfo, ...)
     → Decides: ADD_FIELDS / ADD_NOT_NULL_FIELDS / CHANGE_DATA_TYPE / CHANGE_DATA_TYPE_WITH_NOT_NULL

   Then per-handler post-processing (across all fields):
   - postProcessingForKeyType(allFieldOpsInfos, ...)
     → ADD_PRIMARY_KEY (collects all PK fields)
     → ADD_SORT_KEY (collects all sort key fields)
     → REMOVE_NOT_NULL_CONSTRAINT (for destination-only non-nullable fields)

5. Build operations list:
   - Iterate DestinationOperationType.values() (ordered by executionOrder)
   - For each type: if fields exist in the maps, create DestinationOperation
   - Result: ordered List<DestinationOperation>
```

### Operation Decision Table

| Condition | Operation(s) |
|-----------|-------------|
| Field new (not in destination) | ADD_FIELDS (or ADD_NOT_NULL_FIELDS if PK + destination requires NOT NULL) |
| Field data type changed | CHANGE_DATA_TYPE (or CHANGE_DATA_TYPE_WITH_NOT_NULL if PK) |
| Field nullability: NOT NULL → NULL | REMOVE_NOT_NULL_CONSTRAINT |
| Field nullability: NULL → NOT NULL (PK) | ADD_NOT_NULL_CONSTRAINT |
| Destination-only field with NOT NULL | REMOVE_NOT_NULL_CONSTRAINT (in post-processing) |
| PK fields (post-processing) | ADD_PRIMARY_KEY (collects all PK fields together) |
| Distribution key change (Redshift) | DROP_AND_RECREATE_TABLE (terminal) |
| Sort key change (Redshift) | DROP_SORT_KEY + ADD_SORT_KEY |

### Execution Order (from DestinationOperationType)

```
1.  DROP_AND_RECREATE_TABLE  — Redshift only
2.  DROP_SORT_KEY            — Redshift only
3.  DROP_PRIMARY_KEY         — before field changes to avoid constraint violations
4.  REMOVE_NOT_NULL_CONSTRAINT — before type changes (type change may need nullable column)
5.  DROP_FIELDS              — remove obsolete fields
6.  RENAME_FIELDS            — rename before type change
7.  CHANGE_DATA_TYPE         — type promotion (single or multi-step)
8.  CHANGE_DATA_TYPE_WITH_NOT_NULL — same + re-add NOT NULL after
9.  ADD_FIELDS               — add new columns (nullable)
10. ADD_NOT_NULL_FIELDS      — add new columns with NOT NULL
11. ADD_NOT_NULL_CONSTRAINT  — add NOT NULL to existing columns
12. ADD_PRIMARY_KEY          — add PK constraint (after all field changes)
13. ADD_SORT_KEY             — Redshift only
```

### Key Design Patterns

1. **Strategy Pattern** for handlers — each key type (PK, sort key, dist key, normal, nullability) has its own handler
2. **Two-phase processing** — per-field decisions first, then cross-field post-processing
3. **Immutable operation list** — operations are computed, then executed separately
4. **Adaptive evolution** — uses LCA to determine the evolved type, not just the mapping type

---

## 6. Critical Insight: Evolution is Source vs Destination, Not Source vs Source

The catalog's adaptive schema evolution compares **mapping field type** vs **existing destination field type** to find the LCA. This means:

- If mapping says NUMBER(10,3) and destination has VARCHAR(500), the LCA is VARCHAR (STRING)
- The destination is NEVER narrowed — LCA always produces the wider type
- If no LCA exists (e.g., unsupported type), the field is marked INCONSISTENT

**For PLT**: This maps to our two-pass approach:
- **Pass 1** (`resolve_unified_schema`): source-vs-source LCA — what the sources collectively want
- **Pass 2** (`compute_schema_diff`): resolved-vs-destination LCA — what needs to change in the final table

Pass 2 is where narrowing is blocked: if all sources now have NUMBER(10,3) but the final table has NUMBER(20,6), the LCA of NUMBER(10,3) vs NUMBER(20,6) = NUMBER(20,6). The final table stays wider.

---

## 7. Implications for dbt Macro Design

### `get_widened_type` Implementation Strategy

The catalog's LCA algorithm is:
1. Build ancestor chains for both types (O(D), D ≤ 4)
2. Find first common ancestor (O(D))
3. Compute expansion level at the LCA (O(1) math)

In dbt Jinja, we can replicate this efficiently:

**Option A: Hardcoded lookup table**
- Pre-compute all type pair → result mappings (11 types × 11 = 121 pairs, but symmetric so 66 unique)
- O(1) lookup per call
- Expansion (precision/scale/length) still needs math at runtime

**Option B: Tree traversal in Jinja**
- Represent hierarchy as nested dicts: `{'NUMBER': {'parent': 'STRING', 'expandable': true, ...}}`
- Walk parent pointers to find LCA
- O(D) per call, D ≤ 4

**Recommendation**: Hybrid — hardcode the **base type LCA** (which family the result is in) and compute **expansion levels** (precision/scale/length) at runtime. The base type LCA is the expensive/error-prone part; expansion math is simple.

### Precision/Scale — Can dbt Handle This Natively?

**Partially.** `adapter.expand_target_column_types(from_relation, to_relation)` handles:
- VARCHAR length widening (VARCHAR(100) → VARCHAR(500)) ✓
- NUMBER precision widening within same scale ✓ (but not cross-scale)

**What it CANNOT handle:**
- NUMBER(10,3) vs NUMBER(15,6) → NUMBER(15,6) — the integer-part + max-scale math ✗
- Cross-family widening (NUMBER → VARCHAR) ✗
- TIMESTAMP precision merging ✗

So: dbt's `expand_target_column_types` covers the simple same-family cases. We need custom logic for the precision/scale math and all cross-family resolution.

### Diff Service — Simplified for PLT

The catalog's diff service is complex because it handles:
- Distribution keys (Redshift) — N/A for PLT on Snowflake
- Sort keys (Redshift) — N/A
- PK constraint management — Deferred to Phase 2
- Field mapping/translation — N/A (PLT reads schemas directly)
- Inconsistency detection — Simpler in PLT (we control the sources)

**PLT Phase 1 diff is simpler:**
```
For each column in unified_schema:
  if not in final_table: → ADD_FIELDS
  elif type wider:       → CHANGE_DATA_TYPE (single/multi step)
  elif type narrower:    → BLOCK (log, no-op)

For each column in final_table not in unified:
  → soft-drop (no DDL)

For NOT NULL:
  if final has NOT NULL but unified says nullable: → REMOVE_NOT_NULL_CONSTRAINT
  if ALL sources are NOT NULL but final is nullable: → ADD_NOT_NULL_CONSTRAINT
```

Operations sorted by: REMOVE_NOT_NULL → CHANGE_DATA_TYPE → ADD_FIELDS → ADD_NOT_NULL
