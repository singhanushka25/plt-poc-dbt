{# Compile-time verification of get_widened_type LCA logic.
   Each row represents a bug — if get_widened_type returns the correct value,
   the WHERE clause filters the row and the test passes (no rows = success). #}

SELECT 'number_precision' AS test_case,
       '{{ get_widened_type("NUMBER(10,8)", "NUMBER(15,3)") }}' AS actual,
       'NUMBER(20,8)' AS expected
WHERE '{{ get_widened_type("NUMBER(10,8)", "NUMBER(15,3)") }}' != 'NUMBER(20,8)'

UNION ALL
SELECT 'boolean_float',
       '{{ get_widened_type("BOOLEAN", "FLOAT") }}',
       'VARCHAR(16777216)'
WHERE '{{ get_widened_type("BOOLEAN", "FLOAT") }}' != 'VARCHAR(16777216)'

UNION ALL
SELECT 'ntz_ltz_sibling',
       '{{ get_widened_type("TIMESTAMP_NTZ", "TIMESTAMP_LTZ") }}',
       'TIMESTAMP_TZ'
WHERE '{{ get_widened_type("TIMESTAMP_NTZ", "TIMESTAMP_LTZ") }}' != 'TIMESTAMP_TZ'

UNION ALL
SELECT 'ltz_tz_child',
       '{{ get_widened_type("TIMESTAMP_LTZ", "TIMESTAMP_TZ") }}',
       'TIMESTAMP_TZ'
WHERE '{{ get_widened_type("TIMESTAMP_LTZ", "TIMESTAMP_TZ") }}' != 'TIMESTAMP_TZ'

UNION ALL
SELECT 'date_ltz',
       '{{ get_widened_type("DATE", "TIMESTAMP_LTZ") }}',
       'TIMESTAMP_LTZ'
WHERE '{{ get_widened_type("DATE", "TIMESTAMP_LTZ") }}' != 'TIMESTAMP_LTZ'

UNION ALL
SELECT 'cross_family_fallback',
       '{{ get_widened_type("NUMBER", "FLOAT") }}',
       'VARCHAR(16777216)'
WHERE '{{ get_widened_type("NUMBER", "FLOAT") }}' != 'VARCHAR(16777216)'

UNION ALL
SELECT 'number_same_precision',
       '{{ get_widened_type("NUMBER(10,2)", "NUMBER(10,2)") }}',
       'NUMBER(10,2)'
WHERE '{{ get_widened_type("NUMBER(10,2)", "NUMBER(10,2)") }}' != 'NUMBER(10,2)'

UNION ALL
SELECT 'varchar_widening',
       '{{ get_widened_type("VARCHAR(100)", "VARCHAR(500)") }}',
       'VARCHAR(500)'
WHERE '{{ get_widened_type("VARCHAR(100)", "VARCHAR(500)") }}' != 'VARCHAR(500)'

UNION ALL
SELECT 'boolean_number',
       '{{ get_widened_type("BOOLEAN", "NUMBER(10,0)") }}',
       'NUMBER(10,0)'
WHERE '{{ get_widened_type("BOOLEAN", "NUMBER(10,0)") }}' != 'NUMBER(10,0)'
