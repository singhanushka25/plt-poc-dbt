{% macro plt_md5_hash(column_name) %}
{#
    Applies MD5 hashing to a column.
    Uses Redshift/Snowflake native MD5() which returns a 32-char hex string.
    CAST ensures compatibility regardless of input column type.
#}
  MD5(CAST({{ column_name }} AS VARCHAR))
{% endmacro %}
