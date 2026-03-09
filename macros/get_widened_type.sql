{% macro get_widened_type(type_a, type_b) %}
  {#
    Returns the wider of two Snowflake type strings.
    Inputs:  raw dtype strings, e.g. "NUMBER(10,3)", "VARCHAR(100)", "FLOAT"
    Output:  wider type string, or VARCHAR(256) on cross-family conflict
  #}

  {# Normalise: uppercase, strip spaces #}
  {% set ta = type_a | upper | replace(' ', '') %}
  {% set tb = type_b | upper | replace(' ', '') %}

  {# ── Extract base type and params ──────────────────────────────── #}
  {% if '(' in ta %}
    {% set ta_base   = ta.split('(')[0] %}
    {% set ta_params = ta.split('(')[1].split(')')[0] %}
  {% else %}
    {% set ta_base   = ta %}
    {% set ta_params = '' %}
  {% endif %}

  {% if '(' in tb %}
    {% set tb_base   = tb.split('(')[0] %}
    {% set tb_params = tb.split('(')[1].split(')')[0] %}
  {% else %}
    {% set tb_base   = tb %}
    {% set tb_params = '' %}
  {% endif %}

  {# ── Identical types ────────────────────────────────────────────── #}
  {% if ta == tb %}
    {{ return(type_a) }}
  {% endif %}

  {# ── Same base type, different params ──────────────────────────── #}
  {% if ta_base == tb_base %}

    {% if ta_base in ('NUMBER', 'NUMERIC', 'DECIMAL') %}
      {% if ta_params and tb_params %}
        {% set ta_p = ta_params.split(',')[0] | int %}
        {% set ta_s = ta_params.split(',')[1] | int if ',' in ta_params else 0 %}
        {% set tb_p = tb_params.split(',')[0] | int %}
        {% set tb_s = tb_params.split(',')[1] | int if ',' in tb_params else 0 %}
        {% set p = [ta_p, tb_p] | max %}
        {% set s = [ta_s, tb_s] | max %}
        {{ return('NUMBER(' ~ p ~ ',' ~ s ~ ')') }}
      {% else %}
        {{ return('NUMBER(38,18)') }}
      {% endif %}

    {% elif ta_base in ('VARCHAR', 'TEXT', 'STRING', 'CHARACTERVARYING') %}
      {% if ta_params and tb_params %}
        {% set size = [ta_params | int, tb_params | int] | max %}
        {{ return('VARCHAR(' ~ size ~ ')') }}
      {% else %}
        {{ return('VARCHAR(16777216)') }}
      {% endif %}

    {% elif ta_base in ('TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIMESTAMP') %}
      {% if ta_params and tb_params %}
        {% set p = [ta_params | int, tb_params | int] | max %}
        {{ return(ta_base ~ '(' ~ p ~ ')') }}
      {% else %}
        {{ return(ta_base ~ '(9)') }}
      {% endif %}

    {% elif ta_base == 'TIME' %}
      {% if ta_params and tb_params %}
        {% set p = [ta_params | int, tb_params | int] | max %}
        {{ return('TIME(' ~ p ~ ')') }}
      {% else %}
        {{ return('TIME(9)') }}
      {% endif %}

    {% else %}
      {# FLOAT, BOOLEAN, DATE — same base, no params to compare #}
      {{ return(type_a) }}
    {% endif %}

  {% endif %}

  {# ── Cross-type: safe widening hierarchy (POC subset) ──────────── #}

  {# DATE → any TIMESTAMP #}
  {% if ta_base == 'DATE' and tb_base in ('TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ','TIMESTAMP') %}
    {{ return(type_b) }}
  {% endif %}
  {% if tb_base == 'DATE' and ta_base in ('TIMESTAMP_NTZ','TIMESTAMP_LTZ','TIMESTAMP_TZ','TIMESTAMP') %}
    {{ return(type_a) }}
  {% endif %}

  {# TIMESTAMP_NTZ / TIMESTAMP → TIMESTAMP_TZ #}
  {% if ta_base in ('TIMESTAMP_NTZ','TIMESTAMP') and tb_base == 'TIMESTAMP_TZ' %}
    {{ return(type_b) }}
  {% endif %}
  {% if tb_base in ('TIMESTAMP_NTZ','TIMESTAMP') and ta_base == 'TIMESTAMP_TZ' %}
    {{ return(type_a) }}
  {% endif %}

  {# BOOLEAN → numeric types #}
  {% if ta_base == 'BOOLEAN' and tb_base in ('NUMBER','NUMERIC','DECIMAL','INT','INTEGER','BIGINT','SMALLINT','FLOAT','DOUBLEPRECISION','REAL') %}
    {{ return(type_b) }}
  {% endif %}
  {% if tb_base == 'BOOLEAN' and ta_base in ('NUMBER','NUMERIC','DECIMAL','INT','INTEGER','BIGINT','SMALLINT','FLOAT','DOUBLEPRECISION','REAL') %}
    {{ return(type_a) }}
  {% endif %}

  {# VARCHAR absorbs any non-VARCHAR #}
  {% if ta_base in ('VARCHAR','TEXT','STRING','CHARACTERVARYING') %}
    {{ return(type_a) }}
  {% endif %}
  {% if tb_base in ('VARCHAR','TEXT','STRING','CHARACTERVARYING') %}
    {{ return(type_b) }}
  {% endif %}

  {# ── Cross-family conflict → safe fallback ─────────────────────── #}
  {{ return('VARCHAR(256)') }}

{% endmacro %}
