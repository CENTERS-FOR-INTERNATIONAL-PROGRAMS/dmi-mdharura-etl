{% macro cast_column(
    column_name,
    column_type
  ) -%}
    {{ column_name }} :: {{ column_type }}
{%- endmacro %}
