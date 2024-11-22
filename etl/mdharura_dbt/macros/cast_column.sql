{% macro cast_column(
    column_name,
    column_type
  ) %}
  CAST({{ column_name }} AS {{ column_type }})
{% endmacro %}
