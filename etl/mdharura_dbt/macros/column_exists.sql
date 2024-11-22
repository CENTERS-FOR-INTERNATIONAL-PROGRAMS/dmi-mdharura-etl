{% macro column_exists(
    column_name,
    columns,
    cast_as,
    default_value
  ) %}
  {% if column_name in columns -%}
    {{ cast_column( column_name, cast_as ) }}
  {% else %}
    {{ default_value }}
  {% endif %}
{% endmacro %}
