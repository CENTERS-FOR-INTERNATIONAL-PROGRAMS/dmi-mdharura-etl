{% macro table_exists(
    table_name,
    schema_name
  ) -%}
  {%- set target_relation = adapter.get_relation(
    database = this.database,
    schema = schema_name,
    identifier = table_name
  ) -%}
  {%- set table_exists = target_relation is not none -%}
  {{ return(table_exists) }}
{%- endmacro %}
