{%- set ebs_categories = ["cebs", "hebs", "vebs", "lebs"] -%}
{%- set task_nested_fields = ["units", "suggestions"] -%}
{%- set investigation_form_fields = [
    "event_categories",
    "response_activities",
    "systems_affected_by_event",
] -%}

{%- set response_form_fields = [
    "event_statuses",
    "response_activities",
    "recommendations",
] -%}

{%- set cte_definitions = [] -%}
{# First collect valid CTEs #}
{%- set ebs_forms = ["investigation", "response", "escalation", "summary"] -%}
{%- for category in ebs_categories -%}

    {# Map investigation form CTEs #}
    {%- for ifield in investigation_form_fields %}
        {%- set table_name = (
            "tasks__" ~ category ~ "__investigation_form__" ~ ifield
        ) -%}
        {%- if table_exists(table_name, "central_raw_mdharura") %}
            {% set cte_sql %}
    {{ table_name }} AS (
      SELECT
        _dlt_parent_id,
        string_agg(
          VALUE,
          ', '
          ORDER BY
            _dlt_list_idx
        ) AS results
      FROM
        {{ source(
          'central_raw_mdharura',
          table_name
        ) }}
      GROUP BY
        _dlt_parent_id
    ) {% endset %}
            {% do cte_definitions.append(cte_sql) %}
        {% endif %}
    {%- endfor %}

    {# Map tasks nested fields CTEs #}
    {%- for ifield in response_form_fields %}
        {%- set table_name = "tasks__" ~ category ~ "__response_form__" ~ ifield -%}
        {%- if table_exists(table_name, "central_raw_mdharura") %}
            {% set cte_sql %}
    {{ table_name }} AS (
      SELECT
        _dlt_parent_id,
        string_agg(
          VALUE,
          ', '
          ORDER BY
            _dlt_list_idx
        ) AS results
      FROM
        {{ source(
          'central_raw_mdharura',
          table_name
        ) }}
      GROUP BY
        _dlt_parent_id
    ) {% endset %}
            {% do cte_definitions.append(cte_sql) %}
        {% endif %}
    {%- endfor %}

{%- endfor -%}

{# Map response form CTEs #}
{%- for ifield in task_nested_fields %}
    {%- set table_name = "tasks__" ~ ifield -%}
    {%- if table_exists(table_name, "central_raw_mdharura") %}
        {% set cte_sql %}
    {{ table_name }} AS (
      SELECT
        _dlt_parent_id,
        string_agg(
          VALUE,
          ', '
          ORDER BY
            _dlt_list_idx
        ) AS results
      FROM
        {{ source(
          'central_raw_mdharura',
          table_name
        ) }}
      GROUP BY
        _dlt_parent_id
    ) {% endset %}
        {% do cte_definitions.append(cte_sql) %}
    {% endif %}
{%- endfor -%}

{# Second Render WITH clause if any CTEs exist #}
{% if cte_definitions %} with {{ cte_definitions | join(",\n") }} {% endif %}
select
    tasks.*
    {%- for category in ebs_categories -%}
        {# Select investigation form nested fields as text #}
        {%- for ifield in investigation_form_fields %}
            {%- set cte_name = category ~ "__investigation_form__" ~ ifield ~ "_cte" -%}

            {%- set table_name = (
                "tasks__" ~ category ~ "__investigation_form__" ~ ifield
            ) -%}

            {%- set column_name = category ~ "__investigation_form__" ~ ifield -%}
            {% if table_exists(table_name, "central_raw_mdharura") %}
                , {{ cte_name }}.results as {{ column_name }}
            {% endif %}
        {% endfor %}

        {# Select response form nested fields as text #}
        {%- for ifield in response_form_fields %}
            {%- set cte_name = category ~ "__response_form__" ~ ifield ~ "_cte" -%}

            {%- set table_name = "tasks__" ~ category ~ "__response_form__" ~ ifield -%}

            {%- set column_name = category ~ "__response_form__" ~ ifield -%}
            {% if table_exists(table_name, "central_raw_mdharura") %}
                , {{ cte_name }}.results as {{ column_name }}
            {% endif %}
        {%- endfor -%}
    {%- endfor -%}

    {%- for ifield in task_nested_fields %}
        {%- set cte_name = "tasks" ~ ifield ~ "_cte" -%}
        {%- set table_name = "tasks__" ~ ifield -%}

        {% if table_exists(table_name, "central_raw_mdharura") %}
            , {{ cte_name }}.results as {{ ifield }}
        {% endif %}
    {% endfor %}
from {{ source("central_raw_mdharura", "tasks") }} as tasks
{% for category in ebs_categories %}

    {# Handle nested field for investigation form #}
    {%- for ifield in investigation_form_fields -%}
            {%- set cte_name = category ~ "__investigation_form__" ~ ifield ~ "_cte" -%}

            {%- set table_name = (
                "tasks__" ~ category ~ "__investigation_form__" ~ ifield
            ) -%}
        {% if table_exists(table_name, "central_raw_mdharura") %}
            left join
                {{ table_name }} as {{ cte_name }}
                on tasks._dlt_id = {{ cte_name }}._dlt_parent_id
        {% endif %}
    {% endfor %}

    {# Handle nested field for response form #}
    {%- for ifield in response_form_fields -%}
            {%- set cte_name = category ~ "__response_form__" ~ ifield ~ "_cte" -%}

            {%- set table_name = "tasks__" ~ category ~ "__response_form__" ~ ifield -%}
        {% if table_exists(table_name, "central_raw_mdharura") %}
            left join
                {{ table_name }} as {{ cte_name }}
                on tasks._dlt_id = {{ cte_name }}._dlt_parent_id
        {% endif %}
    {% endfor %}

{% endfor %}

{# Handle nested fields for a task #}
{%- for ifield in task_nested_fields -%}
        {%- set cte_name = "tasks" ~ ifield ~ "_cte" -%}
        {%- set table_name = "tasks__" ~ ifield -%}
    {% if table_exists(table_name, "central_raw_mdharura") %}
        left join
            {{ table_name }} as {{ cte_name }}
            on tasks._dlt_id = {{ cte_name }}._dlt_parent_id
    {% endif %}
{% endfor %}
