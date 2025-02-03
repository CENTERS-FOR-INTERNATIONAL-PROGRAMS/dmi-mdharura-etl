{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_stg_tasks_id ON {{this}} USING btree ("_ID");',
      'CREATE INDEX IF NOT EXISTS idx_stg_tasks_unit ON {{this}} USING btree ("UNIT_ID");',
      'CREATE INDEX IF NOT EXISTS idx_stg_tasks_unit ON {{this}} USING btree ("SIGNAL");',
      'CREATE INDEX IF NOT EXISTS idx_stg_tasks_createdat ON {{this}} USING btree ("CREATEDAT");',
      'CREATE INDEX IF NOT EXISTS idx_stg_tasks_updatedat ON {{this}} USING btree ("UPDATEDAT");'
    ]
) }}


{%- set columns = dbt_utils.get_filtered_columns_in_relation(
  from = source(
    'central_raw_mdharura',
    'tasks'
  )
) -%}
{%- set ebs_categories = ["cebs", "hebs", "vebs"] -%}
-- VERIFICATION FORM FIELDS --
{%- set verification_form_fields = [ ["_id", "text"],
["description", "text"],
["is_matching_signal", "boolean"],
["updated_signal", "text"],
["is_reported_before", "boolean"],
["date_health_threat_started", "timestamptz"],
["informant", "text"],
["other_informant", "text"],
["additional_information", "text"],
["date_verified", "timestamptz"],
["is_threat_still_existing", "boolean"],
["threat_to", "text"],
["date_scdsc_informed", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- INVESTIGATION FORM FIELDS --
{%- set investigation_form_fields = [ ["_id", "text"],
["date_scdsc_informed", "timestamptz"],
["date_investigation_started", "timestamptz"],
["date_event_started", "timestamptz"],
["humans_cases", "integer"],
["humans_cases_hospitalized", "integer"],
["humans_dead", "integer"],
["animals_cases", "integer"],
["animals_dead", "integer"],
["is_cause_known", "text"],
["cause", "text"],
["is_lab_samples_collected", "text"],
["date_sample_collected", "timestamptz"],
["lab_results", "text"],
["date_lab_results_received", "timestamptz"],
["is_new_cased_reported_from_initial_area", "text"],
["is_new_cased_reported_from_new_areas", "text"],
["is_event_setting_promoting_spread", "text"],
["risk_classification", "text"],
["is_event_infectious", "text"],
["event_categories", "text"],
["systems_affected_by_event", "text"],
["response_activities", "text"],
["date_scmoh_informed", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- RESPONSE FORM FIELDS --
{%- set response_form_fields = [ ["_id", "text"],
["event_type", "text"],
["date_scmoh_informed", "timestamptz"],
["date_response_started", "timestamptz"],
["response_activities", "text"],
["other_response_activity", "text"],
["outcome_of_response", "text"],
["recommendations", "text"],
["date_escalated", "timestamptz"],
["date_of_report", "timestamptz"],
["additional_information", "text"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- SUMMARY FORM FIELDS --
{%- set summary_form_fields = [ ["_id", "text"],
["event_status", "text"],
["escalated_to", "text"],
["cause", "text"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- ESCALATION FORM FIELDS --
{%- set escalation_form_fields = [ ["_id", "text"],
["event_type", "text"],
["date_response_started", "timestamptz"],
["reason", "text"],
["reason_other", "text"],
["date_escalated", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- PMEBS REPORT FORM FIELDS --
{%- set pmebs_report_form_fields = [ ["_id", "text"],
["date_detected", "timestamptz"],
["description", "text"],
["source", "text"],
["unit", "text"],
["locality", "text"],
["date_reported", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- PMEBS REQUEST FORM FIELDS --
{%- set pmebs_request_form_fields = [ ["_id", "text"],
["description", "text"],
["unit", "text"],
["locality", "text"],
["date_reported", "timestamptz"],
["date_requested", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
-- PMEBS NOTIFY FORM FIELDS --
{%- set pmebs_notify_form_fields = [ ["_id", "text"],
["description", "text"],
["unit", "text"],
["locality", "text"],
["date_reported", "timestamptz"],
["date_requested", "timestamptz"],
["user", "text"],
["via", "text"],
["spot", "text"],
["created_at", "timestamptz"],
["updated_at", "timestamptz"] ] %}
SELECT
  _id :: text AS "_ID",
  signal :: text AS "SIGNAL",
  signal_id :: text AS "SIGNALID",
  status :: text AS "STATUS",
  state :: text AS "STATE",
  created_at :: timestamptz AS "CREATEDAT",
  updated_at :: timestamptz AS "UPDATEDAT",
  spot :: text AS "SPOT",
  unit :: text AS "UNIT_ID",
  "user" :: text AS "USER_ID",
  {%- for category in ebs_categories %}
    {% set uppercase_category = category | upper -%}
    {{ column_exists(
      category ~ '__id',
      columns,
      'text',
      'NULL'
    ) }} AS "{{uppercase_category}}_ID",
    {{ column_exists(
      category ~ '__created_at',
      columns,
      'timestamptz',
      'NULL'
    ) }} AS "{{uppercase_category}}_CREATEDAT",
    {{ column_exists(
      category ~ '__updated_at',
      columns,
      'timestamptz',
      'NULL'
    ) }} AS "{{uppercase_category}}_UPDATEDAT",
    {%- for vfield,
      vtype in verification_form_fields %}
      {% set uppercase_vfield = vfield | upper | replace(
        "_",
        ""
      ) -%}
      {{ column_exists(
        category ~ '__verification_form__' ~ vfield,
        columns,
        vtype,
        'NULL'
      ) }} AS "{{uppercase_category}}_VERIFICATIONFORM_{{uppercase_vfield}}",
    {%- endfor -%}

    {% for ifield,
      itype in investigation_form_fields %}
      {% set uppercase_ifield = ifield | upper | replace(
        "_",
        ""
      ) -%}
      {# Due to postgres having max size for column name, dlt adds a unique id for each long column #}
      {%- if ifield == "is_new_cased_reported_from_initial_area" and category == "cebs" -%}
        {{ column_exists(
          category ~ '__investigation_form__' ~ 'is_ybjriad_reported_from_initial_area',
          columns,
          itype,
          'NULL'
        ) }} AS "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
        {%- elif ifield == "is_new_cased_reported_from_initial_area" and category == "hebs" -%}
        {{ column_exists(
          category ~ '__investigation_form__' ~ 'is_ss80mqd_reported_from_initial_area',
          columns,
          itype,
          'NULL'
        ) }} AS "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
        {%- elif ifield == "is_new_cased_reported_from_initial_area" and category == "vebs" -%}
        {{ column_exists(
          category ~ '__investigation_form__' ~ 'is_27a34wd_reported_from_initial_area',
          columns,
          itype,
          'NULL'
        ) }} AS "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
      {%- else -%}
        {{ column_exists(
          category ~ '__investigation_form__' ~ ifield,
          columns,
          itype,
          'NULL'
        ) }} AS "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
      {%- endif -%}
    {%- endfor -%}

    {% for rfield,
      rtype in response_form_fields %}
      {% set uppercase_rfield = rfield | upper | replace(
        "_",
        ""
      ) -%}
      {{ column_exists(
        category ~ '__response_form__' ~ rfield,
        columns,
        rtype,
        'NULL'
      ) }} AS "{{uppercase_category}}_RESPONSEFORM_{{uppercase_rfield}}",
    {%- endfor -%}

    {% for efield,
      etype in escalation_form_fields %}
      {% set uppercase_efield = efield | upper | replace(
        "_",
        ""
      ) -%}
      {{ column_exists(
        category ~ '__escalation_form__' ~ efield,
        columns,
        etype,
        'NULL'
      ) }} AS "{{uppercase_category}}_ESCALATIONFORM_{{uppercase_efield}}",
    {%- endfor -%}

    {% for sfield,
      stype in summary_form_fields %}
      {% set uppercase_sfield = sfield | upper | replace(
        "_",
        ""
      ) -%}
      {{ column_exists(
        category ~ '__summary_form__' ~ sfield,
        columns,
        stype,
        'NULL'
      ) }} AS "{{uppercase_category}}_SUMMARYFORM_{{uppercase_sfield}}",
    {%- endfor -%}
  {%- endfor %}

  {% for pmreportfield,
    pmreporttype in pmebs_report_form_fields %}
    {% set uppercase_pmreportfield = pmreportfield | upper | replace(
      "_",
      ""
    ) -%}
    {{ column_exists(
      'pmebs__report_form__' ~ pmreportfield,
      columns,
      pmreporttype,
      'NULL'
    ) }} AS "PMEBS_REPORTFORM_{{uppercase_pmreportfield}}",
  {%- endfor -%}

  {% for pmnotifyfield,
    pmnotifytype in pmebs_notify_form_fields %}
    {% set uppercase_pmnotifyfield = pmnotifyfield | upper | replace(
      "_",
      ""
    ) -%}
    {{ column_exists(
      'pmebs__notify_form__' ~ pmnotifyfield,
      columns,
      pmnotifytype,
      'NULL'
    ) }} AS "PMEBS_NOTIFYFORM_{{uppercase_pmnotifyfield}}",
  {%- endfor -%}

  {% for pmrequestfield,
    pmrequesttype in pmebs_request_form_fields %}
    {% set uppercase_pmrequestfield = pmrequestfield | upper | replace(
      "_",
      ""
    ) -%}
    {{ column_exists(
      'pmebs__request_form__' ~ pmrequestfield,
      columns,
      pmrequesttype,
      'NULL'
    ) }} AS "PMEBS_REQUESTFORM_{{uppercase_pmrequestfield}}",
  {%- endfor %}

  CAST(
    CURRENT_DATE AS DATE
  ) AS "STG_LOAD_DATE"
FROM
  {{ source(
    'central_raw_mdharura',
    'tasks'
  ) }}
