{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_unit ON {{this}} USING btree ("UNIT_ID");',
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_signal ON {{this}} USING btree ("SIGNAL");',
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_createdat ON {{this}} USING btree ("CREATEDAT");',
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_updatedat ON {{this}} USING btree ("UPDATEDAT");',
            'CREATE INDEX IF NOT EXISTS idx_stg_tasks_updatedat ON {{this}} USING btree ("STATE");',
        ]
    )
}}

{%- set columns = dbt_utils.get_filtered_columns_in_relation(
    from=ref("intermediate_stg_tasks")
) -%}
{%- set ebs_categories = ["cebs", "hebs", "vebs", "lebs"] -%}
-- VERIFICATION FORM FIELDS --
{%- set verification_form_fields = [
    ["_id", "text"],
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
    ["updated_at", "timestamptz"],
] %}
-- INVESTIGATION FORM FIELDS --
{%- set investigation_form_fields = [
    ["_id", "text"],
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
    ["updated_at", "timestamptz"],
] %}
-- RESPONSE FORM FIELDS --
{%- set response_form_fields = [
    ["_id", "text"],
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
    ["updated_at", "timestamptz"],
] %}
-- SUMMARY FORM FIELDS --
{%- set summary_form_fields = [
    ["_id", "text"],
    ["event_status", "text"],
    ["escalated_to", "text"],
    ["cause", "text"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}
-- ESCALATION FORM FIELDS --
{%- set escalation_form_fields = [
    ["_id", "text"],
    ["event_type", "text"],
    ["date_response_started", "timestamptz"],
    ["reason", "text"],
    ["reason_other", "text"],
    ["date_escalated", "timestamptz"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}
-- PMEBS REPORT FORM FIELDS --
{%- set pmebs_report_form_fields = [
    ["_id", "text"],
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
    ["updated_at", "timestamptz"],
] %}
-- PMEBS REQUEST FORM FIELDS --
{%- set pmebs_request_form_fields = [
    ["_id", "text"],
    ["description", "text"],
    ["unit", "text"],
    ["locality", "text"],
    ["date_reported", "timestamptz"],
    ["date_requested", "timestamptz"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}
-- PMEBS NOTIFY FORM FIELDS --
{%- set pmebs_notify_form_fields = [
    ["_id", "text"],
    ["description", "text"],
    ["unit", "text"],
    ["locality", "text"],
    ["date_reported", "timestamptz"],
    ["date_requested", "timestamptz"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}

-- LEBS VERIFICATION FORM FIELDS --
{%- set lebs_verification_form_fields = [
    ["_id", "text"],
    ["description", "text"],
    ["is_matching_signal", "boolean"],
    ["updated_signal", "text"],
    ["date_health_threat_started", "timestamptz"],
    ["informant", "text"],
    ["other_informant", "text"],
    ["additional_information", "text"],
    ["date_verified", "timestamptz"],
    ["is_still_happening", "boolean"],
    ["is_reported_before", "boolean"],
    ["date_scdsc_informed", "timestamptz"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}

-- LEBS INVESTIGATION FORM FIELDS --
{%- set lebs_investigation_form_fields = [
    ["_id", "text"],
    ["date_scdsc_informed", "timestamptz"],
    ["date_investigation_started", "timestamptz"],
    ["date_event_started", "timestamptz"],
    ["date_rrt_notified", "timestamptz"],
    ["is_covid19_working_case_definition_met", "text"],
    ["is_event_setting_promoting_spread", "text"],
    ["measure_hand_hygene", "text"],
    ["measure_temp_screening", "text"],
    ["measure_physical_distancing", "text"],
    ["measure_use_of_masks", "text"],
    ["measure_ventilation", "text"],
    ["additional_information", "text"],
    ["risk_classification", "text"],
    ["is_event_infectious", "text"],
    ["event_categories", "text"],
    ["systems_affected_by_event", "text"],
    ["response_activities", "text"],
    ["symptoms", "text"],
    ["symptoms_other", "text"],
    ["is_samples_collected", "text"],
    ["lab_results", "text"],
    ["measure_social_distancing", "text"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}

-- LEBS RESPONSE FORM FIELDS --
{%- set lebs_response_form_fields = [
    ["_id", "text"],
    ["event_type", "text"],
    ["date_scmoh_informed", "timestamptz"],
    ["date_response_started", "timestamptz"],
    ["date_samples_collected", "timestamptz"],
    ["date_of_test_results", "timestamptz"],
    ["is_covid19_working_case_definition_met", "text"],
    ["is_cif_filled_and_samples_collected", "text"],
    ["reasons_no_sample_collected_other", "text"],
    ["response_activities_other", "text"],
    ["is_humans_quaratined_followed_up", "text"],
    ["event_status", "text"],
    ["response_activities", "text"],
    ["additional_response_activities", "text"],
    ["reasons_no_sample_collected", "text"],
    ["humans_quarantined_self", "text"],
    ["humans_quarantined_school", "text"],
    ["humans_quarantined_institutional", "text"],
    ["humans_isolation_school", "text"],
    ["humans_isolation_health_facility", "text"],
    ["humans_isolation_home", "text"],
    ["humans_isolation_institutional", "text"],
    ["humans_dead", "integer"],
    ["humans_positive", "integer"],
    ["humans_tested", "integer"],
    ["humans_cases", "integer"],
    ["humans_quarantined", "integer"],
    ["quarintine_types", "text"],
    ["is_humans_isolated", "text"],
    ["isolation_types", "text"],
    ["event_statuses", "text"],
    ["additional_information", "text"],
    ["user", "text"],
    ["via", "text"],
    ["spot", "text"],
    ["created_at", "timestamptz"],
    ["updated_at", "timestamptz"],
] %}

select
    _id::text as "_ID",
    signal::text as "SIGNAL",
    signal_id::text as "SIGNALID",
    status::text as "STATUS",
    state::text as "STATE",
    via::text as "VIA",
    created_at::timestamptz as "CREATEDAT",
    updated_at::timestamptz as "UPDATEDAT",
    spot::text as "SPOT",
    unit::text as "UNIT_ID",
    units::text as "UNITS",
    suggestions::text as "SUGGESTIONS",
    "user"::text as "USER_ID",
    {%- for category in ebs_categories %}
        {% set uppercase_category = category | upper -%}
        {{ column_exists(category ~ "__id", columns, "text", "NULL") }}
        as "{{uppercase_category}}_ID",
        {{ column_exists(category ~ "__created_at", columns, "timestamptz", "NULL") }}
        as "{{uppercase_category}}_CREATEDAT",
        {{ column_exists(category ~ "__updated_at", columns, "timestamptz", "NULL") }}
        as "{{uppercase_category}}_UPDATEDAT",

        {% if category in ["lebs"] %}
            {%- for lvfield, lvtype in lebs_verification_form_fields %}
                {% set uppercase_lvfield = lvfield | upper | replace("_", "") -%}
                {{
                    column_exists(
                        category ~ "__verification_form__" ~ lvfield,
                        columns,
                        lvtype,
                        "NULL",
                    )
                }} as "{{uppercase_category}}_VERIFICATIONFORM_{{uppercase_lvfield}}",
            {%- endfor -%}
        {% else %}
            {%- for vfield, vtype in verification_form_fields %}
                {% set uppercase_vfield = vfield | upper | replace("_", "") -%}
                {{
                    column_exists(
                        category ~ "__verification_form__" ~ vfield,
                        columns,
                        vtype,
                        "NULL",
                    )
                }} as "{{uppercase_category}}_VERIFICATIONFORM_{{uppercase_vfield}}",
            {%- endfor -%}
        {%- endif -%}

        {% if category in ["lebs"] %}
            {%- for lifield, litype in lebs_investigation_form_fields %}
                {% set uppercase_lifield = lifield | upper | replace("_", "") -%}
                {{
                    column_exists(
                        category ~ "__investigation_form__" ~ lifield,
                        columns,
                        litype,
                        "NULL",
                    )
                }} as "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_lifield}}",
            {%- endfor -%}
        {%- else -%}
            {% for ifield, itype in investigation_form_fields %}
                {% set uppercase_ifield = ifield | upper | replace("_", "") -%}
                {# Due to postgres having max size for column name, dlt adds a unique id for each long column #}
                {%- if ifield == "is_new_cased_reported_from_initial_area" and category == "cebs" -%}
                    {{
                        column_exists(
                            category
                            ~ "__investigation_form__"
                            ~ "is_ybjriad_reported_from_initial_area",
                            columns,
                            itype,
                            "NULL",
                        )
                    }}
                    as "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
                {%- elif ifield == "is_new_cased_reported_from_initial_area" and category == "hebs" -%}
                    {{
                        column_exists(
                            category
                            ~ "__investigation_form__"
                            ~ "is_ss80mqd_reported_from_initial_area",
                            columns,
                            itype,
                            "NULL",
                        )
                    }}
                    as "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
                {%- elif ifield == "is_new_cased_reported_from_initial_area" and category == "vebs" -%}
                    {{
                        column_exists(
                            category
                            ~ "__investigation_form__"
                            ~ "is_27a34wd_reported_from_initial_area",
                            columns,
                            itype,
                            "NULL",
                        )
                    }}
                    as "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
                {%- else -%}
                    {{
                        column_exists(
                            category ~ "__investigation_form__" ~ ifield,
                            columns,
                            itype,
                            "NULL",
                        )
                    }}
                    as "{{uppercase_category}}_INVESTIGATIONFORM_{{uppercase_ifield}}",
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}

        {% for rfield, rtype in response_form_fields %}
            {% set uppercase_rfield = rfield | upper | replace("_", "") -%}
            {{
                column_exists(
                    category ~ "__response_form__" ~ rfield, columns, rtype, "NULL"
                )
            }} as "{{uppercase_category}}_RESPONSEFORM_{{uppercase_rfield}}",
        {%- endfor -%}

        {% for efield, etype in escalation_form_fields %}
            {% set uppercase_efield = efield | upper | replace("_", "") -%}
            {{
                column_exists(
                    category ~ "__escalation_form__" ~ efield, columns, etype, "NULL"
                )
            }} as "{{uppercase_category}}_ESCALATIONFORM_{{uppercase_efield}}",
        {%- endfor -%}

        {% for sfield, stype in summary_form_fields %}
            {% set uppercase_sfield = sfield | upper | replace("_", "") -%}
            {{
                column_exists(
                    category ~ "__summary_form__" ~ sfield, columns, stype, "NULL"
                )
            }} as "{{uppercase_category}}_SUMMARYFORM_{{uppercase_sfield}}",
        {%- endfor -%}
    {%- endfor %}

    {% for pmreportfield, pmreporttype in pmebs_report_form_fields %}
        {% set uppercase_pmreportfield = pmreportfield | upper | replace("_", "") -%}
        {{
            column_exists(
                "pmebs__report_form__" ~ pmreportfield, columns, pmreporttype, "NULL"
            )
        }} as "PMEBS_REPORTFORM_{{uppercase_pmreportfield}}",
    {%- endfor -%}

    {% for pmnotifyfield, pmnotifytype in pmebs_notify_form_fields %}
        {% set uppercase_pmnotifyfield = pmnotifyfield | upper | replace("_", "") -%}
        {{
            column_exists(
                "pmebs__notify_form__" ~ pmnotifyfield, columns, pmnotifytype, "NULL"
            )
        }} as "PMEBS_NOTIFYFORM_{{uppercase_pmnotifyfield}}",
    {%- endfor -%}

    {% for pmrequestfield, pmrequesttype in pmebs_request_form_fields %}
        {% set uppercase_pmrequestfield = pmrequestfield | upper | replace("_", "") -%}
        {{
            column_exists(
                "pmebs__request_form__" ~ pmrequestfield,
                columns,
                pmrequesttype,
                "NULL",
            )
        }} as "PMEBS_REQUESTFORM_{{uppercase_pmrequestfield}}",
    {%- endfor %}

    cast(current_date as date) as "STG_LOAD_DATE"
from {{ ref("intermediate_stg_tasks") }}
