{%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source('central_raw_mdharura', 'tasks')) -%}

select
  _id::text as "_ID",
  signal::text as "SIGNAL",
  signal_id::text as "SIGNALID",
  status::text as "STATUS",
  state::text as "STATE",
  created_at::timestamptz as "CREATEDAT",
  updated_at::timestamptz as "UPDATEDAT",
  spot::text as "SPOT",
  unit::text as "UNIT_ID",
  "user"::text as "USER_ID",
  cebs__created_at::timestamptz as "CEBS_CREATEDAT",
  cebs__updated_at::timestamptz as "CEBS_UPDATEDAT",
  cebs__verification_form___id::text as "CEBS_VERIFICATIONFORM_ID",
  cebs__verification_form__user::text as "CEBS_VERIFICATIONFORM_USER",
  cebs__verification_form__source::text as "CEBS_VERIFICATIONFORM_SOURCE",
  cebs__verification_form__description::text as "CEBS_VERIFICATIONFORM_DESCRIPTION",
  cebs__verification_form__is_matching_signal::text as "CEBS_VERIFICATIONFORM_ISMATCHINGSIGNAL",
  cebs__verification_form__updated_signal::text as "CEBS_VERIFICATIONFORM_UPDATEDSIGNAL",
  cebs__verification_form__is_reported_before::text as "CEBS_VERIFICATIONFORM_ISREPORTEDBEFORE",
  cebs__verification_form__date_health_threat_started::timestamptz as "CEBS_VERIFICATIONFORM_DATEHEALTHTHREATSTARTED",
  cebs__verification_form__informant::text as "CEBS_VERIFICATIONFORM_INFORMANT",
  cebs__verification_form__additional_information::text as "CEBS_VERIFICATIONFORM_ADDITIONALINFORMATION",
  cebs__verification_form__date_verified::timestamptz as "CEBS_VERIFICATIONFORM_DATEVERIFIED",
  cebs__verification_form__is_threat_still_existing::text as "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
  cebs__verification_form__threat_to::text as "CEBS_VERIFICATIONFORM_THREATTO",
  cebs__verification_form__date_scdsc_informed::text as "CEBS_VERIFICATIONFORM_DATESCDSCINFORMED",
  cebs__verification_form__created_at::timestamptz as "CEBS_VERIFICATIONFORM_CREATEDAT",
  cebs__verification_form__updated_at::timestamptz as "CEBS_VERIFICATIONFORM_UPDATEDAT",
  cebs__verification_form__via::text as "CEBS_VERIFICATIONFORM_VIA",

  {% if 'cebs__verification_form__spot' in columns -%}
    cebs__verification_form__spot::text as "CEBS_VERIFICATIONFORM_SPOT",
  {% else %}
    NULL as "CEBS_VERIFICATIONFORM_SPOT",
  {% endif %}

  /* Investigation Form */
  cebs__investigation_form___id::text as "CEBS_INVESTIGATIONFORM_ID",
  cebs__investigation_form__date_scdsc_informed::text as "CEBS_INVESTIGATIONFORM_DATESCDSCINFORMED",
  cebs__investigation_form__date_investigation_started::text as "CEBS_INVESTIGATIONFORM_DATEINVESTIGATIONSTARTED",
  cebs__investigation_form__date_event_started::text as "CEBS_INVESTIGATIONFORM_DATEEVENTSTARTED",
  cebs__investigation_form__symptoms::text as "CEBS_INVESTIGATIONFORM_SYMPTOMS",
  cebs__investigation_form__humans_cases::integer as "CEBS_INVESTIGATIONFORM_HUMANSCASES",
  cebs__investigation_form__humans_dead::integer as "CEBS_INVESTIGATIONFORM_HUMANSDEAD",
  cebs__investigation_form__humans_cases_hospitalized::integer as "CEBS_INVESTIGATIONFORM_HUMANSCASESHOSPITALIZED",
  cebs__investigation_form__animals_cases::integer as "CEBS_INVESTIGATIONFORM_ANIMALSCASES",
  cebs__investigation_form__animals_dead::integer as "CEBS_INVESTIGATIONFORM_ANIMALSDEAD",
  cebs__investigation_form__is_cause_known::text as "CEBS_INVESTIGATIONFORM_ISCAUSEKNOWN",
  cebs__investigation_form__is_lab_samples_collected::text as "CEBS_INVESTIGATIONFORM_ISLABSAMPLESCOLLECTED",

  {% if 'cebs__investigation_form__is_new_cased_reported_from_initial_area' in columns -%}
    cebs__investigation_form__is_new_cased_reported_from_initial_area::text as "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% elif 'cebs__investigation_form__is_ybjriad_reported_from_initial_area' in columns %}
   cebs__investigation_form__is_ybjriad_reported_from_initial_area::text as "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% else %}
    NULL as "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% endif %}

  cebs__investigation_form__is_new_cased_reported_from_new_areas::text as "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMNEWAREAS",
  cebs__investigation_form__is_event_setting_promoting_spread::text as "CEBS_INVESTIGATIONFORM_ISEVENTSETTINGPROMOTINGSPREAD",
  cebs__investigation_form__additional_information::text as "CEBS_INVESTIGATIONFORM_ADDITIONALINFORMATION",
  cebs__investigation_form__risk_classification::text as "CEBS_INVESTIGATIONFORM_RISKCLASSIFICATION",
  cebs__investigation_form__is_event_infectious::text as "CEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
  cebs__investigation_form__date_scmoh_informed::timestamptz as "CEBS_INVESTIGATIONFORM_DATESCMOHINFORMED",
  cebs__investigation_form__created_at::timestamptz as "CEBS_INVESTIGATIONFORM_CREATEDAT",
  cebs__investigation_form__updated_at::timestamptz as "CEBS_INVESTIGATIONFORM_UPDATEDAT",
  cebs__investigation_form__user::text as "CEBS_INVESTIGATIONFORM_USER",
  cebs__investigation_form__via::text as "CEBS_INVESTIGATIONFORM_VIA",

   {% if 'cebs__investigation_form__spot' in columns -%}
    cebs__investigation_form__spot::text as "CEBS_INVESTIGATIONFORM_SPOT",
  {% else %}
    NULL as "CEBS_INVESTIGATIONFORM_SPOT",
  {% endif %}

  /* Response Form */
  cebs__response_form___id::text as "CEBS_RESPONSEFORM_ID",
  cebs__response_form__user::text as "CEBS_RESPONSEFORM_USER",
  cebs__response_form__event_type::text as "CEBS_RESPONSEFORM_EVENTTYPE",
  cebs__response_form__date_scmoh_informed::text as "CEBS_RESPONSEFORM_DATESCMOHINFORMED",
  cebs__response_form__date_response_started::text as "CEBS_RESPONSEFORM_DATERESPONSESTARTED",
  cebs__response_form__outcome_of_response::text as "CEBS_RESPONSEFORM_OUTCOMEOFRESPONSE",
  cebs__response_form__date_of_report::text as "CEBS_RESPONSEFORM_DATEOFREPORT",
  cebs__response_form__additional_information::text as "CEBS_RESPONSEFORM_ADDITIONALINFORMATION",
  cebs__response_form__via::text as "CEBS_RESPONSEFORM_VIA",
  cebs__response_form__date_escalated::timestamptz as "CEBS_RESPONSEFORM_DATEESCALATED",

  {% if 'cebs__response_form__spot' in columns -%}
    cebs__response_form__spot::text as "CEBS_RESPONSEFORM_SPOT",
  {% else %}
    NULL as "CEBS_RESPONSEFORM_SPOT",
  {% endif %}

  cebs__response_form__created_at::timestamptz as "CEBS_RESPONSEFORM_CREATEDAT",
  cebs__response_form__updated_at::timestamptz as "CEBS_RESPONSEFORM_UPDATEDAT",

  /* Summary Form */
  cebs__summary_form___id::text as "CEBS_SUMMARYFORM_ID",
  cebs__summary_form__user::text as "CEBS_SUMMARYFORM_USER",
  cebs__summary_form__via::text as "CEBS_SUMMARYFORM_VIA",
  cebs__summary_form__event_status::text as "CEBS_SUMMARYFORM_EVENTSTATUS",
  cebs__summary_form__cause::text as "CEBS_SUMMARYFORM_CAUSE",

  {% if 'cebs__summary_form__escalated_to' in columns -%}
    cebs__summary_form__escalated_to::text as "CEBS_SUMMARYFORM_ESCALATEDTO",
  {% else %}
    NULL as "CEBS_SUMMARYFORM_SPOT",
  {% endif %}

  {% if 'cebs__summary_form__spot' in columns -%}
    cebs__summary_form__spot::text as "CEBS_SUMMARYFORM_SPOT",
  {% else %}
    NULL as "CEBS_SUMMARYFORM_SPOT",
  {% endif %}

  cebs__summary_form__created_at::timestamptz as "CEBS_SUMMARYFORM_CREATEDAT",
  cebs__summary_form__updated_at::timestamptz as "CEBS_SUMMARYFORM_UPDATEDAT",

  /* Escalation Form */
  cebs__escalation_form___id::text as "CEBS_ESCALATIONFORM_ID",
  cebs__escalation_form__user::text as "CEBS_ESCALATIONFORM_USER",
  cebs__escalation_form__via::text as "CEBS_ESCALATIONFORM_VIA",
  cebs__escalation_form__event_type::text as "CEBS_ESCALATIONFORM_EVENTTYPE",
  cebs__escalation_form__date_response_started::timestamptz as "CEBS_ESCALATIONFORM_DATERESPONSESTARTED",
  cebs__escalation_form__reason::text as "CEBS_ESCALATIONFORM_REASON",
  cebs__escalation_form__reason_other::text as "CEBS_ESCALATIONFORM_REASONOTHER",
  cebs__response_form__date_escalated::timestamptz as "CEBS_ESCALATIONFORM_DATEESCALATED",

  {% if 'cebs__escalation_form__spot' in columns -%}
    cebs__escalation_form__spot::text as "CEBS_ESCALATIONFORM_SPOT",
  {% else %}
    NULL as "CEBS_ESCALATIONFORM_SPOT",
  {% endif %}

  cebs__escalation_form__created_at::timestamptz as "CEBS_ESCALATIONFORM_CREATEDAT",
  cebs__escalation_form__updated_at::timestamptz as "CEBS_ESCALATIONFORM_UPDATEDAT"

from {{ source('central_raw_mdharura', 'tasks') }}
