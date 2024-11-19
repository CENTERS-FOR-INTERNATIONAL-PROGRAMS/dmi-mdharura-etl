{%- set columns = dbt_utils.get_filtered_columns_in_relation(
  from = source(
    'central_raw_mdharura',
    'tasks'
  )
) -%}
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
  -- CEBS FORMS
  cebs__created_at :: timestamptz AS "CEBS_CREATEDAT",
  cebs__updated_at :: timestamptz AS "CEBS_UPDATEDAT",
  cebs__verification_form___id :: text AS "CEBS_VERIFICATIONFORM_ID",
  cebs__verification_form__user :: text AS "CEBS_VERIFICATIONFORM_USER",
  cebs__verification_form__source :: text AS "CEBS_VERIFICATIONFORM_SOURCE",
  cebs__verification_form__description :: text AS "CEBS_VERIFICATIONFORM_DESCRIPTION",
  cebs__verification_form__is_matching_signal :: text AS "CEBS_VERIFICATIONFORM_ISMATCHINGSIGNAL",
  cebs__verification_form__updated_signal :: text AS "CEBS_VERIFICATIONFORM_UPDATEDSIGNAL",
  cebs__verification_form__is_reported_before :: text AS "CEBS_VERIFICATIONFORM_ISREPORTEDBEFORE",
  cebs__verification_form__date_health_threat_started :: timestamptz AS "CEBS_VERIFICATIONFORM_DATEHEALTHTHREATSTARTED",
  cebs__verification_form__informant :: text AS "CEBS_VERIFICATIONFORM_INFORMANT",
  cebs__verification_form__additional_information :: text AS "CEBS_VERIFICATIONFORM_ADDITIONALINFORMATION",
  cebs__verification_form__date_verified :: timestamptz AS "CEBS_VERIFICATIONFORM_DATEVERIFIED",
  cebs__verification_form__is_threat_still_existing :: text AS "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
  cebs__verification_form__threat_to :: text AS "CEBS_VERIFICATIONFORM_THREATTO",
  cebs__verification_form__date_scdsc_informed :: text AS "CEBS_VERIFICATIONFORM_DATESCDSCINFORMED",
  cebs__verification_form__created_at :: timestamptz AS "CEBS_VERIFICATIONFORM_CREATEDAT",
  cebs__verification_form__updated_at :: timestamptz AS "CEBS_VERIFICATIONFORM_UPDATEDAT",
  cebs__verification_form__via :: text AS "CEBS_VERIFICATIONFORM_VIA",
  {% if 'cebs__verification_form__spot' in columns -%}
    cebs__verification_form__spot :: text AS "CEBS_VERIFICATIONFORM_SPOT",
  {% else %}
    NULL AS "CEBS_VERIFICATIONFORM_SPOT",
  {% endif %}
  /* Investigation Form */
  cebs__investigation_form___id :: text AS "CEBS_INVESTIGATIONFORM_ID",
  cebs__investigation_form__date_scdsc_informed :: text AS "CEBS_INVESTIGATIONFORM_DATESCDSCINFORMED",
  cebs__investigation_form__date_investigation_started :: text AS "CEBS_INVESTIGATIONFORM_DATEINVESTIGATIONSTARTED",
  cebs__investigation_form__date_event_started :: text AS "CEBS_INVESTIGATIONFORM_DATEEVENTSTARTED",
  cebs__investigation_form__symptoms :: text AS "CEBS_INVESTIGATIONFORM_SYMPTOMS",
  cebs__investigation_form__humans_cases :: INTEGER AS "CEBS_INVESTIGATIONFORM_HUMANSCASES",
  cebs__investigation_form__humans_dead :: INTEGER AS "CEBS_INVESTIGATIONFORM_HUMANSDEAD",
  cebs__investigation_form__humans_cases_hospitalized :: INTEGER AS "CEBS_INVESTIGATIONFORM_HUMANSCASESHOSPITALIZED",
  cebs__investigation_form__animals_cases :: INTEGER AS "CEBS_INVESTIGATIONFORM_ANIMALSCASES",
  cebs__investigation_form__animals_dead :: INTEGER AS "CEBS_INVESTIGATIONFORM_ANIMALSDEAD",
  cebs__investigation_form__is_cause_known :: text AS "CEBS_INVESTIGATIONFORM_ISCAUSEKNOWN",
  cebs__investigation_form__is_lab_samples_collected :: text AS "CEBS_INVESTIGATIONFORM_ISLABSAMPLESCOLLECTED",
  {% if 'cebs__investigation_form__is_new_cased_reported_from_initial_area' in columns -%}
    cebs__investigation_form__is_new_cased_reported_from_initial_area :: text AS "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
    {% elif 'cebs__investigation_form__is_ybjriad_reported_from_initial_area' in columns %}
    cebs__investigation_form__is_ybjriad_reported_from_initial_area :: text AS "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% else %}
    NULL AS "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% endif %}

  cebs__investigation_form__is_new_cased_reported_from_new_areas :: text AS "CEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMNEWAREAS",
  cebs__investigation_form__is_event_setting_promoting_spread :: text AS "CEBS_INVESTIGATIONFORM_ISEVENTSETTINGPROMOTINGSPREAD",
  cebs__investigation_form__additional_information :: text AS "CEBS_INVESTIGATIONFORM_ADDITIONALINFORMATION",
  cebs__investigation_form__risk_classification :: text AS "CEBS_INVESTIGATIONFORM_RISKCLASSIFICATION",
  cebs__investigation_form__is_event_infectious :: text AS "CEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
  cebs__investigation_form__date_scmoh_informed :: timestamptz AS "CEBS_INVESTIGATIONFORM_DATESCMOHINFORMED",
  cebs__investigation_form__created_at :: timestamptz AS "CEBS_INVESTIGATIONFORM_CREATEDAT",
  cebs__investigation_form__updated_at :: timestamptz AS "CEBS_INVESTIGATIONFORM_UPDATEDAT",
  cebs__investigation_form__user :: text AS "CEBS_INVESTIGATIONFORM_USER",
  cebs__investigation_form__via :: text AS "CEBS_INVESTIGATIONFORM_VIA",
  {% if 'cebs__investigation_form__spot' in columns -%}
    cebs__investigation_form__spot :: text AS "CEBS_INVESTIGATIONFORM_SPOT",
  {% else %}
    NULL AS "CEBS_INVESTIGATIONFORM_SPOT",
  {% endif %}
  /* Response Form */
  cebs__response_form___id :: text AS "CEBS_RESPONSEFORM_ID",
  cebs__response_form__user :: text AS "CEBS_RESPONSEFORM_USER",
  cebs__response_form__event_type :: text AS "CEBS_RESPONSEFORM_EVENTTYPE",
  cebs__response_form__date_scmoh_informed :: text AS "CEBS_RESPONSEFORM_DATESCMOHINFORMED",
  cebs__response_form__date_response_started :: text AS "CEBS_RESPONSEFORM_DATERESPONSESTARTED",
  cebs__response_form__outcome_of_response :: text AS "CEBS_RESPONSEFORM_OUTCOMEOFRESPONSE",
  cebs__response_form__date_of_report :: text AS "CEBS_RESPONSEFORM_DATEOFREPORT",
  cebs__response_form__additional_information :: text AS "CEBS_RESPONSEFORM_ADDITIONALINFORMATION",
  cebs__response_form__via :: text AS "CEBS_RESPONSEFORM_VIA",
  cebs__response_form__date_escalated :: timestamptz AS "CEBS_RESPONSEFORM_DATEESCALATED",
  {% if 'cebs__response_form__spot' in columns -%}
    cebs__response_form__spot :: text AS "CEBS_RESPONSEFORM_SPOT",
  {% else %}
    NULL AS "CEBS_RESPONSEFORM_SPOT",
  {% endif %}

  cebs__response_form__created_at :: timestamptz AS "CEBS_RESPONSEFORM_CREATEDAT",
  cebs__response_form__updated_at :: timestamptz AS "CEBS_RESPONSEFORM_UPDATEDAT",
  /* Summary Form */
  cebs__summary_form___id :: text AS "CEBS_SUMMARYFORM_ID",
  cebs__summary_form__user :: text AS "CEBS_SUMMARYFORM_USER",
  cebs__summary_form__via :: text AS "CEBS_SUMMARYFORM_VIA",
  cebs__summary_form__event_status :: text AS "CEBS_SUMMARYFORM_EVENTSTATUS",
  cebs__summary_form__cause :: text AS "CEBS_SUMMARYFORM_CAUSE",
  {% if 'cebs__summary_form__escalated_to' in columns -%}
    cebs__summary_form__escalated_to :: text AS "CEBS_SUMMARYFORM_ESCALATEDTO",
  {% else %}
    NULL AS "CEBS_SUMMARYFORM_SPOT",
  {% endif %}

  {% if 'cebs__summary_form__spot' in columns -%}
    cebs__summary_form__spot :: text AS "CEBS_SUMMARYFORM_SPOT",
  {% else %}
    NULL AS "CEBS_SUMMARYFORM_SPOT",
  {% endif %}

  cebs__summary_form__created_at :: timestamptz AS "CEBS_SUMMARYFORM_CREATEDAT",
  cebs__summary_form__updated_at :: timestamptz AS "CEBS_SUMMARYFORM_UPDATEDAT",
  /* Escalation Form */
  cebs__escalation_form___id :: text AS "CEBS_ESCALATIONFORM_ID",
  cebs__escalation_form__user :: text AS "CEBS_ESCALATIONFORM_USER",
  cebs__escalation_form__via :: text AS "CEBS_ESCALATIONFORM_VIA",
  cebs__escalation_form__event_type :: text AS "CEBS_ESCALATIONFORM_EVENTTYPE",
  cebs__escalation_form__date_response_started :: timestamptz AS "CEBS_ESCALATIONFORM_DATERESPONSESTARTED",
  cebs__escalation_form__reason :: text AS "CEBS_ESCALATIONFORM_REASON",
  cebs__escalation_form__reason_other :: text AS "CEBS_ESCALATIONFORM_REASONOTHER",
  cebs__response_form__date_escalated :: timestamptz AS "CEBS_ESCALATIONFORM_DATEESCALATED",
  {% if 'cebs__escalation_form__spot' in columns -%}
    cebs__escalation_form__spot :: text AS "CEBS_ESCALATIONFORM_SPOT",
  {% else %}
    NULL AS "CEBS_ESCALATIONFORM_SPOT",
  {% endif %}

  cebs__escalation_form__created_at :: timestamptz AS "CEBS_ESCALATIONFORM_CREATEDAT",
  cebs__escalation_form__updated_at :: timestamptz AS "CEBS_ESCALATIONFORM_UPDATEDAT",
  --- HEBS FORMS ---
  hebs__created_at :: timestamptz AS "HEBS_CREATEDAT",
  hebs__updated_at :: timestamptz AS "HEBS_UPDATEDAT",
  hebs__verification_form___id :: text AS "HEBS_VERIFICATIONFORM_ID",
  hebs__verification_form__user :: text AS "HEBS_VERIFICATIONFORM_USER",
  hebs__verification_form__source :: text AS "HEBS_VERIFICATIONFORM_SOURCE",
  hebs__verification_form__description :: text AS "HEBS_VERIFICATIONFORM_DESCRIPTION",
  hebs__verification_form__is_matching_signal :: text AS "HEBS_VERIFICATIONFORM_ISMATCHINGSIGNAL",
  hebs__verification_form__updated_signal :: text AS "HEBS_VERIFICATIONFORM_UPDATEDSIGNAL",
  hebs__verification_form__is_reported_before :: text AS "HEBS_VERIFICATIONFORM_ISREPORTEDBEFORE",
  hebs__verification_form__date_health_threat_started :: timestamptz AS "HEBS_VERIFICATIONFORM_DATEHEALTHTHREATSTARTED",
  hebs__verification_form__informant :: text AS "HEBS_VERIFICATIONFORM_INFORMANT",
  hebs__verification_form__additional_information :: text AS "HEBS_VERIFICATIONFORM_ADDITIONALINFORMATION",
  hebs__verification_form__date_verified :: timestamptz AS "HEBS_VERIFICATIONFORM_DATEVERIFIED",
  hebs__verification_form__is_threat_still_existing :: text AS "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
  hebs__verification_form__threat_to :: text AS "HEBS_VERIFICATIONFORM_THREATTO",
  hebs__verification_form__date_scdsc_informed :: text AS "HEBS_VERIFICATIONFORM_DATESCDSCINFORMED",
  hebs__verification_form__created_at :: timestamptz AS "HEBS_VERIFICATIONFORM_CREATEDAT",
  hebs__verification_form__updated_at :: timestamptz AS "HEBS_VERIFICATIONFORM_UPDATEDAT",
  hebs__verification_form__via :: text AS "HEBS_VERIFICATIONFORM_VIA",
  {% if 'hebs__verification_form__spot' in columns -%}
    hebs__verification_form__spot :: text AS "HEBS_VERIFICATIONFORM_SPOT",
  {% else %}
    NULL AS "HEBS_VERIFICATIONFORM_SPOT",
  {% endif %}
  /* Investigation Form */
  hebs__investigation_form___id :: text AS "HEBS_INVESTIGATIONFORM_ID",
  hebs__investigation_form__date_scdsc_informed :: text AS "HEBS_INVESTIGATIONFORM_DATESCDSCINFORMED",
  hebs__investigation_form__date_investigation_started :: text AS "HEBS_INVESTIGATIONFORM_DATEINVESTIGATIONSTARTED",
  hebs__investigation_form__date_event_started :: text AS "HEBS_INVESTIGATIONFORM_DATEEVENTSTARTED",
  hebs__investigation_form__symptoms :: text AS "HEBS_INVESTIGATIONFORM_SYMPTOMS",
  hebs__investigation_form__humans_cases :: INTEGER AS "HEBS_INVESTIGATIONFORM_HUMANSCASES",
  hebs__investigation_form__humans_dead :: INTEGER AS "HEBS_INVESTIGATIONFORM_HUMANSDEAD",
  hebs__investigation_form__humans_cases_hospitalized :: INTEGER AS "HEBS_INVESTIGATIONFORM_HUMANSCASESHOSPITALIZED",
  hebs__investigation_form__animals_cases :: INTEGER AS "HEBS_INVESTIGATIONFORM_ANIMALSCASES",
  hebs__investigation_form__animals_dead :: INTEGER AS "HEBS_INVESTIGATIONFORM_ANIMALSDEAD",
  hebs__investigation_form__is_cause_known :: text AS "HEBS_INVESTIGATIONFORM_ISCAUSEKNOWN",
  hebs__investigation_form__is_lab_samples_collected :: text AS "HEBS_INVESTIGATIONFORM_ISLABSAMPLESCOLLECTED",
  {% if 'hebs__investigation_form__is_new_cased_reported_from_initial_area' in columns -%}
    hebs__investigation_form__is_new_cased_reported_from_initial_area :: text AS "HEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
    {% elif 'hebs__investigation_form__is_ybjriad_reported_from_initial_area' in columns %}
    hebs__investigation_form__is_ybjriad_reported_from_initial_area :: text AS "HEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% else %}
    NULL AS "HEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMINITIALAREA",
  {% endif %}

  hebs__investigation_form__is_new_cased_reported_from_new_areas :: text AS "HEBS_INVESTIGATIONFORM_ISNEWCASEDREPORTEDFROMNEWAREAS",
  hebs__investigation_form__is_event_setting_promoting_spread :: text AS "HEBS_INVESTIGATIONFORM_ISEVENTSETTINGPROMOTINGSPREAD",
  hebs__investigation_form__additional_information :: text AS "HEBS_INVESTIGATIONFORM_ADDITIONALINFORMATION",
  hebs__investigation_form__risk_classification :: text AS "HEBS_INVESTIGATIONFORM_RISKCLASSIFICATION",
  hebs__investigation_form__is_event_infectious :: text AS "HEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
  hebs__investigation_form__date_scmoh_informed :: timestamptz AS "HEBS_INVESTIGATIONFORM_DATESCMOHINFORMED",
  hebs__investigation_form__created_at :: timestamptz AS "HEBS_INVESTIGATIONFORM_CREATEDAT",
  hebs__investigation_form__updated_at :: timestamptz AS "HEBS_INVESTIGATIONFORM_UPDATEDAT",
  hebs__investigation_form__user :: text AS "HEBS_INVESTIGATIONFORM_USER",
  hebs__investigation_form__via :: text AS "HEBS_INVESTIGATIONFORM_VIA",
  {% if 'hebs__investigation_form__spot' in columns -%}
    hebs__investigation_form__spot :: text AS "HEBS_INVESTIGATIONFORM_SPOT",
  {% else %}
    NULL AS "HEBS_INVESTIGATIONFORM_SPOT",
  {% endif %}
  /* Response Form */
  hebs__response_form___id :: text AS "HEBS_RESPONSEFORM_ID",
  hebs__response_form__user :: text AS "HEBS_RESPONSEFORM_USER",
  hebs__response_form__event_type :: text AS "HEBS_RESPONSEFORM_EVENTTYPE",
  hebs__response_form__date_scmoh_informed :: text AS "HEBS_RESPONSEFORM_DATESCMOHINFORMED",
  hebs__response_form__date_response_started :: text AS "HEBS_RESPONSEFORM_DATERESPONSESTARTED",
  hebs__response_form__outcome_of_response :: text AS "HEBS_RESPONSEFORM_OUTCOMEOFRESPONSE",
  hebs__response_form__date_of_report :: text AS "HEBS_RESPONSEFORM_DATEOFREPORT",
  hebs__response_form__additional_information :: text AS "HEBS_RESPONSEFORM_ADDITIONALINFORMATION",
  hebs__response_form__via :: text AS "HEBS_RESPONSEFORM_VIA",
  hebs__response_form__date_escalated :: timestamptz AS "HEBS_RESPONSEFORM_DATEESCALATED",
  {% if 'hebs__response_form__spot' in columns -%}
    hebs__response_form__spot :: text AS "HEBS_RESPONSEFORM_SPOT",
  {% else %}
    NULL AS "HEBS_RESPONSEFORM_SPOT",
  {% endif %}

  hebs__response_form__created_at :: timestamptz AS "HEBS_RESPONSEFORM_CREATEDAT",
  hebs__response_form__updated_at :: timestamptz AS "HEBS_RESPONSEFORM_UPDATEDAT",
  /* Summary Form */
  hebs__summary_form___id :: text AS "HEBS_SUMMARYFORM_ID",
  hebs__summary_form__user :: text AS "HEBS_SUMMARYFORM_USER",
  hebs__summary_form__via :: text AS "HEBS_SUMMARYFORM_VIA",
  hebs__summary_form__event_status :: text AS "HEBS_SUMMARYFORM_EVENTSTATUS",
  hebs__summary_form__cause :: text AS "HEBS_SUMMARYFORM_CAUSE",
  {% if 'hebs__summary_form__escalated_to' in columns -%}
    hebs__summary_form__escalated_to :: text AS "HEBS_SUMMARYFORM_ESCALATEDTO",
  {% else %}
    NULL AS "HEBS_SUMMARYFORM_SPOT",
  {% endif %}

  {% if 'hebs__summary_form__spot' in columns -%}
    hebs__summary_form__spot :: text AS "HEBS_SUMMARYFORM_SPOT",
  {% else %}
    NULL AS "HEBS_SUMMARYFORM_SPOT",
  {% endif %}

  hebs__summary_form__created_at :: timestamptz AS "HEBS_SUMMARYFORM_CREATEDAT",
  hebs__summary_form__updated_at :: timestamptz AS "HEBS_SUMMARYFORM_UPDATEDAT",
  /* Escalation Form */
  hebs__escalation_form___id :: text AS "HEBS_ESCALATIONFORM_ID",
  hebs__escalation_form__user :: text AS "HEBS_ESCALATIONFORM_USER",
  hebs__escalation_form__via :: text AS "HEBS_ESCALATIONFORM_VIA",
  hebs__escalation_form__event_type :: text AS "HEBS_ESCALATIONFORM_EVENTTYPE",
  hebs__escalation_form__date_response_started :: timestamptz AS "HEBS_ESCALATIONFORM_DATERESPONSESTARTED",
  hebs__escalation_form__reason :: text AS "HEBS_ESCALATIONFORM_REASON",
  hebs__escalation_form__reason_other :: text AS "HEBS_ESCALATIONFORM_REASONOTHER",
  hebs__response_form__date_escalated :: timestamptz AS "HEBS_ESCALATIONFORM_DATEESCALATED",
  {% if 'hebs__escalation_form__spot' in columns -%}
    hebs__escalation_form__spot :: text AS "HEBS_ESCALATIONFORM_SPOT",
  {% else %}
    NULL AS "HEBS_ESCALATIONFORM_SPOT",
  {% endif %}

  hebs__escalation_form__created_at :: timestamptz AS "HEBS_ESCALATIONFORM_CREATEDAT",
  hebs__escalation_form__updated_at :: timestamptz AS "HEBS_ESCALATIONFORM_UPDATEDAT"
FROM
  {{ source(
    'central_raw_mdharura',
    'tasks'
  ) }}
