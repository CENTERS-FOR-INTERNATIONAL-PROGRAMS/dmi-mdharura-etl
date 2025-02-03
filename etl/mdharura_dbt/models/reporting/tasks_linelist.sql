SELECT
  dim_date.date AS "DATE",
  dim_epi_week.week_number AS "EPI_WEEK",
  dim_epi_week.year AS "YEAR",
  county."NAME" AS "COUNTY",
  county."UID" AS "COUNTY_UID",
  county."_ID" AS "COUNTY_ID",
  sub_county."NAME" AS "SUB_COUNTY",
  sub_county."UID" AS "SUB_COUNTY_UID",
  sub_county."_ID" AS "SUB_COUNTY_ID",
  COALESCE(
    community_unit."NAME",
    health_facility."NAME",
    sub_county."NAME",
    'unset'
  ) AS "UNIT_NAME",
  COALESCE(
    community_unit."TYPE",
    health_facility."TYPE",
    sub_county."TYPE",
    'unset'
  ) AS "UNIT_TYPE",
  COALESCE(
    community_unit."UID",
    health_facility."UID",
    sub_county."UID",
    'unset'
  ) AS "UNIT_UID",
  tasks.*
FROM
  {{ ref('fct_tasks') }} AS tasks
  LEFT JOIN {{ ref('dim_date') }} AS dim_date
  ON dim_date.date_key = tasks."DATE_KEY"
  LEFT JOIN {{ ref('dim_epi_week') }} AS dim_epi_week
  ON dim_epi_week.epi_week_key = tasks."EPI_WEEK_KEY"
  LEFT JOIN {{ ref('fct_county_units') }} AS county
  ON county."COUNTY_KEY" = tasks."COUNTY_KEY"
  LEFT JOIN {{ ref('fct_sub_county_units') }} AS sub_county
  ON sub_county."SUB_COUNTY_KEY" = tasks."SUB_COUNTY_KEY"
  LEFT JOIN {{ ref('fct_community_units') }} AS community_unit
  ON community_unit."COMMUNITY_UNIT_KEY" = tasks."UNIT_KEY"
  AND community_unit."_ID" = tasks."UNIT_ID"
  LEFT JOIN {{ ref('fct_health_facilities') }} AS health_facility
  ON health_facility."HEALTH_FACILITY_KEY" = tasks."UNIT_KEY"
  AND health_facility."_ID" = tasks."UNIT_ID"
