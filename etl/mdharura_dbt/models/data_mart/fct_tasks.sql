SELECT
  dim_date.date_key AS "DATE_KEY",
  COALESCE(
    dim_epi_week.epi_week_key,
    'unset'
  ) AS "EPI_WEEK_KEY",
  COALESCE(
    community_unit."COUNTY_KEY",
    health_facility."COUNTY_KEY",
    'unset'
  ) AS "COUNTY_KEY",
  COALESCE(
    community_unit."SUB_COUNTY_KEY",
    health_facility."SUB_COUNTY_KEY",
    'unset'
  ) AS "SUB_COUNTY_KEY",
  COALESCE(
    community_unit."COMMUNITY_UNIT_KEY",
    health_facility."HEALTH_FACILITY_KEY",
    'unset'
  ) AS "UNIT_KEY",
  tasks.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_tasks') }} AS tasks
  LEFT JOIN {{ ref('dim_date') }} AS dim_date
  ON dim_date.date = tasks."CREATEDAT" :: DATE
  LEFT JOIN {{ ref('dim_epi_week') }} AS dim_epi_week
  ON tasks."CREATEDAT" :: DATE >= dim_epi_week.start_of_week
  AND tasks."CREATEDAT" :: DATE <= dim_epi_week.end_of_week
  LEFT JOIN {{ ref('fct_community_units') }} AS community_unit
  ON community_unit."_ID" = tasks."UNIT_ID"
  LEFT JOIN {{ ref('fct_health_facilities') }} AS health_facility
  ON health_facility."_ID" = tasks."UNIT_ID"
