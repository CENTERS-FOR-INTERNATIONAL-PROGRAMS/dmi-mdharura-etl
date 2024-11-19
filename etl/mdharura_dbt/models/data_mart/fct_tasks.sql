SELECT
  dim_date.date_key AS "DATE_KEY",
  COALESCE(
    community_unit."COUNTY_KEY",
    'unset'
  ) AS "COUNTY_KEY",
  COALESCE(
    community_unit."SUB_COUNTY_KEY",
    'unset'
  ) AS "SUB_COUNTY_KEY",
  COALESCE(
    community_unit."COMMUNITY_UNIT_KEY",
    'unset'
  ) AS "COMMUNITY_UNIT_KEY",
  tasks.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_tasks') }} AS tasks
  LEFT JOIN {{ ref('dim_date') }} AS dim_date
  ON dim_date.date = tasks."CREATEDAT" :: DATE
  LEFT JOIN {{ ref('fct_community_units') }} AS community_unit
  ON community_unit."_ID" = tasks."UNIT_ID"
