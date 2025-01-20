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
    'unset'
  ) AS "UNIT_NAME",
  COALESCE(
    community_unit."TYPE",
    health_facility."TYPE",
    'unset'
  ) AS "UNIT_TYPE",
  COALESCE(
    community_unit."UID",
    health_facility."UID",
    'unset'
  ) AS "UNIT_UID",
  COALESCE(
    community_unit."_ID",
    health_facility."_ID",
    'unset'
  ) AS "UNIT_ID",
  -- community_unit."UID" AS "UNIT_UID",
  -- community_unit."_ID" AS "UNIT_ID",
  SUM(
    CASE
      WHEN "SIGNAL" IN(
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        'h1',
        'h2',
        'h3',
        'v1',
        'v2',
        'l1',
        'l2',
        'l3',
        'p1',
        'p2',
        'p3',
        'm1',
        'm2',
        'm3'
      ) THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_REPORTED",
  SUM(
    CASE
      WHEN "CEBS_VERIFICATIONFORM_ID" IS NOT NULL
      OR "HEBS_VERIFICATIONFORM_ID" IS NOT NULL
      OR "VEBS_VERIFICATIONFORM_ID" IS NOT NULL -- OR "LEBS_VERIFICATIONFORM_ID" IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_VERIFIED",
  SUM(
    CASE
      WHEN "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      )
      OR "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      )
      OR "VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      ) -- OR "LEBS_VERIFICATIONFORM_ISSTILLHAPPENING" IN (
      --   'Yes',
      --   'yes'
      -- )
      THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_VERIFIED_TRUE",
  -- CEBS Signals --
  SUM(
    CASE
      WHEN "CEBS_INVESTIGATIONFORM_ID" IS NOT NULL
      OR "HEBS_INVESTIGATIONFORM_ID" IS NOT NULL
      OR "VEBS_INVESTIGATIONFORM_ID" IS NOT NULL -- OR "LEBS_INVESTIGATIONFORM_ID" IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_RISK_ASSESSED",
  SUM(
    CASE
      WHEN "CEBS_RESPONSEFORM_ID" IS NOT NULL
      OR "HEBS_RESPONSEFORM_ID" IS NOT NULL
      OR "VEBS_RESPONSEFORM_ID" IS NOT NULL -- OR "LEBS_RESPONSEFORM_ID" IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_RESPONDED",
  SUM(
    CASE
      WHEN "CEBS_ESCALATIONFORM_ID" IS NOT NULL
      OR "HEBS_ESCALATIONFORM_ID" IS NOT NULL
      OR "VEBS_ESCALATIONFORM_ID" IS NOT NULL -- OR "LEBS_ESCALATIONFORM_ID" IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS "SIGNALS_ESCALATED",
  SUM(
    CASE
      WHEN "SIGNAL" IN(
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7'
      ) THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_REPORTED",
  SUM(
    CASE
      WHEN "CEBS_VERIFICATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_VERIFIED",
  SUM(
    CASE
      WHEN "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      ) THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_VERIFIED_TRUE",
  SUM(
    CASE
      WHEN "CEBS_INVESTIGATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_RISK_ASSESSED",
  SUM(
    CASE
      WHEN "CEBS_RESPONSEFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_RESPONDED",
  SUM(
    CASE
      WHEN "CEBS_ESCALATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "CEBS_SIGNALS_ESCALATED",
  -- HEBS Signals ---
  SUM(
    CASE
      WHEN "SIGNAL" IN(
        'h1',
        'h2',
        'h3'
      ) THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_REPORTED",
  SUM(
    CASE
      WHEN "HEBS_VERIFICATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_VERIFIED",
  SUM(
    CASE
      WHEN "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      ) THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_VERIFIED_TRUE",
  SUM(
    CASE
      WHEN "HEBS_INVESTIGATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_RISK_ASSESSED",
  SUM(
    CASE
      WHEN "HEBS_RESPONSEFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_RESPONDED",
  SUM(
    CASE
      WHEN "HEBS_ESCALATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "HEBS_SIGNALS_ESCALATED",
  -- VEBS Signals ---
  SUM(
    CASE
      WHEN "SIGNAL" IN(
        'v1',
        'v2'
      ) THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_REPORTED",
  SUM(
    CASE
      WHEN "VEBS_VERIFICATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_VERIFIED",
  SUM(
    CASE
      WHEN "VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" IN (
        'Yes',
        'yes'
      ) THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_VERIFIED_TRUE",
  SUM(
    CASE
      WHEN "VEBS_INVESTIGATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_RISK_ASSESSED",
  SUM(
    CASE
      WHEN "VEBS_RESPONSEFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_RESPONDED",
  SUM(
    CASE
      WHEN "VEBS_ESCALATIONFORM_ID" IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS "VEBS_SIGNALS_ESCALATED"
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
WHERE
  tasks."STATE" = 'live'
GROUP BY
  dim_date.date,
  dim_epi_week.week_number,
  dim_epi_week.year,
  county."NAME",
  county."UID",
  county."_ID",
  sub_county."NAME",
  sub_county."UID",
  sub_county."_ID",
  community_unit."NAME",
  community_unit."UID",
  community_unit."_ID",
  community_unit."TYPE",
  health_facility."NAME",
  health_facility."UID",
  health_facility."_ID",
  health_facility."TYPE"
