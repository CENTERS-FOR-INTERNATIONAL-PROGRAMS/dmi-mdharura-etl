WITH recursive units_hierarchy AS (
  -- Base case: community unit matches the official community unit
  SELECT
    units.*,
    health_facilities.county_key,
    health_facilities.sub_county_key,
    health_facilities.health_facility_key
  FROM
    {{ ref('stg_units') }} AS units
    LEFT JOIN {{ ref('dim_health_facilities') }} AS health_facilities
    ON health_facilities.uid = units."UID"
  WHERE
    units."TYPE" = 'Health facility'
    AND units."UID" IS NOT NULL
  UNION ALL
    -- Recursive case: community unit does not match the official community unit
  SELECT
    m_units.*,
    subcounty_units."COUNTY_KEY" AS county_key,
    subcounty_units."SUB_COUNTY_KEY" AS sub_county_key,
    'unset' AS health_facility_key
  FROM
    {{ ref('stg_units') }} AS m_units
    LEFT JOIN {{ ref('fct_sub_county_units') }} AS subcounty_units
    ON subcounty_units."_ID" = m_units."PARENT"
  WHERE
    m_units."TYPE" = 'Health facility'
    AND m_units."UID" IS NULL
)
SELECT
  COALESCE(
    health_facilities.county_key,
    'unset'
  ) AS "COUNTY_KEY",
  COALESCE(
    health_facilities.sub_county_key,
    'unset'
  ) AS "SUB_COUNTY_KEY",
  COALESCE(
    health_facilities.health_facility_key,
    'unset'
  ) AS "HEALTH_FACILITY_KEY",
  units.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_units') }} AS units
  LEFT JOIN units_hierarchy AS health_facilities
  ON health_facilities."_ID" = units."_ID"
WHERE
  units."TYPE" = 'Health facility'
