WITH recursive units_hierarchy AS (
  -- Base case: community unit matches the official community unit
  SELECT
    units.*,
    community_units.county_key,
    community_units.sub_county_key,
    community_units.community_unit_key
  FROM
    {{ ref('stg_units') }} AS units
    LEFT JOIN {{ ref('dim_community_units') }} AS community_units
    ON community_units.uid = units."UID"
  WHERE
    units."TYPE" = 'Community unit'
    AND units."UID" IS NOT NULL
  UNION ALL
    -- Recursive case: community unit does not match the official community unit
  SELECT
    m_units.*,
    subcounty_units."COUNTY_KEY" AS county_key,
    subcounty_units."SUB_COUNTY_KEY" AS sub_county_key,
    'unset' AS community_unit_key
  FROM
    {{ ref('stg_units') }} AS m_units
    LEFT JOIN {{ ref('fct_sub_county_units') }} AS subcounty_units
    ON subcounty_units."_ID" = m_units."PARENT"
  WHERE
    m_units."TYPE" = 'Community unit'
    AND m_units."UID" IS NULL
)
SELECT
  COALESCE(
    community_units.county_key,
    'unset'
  ) AS "COUNTY_KEY",
  COALESCE(
    community_units.sub_county_key,
    'unset'
  ) AS "SUB_COUNTY_KEY",
  COALESCE(
    community_units.community_unit_key,
    'unset'
  ) AS "COMMUNITY_UNIT_KEY",
  units.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_units') }} AS units
  LEFT JOIN units_hierarchy AS community_units
  ON community_units."_ID" = units."_ID"
WHERE
  units."TYPE" = 'Community unit'
