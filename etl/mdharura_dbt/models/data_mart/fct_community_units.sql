-- TODO: Cater for unset community units with valid subcounties. i.e community units created manually from mdharura app
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
  LEFT JOIN {{ ref('dim_community_units') }} AS community_units
  ON community_units.uid = units."UID"
WHERE
  units."TYPE" = 'Community unit'
