-- TODO: Cater for unset community units with valid subcounties. i.e community units created manually from mdharura app
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
  LEFT JOIN {{ ref('dim_health_facilities') }} AS health_facilities
  ON health_facilities.uid = units."UID"
WHERE
  units."TYPE" = 'Health facility'
