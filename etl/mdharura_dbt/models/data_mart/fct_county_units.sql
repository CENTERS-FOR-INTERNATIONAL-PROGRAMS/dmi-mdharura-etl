SELECT
  COALESCE(
    county.county_key,
    'unset'
  ) AS "COUNTY_KEY",
  units.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_units') }} AS units
  LEFT JOIN {{ ref('dim_county') }} AS county
  ON CONCAT(
    county.county,
    ' ',
    'County'
  ) = units."NAME"
WHERE
  units."TYPE" = 'County'
