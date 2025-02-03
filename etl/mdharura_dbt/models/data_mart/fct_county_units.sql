{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_fct_county_key ON {{this}} USING btree ("COUNTY_KEY");',
      'CREATE INDEX IF NOT EXISTS idx_fct_county_id ON {{this}} USING btree ("_ID");',
      'CREATE INDEX IF NOT EXISTS idx_fct_county_parent ON {{this}} USING btree ("PARENT");',
      'CREATE INDEX IF NOT EXISTS idx_fct_county_uid ON {{this}} USING btree ("UID");',
      'CREATE INDEX IF NOT EXISTS idx_fct_county_code ON {{this}} USING btree ("CODE");',
      'CREATE INDEX IF NOT EXISTS idx_fct_county_type ON {{this}} USING btree ("TYPE");'
    ]
) }}

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
