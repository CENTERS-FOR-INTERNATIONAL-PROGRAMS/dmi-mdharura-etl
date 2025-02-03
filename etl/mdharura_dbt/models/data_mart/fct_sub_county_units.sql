{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_county_key ON {{this}} USING btree ("COUNTY_KEY");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_id ON {{this}} USING btree ("_ID");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_parent ON {{this}} USING btree ("PARENT");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_uid ON {{this}} USING btree ("UID");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_code ON {{this}} USING btree ("CODE");',
      'CREATE INDEX IF NOT EXISTS idx_fct_scounty_type ON {{this}} USING btree ("TYPE");'
    ]
) }}


SELECT
  COALESCE(
    subcounty.county_key,
    'unset'
  ) AS "COUNTY_KEY",
  COALESCE(
    subcounty.sub_county_key,
    'unset'
  ) AS "SUB_COUNTY_KEY",
  units.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS "LOAD_DATE"
FROM
  {{ ref('stg_units') }} AS units
  LEFT JOIN {{ ref('dim_sub_county') }} AS subcounty
  ON CONCAT(
    subcounty.sub_county,
    ' ',
    'Sub County'
  ) = CASE
    WHEN units."NAME" = CONCAT(
      'Buuri',
      ' ',
      ' ',
      'East Sub County'
    ) THEN 'Buuri East Sub County'
    WHEN units."NAME" = 'Suguta Sub county' THEN 'Suguta Sub County'
    ELSE units."NAME"
  END
WHERE
  units."TYPE" = 'Subcounty'
