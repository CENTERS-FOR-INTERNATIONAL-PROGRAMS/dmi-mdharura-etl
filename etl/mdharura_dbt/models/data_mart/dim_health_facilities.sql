{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_uid ON {{this}} USING btree ("uid");',
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_name ON {{this}} USING btree ("name");',
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_county_key ON {{this}} USING btree ("county_key");',
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_subcounty_key ON {{this}} USING btree ("sub_county_key");',
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_key ON {{this}} USING btree ("health_facility_key");',
      'CREATE INDEX IF NOT EXISTS idx_dim_hf_code ON {{this}} USING btree ("code");'
    ]
) }}


WITH source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(
      ['units_source.uid']
    ) }} AS health_facility_key,
    units_source.uid AS "uid",
    units_source.facility AS "name",
    units_source.mflcode AS code,
    dim_county.county_key,
    dim_sub_county.sub_county_key
  FROM
    {{ ref('kenya_health_facilities') }} AS units_source
    LEFT JOIN {{ ref('dim_county') }} AS dim_county
    ON CONCAT(
      dim_county.county,
      ' ',
      'County'
    ) = units_source.county
    LEFT JOIN {{ ref('dim_sub_county') }} AS dim_sub_county
    ON CONCAT(
      dim_sub_county.sub_county,
      ' ',
      'Sub County'
    ) = CASE
      WHEN units_source.subcounty = CONCAT(
        'Buuri',
        ' ',
        ' ',
        'East Sub County'
      ) THEN 'Buuri East Sub County'
      WHEN units_source.subcounty = 'Suguta Sub county' THEN 'Suguta Sub County'
      ELSE units_source.subcounty
    END
  UNION
  SELECT
    'unset' AS "uid",
    'unset' AS "name",
    'unset' AS code,
    'unset' AS county_key,
    'unset' AS sub_county_key,
    'unset' AS health_facility_key
)
SELECT
  source_data.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS load_date
FROM
  source_data
