{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_dim_sub_county_name ON {{this}} USING btree ("sub_county");',
      'CREATE INDEX IF NOT EXISTS idx_dim_sub_county_key ON {{this}} USING btree ("sub_county_key");',
      'CREATE INDEX IF NOT EXISTS idx_dim_sub_ccounty_key ON {{this}} USING btree ("county_key");'
    ]
) }}


WITH sub_county_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['sub_county_source.unit_name']) }} AS sub_county_key,
    SUBSTRING(
      sub_county_source.unit_name
      FROM
        '^(.*?) Sub County'
    ) AS sub_county,
    sub_county_source.unit_code AS sub_county_unit_code,
    iso_codes.sub_county_iso_code AS iso_code,
    dim_county.county_key
  FROM
    {{ ref('kenya_sub_counties') }} AS sub_county_source
    LEFT JOIN {{ ref('dim_county') }} AS dim_county
    ON CONCAT(
      dim_county.county,
      ' ',
      'County'
    ) = sub_county_source.county
    LEFT JOIN {{ ref('county_sub_county_iso_codes') }} AS iso_codes
    ON iso_codes.sub_county = sub_county_source.unit_name
  UNION
  SELECT
    'unset' AS sub_county_key,
    'unset' AS sub_county,
    'unset' AS sub_county_unit_code,
    'unset' AS iso_code,
    'unset' AS county_key
)
SELECT
  sub_county_data.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS load_date
FROM
  sub_county_data
