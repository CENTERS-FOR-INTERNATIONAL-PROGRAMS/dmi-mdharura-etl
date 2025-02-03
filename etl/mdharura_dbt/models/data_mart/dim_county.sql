{{ config(
  post_hook = 
    [
      'CREATE INDEX IF NOT EXISTS idx_dim_county_name ON {{this}} USING btree ("county");',
      'CREATE INDEX IF NOT EXISTS idx_dim_county_key ON {{this}} USING btree ("county_key");'
    ]
) }}


WITH county_iso_code_source AS (
  SELECT
    DISTINCT county,
    county_iso_code
  FROM
    {{ ref('county_sub_county_iso_codes') }}
),
source_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(
      ['county_source.county']
    ) }} AS county_key,
    county_source.*,
    county_iso_code_source.county_iso_code AS iso_code
  FROM
    {{ ref('kenya_counties') }} AS county_source
    LEFT JOIN county_iso_code_source
    ON county_iso_code_source.county = CONCAT(
      county_source.county,
      ' ',
      'County'
    )
  UNION
  SELECT
    'unset' AS county_key,
    'unset' AS county,
    'unset' AS code,
    -999 AS area_sqkm,
    'unset' AS iso_code
)
SELECT
  source_data.*,
  CAST(
    CURRENT_DATE AS DATE
  ) AS load_date
FROM
  source_data
