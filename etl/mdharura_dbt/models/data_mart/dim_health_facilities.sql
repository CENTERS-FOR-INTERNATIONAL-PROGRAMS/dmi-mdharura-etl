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
