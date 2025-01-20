{%- set columns = dbt_utils.get_filtered_columns_in_relation(
  from = source(
    'central_raw_mdharura',
    'units'
  )
) -%}
SELECT
  _id :: text AS "_ID",
  -- _status::text as "_STATUS",
  state :: text AS "STATE",
  NAME :: text AS "NAME",
  TYPE :: text AS "TYPE",
  code :: text AS "CODE",
  UID :: text AS "UID",
  PARENT :: text AS "PARENT",
  ---
  {{ column_exists(
    "date_last_reported__test",
    columns,
    'timestamptz',
    'NULL'
  ) }} AS "DATELASTREPORTED_TEST",
  ---
  {{ column_exists(
    "date_last_reported__live",
    columns,
    'timestamptz',
    'NULL'
  ) }} AS "DATELASTREPORTED_LIVE",
  ---
  {{ column_exists(
    "date_last_reported__created_at",
    columns,
    'timestamptz',
    'NULL'
  ) }} AS "DATELASTREPORTED_CREATEDAT",
  ---
  {{ column_exists(
    "date_last_reported__updated_at",
    columns,
    'timestamptz',
    'NULL'
  ) }} AS "DATELASTREPORTED_UPDATEDAT",
  ---
  created_at :: timestamptz AS "CREATEDAT",
  updated_at :: timestamptz AS "UPDATEDAT"
FROM
  {{ source(
    'central_raw_mdharura',
    'units'
  ) }}
