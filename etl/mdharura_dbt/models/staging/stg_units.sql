select 
  _id::text as "_ID",
  -- _status::text as "_STATUS",
  state::text as "STATE",
  name::text as "NAME",
  type::text as "TYPE",
  code::text as "CODE",
  uid::text as "UID",
  parent::text as "PARENT",
  date_last_reported__test::timestamptz as "DATELASTREPORTED_TEST",
  date_last_reported__live::timestamptz as "DATELASTREPORTED_LIVE",
  date_last_reported__created_at::timestamptz as "DATELASTREPORTED_CREATEDAT",
  date_last_reported__updated_at::timestamptz as "DATELASTREPORTED_UPDATEDAT",
  created_at::timestamptz as "CREATEDAT",
  updated_at::timestamptz as "UPDATEDAT"

from {{ source('central_raw_mdharura', 'units') }}