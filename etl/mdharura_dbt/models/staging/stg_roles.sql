select
    _id::text as "_ID",
    status::text as "STATUS",
    "user"::text as "USER",
    spot::text as "SPOT",
    unit::text as "UNIT",

    date_last_reported__test::timestamptz as "DATELASTREPORTED_TEST",
    date_last_reported__live::timestamptz as "DATELASTREPORTED_LIVE",
    date_last_reported__created_at::timestamptz as "DATELASTREPORTED_CREATEDAT",
    date_last_reported__updated_at::timestamptz as "DATELASTREPORTED_UPDATEDAT",

    date_last_verified__test::timestamptz as "DATELASTVERIFIED_TEST",
    date_last_verified__live::timestamptz as "DATELASTVERIFIED_LIVE",
    date_last_verified__created_at::timestamptz as "DATELASTVERIFIED_CREATEDAT",
    date_last_verified__updated_at::timestamptz as "DATELASTVERIFIED_UPDATEDAT",

    created_at::timestamptz as "CREATEDAT",
    updated_at::timestamptz as "UPDATEDAT"

from {{ source("central_raw_mdharura", "roles") }}
