{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_stg_units_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_stg_units_name ON {{this}} USING btree ("NAME");',
            'CREATE INDEX IF NOT EXISTS idx_stg_units_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_stg_units_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_stg_units_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_stg_units_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}


{%- set columns = dbt_utils.get_filtered_columns_in_relation(
    from=source("central_raw_mdharura", "units")
) -%}
select
    _id::text as "_ID",
    -- _status::text as "_STATUS",
    state::text as "STATE",
    name::text as "NAME",
    type::text as "TYPE",
    code::text as "CODE",
    uid::text as "UID",
    parent::text as "PARENT",
    {{ column_exists("date_last_reported__test", columns, "timestamptz", "NULL") }}
    as "DATELASTREPORTED_TEST",
    {{ column_exists("date_last_reported__live", columns, "timestamptz", "NULL") }}
    as "DATELASTREPORTED_LIVE",
    {{
        column_exists(
            "date_last_reported__created_at", columns, "timestamptz", "NULL"
        )
    }} as "DATELASTREPORTED_CREATEDAT",
    {{
        column_exists(
            "date_last_reported__updated_at", columns, "timestamptz", "NULL"
        )
    }} as "DATELASTREPORTED_UPDATEDAT",
    created_at::timestamptz as "CREATEDAT",
    updated_at::timestamptz as "UPDATEDAT"
from {{ source("central_raw_mdharura", "units") }}
