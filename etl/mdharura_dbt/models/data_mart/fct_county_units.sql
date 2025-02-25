{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_county_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_county_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_county_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_county_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_county_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}

select
    coalesce(county.county_key, 'unset') as "COUNTY_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join
    {{ ref("dim_county") }} as county
    on concat(county.county, ' ', 'County') = units."NAME"
where units."TYPE" = 'County'
