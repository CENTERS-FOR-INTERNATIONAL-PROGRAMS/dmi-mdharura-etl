{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_scounty_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}


select
    coalesce(subcounty.county_key, 'unset') as "COUNTY_KEY",
    coalesce(subcounty.sub_county_key, 'unset') as "SUB_COUNTY_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join
    {{ ref("dim_sub_county") }} as subcounty
    on concat(subcounty.sub_county, ' ', 'Sub County') = case
        when units."NAME" = concat('Buuri', ' ', ' ', 'East Sub County')
        then 'Buuri East Sub County'
        when units."NAME" = 'Suguta Sub county'
        then 'Suguta Sub County'
        else units."NAME"
    end
where units."TYPE" = 'Subcounty'
