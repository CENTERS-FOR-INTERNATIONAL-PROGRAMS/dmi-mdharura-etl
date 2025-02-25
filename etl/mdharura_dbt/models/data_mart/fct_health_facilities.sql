{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_key ON {{this}} USING btree ("HEALTH_FACILITY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_hf_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}


with recursive
    units_hierarchy as (
        -- Base case: community unit matches the official community unit
        select
            units.*,
            health_facilities.county_key,
            health_facilities.sub_county_key,
            health_facilities.health_facility_key
        from {{ ref("stg_units") }} as units
        left join
            {{ ref("dim_health_facilities") }} as health_facilities
            on health_facilities.uid = units."UID"
        where units."TYPE" = 'Health facility' and units."UID" is not null
        union all
        -- Recursive case: community unit does not match the official community unit
        select
            m_units.*,
            subcounty_units."COUNTY_KEY" as county_key,
            subcounty_units."SUB_COUNTY_KEY" as sub_county_key,
            'unset' as health_facility_key
        from {{ ref("stg_units") }} as m_units
        left join
            {{ ref("fct_sub_county_units") }} as subcounty_units
            on subcounty_units."_ID" = m_units."PARENT"
        where m_units."TYPE" = 'Health facility' and m_units."UID" is null
    )
select
    coalesce(health_facilities.county_key, 'unset') as "COUNTY_KEY",
    coalesce(health_facilities.sub_county_key, 'unset') as "SUB_COUNTY_KEY",
    coalesce(health_facilities.health_facility_key, 'unset') as "HEALTH_FACILITY_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join units_hierarchy as health_facilities on health_facilities."_ID" = units."_ID"
where units."TYPE" = 'Health facility'
