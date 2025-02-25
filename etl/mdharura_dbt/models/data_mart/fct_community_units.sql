{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_key ON {{this}} USING btree ("COMMUNITY_UNIT_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_cu_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}

with recursive
    units_hierarchy as (
        -- Base case: community unit matches the official community unit
        select
            units.*,
            community_units.county_key,
            community_units.sub_county_key,
            community_units.community_unit_key
        from {{ ref("stg_units") }} as units
        left join
            {{ ref("dim_community_units") }} as community_units
            on community_units.uid = units."UID"
        where units."TYPE" = 'Community unit' and units."UID" is not null
        union all
        -- Recursive case: community unit does not match the official community unit
        select
            m_units.*,
            subcounty_units."COUNTY_KEY" as county_key,
            subcounty_units."SUB_COUNTY_KEY" as sub_county_key,
            'unset' as community_unit_key
        from {{ ref("stg_units") }} as m_units
        left join
            {{ ref("fct_sub_county_units") }} as subcounty_units
            on subcounty_units."_ID" = m_units."PARENT"
        where m_units."TYPE" = 'Community unit' and m_units."UID" is null
    )
select
    coalesce(community_units.county_key, 'unset') as "COUNTY_KEY",
    coalesce(community_units.sub_county_key, 'unset') as "SUB_COUNTY_KEY",
    coalesce(community_units.community_unit_key, 'unset') as "COMMUNITY_UNIT_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join units_hierarchy as community_units on community_units."_ID" = units."_ID"
where units."TYPE" = 'Community unit'
