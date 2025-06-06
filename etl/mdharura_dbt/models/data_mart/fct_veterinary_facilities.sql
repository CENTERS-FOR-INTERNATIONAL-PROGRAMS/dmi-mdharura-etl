{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_key ON {{this}} USING btree ("VETERINARY_FACILITY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_vf_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}

with
    units_hierarchy as (
        select
            m_units."UNIT_KEY" as veterinary_key,
            m_units.*,
            subcounty_units."COUNTY_KEY" as county_key,
            subcounty_units."SUB_COUNTY_KEY" as sub_county_key
        from {{ ref("stg_units") }} as m_units
        left join
            {{ ref("fct_sub_county_units") }} as subcounty_units
            on subcounty_units."_ID" = m_units."PARENT"
        where m_units."TYPE" = 'Veterinary facility' and m_units."UID" is null
    )
select
    coalesce(veterinary_facility.county_key, 'unset') as "COUNTY_KEY",
    coalesce(veterinary_facility.sub_county_key, 'unset') as "SUB_COUNTY_KEY",
    coalesce(veterinary_facility.veterinary_key, 'unset') as "VETERINARY_FACILITY_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join
    units_hierarchy as veterinary_facility on veterinary_facility."_ID" = units."_ID"
where units."TYPE" = 'Veterinary facility'
