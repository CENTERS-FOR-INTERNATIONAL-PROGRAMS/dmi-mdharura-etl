{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_scounty_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_key ON {{this}} USING btree ("LEARNING_INSTITUTION_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_lf_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}

with
    units_hierarchy as (
        select
            m_units."UNIT_KEY" as learning_institution_key,
            m_units.*,
            subcounty_units."COUNTY_KEY" as county_key,
            subcounty_units."SUB_COUNTY_KEY" as sub_county_key
        from {{ ref("stg_units") }} as m_units
        left join
            {{ ref("fct_sub_county_units") }} as subcounty_units
            on subcounty_units."_ID" = m_units."PARENT"
        where m_units."TYPE" = 'Learning institution' and m_units."UID" is null
    )
select
    coalesce(learning_insitutions.county_key, 'unset') as "COUNTY_KEY",
    coalesce(learning_insitutions.sub_county_key, 'unset') as "SUB_COUNTY_KEY",
    coalesce(
        learning_insitutions.learning_institution_key, 'unset'
    ) as "LEARNING_INSTITUTION_KEY",
    units.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
left join
    units_hierarchy as learning_insitutions on learning_insitutions."_ID" = units."_ID"
where units."TYPE" = 'Learning institution'
