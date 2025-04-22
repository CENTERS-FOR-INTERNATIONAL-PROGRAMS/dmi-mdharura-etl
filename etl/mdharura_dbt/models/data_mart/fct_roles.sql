{{
    config(
        post_hook=[
            "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_spot ON {{this}} USING btree ("SPOT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_user ON {{this}} USING btree ("USER");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_unit_key ON {{this}} USING btree ("UNIT_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_sub_county_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_epi_week_key ON {{this}} USING btree ("EPI_WEEK_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_roles_date_key ON {{this}} USING btree ("DATE_KEY");',
        ]
    )
}}

select
    dim_date.date_key as "DATE_KEY",
    coalesce(dim_epi_week.epi_week_key, 'unset') as "EPI_WEEK_KEY",
    coalesce(
        community_unit."COUNTY_KEY",
        health_facility."COUNTY_KEY",
        subcounty."COUNTY_KEY",
        'unset'
    ) as "COUNTY_KEY",
    coalesce(
        community_unit."SUB_COUNTY_KEY",
        health_facility."SUB_COUNTY_KEY",
        subcounty."SUB_COUNTY_KEY",
        'unset'
    ) as "SUB_COUNTY_KEY",
    coalesce(
        community_unit."COMMUNITY_UNIT_KEY",
        health_facility."HEALTH_FACILITY_KEY",
        subcounty."SUB_COUNTY_KEY",
        'unset'
    ) as "UNIT_KEY",
    roles.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_roles") }} as roles
left join {{ ref("dim_date") }} as dim_date on dim_date.date = roles."CREATEDAT"::date
left join
    {{ ref("dim_epi_week") }} as dim_epi_week
    on roles."CREATEDAT"::date >= dim_epi_week.start_of_week
    and roles."CREATEDAT"::date <= dim_epi_week.end_of_week
left join
    {{ ref("fct_community_units") }} as community_unit
    on community_unit."_ID" = roles."UNIT"
left join
    {{ ref("fct_health_facilities") }} as health_facility
    on health_facility."_ID" = roles."UNIT"
left join
    {{ ref("fct_sub_county_units") }} as subcounty on subcounty."_ID" = roles."UNIT"
