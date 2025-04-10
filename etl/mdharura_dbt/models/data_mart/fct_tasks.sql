{{
    config(
        post_hook=[
            "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_signal ON {{this}} USING btree ("SIGNAL");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_state ON {{this}} USING btree ("STATE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_unit_key ON {{this}} USING btree ("UNIT_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_county_key ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_sub_county_key ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_epi_week_key ON {{this}} USING btree ("EPI_WEEK_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_date_key ON {{this}} USING btree ("DATE_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_hebs_v_threat ON {{this}} USING btree ("HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_cebs_v_threat ON {{this}} USING btree ("CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_vebs_v_threat ON {{this}} USING btree ("VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING");',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_hebs_r_recommendations ON {{this}} USING gin ("HEBS_RESPONSEFORM_RECOMMENDATIONS" gin_trgm_ops);',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_cebs_r_recommendations ON {{this}} USING gin ("CEBS_RESPONSEFORM_RECOMMENDATIONS" gin_trgm_ops);',
            'CREATE INDEX IF NOT EXISTS idx_fct_tasks_vebs_r_recommendations ON {{this}} USING gin ("VEBS_RESPONSEFORM_RECOMMENDATIONS" gin_trgm_ops);',
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
    tasks.*,
    cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_tasks") }} as tasks
left join {{ ref("dim_date") }} as dim_date on dim_date.date = tasks."CREATEDAT"::date
left join
    {{ ref("dim_epi_week") }} as dim_epi_week
    on tasks."CREATEDAT"::date >= dim_epi_week.start_of_week
    and tasks."CREATEDAT"::date <= dim_epi_week.end_of_week
left join
    {{ ref("fct_community_units") }} as community_unit
    on community_unit."_ID" = tasks."UNIT_ID"
left join
    {{ ref("fct_health_facilities") }} as health_facility
    on health_facility."_ID" = tasks."UNIT_ID"
left join
    {{ ref("fct_sub_county_units") }} as subcounty on subcounty."_ID" = tasks."UNIT_ID"
