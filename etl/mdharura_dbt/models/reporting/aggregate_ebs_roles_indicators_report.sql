{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_date ON {{this}} USING btree ("DATE");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_epiweek ON {{this}} USING btree ("EPI_WEEK");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_year ON {{this}} USING btree ("YEAR");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_county ON {{this}} USING btree ("COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_subcounty ON {{this}} USING btree ("SUB_COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_unit_name ON {{this}} USING btree ("UNIT_NAME");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_unit_id ON {{this}} USING btree ("UNIT_ID");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_role_indicators_unit_type ON {{this}} USING btree ("UNIT_TYPE");',
        ]
    )
}}


with
    final_data as (
        select
            r."COUNTY_KEY",
            r."SUB_COUNTY_KEY",
            r."UNIT_KEY",
            r."DATE_KEY",
            r."EPI_WEEK_KEY",
            r."UNIT_ID",
            0 as "CHVS_REGISTERED",
            0 as "CHAS_REGISTERED",
            0 as "AHAS_REGISTERED",
            0 as "CDRS_REGISTERED",
            0 as "SFPS_REGISTERED",
            0 as "HCWS_REGISTERED",
            0 as "CEBS_REGISTERED",
            0 as "HEBS_REGISTERED",
            0 as "VEBS_REGISTERED",
            0 as "VIEWERS_REGISTERED",
            r."CHVS_REPORTING",
            r."CDRS_REPORTING",
            r."CHAS_REPORTING",
            r."AHAS_REPORTING",
            r."HCWS_REPORTING",
            r."SFPS_REPORTING",
            r."SFPS_VERIFYING",
            r."CHAS_VERIFYING",
            r."AHAS_VERIFYING",
            r."CEBS_INVESTIGATING",
            r."HEBS_INVESTIGATING",
            r."VEBS_INVESTIGATING",
            r."CEBS_RESPONDING",
            r."HEBS_RESPONDING",
            r."VEBS_RESPONDING",
            r."CEBS_ESCALATING",
            r."HEBS_ESCALATING",
            r."VEBS_ESCALATING",
            r."CEBS_SUMMARIZING",
            r."HEBS_SUMMARIZING",
            r."VEBS_SUMMARIZING"

        from {{ ref("intermediate_reporting_roles") }} as r
        union all
        select
            t."COUNTY_KEY",
            t."SUB_COUNTY_KEY",
            t."UNIT_KEY",
            t."DATE_KEY",
            t."EPI_WEEK_KEY",
            t."UNIT_ID",
            t."CHVS_REGISTERED",
            t."CHAS_REGISTERED",
            t."AHAS_REGISTERED",
            t."CDRS_REGISTERED",
            t."SFPS_REGISTERED",
            t."HCWS_REGISTERED",
            t."CEBS_REGISTERED",
            t."HEBS_REGISTERED",
            t."VEBS_REGISTERED",
            t."VIEWERS_REGISTERED",
            0 as "CHVS_REPORTING",
            0 as "CDRS_REPORTING",
            0 as "CHAS_REPORTING",
            0 as "AHAS_REPORTING",
            0 as "HCWS_REPORTING",
            0 as "SFPS_REPORTING",
            0 as "SFPS_VERIFYING",
            0 as "CHAS_VERIFYING",
            0 as "AHAS_VERIFYING",
            0 as "CEBS_INVESTIGATING",
            0 as "HEBS_INVESTIGATING",
            0 as "VEBS_INVESTIGATING",
            0 as "CEBS_RESPONDING",
            0 as "HEBS_RESPONDING",
            0 as "VEBS_RESPONDING",
            0 as "CEBS_ESCALATING",
            0 as "HEBS_ESCALATING",
            0 as "VEBS_ESCALATING",
            0 as "CEBS_SUMMARIZING",
            0 as "HEBS_SUMMARIZING",
            0 as "VEBS_SUMMARIZING"

        from {{ ref("intermediate_user_roles") }} as t
    )

select
    dim_date.date as "DATE",
    dim_epi_week.week_number as "EPI_WEEK",
    dim_epi_week.year as "YEAR",
    county."NAME" as "COUNTY",
    county."UID" as "COUNTY_UID",
    county."_ID" as "COUNTY_ID",
    sub_county."NAME" as "SUB_COUNTY",
    sub_county."UID" as "SUB_COUNTY_UID",
    sub_county."_ID" as "SUB_COUNTY_ID",
    coalesce(
        community_unit."NAME",
        health_facility."NAME",
        county."NAME",
        country."NAME",
        'unset'
    ) as "UNIT_NAME",
    coalesce(
        community_unit."TYPE",
        health_facility."TYPE",
        county."TYPE",
        country."TYPE",
        'unset'
    ) as "UNIT_TYPE",
    coalesce(
        community_unit."UID",
        health_facility."UID",
        county."UID",
        country."UID",
        'unset'
    ) as "UNIT_UID",
    coalesce(
        community_unit."_ID",
        health_facility."_ID",
        county."_ID",
        country."_ID",
        'unset'
    ) as "UNIT_ID",

    sum(roles."CHVS_REGISTERED") as "CHVS_REGISTERED",
    sum(roles."CHVS_REPORTING") as "CHVS_REPORTING",

    sum(roles."CDRS_REGISTERED") as "CDRS_REGISTERED",
    sum(roles."CDRS_REPORTING") as "CDRS_REPORTING",

    sum(roles."HCWS_REGISTERED") as "HCWS_REGISTERED",
    sum(roles."HCWS_REPORTING") as "HCWS_REPORTING",

    sum(roles."CHAS_REGISTERED") as "CHAS_REGISTERED",
    sum(roles."CHAS_REPORTING") as "CHAS_REPORTING",
    sum(roles."CHAS_VERIFYING") as "CHAS_VERIFYING",

    sum(roles."AHAS_REGISTERED") as "AHAS_REGISTERED",
    sum(roles."AHAS_REPORTING") as "AHAS_REPORTING",
    sum(roles."AHAS_VERIFYING") as "AHAS_VERIFYING",

    sum(roles."SFPS_REGISTERED") as "SFPS_REGISTERED",
    sum(roles."SFPS_REPORTING") as "SFPS_REPORTING",
    sum(roles."SFPS_VERIFYING") as "SFPS_VERIFYING",

    sum(roles."VIEWERS_REGISTERED") as "VIEWERS_REGISTERED",

    sum(roles."CEBS_REGISTERED") as "CEBS_REGISTERED",
    sum(roles."CEBS_INVESTIGATING") as "CEBS_INVESTIGATING",
    sum(roles."CEBS_RESPONDING") as "CEBS_RESPONDING",
    sum(roles."CEBS_ESCALATING") as "CEBS_ESCALATING",
    sum(roles."CEBS_SUMMARIZING") as "CEBS_SUMMARIZING",

    sum(roles."HEBS_REGISTERED") as "HEBS_REGISTERED",
    sum(roles."HEBS_INVESTIGATING") as "HEBS_INVESTIGATING",
    sum(roles."HEBS_RESPONDING") as "HEBS_RESPONDING",
    sum(roles."HEBS_ESCALATING") as "HEBS_ESCALATING",
    sum(roles."HEBS_SUMMARIZING") as "HEBS_SUMMARIZING",

    sum(roles."VEBS_REGISTERED") as "VEBS_REGISTERED",
    sum(roles."VEBS_INVESTIGATING") as "VEBS_INVESTIGATING",
    sum(roles."VEBS_RESPONDING") as "VEBS_RESPONDING",
    sum(roles."VEBS_ESCALATING") as "VEBS_ESCALATING",
    sum(roles."VEBS_SUMMARIZING") as "VEBS_SUMMARIZING"

from final_data as roles
left join {{ ref("dim_date") }} as dim_date on dim_date.date_key = roles."DATE_KEY"
left join
    {{ ref("dim_epi_week") }} as dim_epi_week
    on dim_epi_week.epi_week_key = roles."EPI_WEEK_KEY"
left join
    {{ ref("fct_county_units") }} as county on county."COUNTY_KEY" = roles."COUNTY_KEY"
left join
    {{ ref("fct_sub_county_units") }} as sub_county
    on sub_county."SUB_COUNTY_KEY" = roles."SUB_COUNTY_KEY"
left join
    {{ ref("fct_community_units") }} as community_unit
    on community_unit."COMMUNITY_UNIT_KEY" = roles."UNIT_KEY"
    and community_unit."_ID" = roles."UNIT_ID"
left join
    {{ ref("fct_health_facilities") }} as health_facility
    on health_facility."HEALTH_FACILITY_KEY" = roles."UNIT_KEY"
    and health_facility."_ID" = roles."UNIT_ID"

left join
    {{ ref("fct_country_unit") }} as country on country."COUNTRY_KEY" = roles."UNIT_KEY"

group by
    dim_date.date,
    dim_epi_week.week_number,
    dim_epi_week.year,
    county."NAME",
    county."UID",
    county."_ID",
    county."TYPE",
    sub_county."NAME",
    sub_county."UID",
    sub_county."_ID",
    sub_county."TYPE",
    community_unit."NAME",
    community_unit."UID",
    community_unit."_ID",
    community_unit."TYPE",
    health_facility."NAME",
    health_facility."TYPE",
    health_facility."UID",
    health_facility."_ID",
    country."NAME",
    country."TYPE",
    country."UID",
    country."_ID"
