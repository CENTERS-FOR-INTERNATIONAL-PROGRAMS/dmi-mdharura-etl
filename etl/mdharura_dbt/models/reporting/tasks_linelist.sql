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
        community_unit."NAME", health_facility."NAME", sub_county."NAME", 'unset'
    ) as "UNIT_NAME",
    coalesce(
        community_unit."TYPE", health_facility."TYPE", sub_county."TYPE", 'unset'
    ) as "UNIT_TYPE",
    coalesce(
        community_unit."UID", health_facility."UID", sub_county."UID", 'unset'
    ) as "UNIT_UID",
    tasks.*
from {{ ref("fct_tasks") }} as tasks
left join {{ ref("dim_date") }} as dim_date on dim_date.date_key = tasks."DATE_KEY"
left join
    {{ ref("dim_epi_week") }} as dim_epi_week
    on dim_epi_week.epi_week_key = tasks."EPI_WEEK_KEY"
left join
    {{ ref("fct_county_units") }} as county on county."COUNTY_KEY" = tasks."COUNTY_KEY"
left join
    {{ ref("fct_sub_county_units") }} as sub_county
    on sub_county."SUB_COUNTY_KEY" = tasks."SUB_COUNTY_KEY"
left join
    {{ ref("fct_community_units") }} as community_unit
    on community_unit."COMMUNITY_UNIT_KEY" = tasks."UNIT_KEY"
    and community_unit."_ID" = tasks."UNIT_ID"
left join
    {{ ref("fct_health_facilities") }} as health_facility
    on health_facility."HEALTH_FACILITY_KEY" = tasks."UNIT_KEY"
    and health_facility."_ID" = tasks."UNIT_ID"
