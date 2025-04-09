{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_date ON {{this}} USING btree ("DATE");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_epiweek ON {{this}} USING btree ("EPI_WEEK");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_year ON {{this}} USING btree ("YEAR");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_county ON {{this}} USING btree ("COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_subcounty ON {{this}} USING btree ("SUB_COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_unit_name ON {{this}} USING btree ("UNIT_NAME");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_aggregate_indicators_unit_type ON {{this}} USING btree ("UNIT_TYPE");',
        ]
    )
}}

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
    coalesce(community_unit."NAME", health_facility."NAME", 'unset') as "UNIT_NAME",
    coalesce(community_unit."TYPE", health_facility."TYPE", 'unset') as "UNIT_TYPE",
    coalesce(community_unit."UID", health_facility."UID", 'unset') as "UNIT_UID",
    coalesce(community_unit."_ID", health_facility."_ID", 'unset') as "UNIT_ID",
    sum(
        case
            when
                "SIGNAL" in (
                    '1',
                    '2',
                    '3',
                    '4',
                    '5',
                    '6',
                    '7',
                    'h1',
                    'h2',
                    'h3',
                    'v1',
                    'v2',
                    'l1',
                    'l2',
                    'l3',
                    'p1',
                    'p2',
                    'p3',
                    'm1',
                    'm2',
                    'm3'
                )
            then 1
            else 0
        end
    ) as "SIGNALS_REPORTED",
    sum(
        case
            when
                "CEBS_VERIFICATIONFORM_ID" is not null
                or "HEBS_VERIFICATIONFORM_ID" is not null
                or "VEBS_VERIFICATIONFORM_ID" is not null  -- OR "LEBS_VERIFICATIONFORM_ID" IS NOT NULL
            then 1
            else 0
        end
    ) as "SIGNALS_VERIFIED",
    sum(
        case
            when
                "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
                or "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
                or "VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')  -- OR "LEBS_VERIFICATIONFORM_ISSTILLHAPPENING" IN (
            -- 'Yes',
            -- 'yes'
            -- )
            then 1
            else 0
        end
    ) as "SIGNALS_VERIFIED_TRUE",
    -- CEBS Signals --
    sum(
        case
            when
                "CEBS_INVESTIGATIONFORM_ID" is not null
                or "HEBS_INVESTIGATIONFORM_ID" is not null
                or "VEBS_INVESTIGATIONFORM_ID" is not null  -- OR "LEBS_INVESTIGATIONFORM_ID" IS NOT NULL
            then 1
            else 0
        end
    ) as "SIGNALS_RISK_ASSESSED",
    sum(
        case
            when
                "CEBS_RESPONSEFORM_ID" is not null
                or "HEBS_RESPONSEFORM_ID" is not null
                or "VEBS_RESPONSEFORM_ID" is not null  -- OR "LEBS_RESPONSEFORM_ID" IS NOT NULL
            then 1
            else 0
        end
    ) as "SIGNALS_RESPONDED",
    sum(
        case
            when
                "CEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "CEBS_ESCALATIONFORM_ID" is null
                or "HEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "HEBS_ESCALATIONFORM_ID" is null
                or "VEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "VEBS_ESCALATIONFORM_ID" is null
            then 1
            else 0
        end
    ) as "SIGNALS_TO_BE_ESCALATED",
    sum(
        case
            when
                "CEBS_ESCALATIONFORM_ID" is not null
                or "HEBS_ESCALATIONFORM_ID" is not null
                or "VEBS_ESCALATIONFORM_ID" is not null
            then 1
            else 0
        end
    ) as "SIGNALS_ESCALATED",
    sum(
        case when "SIGNAL" in ('1', '2', '3', '4', '5', '6', '7') then 1 else 0 end
    ) as "CEBS_SIGNALS_REPORTED",
    sum(
        case when "CEBS_VERIFICATIONFORM_ID" is not null then 1 else 0 end
    ) as "CEBS_SIGNALS_VERIFIED",
    sum(
        case
            when "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
            then 1
            else 0
        end
    ) as "CEBS_SIGNALS_VERIFIED_TRUE",
    sum(
        case when "CEBS_INVESTIGATIONFORM_ID" is not null then 1 else 0 end
    ) as "CEBS_SIGNALS_RISK_ASSESSED",
    sum(
        case when "CEBS_RESPONSEFORM_ID" is not null then 1 else 0 end
    ) as "CEBS_SIGNALS_RESPONDED",

    sum(
        case
            when
                "CEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "CEBS_ESCALATIONFORM_ID" is null
            then 1
            else 0
        end
    ) as "CEBS_SIGNALS_TO_BE_ESCALATED",
    sum(
        case when "CEBS_ESCALATIONFORM_ID" is not null then 1 else 0 end
    ) as "CEBS_SIGNALS_ESCALATED",
    -- HEBS Signals ---
    sum(
        case when "SIGNAL" in ('h1', 'h2', 'h3') then 1 else 0 end
    ) as "HEBS_SIGNALS_REPORTED",
    sum(
        case when "HEBS_VERIFICATIONFORM_ID" is not null then 1 else 0 end
    ) as "HEBS_SIGNALS_VERIFIED",
    sum(
        case
            when "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
            then 1
            else 0
        end
    ) as "HEBS_SIGNALS_VERIFIED_TRUE",
    sum(
        case when "HEBS_INVESTIGATIONFORM_ID" is not null then 1 else 0 end
    ) as "HEBS_SIGNALS_RISK_ASSESSED",
    sum(
        case when "HEBS_RESPONSEFORM_ID" is not null then 1 else 0 end
    ) as "HEBS_SIGNALS_RESPONDED",
    sum(
        case
            when
                "HEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "HEBS_ESCALATIONFORM_ID" is null
            then 1
            else 0
        end
    ) as "HEBS_SIGNALS_TO_BE_ESCALATED",

    sum(
        case when "HEBS_ESCALATIONFORM_ID" is not null then 1 else 0 end
    ) as "HEBS_SIGNALS_ESCALATED",
    -- VEBS Signals ---
    sum(
        case when "SIGNAL" in ('v1', 'v2') then 1 else 0 end
    ) as "VEBS_SIGNALS_REPORTED",
    sum(
        case when "VEBS_VERIFICATIONFORM_ID" is not null then 1 else 0 end
    ) as "VEBS_SIGNALS_VERIFIED",
    sum(
        case
            when "VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
            then 1
            else 0
        end
    ) as "VEBS_SIGNALS_VERIFIED_TRUE",
    sum(
        case when "VEBS_INVESTIGATIONFORM_ID" is not null then 1 else 0 end
    ) as "VEBS_SIGNALS_RISK_ASSESSED",
    sum(
        case when "VEBS_RESPONSEFORM_ID" is not null then 1 else 0 end
    ) as "VEBS_SIGNALS_RESPONDED",
    sum(
        case
            when
                "VEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "VEBS_ESCALATIONFORM_ID" is null
            then 1
            else 0
        end
    ) as "VEBS_SIGNALS_TO_BE_ESCALATED",
    sum(
        case when "VEBS_ESCALATIONFORM_ID" is not null then 1 else 0 end
    ) as "VEBS_SIGNALS_ESCALATED",

    sum(
        case when "VIA" in ('SMS', 'sms') then 1 else 0 end
    ) as "SIGNALS_REPORTED_VIA_SMS",
    sum(
        case
            when
                "CEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
                or "HEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
                or "VEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
            then 1
            else 0
        end
    ) as "SIGNALS_VERIFIED_VIA_SMS",
    sum(
        case
            when
                "CEBS_INVESTIGATIONFORM_VIA" in ('SMS', 'sms')
                or "HEBS_INVESTIGATIONFORM_VIA" in ('SMS', 'sms')
                or "VEBS_INVESTIGATIONFORM_VIA" in ('SMS', 'sms')
            then 1
            else 0
        end
    ) as "SIGNALS_INVESTIGATED_VIA_SMS",
    sum(
        case
            when
                "CEBS_RESPONSEFORM_VIA" in ('SMS', 'sms')
                or "HEBS_RESPONSEFORM_VIA" in ('SMS', 'sms')
                or "VEBS_RESPONSEFORM_VIA" in ('SMS', 'sms')
            then 1
            else 0
        end
    ) as "SIGNALS_RESPONDED_VIA_SMS",
    sum(
        case
            when
                "CEBS_ESCALATIONFORM_VIA" in ('SMS', 'sms')
                or "HEBS_ESCALATIONFORM_VIA" in ('SMS', 'sms')
                or "VEBS_ESCALATIONFORM_VIA" in ('SMS', 'sms')
            then 1
            else 0
        end
    ) as "SIGNALS_ESCALATED_VIA_SMS",
    sum(
        case
            when
                "CEBS_SUMMARYFORM_VIA" in ('SMS', 'sms')
                or "HEBS_SUMMARYFORM_VIA" in ('SMS', 'sms')
                or "VEBS_SUMMARYFORM_VIA" in ('SMS', 'sms')
            then 1
            else 0
        end
    ) as "SIGNALS_SUMMARIZED_VIA_SMS"
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
where tasks."STATE" = 'live'
group by
    dim_date.date,
    dim_epi_week.week_number,
    dim_epi_week.year,
    county."NAME",
    county."UID",
    county."_ID",
    sub_county."NAME",
    sub_county."UID",
    sub_county."_ID",
    community_unit."NAME",
    community_unit."UID",
    community_unit."_ID",
    community_unit."TYPE",
    health_facility."NAME",
    health_facility."UID",
    health_facility."_ID",
    health_facility."TYPE"
