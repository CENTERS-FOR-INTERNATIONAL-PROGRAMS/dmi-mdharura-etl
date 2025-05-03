{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_date ON {{this}} USING btree ("DATE");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_epiweek ON {{this}} USING btree ("EPI_WEEK");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_year ON {{this}} USING btree ("YEAR");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_county ON {{this}} USING btree ("COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_subcounty ON {{this}} USING btree ("SUB_COUNTY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_unit_name ON {{this}} USING btree ("UNIT_NAME");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_unit_id ON {{this}} USING btree ("UNIT_ID");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_agg_indicators_unit_type ON {{this}} USING btree ("UNIT_TYPE");',
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
    coalesce(
        community_unit."NAME",
        health_facility."NAME",
        veterinary_facility."NAME",
        learning_institution."NAME",
        sub_county."NAME",
        county."NAME",
        country."NAME",
        'unset'
    ) as "UNIT_NAME",
    coalesce(
        community_unit."TYPE",
        health_facility."TYPE",
        veterinary_facility."TYPE",
        learning_institution."TYPE",
        county."TYPE",
        sub_county."TYPE",
        country."TYPE",
        'unset'
    ) as "UNIT_TYPE",
    coalesce(
        community_unit."UID",
        health_facility."UID",
        veterinary_facility."UID",
        learning_institution."UID",
        sub_county."UID",
        county."UID",
        country."UID",
        'unset'
    ) as "UNIT_UID",
    coalesce(
        community_unit."_ID",
        health_facility."_ID",
        veterinary_facility."_ID",
        learning_institution."_ID",
        sub_county."_ID",
        county."_ID",
        country."_ID",
        'unset'
    ) as "UNIT_ID",
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
                or "VEBS_VERIFICATIONFORM_ID" is not null
                or "LEBS_VERIFICATIONFORM_ID" is not null
            then 1
            else 0
        end
    ) as "SIGNALS_VERIFIED",
    sum(
        case
            when
                "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
                or "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
                or "VEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING" in ('Yes', 'yes')
                or "LEBS_VERIFICATIONFORM_ISSTILLHAPPENING" in ('Yes', 'yes')
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
                or "VEBS_INVESTIGATIONFORM_ID" is not null
                or "LEBS_INVESTIGATIONFORM_ID" is not null
            then 1
            else 0
        end
    ) as "SIGNALS_RISK_ASSESSED",
    sum(
        case
            when
                "CEBS_RESPONSEFORM_ID" is not null
                or "HEBS_RESPONSEFORM_ID" is not null
                or "VEBS_RESPONSEFORM_ID" is not null
                or "LEBS_RESPONSEFORM_ID" is not null
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
                or "LEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "LEBS_ESCALATIONFORM_ID" is null
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
                or "LEBS_ESCALATIONFORM_ID" is not null
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

    -- LEBS Signals ---
    sum(
        case
            when "SIGNAL" in ('l1', 'l2', 'l3', 'p1', 'p2', 'p3', 'm1', 'm2', 'm3')
            then 1
            else 0
        end
    ) as "LEBS_SIGNALS_REPORTED",
    sum(
        case when "LEBS_VERIFICATIONFORM_ID" is not null then 1 else 0 end
    ) as "LEBS_SIGNALS_VERIFIED",
    sum(
        case
            when "LEBS_VERIFICATIONFORM_ISSTILLHAPPENING" in ('Yes', 'yes')
            then 1
            else 0
        end
    ) as "LEBS_SIGNALS_VERIFIED_TRUE",
    sum(
        case when "LEBS_INVESTIGATIONFORM_ID" is not null then 1 else 0 end
    ) as "LEBS_SIGNALS_RISK_ASSESSED",
    sum(
        case when "LEBS_RESPONSEFORM_ID" is not null then 1 else 0 end
    ) as "LEBS_SIGNALS_RESPONDED",
    sum(
        case
            when
                "LEBS_RESPONSEFORM_RECOMMENDATIONS" like '%Escalate to higher level%'
                and "LEBS_ESCALATIONFORM_ID" is null
            then 1
            else 0
        end
    ) as "LEBS_SIGNALS_TO_BE_ESCALATED",
    sum(
        case when "LEBS_ESCALATIONFORM_ID" is not null then 1 else 0 end
    ) as "LEBS_SIGNALS_ESCALATED",

    sum(
        case when "VIA" in ('SMS', 'sms') then 1 else 0 end
    ) as "SIGNALS_REPORTED_VIA_SMS",
    sum(
        case
            when
                "CEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
                or "HEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
                or "VEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
                or "LEBS_VERIFICATIONFORM_VIA" in ('SMS', 'sms')
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
                or "LEBS_INVESTIGATIONFORM_VIA" in ('SMS', 'sms')
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
                or "LEBS_RESPONSEFORM_VIA" in ('SMS', 'sms')
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
                or "LEBS_ESCALATIONFORM_VIA" in ('SMS', 'sms')
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
                or "LEBS_SUMMARYFORM_VIA" in ('SMS', 'sms')
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

left join
    {{ ref("fct_learning_institutions") }} as learning_institution
    on learning_institution."LEARNING_INSTITUTION_KEY" = tasks."UNIT_KEY"
    and learning_institution."_ID" = tasks."UNIT_ID"

left join
    {{ ref("fct_veterinary_facilities") }} as veterinary_facility
    on veterinary_facility."VETERINARY_FACILITY_KEY" = tasks."UNIT_KEY"
    and veterinary_facility."_ID" = tasks."UNIT_ID"

left join
    {{ ref("fct_country_unit") }} as country on country."COUNTRY_KEY" = tasks."UNIT_KEY"
where tasks."STATE" = 'live'
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
    health_facility."UID",
    health_facility."_ID",
    health_facility."TYPE",
    learning_institution."NAME",
    learning_institution."UID",
    learning_institution."_ID",
    learning_institution."TYPE",
    veterinary_facility."NAME",
    veterinary_facility."UID",
    veterinary_facility."_ID",
    veterinary_facility."TYPE",
    country."NAME",
    country."TYPE",
    country."UID",
    country."_ID"
