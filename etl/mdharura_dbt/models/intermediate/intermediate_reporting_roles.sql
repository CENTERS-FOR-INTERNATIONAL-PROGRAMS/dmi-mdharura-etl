{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_date ON {{this}} USING btree ("DATE_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_epiweek ON {{this}} USING btree ("EPI_WEEK_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_county ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_subcounty ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_unit ON {{this}} USING btree ("UNIT_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_rpt_role_indicators_unit_id ON {{this}} USING btree ("UNIT_ID");',
        ]
    )
}}

select
    tasks."COUNTY_KEY" as "COUNTY_KEY",
    tasks."SUB_COUNTY_KEY" as "SUB_COUNTY_KEY",
    tasks."UNIT_KEY" as "UNIT_KEY",
    tasks."DATE_KEY" as "DATE_KEY",
    tasks."EPI_WEEK_KEY" as "EPI_WEEK_KEY",
    tasks."UNIT_ID" as "UNIT_ID",
    count(
        distinct case when reporting_role."SPOT" = 'CHV' then reporting_role."USER" end
    ) as "CHVS_REPORTING",
    count(
        distinct case when reporting_role."SPOT" = 'CDR' then reporting_role."USER" end
    ) as "CDRS_REPORTING",
    count(
        distinct case when reporting_role."SPOT" = 'CHA' then reporting_role."USER" end
    ) as "CHAS_REPORTING",
    count(
        distinct case when reporting_role."SPOT" = 'AHA' then reporting_role."USER" end
    ) as "AHAS_REPORTING",
    count(
        distinct case when reporting_role."SPOT" = 'HCW' then reporting_role."USER" end
    ) as "HCWS_REPORTING",
    count(
        distinct case when reporting_role."SPOT" = 'SFP' then reporting_role."USER" end
    ) as "SFPS_REPORTING",
    count(
        distinct case when verifying_role."SPOT" = 'SFP' then verifying_role."USER" end
    ) as "SFPS_VERIFYING",
    count(
        distinct case when verifying_role."SPOT" = 'CHA' then verifying_role."USER" end
    ) as "CHAS_VERIFYING",
    count(
        distinct case when verifying_role."SPOT" = 'AHA' then verifying_role."USER" end
    ) as "AHAS_VERIFYING",
    count(
        distinct case when assessing_role."SPOT" = 'CEBS' then assessing_role."USER" end
    ) as "CEBS_INVESTIGATING",
    count(
        distinct case when assessing_role."SPOT" = 'HEBS' then assessing_role."USER" end
    ) as "HEBS_INVESTIGATING",
    count(
        distinct case when assessing_role."SPOT" = 'HEBS' then assessing_role."USER" end
    ) as "VEBS_INVESTIGATING",
    count(
        distinct case
            when responding_role."SPOT" = 'CEBS' then responding_role."USER"
        end
    ) as "CEBS_RESPONDING",
    count(
        distinct case
            when responding_role."SPOT" = 'HEBS' then responding_role."USER"
        end
    ) as "HEBS_RESPONDING",
    count(
        distinct case
            when responding_role."SPOT" = 'HEBS' then responding_role."USER"
        end
    ) as "VEBS_RESPONDING",

    count(
        distinct case
            when escalating_role."SPOT" = 'CEBS' then escalating_role."USER"
        end
    ) as "CEBS_ESCALATING",
    count(
        distinct case
            when escalating_role."SPOT" = 'HEBS' then escalating_role."USER"
        end
    ) as "HEBS_ESCALATING",
    count(
        distinct case
            when escalating_role."SPOT" = 'HEBS' then escalating_role."USER"
        end
    ) as "VEBS_ESCALATING",

    count(
        distinct case
            when summarizing_role."SPOT" = 'CEBS' then summarizing_role."USER"
        end
    ) as "CEBS_SUMMARIZING",
    count(
        distinct case
            when summarizing_role."SPOT" = 'HEBS' then summarizing_role."USER"
        end
    ) as "HEBS_SUMMARIZING",
    count(
        distinct case
            when summarizing_role."SPOT" = 'HEBS' then summarizing_role."USER"
        end
    ) as "VEBS_SUMMARIZING"

from {{ ref("fct_tasks") }} as tasks
left join
    {{ ref("fct_roles") }} as reporting_role on reporting_role."USER" = tasks."USER_ID"
left join
    {{ ref("fct_roles") }} as verifying_role
    on verifying_role."USER" = tasks."CEBS_VERIFICATIONFORM_USER"
    or verifying_role."USER" = tasks."HEBS_VERIFICATIONFORM_USER"
    or verifying_role."USER" = tasks."VEBS_VERIFICATIONFORM_USER"
left join
    {{ ref("fct_roles") }} as assessing_role
    on assessing_role."USER" = tasks."CEBS_INVESTIGATIONFORM_USER"
    or assessing_role."USER" = tasks."HEBS_INVESTIGATIONFORM_USER"
    or assessing_role."USER" = tasks."VEBS_INVESTIGATIONFORM_USER"
left join
    {{ ref("fct_roles") }} as responding_role
    on responding_role."USER" = tasks."CEBS_RESPONSEFORM_USER"
    or responding_role."USER" = tasks."HEBS_RESPONSEFORM_USER"
    or responding_role."USER" = tasks."VEBS_RESPONSEFORM_USER"
left join
    {{ ref("fct_roles") }} as escalating_role
    on escalating_role."USER" = tasks."CEBS_ESCALATIONFORM_USER"
    or escalating_role."USER" = tasks."HEBS_ESCALATIONFORM_USER"
    or escalating_role."USER" = tasks."VEBS_ESCALATIONFORM_USER"
left join
    {{ ref("fct_roles") }} as summarizing_role
    on summarizing_role."USER" = tasks."CEBS_SUMMARYFORM_USER"
    or summarizing_role."USER" = tasks."HEBS_SUMMARYFORM_USER"
    or summarizing_role."USER" = tasks."VEBS_SUMMARYFORM_USER"
where tasks."STATE" = 'live'
group by
    tasks."COUNTY_KEY",
    tasks."SUB_COUNTY_KEY",
    tasks."UNIT_KEY",
    tasks."DATE_KEY",
    tasks."EPI_WEEK_KEY",
    tasks."UNIT_ID"
