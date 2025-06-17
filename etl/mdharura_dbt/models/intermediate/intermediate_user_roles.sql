{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_date ON {{this}} USING btree ("DATE_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_epiweek ON {{this}} USING btree ("EPI_WEEK_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_county ON {{this}} USING btree ("COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_subcounty ON {{this}} USING btree ("SUB_COUNTY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_unit ON {{this}} USING btree ("UNIT_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_usr_role_indicators_unit_id ON {{this}} USING btree ("UNIT_ID");',
        ]
    )
}}

select
    roles."COUNTY_KEY" as "COUNTY_KEY",
    roles."SUB_COUNTY_KEY" as "SUB_COUNTY_KEY",
    roles."UNIT_KEY" as "UNIT_KEY",
    roles."DATE_KEY" as "DATE_KEY",
    roles."EPI_WEEK_KEY" as "EPI_WEEK_KEY",
    roles."UNIT" as "UNIT_ID",
    sum(case when "SPOT" in ('CHV') then 1 else 0 end) as "CHVS_REGISTERED",
    sum(case when "SPOT" in ('CDR') then 1 else 0 end) as "CDRS_REGISTERED",
    sum(case when "SPOT" in ('CHA') then 1 else 0 end) as "CHAS_REGISTERED",
    sum(case when "SPOT" in ('AHA') then 1 else 0 end) as "AHAS_REGISTERED",
    sum(case when "SPOT" in ('HCW') then 1 else 0 end) as "HCWS_REGISTERED",
    sum(case when "SPOT" in ('SFP') then 1 else 0 end) as "SFPS_REGISTERED",
    sum(case when "SPOT" in ('CEBS') then 1 else 0 end) as "CEBS_REGISTERED",
    sum(case when "SPOT" in ('HEBS') then 1 else 0 end) as "HEBS_REGISTERED",
    sum(case when "SPOT" in ('VEBS') then 1 else 0 end) as "VEBS_REGISTERED",
    sum(case when "SPOT" in ('VIEWER') then 1 else 0 end) as "VIEWERS_REGISTERED"
from {{ ref("fct_roles") }} as roles
group by
    roles."COUNTY_KEY",
    roles."SUB_COUNTY_KEY",
    roles."UNIT_KEY",
    roles."DATE_KEY",
    roles."EPI_WEEK_KEY",
    roles."UNIT"
