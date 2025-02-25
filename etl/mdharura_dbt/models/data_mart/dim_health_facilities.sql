{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_uid ON {{this}} USING btree ("uid");',
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_name ON {{this}} USING btree ("name");',
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_county_key ON {{this}} USING btree ("county_key");',
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_subcounty_key ON {{this}} USING btree ("sub_county_key");',
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_key ON {{this}} USING btree ("health_facility_key");',
            'CREATE INDEX IF NOT EXISTS idx_dim_hf_code ON {{this}} USING btree ("code");',
        ]
    )
}}


with
    source_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["units_source.uid"]) }}
            as health_facility_key,
            units_source.uid as "uid",
            units_source.facility as "name",
            units_source.mflcode as code,
            dim_county.county_key,
            dim_sub_county.sub_county_key
        from {{ ref("kenya_health_facilities") }} as units_source
        left join
            {{ ref("dim_county") }} as dim_county
            on concat(dim_county.county, ' ', 'County') = units_source.county
        left join
            {{ ref("dim_sub_county") }} as dim_sub_county
            on concat(dim_sub_county.sub_county, ' ', 'Sub County') = case
                when
                    units_source.subcounty
                    = concat('Buuri', ' ', ' ', 'East Sub County')
                then 'Buuri East Sub County'
                when units_source.subcounty = 'Suguta Sub county'
                then 'Suguta Sub County'
                else units_source.subcounty
            end
        union
        select
            'unset' as "uid",
            'unset' as "name",
            'unset' as code,
            'unset' as county_key,
            'unset' as sub_county_key,
            'unset' as health_facility_key
    )
select source_data.*, cast(current_date as date) as load_date
from source_data
