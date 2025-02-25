{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_dim_cu_uid ON {{this}} USING btree ("uid");',
            'CREATE INDEX IF NOT EXISTS idx_dim_cu_code ON {{this}} USING btree ("code");',
        ]
    )
}}


with
    source_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["units_source.community_unit_uid"]) }}
            as community_unit_key,
            units_source.community_unit_uid as "uid",
            units_source.community_unit as "name",
            units_source.community_code as code,
            dim_county.county_key,
            dim_sub_county.sub_county_key
        from {{ ref("kenya_community_units") }} as units_source
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
            'unset' as community_unit_key
    )
select source_data.*, cast(current_date as date) as load_date
from source_data
