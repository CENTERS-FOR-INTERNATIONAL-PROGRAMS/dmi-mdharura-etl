{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_dim_county_name ON {{this}} USING btree ("county");',
            'CREATE INDEX IF NOT EXISTS idx_dim_county_key ON {{this}} USING btree ("county_key");',
        ]
    )
}}


with
    county_iso_code_source as (
        select distinct county, county_iso_code
        from {{ ref("county_sub_county_iso_codes") }}
    ),
    source_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["county_source.county"]) }}
            as county_key,
            county_source.*,
            county_iso_code_source.county_iso_code as iso_code
        from {{ ref("kenya_counties") }} as county_source
        left join
            county_iso_code_source
            on county_iso_code_source.county
            = concat(county_source.county, ' ', 'County')
        union
        select
            'unset' as county_key,
            'unset' as county,
            'unset' as code,
            -999 as area_sqkm,
            'unset' as iso_code
    )
select source_data.*, cast(current_date as date) as load_date
from source_data
