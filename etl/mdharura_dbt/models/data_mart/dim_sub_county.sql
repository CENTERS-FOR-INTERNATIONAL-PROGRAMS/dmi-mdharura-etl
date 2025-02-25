{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_dim_sub_county_name ON {{this}} USING btree ("sub_county");',
            'CREATE INDEX IF NOT EXISTS idx_dim_sub_county_key ON {{this}} USING btree ("sub_county_key");',
            'CREATE INDEX IF NOT EXISTS idx_dim_sub_ccounty_key ON {{this}} USING btree ("county_key");',
        ]
    )
}}


with
    sub_county_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["sub_county_source.unit_name"]) }}
            as sub_county_key,
            substring(
                sub_county_source.unit_name from '^(.*?) Sub County'
            ) as sub_county,
            sub_county_source.unit_code as sub_county_unit_code,
            iso_codes.sub_county_iso_code as iso_code,
            dim_county.county_key
        from {{ ref("kenya_sub_counties") }} as sub_county_source
        left join
            {{ ref("dim_county") }} as dim_county
            on concat(dim_county.county, ' ', 'County') = sub_county_source.county
        left join
            {{ ref("county_sub_county_iso_codes") }} as iso_codes
            on iso_codes.sub_county = sub_county_source.unit_name
        union
        select
            'unset' as sub_county_key,
            'unset' as sub_county,
            'unset' as sub_county_unit_code,
            'unset' as iso_code,
            'unset' as county_key
    )
select sub_county_data.*, cast(current_date as date) as load_date
from sub_county_data
