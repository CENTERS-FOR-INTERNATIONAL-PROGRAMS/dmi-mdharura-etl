{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_fct_country_key ON {{this}} USING btree ("COUNTRY_KEY");',
            'CREATE INDEX IF NOT EXISTS idx_fct_country_id ON {{this}} USING btree ("_ID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_country_parent ON {{this}} USING btree ("PARENT");',
            'CREATE INDEX IF NOT EXISTS idx_fct_country_uid ON {{this}} USING btree ("UID");',
            'CREATE INDEX IF NOT EXISTS idx_fct_country_code ON {{this}} USING btree ("CODE");',
            'CREATE INDEX IF NOT EXISTS idx_fct_country_type ON {{this}} USING btree ("TYPE");',
        ]
    )
}}

select
    'kenyacountry' as "COUNTRY_KEY", units.*, cast(current_date as date) as "LOAD_DATE"
from {{ ref("stg_units") }} as units
where units."TYPE" = 'Country'
