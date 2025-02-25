{{
    config(
        post_hook=[
            'CREATE INDEX IF NOT EXISTS idx_dim_date_name ON {{this}} USING btree ("date");',
            'CREATE INDEX IF NOT EXISTS idx_dim_date_key ON {{this}} USING btree ("date_key");',
        ]
    )
}}

with
    date_spine as (
        {{-
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2020-01-01' as date)",
                end_date="cast('2040-12-31' as date)",
            )
        -}}
    ),
    final_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["date_day"]) }} as date_key,
            cast(date_day as date) as date,
            date_part('year', date_day) as year,
            date_part('month', date_day) as month,
            date_part('quarter', date_day) as calendarquarter
        from date_spine

        union
        select
            'unset' as date_key,
            '1900-01-01'::date as date,
            -999 as year,
            -999 as month,
            -999 calendarquarter
    )
select final_data.*, cast(current_date as date) as load_date
from final_data
