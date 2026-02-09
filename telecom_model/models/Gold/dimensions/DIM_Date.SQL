{{ 
    config(
        materialized='table',
        schema='gold'
    ) 
}}

with generate_date_cte as (
    select generate_series(
        '2026-01-01'::date,
        '2026-12-31'::date,
        interval '1 day'
    ) as date_series
),

date_dimension as (
    select
        md5(TO_CHAR(g.date_series, 'YYYY-MM-DD')) AS date_sk,
        g.date_series,
        extract(year from g.date_series) as year,
        extract(quarter from g.date_series) as quarter,
        extract(month from g.date_series) as month,
        to_char(g.date_series, 'Month') as month_name,
        extract(day from g.date_series) as day,
        extract(doy from g.date_series) as day_of_year,
        to_char(g.date_series, 'Day') as day_name,
        extract(week from g.date_series) as week_of_year,
        case when extract(dow from g.date_series) in (0,6) then true else false end as is_weekend,
        date_trunc('month', g.date_series)::date as first_day_of_month,
        (date_trunc('month', g.date_series) + interval '1 month - 1 day')::date as last_day_of_month,
        date_trunc('week', g.date_series)::date as week_start_date,
        (date_trunc('week', g.date_series) + interval '6 day')::date as week_end_date
    from generate_date_cte g
)

select *
from date_dimension

