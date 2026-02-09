{{ 
    config(
        materialized='table',
        schema='gold'
    ) 
}}

with numbers as (

    select generate_series(0, 1439) as minute_of_day

),

time_dimension as (

    select
        minute_of_day as time_sk,

        floor(minute_of_day / 60) as hour_of_day,
        minute_of_day % 60 as minute_of_hour,

        case 
            when floor(minute_of_day / 60) < 12 then 'AM'
            else 'PM'
        end as am_pm,

        case 
            when floor(minute_of_day / 60) = 0 then 12
            when floor(minute_of_day / 60) > 12 then floor(minute_of_day / 60) - 12
            else floor(minute_of_day / 60)
        end as hour_12,

        lpad(floor(minute_of_day / 60)::text, 2, '0')
            || ':' ||
        lpad((minute_of_day % 60)::text, 2, '0') as time_24hr,

        lpad(
            case 
                when floor(minute_of_day / 60) = 0 then 12
                when floor(minute_of_day / 60) > 12 then floor(minute_of_day / 60) - 12
                else floor(minute_of_day / 60)
            end::text, 2, '0'
        )
        || ':' ||
        lpad((minute_of_day % 60)::text, 2, '0')
        || ' ' ||
        case 
            when floor(minute_of_day / 60) < 12 then 'AM'
            else 'PM'
        end as time_12hr

    from numbers
)

select *
from time_dimension
