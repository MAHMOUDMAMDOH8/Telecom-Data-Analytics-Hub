-- Test: Verify date dimension has consecutive days without gaps
-- Purpose: Ensure date dimension is complete and sequential for proper time-based analysis
-- Expected: 0 rows (no gaps in dates)

with date_sequence as (
    select distinct
        date_series,
        lead(date_series) over (order by date_series) as next_date,
        age(lead(date_series) over (order by date_series), date_series) as date_difference
    from {{ ref('DIM_Date') }}
),
date_gaps as (
    select 
        date_series,
        next_date,
        date_difference,
        case 
            when date_difference != '1 day'::interval then 'GAP FOUND'
            else 'OK'
        end as gap_status
    from date_sequence
    where next_date is not null
)

select * from date_gaps
where gap_status = 'GAP FOUND'
