-- Test: Verify time dimension has all 1440 minutes (0-1439) of the day
-- Purpose: Ensure complete time coverage for time-based analytical queries
-- Expected: Exactly 1440 rows, no gaps or duplicates

with time_check as (
    select 
        count(*) as total_records,
        count(distinct time_sk) as distinct_time_keys,
        min(time_sk) as min_minute,
        max(time_sk) as max_minute
    from {{ ref('DIM_Time') }}
)

select *
from time_check
where 
    total_records != 1440
    or distinct_time_keys != 1440
    or min_minute != 0
    or max_minute != 1439
