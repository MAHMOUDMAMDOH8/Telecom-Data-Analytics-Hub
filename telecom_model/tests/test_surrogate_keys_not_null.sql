-- Test: Verify surrogate keys are never NULL in dimension tables
-- Purpose: Ensure data integrity by confirming all surrogate keys are populated
-- Expected: 0 rows (no NULLs)

with surrogate_key_validation as (
    select 'DIM_agent' as dimension_name, count(*) as null_count
    from {{ ref('DIM_agent') }}
    where agent_sk is null
    
    union all
    
    select 'DIM_cell_site' as dimension_name, count(*) as null_count
    from {{ ref('DIM_cell_site') }}
    where cell_site_sk is null
    
    union all
    
    select 'DIM_Device' as dimension_name, count(*) as null_count
    from {{ ref('DIM_Device') }}
    where device_sk is null
    
    union all
    
    select 'DIM_Users' as dimension_name, count(*) as null_count
    from {{ ref('DIM_Users') }}
    where user_sk is null
    
    union all
    
    select 'DIM_Date' as dimension_name, count(*) as null_count
    from {{ ref('DIM_Date') }}
    where date_sk is null
    
    union all
    
    select 'DIM_Time' as dimension_name, count(*) as null_count
    from {{ ref('DIM_Time') }}
    where time_sk is null
)

select * from surrogate_key_validation
where null_count > 0
