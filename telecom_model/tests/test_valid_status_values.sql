-- Test: Verify status values are from allowed domain
-- Purpose: Prevent invalid status values from entering the data warehouse
-- Expected: 0 rows (all valid statuses)



with valid_statuses as (
    select 'completed' as valid_status
    union all select 'dropped'
    union all select 'missed'
    union all select 'active'
    union all select 'inactive'
    union all select 'pending'
    union all select 'failed'
    union all select 'rejected'
    union all select 'success'
    union all select 'Active'
    union all select 'Prepaid'
    union all select 'Postpaid'
    union all select 'unknown'
    union all select 'completed'
    union all select 'in-progress'
    union all select 'delivered'
    union all select 'sent'
    union all select 'Training'
    union all select 'On Leave'
)

select 
    'Fact_Call' as fact_table,
    status,
    count(*) as row_count
from {{ ref('Fact_Call') }}
where status not in (select valid_status from valid_statuses)
    and status is not null
group by status

union all

select 
    'Fact_SMS' as fact_table,
    status,
    count(*) as row_count
from {{ ref('Fact_SMS') }}
where status not in (select valid_status from valid_statuses)
    and status is not null
group by status

union all

select 
    'DIM_Users' as fact_table,
    status,
    count(*) as row_count
from {{ ref('DIM_Users') }}
where status not in (select valid_status from valid_statuses)
    and status is not null
group by status

union all

select 
    'DIM_agent' as fact_table,
    status,
    count(*) as row_count
from {{ ref('DIM_agent') }}
where status not in (select valid_status from valid_statuses)
    and status is not null
group by status
