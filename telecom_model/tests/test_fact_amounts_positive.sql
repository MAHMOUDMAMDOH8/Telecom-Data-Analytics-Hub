-- Test: Verify all monetary amounts in fact tables are positive
-- Purpose: Detect data quality issues and invalid negative amounts
-- Expected: 0 rows (all amounts >= 0)

with amount_validation as (
    select 
        'Fact_Call' as fact_table,
        count(*) as invalid_amount_count
    from {{ ref('Fact_Call') }}
    where amount::float < 0 or amount is null
    
    union all
    
    select 
        'Fact_SMS' as fact_table,
        count(*) as invalid_amount_count
    from {{ ref('Fact_SMS') }}
    where amount::float < 0 or amount is null
    
    union all
    
    select 
        'Fact_payment' as fact_table,
        count(*) as invalid_amount_count
    from {{ ref('Fact_payment') }}
    where amount::float < 0 or amount is null
    
    union all
    
    select 
        'Fact_recharge' as fact_table,
        count(*) as invalid_amount_count
    from {{ ref('Fact_recharge') }}
    where amount::float < 0 or amount is null
    
)

select * from amount_validation
where invalid_amount_count > 0
