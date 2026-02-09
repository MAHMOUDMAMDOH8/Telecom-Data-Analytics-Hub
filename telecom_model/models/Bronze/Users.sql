{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='user_sk',
        indexes=[
            {"columns":['user_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}

with User_cte as (
    select
        distinct phone_number,
        city,
        status,
        customer_type,
        gender,
        age_group,
        activation_date,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ref('CDC_Users')}}
    where phone_number is not null
),
User_with_sk as (
    select
        md5(phone_number) as user_sk,
        *
    from User_cte
)

{% if is_incremental() %}

select * from User_with_sk
where user_sk not in (select distinct user_sk from {{ this }})
{% else %}

select * from User_with_sk
{% endif %}