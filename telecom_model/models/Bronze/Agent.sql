{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='agent_sk',
        indexes=[
            {"columns":['agent_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Agent_cte as (
    select
        distinct agent_id,
        name,
        department,
        status,
        city,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ref('CDC_Agent')}}
    where agent_id is not null
),
Agent_with_sk as (
    select
        md5(agent_id || name || department || status) as agent_sk,
        *
    from Agent_cte
)

{% if is_incremental() %}
select * from Agent_with_sk
where agent_sk not in (select distinct agent_sk from {{ this }})
{% else %}
select * from Agent_with_sk
{% endif %}