{{ 
    config(
        materialized='incremental',
        unique_key='agent_sk'
    ) 
}}

with agent_selected_columns as (

    select
        agent_sk,
        agent_id,
        name as agent_name,
        department,
        status,
        city,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('Agent') }}

)

select *
from agent_selected_columns

-- ✅ Unknown / Default Agent for handling missing or unmatched agent references in fact tables
union all
select
    '-1'      as agent_sk,
    '-1 '         as agent_id,
    'Unknown'     as agent_name,
    'unknown'     as department,
    'unknown'     as status,
    'unknown'     as city,
    null          as dbt_valid_from,
    null          as dbt_valid_to
