{{
    config(
        materialized='incremental',
        unique_key='cell_site_sk'
    )
}}
with cell_site_selected_columns as (
    select
        cell_site_sk,
        cell_id,
        site_name as cell_site_name,
        city,
        latitude::FLOAT as latitude,
        longitude::FLOAT as longitude,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('cell_site') }}
)
select *
from cell_site_selected_columns
-- ✅ Unknown / Default Cell Site for handling missing or unmatched cell site references in fact tables
union all
select
    '-1'      as cell_site_sk,
    '-1'          as cell_id,
    'Unknown'     as cell_site_name,
    'unknown'     as city,
    0.0::FLOAT          as latitude,
    0.0::FLOAT          as longitude,
    null          as dbt_valid_from,
    null          as dbt_valid_to
