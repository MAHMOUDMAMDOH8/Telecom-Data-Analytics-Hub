{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='cell_site_sk',
        indexes=[
            {"columns":['cell_site_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Cell_Site_cte as (
    select
        distinct cell_id,
        site_name,
        city,
        latitude,
        longitude,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ref('CDC_Cell_Site')}}
    where cell_id is not null
),
Cell_Site_with_sk as (
    select
        md5(cell_id || site_name || city) as cell_site_sk,
        *
    from Cell_Site_cte
)

{% if is_incremental() %}

select * from Cell_Site_with_sk
where cell_site_sk not in (select distinct cell_site_sk from {{ this }})
{% else %}
select * from Cell_Site_with_sk
{% endif %}