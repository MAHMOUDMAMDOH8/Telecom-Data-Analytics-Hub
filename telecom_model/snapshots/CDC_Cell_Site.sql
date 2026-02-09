{% snapshot CDC_Cell_Site  %}
{{
    config(
        unique_key='cell_id',
        strategy='check',
        check_cols=['site_name', 'city', 'latitude', 'longitude'],
        target_schema='snapshots'
    )
}}

select 
    cell_id :: VARCHAR(20) as cell_id,
    site_name :: VARCHAR(100) as site_name,
    city :: VARCHAR(50) as city,
    latitude :: FLOAT as latitude,
    longitude :: FLOAT as longitude
from {{ source('telecom_row', 'cell_sites') }}

{% endsnapshot %}