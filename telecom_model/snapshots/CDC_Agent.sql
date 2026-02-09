{% snapshot CDC_Agent  %}
{{
    config(
        unique_key='agent_id',
        strategy='check',
        check_cols=['name', 'department', 'status', 'city'],
        target_schema='snapshots'
    )
}}

select 
    agent_id :: VARCHAR(20) as agent_id,
    name :: VARCHAR(100) as name,
    department :: VARCHAR(50) as department,
    status :: VARCHAR(20) as status,
    city :: VARCHAR(50) as city
from {{ source('telecom_row', 'agents') }}

{% endsnapshot %}