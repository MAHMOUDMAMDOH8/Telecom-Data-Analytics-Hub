-- Test: Verify geographic coordinates (latitude/longitude) are within valid ranges
-- Purpose: Detect invalid geographic data in cell site dimension
-- Expected: 0 rows (all coordinates valid)

select 
    cell_site_sk,
    cell_id,
    city,
    latitude,
    longitude,
    case 
        when latitude < -90 or latitude > 90 then 'Invalid Latitude'
        when longitude < -180 or longitude > 180 then 'Invalid Longitude'
        when latitude is null or longitude is null then 'NULL Coordinate'
        else 'Valid'
    end as coordinate_status
from {{ ref('DIM_cell_site') }}
where 
    latitude < -90 
    or latitude > 90 
    or longitude < -180 
    or longitude > 180
    or latitude is null
    or longitude is null
