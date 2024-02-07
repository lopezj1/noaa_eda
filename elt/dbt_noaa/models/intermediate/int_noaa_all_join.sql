with 
    catches as {{ ref('stg_noaa__catches') }},
    sizes as {{ ref('stg_noaa__sizes') }},
    trips as {{ ref('stg_noaa__trips') }}

select * 
from trips t
left join catches c on c.noaa_id = t.noaa_id
left join sizes s on s.noaa_id = t.noaa_id