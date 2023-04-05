{{ config(materialized='table',
            partition_by={
                "field": "timestamp",
                "data_type": "timestamp",
                "granularity": "month"
            },
        ) 
}}

select
    se.ts,
    solar_energy_mw,
    wind_energy_onshore_mw,
    wind_energy_offshore_mw,
    wind_energy_total_mw,
    solar_energy_mw + wind_energy_total_mw as energy_total_mw
from {{ ref('stg_solar') }} as se
inner join {{ ref('stg_wind') }} as we
on se.ts = we.ts
