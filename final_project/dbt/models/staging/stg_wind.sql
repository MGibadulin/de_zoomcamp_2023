{{ config(materialized='view') }}

select
    timestamp as ts,
    wind_energy_onshore_mw,
    wind_energy_offshore_mw,
    wind_energy_total_mw
from {{ source('stg_energy', 'wind_energy') }}
where timestamp is not null