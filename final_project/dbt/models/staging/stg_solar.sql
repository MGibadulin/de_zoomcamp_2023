{{ config(materialized='view') }}

select
    timestamp as ts,
    solar_energy_mw
from {{ source('stg_energy', 'solar_energy') }}
where timestamp is not null