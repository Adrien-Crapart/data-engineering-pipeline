-- Singular test: verify all temperatures fall within a physically plausible range.
-- Fails if any temperature is below -90°C or above 60°C (Earth surface extremes).

select
    city_name,
    temperature_celsius
from {{ ref('stg_weather_current') }}
where temperature_celsius < -90
   or temperature_celsius > 60
