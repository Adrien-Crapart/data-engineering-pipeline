-- Fact table: individual weather observations.
-- Cleaned, typed, and enriched current weather readings.
-- This is the canonical business-domain fact table for weather events.

select
    row_id,
    city_name,
    country_code,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    pressure_hpa,
    wind_speed_ms,
    weather_condition,
    weather_description,
    cloud_coverage_percent,
    visibility_meters,
    measured_at,
    load_id,
    loaded_at
from {{ ref('stg_weather_current') }}
