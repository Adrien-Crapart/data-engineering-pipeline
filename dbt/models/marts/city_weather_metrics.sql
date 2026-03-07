-- Mart model: aggregated weather metrics per city.
-- Provides an overview of each monitored city across all collected data.

with current_obs as (
    select
        city_name,
        country_code,
        latitude,
        longitude,
        temperature_celsius,
        humidity_percent,
        pressure_hpa,
        wind_speed_ms,
        measured_at as observed_at
    from {{ ref('stg_weather_current') }}
),

metrics as (
    select
        city_name,
        max(country_code)                               as country_code,
        max(latitude)                                    as latitude,
        max(longitude)                                   as longitude,
        count(*)                                         as total_observations,
        round(avg(temperature_celsius)::numeric, 1)      as avg_temperature_celsius,
        round(min(temperature_celsius)::numeric, 1)      as min_temperature_celsius,
        round(max(temperature_celsius)::numeric, 1)      as max_temperature_celsius,
        round(avg(humidity_percent)::numeric, 1)          as avg_humidity_percent,
        round(avg(pressure_hpa)::numeric, 1)              as avg_pressure_hpa,
        round(avg(wind_speed_ms)::numeric, 2)             as avg_wind_speed_ms,
        min(observed_at)                                  as first_observation,
        max(observed_at)                                  as last_observation
    from current_obs
    group by city_name
)

select * from metrics
order by city_name
