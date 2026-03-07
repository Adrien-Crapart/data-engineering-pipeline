-- Mart model: daily weather summary per city.
-- Combines current observations and forecast data into daily aggregates.

with current_obs as (
    select
        city_name,
        date_trunc('day', measured_at) as date_day,
        temperature_celsius,
        feels_like_celsius,
        humidity_percent,
        pressure_hpa,
        wind_speed_ms,
        weather_condition
    from {{ ref('stg_weather_current') }}
),

forecast_obs as (
    select
        city_name,
        date_trunc('day', forecast_at) as date_day,
        temperature_celsius,
        feels_like_celsius,
        humidity_percent,
        pressure_hpa,
        wind_speed_ms,
        weather_condition
    from {{ ref('stg_weather_forecast') }}
),

combined as (
    select * from current_obs
    union all
    select * from forecast_obs
),

daily_summary as (
    select
        city_name,
        date_day,
        round(avg(temperature_celsius)::numeric, 1)   as avg_temperature_celsius,
        round(min(temperature_celsius)::numeric, 1)    as min_temperature_celsius,
        round(max(temperature_celsius)::numeric, 1)    as max_temperature_celsius,
        round(avg(feels_like_celsius)::numeric, 1)     as avg_feels_like_celsius,
        round(avg(humidity_percent)::numeric, 1)        as avg_humidity_percent,
        round(avg(pressure_hpa)::numeric, 1)            as avg_pressure_hpa,
        round(avg(wind_speed_ms)::numeric, 2)           as avg_wind_speed_ms,
        mode() within group (order by weather_condition) as dominant_weather_condition,
        count(*)                                        as observation_count
    from combined
    group by city_name, date_day
)

select * from daily_summary
order by city_name, date_day
