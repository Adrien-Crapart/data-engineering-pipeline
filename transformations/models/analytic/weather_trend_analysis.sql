-- Analytic model: rolling 7-day weather trend per city.
-- Calculates moving averages for temperature, humidity, and wind speed
-- to identify weather trends and anomalies.

with daily as (
    select
        city_name,
        date_day,
        avg_temperature_celsius,
        avg_humidity_percent,
        avg_wind_speed_ms,
        observation_count
    from {{ ref('weather_daily_summary') }}
),

trends as (
    select
        city_name,
        date_day,
        avg_temperature_celsius,
        avg_humidity_percent,
        avg_wind_speed_ms,
        observation_count,
        round(avg(avg_temperature_celsius) over (
            partition by city_name order by date_day
            rows between 6 preceding and current row
        ), 1) as temp_7d_moving_avg,
        round(avg(avg_humidity_percent) over (
            partition by city_name order by date_day
            rows between 6 preceding and current row
        ), 1) as humidity_7d_moving_avg,
        round(avg(avg_wind_speed_ms) over (
            partition by city_name order by date_day
            rows between 6 preceding and current row
        ), 2) as wind_7d_moving_avg,
        avg_temperature_celsius - lag(avg_temperature_celsius, 1) over (
            partition by city_name order by date_day
        ) as temp_day_over_day_change
    from daily
)

select * from trends
order by city_name, date_day desc
