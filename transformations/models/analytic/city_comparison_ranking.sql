-- Analytic model: cross-city ranking and comparison.
-- Ranks cities on multiple weather dimensions for comparative analysis.

with city_stats as (
    select
        city_name,
        country_code,
        avg_temperature_celsius,
        avg_humidity_percent,
        avg_wind_speed_ms,
        total_observations,
        first_observation,
        last_observation
    from {{ ref('city_weather_metrics') }}
),

ranked as (
    select
        *,
        rank() over (order by avg_temperature_celsius desc) as temp_rank_warmest,
        rank() over (order by avg_temperature_celsius asc)  as temp_rank_coldest,
        rank() over (order by avg_humidity_percent desc)    as humidity_rank,
        rank() over (order by avg_wind_speed_ms desc)       as wind_rank,
        rank() over (order by total_observations desc)      as data_completeness_rank,
        avg_temperature_celsius - (
            select avg(avg_temperature_celsius) from city_stats
        ) as temp_vs_mean
    from city_stats
)

select * from ranked
order by temp_rank_warmest
