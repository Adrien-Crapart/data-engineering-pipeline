-- Staging model: unnest and clean forecast entries from the raw API response.
-- Reads DLT-normalized Parquet from S3 via dbt-duckdb external sources.
-- Joins forecast list with parent city data and weather conditions.

with source as (
    select * from {{ source('openweather_raw', 'weather_forecast') }}
),

forecast_entries as (
    select * from {{ source('openweather_raw', 'weather_forecast__list') }}
),

weather_conditions as (
    select * from {{ source('openweather_raw', 'weather_forecast__list__weather') }}
),

renamed as (
    select
        s.city__name                    as city_name,
        s.city__country                 as country_code,
        s.city__coord__lat              as latitude,
        s.city__coord__lon              as longitude,
        f.main__temp                    as temperature_celsius,
        f.main__feels_like              as feels_like_celsius,
        f.main__humidity                as humidity_percent,
        f.main__pressure                as pressure_hpa,
        f.wind__speed                   as wind_speed_ms,
        w.main                          as weather_condition,
        w.description                   as weather_description,
        f.clouds__all                   as cloud_coverage_percent,
        f.pop                           as precipitation_probability,
        to_timestamp(f.dt)              as forecast_at,
        s._dlt_load_id                  as load_id,
        f._dlt_id                       as row_id,
        current_timestamp               as loaded_at
    from source s
    inner join forecast_entries f
        on s._dlt_id = f._dlt_parent_id
    left join weather_conditions w
        on f._dlt_id = w._dlt_parent_id
        and w._dlt_list_idx = 0
),

quality_checked as (
    select *
    from renamed
    where city_name is not null
      and temperature_celsius is not null
      and forecast_at is not null
      and temperature_celsius between -90 and 60
      and (humidity_percent is null or humidity_percent between 0 and 100)
)

select * from quality_checked
