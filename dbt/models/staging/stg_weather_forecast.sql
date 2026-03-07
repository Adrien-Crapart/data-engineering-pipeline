-- Staging model: unnest and clean forecast entries from the raw API response.

with source as (
    select * from {{ source('raw', 'weather_forecast') }}
),

forecast_entries as (
    select * from {{ source('raw', 'weather_forecast__list') }}
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
        f.weather__0__main              as weather_condition,
        f.weather__0__description       as weather_description,
        f.clouds__all                   as cloud_coverage_percent,
        f.pop                           as precipitation_probability,
        to_timestamp(f.dt)              as forecast_at,
        s._dlt_load_id                  as load_id,
        f._dlt_id                       as row_id,
        current_timestamp               as loaded_at
    from source s
    inner join forecast_entries f
        on s._dlt_id = f._dlt_parent_id
)

select * from renamed
