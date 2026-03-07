-- Staging model: clean and type raw current weather observations.

with source as (
    select * from {{ source('raw', 'weather_current') }}
),

renamed as (
    select
        name                        as city_name,
        sys__country                as country_code,
        main__temp                  as temperature_celsius,
        main__feels_like            as feels_like_celsius,
        main__humidity              as humidity_percent,
        main__pressure              as pressure_hpa,
        wind__speed                 as wind_speed_ms,
        weather__0__main            as weather_condition,
        weather__0__description     as weather_description,
        coord__lat                  as latitude,
        coord__lon                  as longitude,
        clouds__all                 as cloud_coverage_percent,
        visibility                  as visibility_meters,
        to_timestamp(dt)            as measured_at,
        _dlt_load_id                as load_id,
        _dlt_id                     as row_id,
        current_timestamp           as loaded_at
    from source
)

select * from renamed
