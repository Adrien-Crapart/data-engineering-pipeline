-- Quarantine table: rows from raw weather_current that fail schema/type validation.
-- Captures rejected records for triage and debugging.

with source as (
    select * from {{ source('openweather_raw', 'weather_current') }}
),

weather_conditions as (
    select * from {{ source('openweather_raw', 'weather_current__weather') }}
),

joined as (
    select
        s.name                        as city_name,
        s.sys__country                as country_code,
        s.main__temp                  as temperature_celsius,
        s.main__feels_like            as feels_like_celsius,
        s.main__humidity              as humidity_percent,
        s.main__pressure              as pressure_hpa,
        s.wind__speed                 as wind_speed_ms,
        w.main                        as weather_condition,
        w.description                 as weather_description,
        s.coord__lat                  as latitude,
        s.coord__lon                  as longitude,
        s.clouds__all                 as cloud_coverage_percent,
        s.visibility                  as visibility_meters,
        to_timestamp(s.dt)            as measured_at,
        s._dlt_load_id                as load_id,
        s._dlt_id                     as row_id,
        current_timestamp             as loaded_at
    from source s
    left join weather_conditions w
        on s._dlt_id = w._dlt_parent_id
        and w._dlt_list_idx = 0
),

quarantined as (
    select
        *,
        case
            when city_name is null then 'missing_city_name'
            when temperature_celsius is null then 'missing_temperature'
            when measured_at is null then 'missing_measured_at'
            when temperature_celsius < -90 or temperature_celsius > 60 then 'temperature_out_of_range'
            when humidity_percent < 0 or humidity_percent > 100 then 'humidity_out_of_range'
            else 'unknown'
        end as rejection_reason
    from joined
    where city_name is null
       or temperature_celsius is null
       or measured_at is null
       or temperature_celsius < -90 or temperature_celsius > 60
       or humidity_percent < 0 or humidity_percent > 100
)

select * from quarantined
