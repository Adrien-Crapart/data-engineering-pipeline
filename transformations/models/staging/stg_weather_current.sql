-- Staging model: clean and type raw current weather observations.
-- Reads DLT-normalized Parquet from S3 via dbt-duckdb external sources.
-- Joins parent table with child weather conditions table.

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

quality_checked as (
    select *
    from joined
    where city_name is not null
      and temperature_celsius is not null
      and measured_at is not null
      and temperature_celsius between -90 and 60
      and (humidity_percent is null or humidity_percent between 0 and 100)
)

select * from quality_checked
