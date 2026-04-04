-- Quarantine table: rows from raw weather_forecast that fail schema/type validation.

{% set bucket = var('s3_bucket') %}
{% set prefix = var('s3_raw_prefix') %}

with source as (
    select * from read_parquet('s3://{{ bucket }}/{{ prefix }}/weather_forecast/*.parquet', hive_partitioning=false)
),

forecast_entries as (
    select * from read_parquet('s3://{{ bucket }}/{{ prefix }}/weather_forecast__list/*.parquet', hive_partitioning=false)
),

weather_conditions as (
    select * from read_parquet('s3://{{ bucket }}/{{ prefix }}/weather_forecast__list__weather/*.parquet', hive_partitioning=false)
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

quarantined as (
    select
        *,
        case
            when city_name is null then 'missing_city_name'
            when temperature_celsius is null then 'missing_temperature'
            when forecast_at is null then 'missing_forecast_at'
            when temperature_celsius < -90 or temperature_celsius > 60 then 'temperature_out_of_range'
            when humidity_percent < 0 or humidity_percent > 100 then 'humidity_out_of_range'
            else 'unknown'
        end as rejection_reason
    from renamed
    where city_name is null
       or temperature_celsius is null
       or forecast_at is null
       or temperature_celsius < -90 or temperature_celsius > 60
       or humidity_percent < 0 or humidity_percent > 100
)

select * from quarantined
