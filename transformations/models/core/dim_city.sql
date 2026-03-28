-- Conformed dimension: city reference table.
-- Deduplicates city metadata from weather observations into a single
-- authoritative source of city information for the business domain.

with city_data as (
    select
        city_name,
        country_code,
        latitude,
        longitude,
        row_number() over (partition by city_name order by measured_at desc) as rn
    from {{ ref('stg_weather_current') }}
),

deduped as (
    select
        city_name,
        country_code,
        latitude,
        longitude
    from city_data
    where rn = 1
)

select * from deduped
