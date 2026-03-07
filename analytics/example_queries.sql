-- ============================================================
-- Example Analytics Queries
-- ============================================================
-- These queries demonstrate usage of the mart layer models
-- built by the weather data pipeline.
-- ============================================================

-- Top 5 hottest cities by average temperature
SELECT
    city_name,
    ROUND(avg_temperature_celsius::numeric, 1) AS avg_temp,
    total_observations
FROM analytics.city_weather_metrics
ORDER BY avg_temperature_celsius DESC
LIMIT 5;

-- Daily temperature trend for a specific city
SELECT
    date_day,
    city_name,
    ROUND(avg_temperature_celsius::numeric, 1) AS avg_temp,
    ROUND(min_temperature_celsius::numeric, 1) AS min_temp,
    ROUND(max_temperature_celsius::numeric, 1) AS max_temp
FROM analytics.weather_daily_summary
WHERE city_name = 'Paris'
ORDER BY date_day DESC
LIMIT 30;

-- Cities with extreme humidity (above 80%)
SELECT
    city_name,
    ROUND(avg_humidity_percent::numeric, 1) AS avg_humidity,
    total_observations
FROM analytics.city_weather_metrics
WHERE avg_humidity_percent > 80
ORDER BY avg_humidity_percent DESC;

-- Wind speed comparison across cities
SELECT
    city_name,
    ROUND(avg_wind_speed_ms::numeric, 2) AS avg_wind,
    ROUND(max_wind_speed_ms::numeric, 2) AS max_wind
FROM analytics.city_weather_metrics
ORDER BY avg_wind_speed_ms DESC;

-- Temperature variance by day (detect weather instability)
SELECT
    date_day,
    city_name,
    ROUND((max_temperature_celsius - min_temperature_celsius)::numeric, 1) AS temp_range,
    observation_count
FROM analytics.weather_daily_summary
WHERE (max_temperature_celsius - min_temperature_celsius) > 10
ORDER BY temp_range DESC
LIMIT 20;

-- Latest observations per city
SELECT
    s.city_name,
    s.temperature_celsius,
    s.humidity_percent,
    s.wind_speed_ms,
    s.weather_description,
    s.measured_at
FROM staging.stg_weather_current s
INNER JOIN (
    SELECT city_name, MAX(measured_at) AS latest
    FROM staging.stg_weather_current
    GROUP BY city_name
) latest ON s.city_name = latest.city_name AND s.measured_at = latest.latest
ORDER BY s.city_name;
