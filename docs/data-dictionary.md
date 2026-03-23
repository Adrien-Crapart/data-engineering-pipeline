# Data Dictionary

Formal documentation of all schemas, tables, and columns in the pipeline.

## Raw Layer (MinIO S3 Parquet)

### `weather_current`

Current weather observations from OpenWeather API.

| Column | Type | Description |
|--------|------|-------------|
| `name` | string | City name |
| `dt` | integer | Unix timestamp of measurement |
| `main__temp` | float | Temperature in Celsius |
| `main__feels_like` | float | Feels-like temperature in Celsius |
| `main__humidity` | integer | Humidity percentage (0-100) |
| `main__pressure` | integer | Atmospheric pressure in hPa |
| `wind__speed` | float | Wind speed in m/s |
| `wind__deg` | integer | Wind direction in degrees |
| `weather__0__main` | string | Weather condition group (e.g., Clouds, Rain) |
| `weather__0__description` | string | Detailed weather description |
| `clouds__all` | integer | Cloud coverage percentage |
| `visibility` | integer | Visibility in meters |
| `coord__lat` | float | Latitude |
| `coord__lon` | float | Longitude |
| `sys__country` | string | Country code (ISO 3166) |
| `_extraction_city` | string | City name used for extraction |
| `_extraction_timestamp` | float | Unix timestamp of extraction |
| `_dlt_load_id` | string | dlt load identifier for lineage |
| `_dlt_id` | string | Unique row identifier from dlt |

### `weather_forecast`

5-day / 3-hour forecast parent table.

| Column | Type | Description |
|--------|------|-------------|
| `city__name` | string | City name |
| `city__country` | string | Country code |
| `city__coord__lat` | float | Latitude |
| `city__coord__lon` | float | Longitude |
| `cnt` | integer | Number of forecast entries |
| `_dlt_load_id` | string | dlt load identifier |
| `_dlt_id` | string | Unique row identifier |

### `weather_forecast__list`

Unnested forecast entries (child of `weather_forecast`).

| Column | Type | Description |
|--------|------|-------------|
| `dt` | integer | Forecast Unix timestamp |
| `main__temp` | float | Forecast temperature (Celsius) |
| `main__feels_like` | float | Feels-like temperature |
| `main__humidity` | integer | Humidity percentage |
| `main__pressure` | integer | Atmospheric pressure (hPa) |
| `wind__speed` | float | Wind speed (m/s) |
| `weather__0__main` | string | Weather condition group |
| `weather__0__description` | string | Detailed description |
| `clouds__all` | integer | Cloud coverage percentage |
| `pop` | float | Precipitation probability (0-1) |
| `_dlt_id` | string | Unique row identifier |
| `_dlt_parent_id` | string | Reference to parent forecast row |

## Staging Layer (DuckDB Views -> PostgreSQL)

### `staging.stg_weather_current`

Cleaned and typed current weather observations.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `city_name` | string | `name` | City name |
| `country_code` | string | `sys__country` | ISO country code |
| `temperature_celsius` | float | `main__temp` | Temperature (C) |
| `feels_like_celsius` | float | `main__feels_like` | Feels-like temp (C) |
| `humidity_percent` | integer | `main__humidity` | Humidity (%) |
| `pressure_hpa` | integer | `main__pressure` | Pressure (hPa) |
| `wind_speed_ms` | float | `wind__speed` | Wind speed (m/s) |
| `weather_condition` | string | `weather__0__main` | Condition group |
| `weather_description` | string | `weather__0__description` | Detailed description |
| `latitude` | float | `coord__lat` | Latitude |
| `longitude` | float | `coord__lon` | Longitude |
| `cloud_coverage_percent` | integer | `clouds__all` | Cloud coverage (%) |
| `visibility_meters` | integer | `visibility` | Visibility (m) |
| `measured_at` | timestamp | `to_timestamp(dt)` | Measurement time |
| `load_id` | string | `_dlt_load_id` | dlt load ID |
| `row_id` | string | `_dlt_id` | Unique row ID |
| `loaded_at` | timestamp | `current_timestamp` | Processing time |

### `staging.stg_weather_forecast`

Cleaned forecast entries joined with parent metadata.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `city_name` | string | `city__name` | City name |
| `country_code` | string | `city__country` | ISO country code |
| `latitude` | float | `city__coord__lat` | Latitude |
| `longitude` | float | `city__coord__lon` | Longitude |
| `temperature_celsius` | float | `main__temp` | Forecast temp (C) |
| `feels_like_celsius` | float | `main__feels_like` | Feels-like temp (C) |
| `humidity_percent` | integer | `main__humidity` | Humidity (%) |
| `pressure_hpa` | integer | `main__pressure` | Pressure (hPa) |
| `wind_speed_ms` | float | `wind__speed` | Wind speed (m/s) |
| `weather_condition` | string | `weather__0__main` | Condition group |
| `weather_description` | string | `weather__0__description` | Detailed description |
| `cloud_coverage_percent` | integer | `clouds__all` | Cloud coverage (%) |
| `precipitation_probability` | float | `pop` | Precip probability (0-1) |
| `forecast_at` | timestamp | `to_timestamp(dt)` | Forecast time |
| `load_id` | string | `_dlt_load_id` | dlt load ID |
| `row_id` | string | `_dlt_id` | Unique row ID |
| `loaded_at` | timestamp | `current_timestamp` | Processing time |

## Mart Layer (PostgreSQL Tables)

### `mart.weather_daily_summary`

Daily per-city weather aggregates.

| Column | Type | Description |
|--------|------|-------------|
| `city_name` | string | City name |
| `date_day` | date | Calendar date |
| `avg_temperature_celsius` | float | Average temperature |
| `min_temperature_celsius` | float | Minimum temperature |
| `max_temperature_celsius` | float | Maximum temperature |
| `avg_feels_like_celsius` | float | Average feels-like |
| `avg_humidity_percent` | float | Average humidity |
| `avg_pressure_hpa` | float | Average pressure |
| `avg_wind_speed_ms` | float | Average wind speed |
| `dominant_weather_condition` | string | Most frequent condition |
| `observation_count` | integer | Number of observations |

### `mart.city_weather_metrics`

Per-city aggregated metrics across all collected data.

| Column | Type | Description |
|--------|------|-------------|
| `city_name` | string | City name (unique) |
| `country_code` | string | ISO country code |
| `latitude` | float | Latitude |
| `longitude` | float | Longitude |
| `total_observations` | integer | Total observation count |
| `avg_temperature_celsius` | float | Average temperature |
| `min_temperature_celsius` | float | All-time minimum temp |
| `max_temperature_celsius` | float | All-time maximum temp |
| `avg_humidity_percent` | float | Average humidity |
| `avg_pressure_hpa` | float | Average pressure |
| `avg_wind_speed_ms` | float | Average wind speed |
| `first_observation` | timestamp | Earliest observation |
| `last_observation` | timestamp | Latest observation |
