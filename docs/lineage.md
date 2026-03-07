# Data Lineage

## End-to-End Lineage

```mermaid
flowchart TD
    subgraph source ["Source: OpenWeather API"]
        EP1["GET /data/2.5/weather<br/>(per city)"]
        EP2["GET /data/2.5/forecast<br/>(per city)"]
    end

    subgraph raw ["Layer: raw (PostgreSQL)"]
        R1["raw.weather_current<br/>One row per city per extraction"]
        R2["raw.weather_forecast<br/>One row per city (parent)"]
        R3["raw.weather_forecast__list<br/>One row per 3h slot (child)"]
    end

    subgraph staging ["Layer: staging (dbt views)"]
        S1["stg_weather_current<br/>Renamed + typed columns"]
        S2["stg_weather_forecast<br/>Joined parent→child, renamed"]
    end

    subgraph mart ["Layer: mart (dbt tables)"]
        M1["weather_daily_summary<br/>Daily aggregates per city"]
        M2["city_weather_metrics<br/>Overall metrics per city"]
    end

    subgraph quality ["Data Quality: Soda Core"]
        Q1["19 SodaCL checks"]
    end

    EP1 -->|"dlt extract + load"| R1
    EP2 -->|"dlt extract + load"| R2
    EP2 -->|"dlt unnest"| R3

    R1 -->|"SELECT + rename + cast"| S1
    R2 -->|"JOIN on _dlt_parent_id"| S2
    R3 -->|"JOIN on _dlt_parent_id"| S2

    S1 -->|"GROUP BY city, day"| M1
    S2 -->|"GROUP BY city, day"| M1
    S1 -->|"GROUP BY city"| M2

    S1 --> Q1
    S2 --> Q1
    M1 --> Q1
    M2 --> Q1
```

## Column Lineage

### `stg_weather_current`

| Target Column | Source | Transformation |
|--------------|--------|----------------|
| `city_name` | `raw.weather_current.name` | Direct mapping |
| `country_code` | `raw.weather_current.sys__country` | Direct mapping |
| `temperature_celsius` | `raw.weather_current.main__temp` | Direct mapping (metric units) |
| `humidity_percent` | `raw.weather_current.main__humidity` | Direct mapping |
| `measured_at` | `raw.weather_current.dt` | `to_timestamp(dt)` |

### `stg_weather_forecast`

| Target Column | Source | Transformation |
|--------------|--------|----------------|
| `city_name` | `raw.weather_forecast.city__name` | JOIN parent table |
| `temperature_celsius` | `raw.weather_forecast__list.main__temp` | Direct mapping |
| `forecast_at` | `raw.weather_forecast__list.dt` | `to_timestamp(dt)` |

### `weather_daily_summary`

| Target Column | Source | Transformation |
|--------------|--------|----------------|
| `date_day` | `stg_*.measured_at / forecast_at` | `date_trunc('day', ...)` |
| `avg_temperature_celsius` | `stg_*.temperature_celsius` | `avg()` grouped by city + day |
| `dominant_weather_condition` | `stg_*.weather_condition` | `mode()` grouped by city + day |

### `city_weather_metrics`

| Target Column | Source | Transformation |
|--------------|--------|----------------|
| `total_observations` | `stg_weather_current.*` | `count(*)` grouped by city |
| `avg_temperature_celsius` | `stg_weather_current.temperature_celsius` | `avg()` grouped by city |
| `first_observation` | `stg_weather_current.measured_at` | `min()` grouped by city |
