# Analytics

Local analytics engine powered by DuckDB for ad-hoc queries and data exploration.

## Purpose

Provides a lightweight analytical layer on top of the warehouse data, enabling fast
SQL queries, Parquet/CSV exports, and local prototyping without requiring the full
Docker stack.

## Key Files

| File | Description |
|------|-------------|
| `duckdb_analytics.py` | `WeatherAnalytics` class — connects to PostgreSQL via DuckDB, runs analytical queries, exports to Parquet/CSV |
| `example_queries.sql` | Sample SQL queries for weather data exploration |

## Usage

```python
from analytics.duckdb_analytics import WeatherAnalytics

analytics = WeatherAnalytics()
df = analytics.query("SELECT * FROM mart.city_weather_metrics")
analytics.export_parquet(df, "output.parquet")
```

## Dependencies

- `duckdb` (optional dependency group `analytics`)
