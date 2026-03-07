"""
DuckDB Analytics Engine.

Provides fast, local analytical queries on top of the PostgreSQL
data warehouse. DuckDB can attach to PostgreSQL directly via its
postgres scanner extension, enabling OLAP queries without data movement.

Usage:
    from analytics.duckdb_analytics import WeatherAnalytics

    analytics = WeatherAnalytics.from_env()
    analytics.top_hottest_cities(limit=5)
    analytics.daily_temperature_trend("Paris", days=30)
    analytics.export_to_parquet("output/weather_export.parquet")
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DuckDBConfig:
    """Configuration for DuckDB PostgreSQL connection."""

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "weather_db"
    postgres_user: str = "airflow"
    postgres_password: str = "airflow"

    @classmethod
    def from_env(cls) -> DuckDBConfig:
        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "weather_db"),
            postgres_user=os.getenv("POSTGRES_USER", "airflow"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )

    @property
    def dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


class WeatherAnalytics:
    """Analytical query engine using DuckDB on top of PostgreSQL data."""

    def __init__(self, config: DuckDBConfig | None = None) -> None:
        self.config = config or DuckDBConfig.from_env()
        self.conn = duckdb.connect()
        self._attach_postgres()

    def _attach_postgres(self) -> None:
        """Attach PostgreSQL as a data source via DuckDB postgres scanner."""
        self.conn.execute("INSTALL postgres; LOAD postgres;")
        self.conn.execute(
            f"ATTACH '{self.config.dsn}' AS pg (TYPE POSTGRES, READ_ONLY);"
        )
        logger.info("Attached PostgreSQL at %s:%s", self.config.postgres_host, self.config.postgres_port)

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as list of dicts."""
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def query_df(self, sql: str) -> Any:
        """Execute a SQL query and return a DuckDB relation (lazy)."""
        return self.conn.sql(sql)

    def top_hottest_cities(self, limit: int = 5) -> list[dict[str, Any]]:
        """Get the hottest cities by average temperature."""
        return self.query(f"""
            SELECT
                city_name,
                ROUND(avg_temperature_celsius::NUMERIC, 1) AS avg_temp_c,
                total_observations
            FROM pg.staging.stg_weather_current
            GROUP BY city_name
            ORDER BY AVG(temperature_celsius) DESC
            LIMIT {limit}
        """)

    def daily_temperature_trend(self, city: str, days: int = 30) -> list[dict[str, Any]]:
        """Get daily temperature trend for a city."""
        return self.query(f"""
            SELECT
                measured_at::DATE AS date_day,
                ROUND(AVG(temperature_celsius)::NUMERIC, 1) AS avg_temp,
                ROUND(MIN(temperature_celsius)::NUMERIC, 1) AS min_temp,
                ROUND(MAX(temperature_celsius)::NUMERIC, 1) AS max_temp,
                COUNT(*) AS observations
            FROM pg.staging.stg_weather_current
            WHERE city_name = '{city}'
            GROUP BY measured_at::DATE
            ORDER BY date_day DESC
            LIMIT {days}
        """)

    def weather_summary_stats(self) -> list[dict[str, Any]]:
        """Get comprehensive weather statistics per city."""
        return self.query("""
            SELECT
                city_name,
                COUNT(*) AS total_records,
                ROUND(AVG(temperature_celsius)::NUMERIC, 1) AS avg_temp,
                ROUND(STDDEV(temperature_celsius)::NUMERIC, 2) AS temp_stddev,
                ROUND(AVG(humidity_percent)::NUMERIC, 1) AS avg_humidity,
                ROUND(AVG(wind_speed_ms)::NUMERIC, 2) AS avg_wind,
                MIN(measured_at) AS first_observation,
                MAX(measured_at) AS last_observation
            FROM pg.staging.stg_weather_current
            GROUP BY city_name
            ORDER BY total_records DESC
        """)

    def cross_city_comparison(self) -> list[dict[str, Any]]:
        """Compare weather metrics across all monitored cities."""
        return self.query("""
            WITH city_stats AS (
                SELECT
                    city_name,
                    AVG(temperature_celsius) AS avg_temp,
                    AVG(humidity_percent) AS avg_humidity,
                    AVG(wind_speed_ms) AS avg_wind,
                    COUNT(*) AS obs_count
                FROM pg.staging.stg_weather_current
                GROUP BY city_name
            )
            SELECT
                city_name,
                ROUND(avg_temp::NUMERIC, 1) AS avg_temp_c,
                ROUND(avg_humidity::NUMERIC, 1) AS avg_humidity_pct,
                ROUND(avg_wind::NUMERIC, 2) AS avg_wind_ms,
                obs_count,
                ROUND((avg_temp - (SELECT AVG(avg_temp) FROM city_stats))::NUMERIC, 1) AS temp_vs_mean
            FROM city_stats
            ORDER BY avg_temp DESC
        """)

    def export_to_parquet(self, output_path: str) -> str:
        """Export staging data to Parquet format for downstream use."""
        self.conn.execute(f"""
            COPY (
                SELECT * FROM pg.staging.stg_weather_current
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        logger.info("Exported data to %s", output_path)
        return output_path

    def export_to_csv(self, output_path: str) -> str:
        """Export staging data to CSV format."""
        self.conn.execute(f"""
            COPY (
                SELECT * FROM pg.staging.stg_weather_current
            ) TO '{output_path}' (FORMAT CSV, HEADER TRUE);
        """)
        logger.info("Exported data to %s", output_path)
        return output_path

    def close(self) -> None:
        """Close DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> WeatherAnalytics:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Weather Analytics (DuckDB) ===\n")

    with WeatherAnalytics() as analytics:
        print("--- Top Hottest Cities ---")
        for row in analytics.top_hottest_cities():
            print(f"  {row['city_name']}: {row['avg_temp_c']}°C ({row['total_observations']} obs)")

        print("\n--- Cross-City Comparison ---")
        for row in analytics.cross_city_comparison():
            print(
                f"  {row['city_name']}: "
                f"temp={row['avg_temp_c']}°C "
                f"humidity={row['avg_humidity_pct']}% "
                f"wind={row['avg_wind_ms']}m/s "
                f"(vs mean: {row['temp_vs_mean']:+}°C)"
            )

        print("\n--- Summary Stats ---")
        for row in analytics.weather_summary_stats():
            print(
                f"  {row['city_name']}: "
                f"{row['total_records']} records, "
                f"avg={row['avg_temp']}°C (±{row['temp_stddev']})"
            )
