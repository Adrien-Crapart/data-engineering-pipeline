"""Pipeline configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PipelineConfig:
    """Typed configuration for the OpenWeather ingestion pipeline."""

    api_key: str
    cities: list[str]
    base_url: str = "https://api.openweathermap.org/data/2.5"
    units: str = "metric"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "datawarehouse"
    postgres_user: str = "datawarehouse_user"
    postgres_password: str = "datawarehouse_password"
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "weather-data-lake"
    minio_secure: bool = False

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """Build config from environment variables with validation."""
        api_key = os.getenv("OPENWEATHER_API_KEY", "")
        if not api_key or api_key == "your_api_key_here":
            raise ValueError(
                "OPENWEATHER_API_KEY is not set. "
                "Get a free key at https://openweathermap.org/api"
            )

        raw_cities = os.getenv("WEATHER_CITIES", "Paris,Lyon,Marseille,Toulouse,Nice")
        cities = [c.strip() for c in raw_cities.split(",") if c.strip()]
        if not cities:
            raise ValueError("WEATHER_CITIES must contain at least one city name.")

        minio_endpoint_raw = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        minio_endpoint = minio_endpoint_raw.replace("http://", "").replace("https://", "")

        return cls(
            api_key=api_key,
            cities=cities,
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "weather_db"),
            postgres_user=os.getenv("POSTGRES_USER", "airflow"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            minio_endpoint=minio_endpoint,
            minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET_NAME", "weather-data-lake"),
            minio_secure=minio_endpoint_raw.startswith("https://"),
        )

    @property
    def postgres_dsn(self) -> str:
        """Return a PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
