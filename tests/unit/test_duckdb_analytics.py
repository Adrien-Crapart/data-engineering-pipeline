"""Unit tests for the DuckDB analytics engine."""

import duckdb
import pytest

from analytics.duckdb_analytics import DuckDBConfig, WeatherAnalytics


class TestDuckDBConfig:
    """Validate DuckDB configuration."""

    def test_default_values(self):
        config = DuckDBConfig()
        assert config.postgres_host == "localhost"
        assert config.postgres_port == 5432
        assert config.postgres_db == "weather_db"

    def test_dsn_format(self):
        config = DuckDBConfig(
            postgres_host="myhost",
            postgres_port=5433,
            postgres_db="testdb",
            postgres_user="user",
            postgres_password="pass",
        )
        assert "host=myhost" in config.dsn
        assert "port=5433" in config.dsn
        assert "dbname=testdb" in config.dsn


class TestDuckDBLocal:
    """Test DuckDB operations using in-memory database (no PostgreSQL needed)."""

    def test_query_returns_list_of_dicts(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test_weather (city TEXT, temp FLOAT)")
        conn.execute("INSERT INTO test_weather VALUES ('Paris', 15.5), ('Lyon', 12.3)")

        result = conn.execute("SELECT * FROM test_weather ORDER BY city")
        columns = [desc[0] for desc in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

        assert len(rows) == 2
        assert rows[0]["city"] == "Lyon"
        assert rows[1]["temp"] == pytest.approx(15.5)
        conn.close()

    def test_aggregation_queries(self):
        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE weather_data (
                city TEXT, temperature FLOAT, humidity FLOAT
            )
        """)
        conn.execute("""
            INSERT INTO weather_data VALUES
                ('Paris', 15.0, 65.0),
                ('Paris', 18.0, 70.0),
                ('Lyon', 12.0, 55.0),
                ('Lyon', 14.0, 60.0)
        """)

        result = conn.execute("""
            SELECT
                city,
                ROUND(AVG(temperature)::NUMERIC, 1) AS avg_temp,
                COUNT(*) AS obs
            FROM weather_data
            GROUP BY city
            ORDER BY avg_temp DESC
        """).fetchall()

        assert len(result) == 2
        assert result[0][0] == "Paris"
        assert result[0][1] == pytest.approx(16.5)
        conn.close()

    def test_parquet_export(self, tmp_path):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE export_test (id INT, value TEXT)")
        conn.execute("INSERT INTO export_test VALUES (1, 'hello'), (2, 'world')")

        output = str(tmp_path / "test_export.parquet")
        conn.execute(f"COPY export_test TO '{output}' (FORMAT PARQUET)")

        imported = conn.execute(f"SELECT * FROM read_parquet('{output}')").fetchall()
        assert len(imported) == 2
        conn.close()

    def test_csv_export(self, tmp_path):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE csv_test (city TEXT, temp FLOAT)")
        conn.execute("INSERT INTO csv_test VALUES ('Paris', 15.5)")

        output = str(tmp_path / "test_export.csv")
        conn.execute(f"COPY csv_test TO '{output}' (FORMAT CSV, HEADER TRUE)")

        content = open(output).read()
        assert "Paris" in content
        assert "15.5" in content
        conn.close()
