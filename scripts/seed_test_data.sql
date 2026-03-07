-- Seed test data to validate dbt models without requiring an API key.
-- This script populates the raw schema with realistic weather data.

-- Current weather observations
CREATE TABLE IF NOT EXISTS raw.weather_current (
    _dlt_load_id text,
    _dlt_id text,
    name text,
    dt bigint,
    visibility bigint,
    main__temp double precision,
    main__feels_like double precision,
    main__humidity bigint,
    main__pressure bigint,
    wind__speed double precision,
    wind__deg bigint,
    weather__0__main text,
    weather__0__description text,
    clouds__all bigint,
    sys__country text,
    coord__lat double precision,
    coord__lon double precision
);

INSERT INTO raw.weather_current VALUES
('load_001', 'id_001', 'Paris',     1709827200, 10000, 12.5, 10.8, 72, 1015, 4.2, 220, 'Clouds', 'overcast clouds', 90, 'FR', 48.8534, 2.3488),
('load_001', 'id_002', 'Lyon',      1709827200, 8000,  14.1, 12.3, 65, 1013, 3.1, 180, 'Clear',  'clear sky',       10, 'FR', 45.7640, 4.8357),
('load_001', 'id_003', 'Marseille', 1709827200, 10000, 16.3, 15.0, 58, 1012, 5.5, 160, 'Clear',  'clear sky',       5,  'FR', 43.2965, 5.3698),
('load_001', 'id_004', 'Toulouse',  1709827200, 9000,  13.8, 12.1, 68, 1014, 2.8, 200, 'Rain',   'light rain',      75, 'FR', 43.6047, 1.4442),
('load_001', 'id_005', 'Nice',      1709827200, 10000, 15.9, 14.5, 55, 1016, 3.9, 140, 'Clear',  'clear sky',       0,  'FR', 43.7102, 7.2620);

-- Forecast parent table
CREATE TABLE IF NOT EXISTS raw.weather_forecast (
    _dlt_load_id text,
    _dlt_id text,
    city__name text,
    city__country text,
    city__coord__lat double precision,
    city__coord__lon double precision,
    cnt bigint
);

INSERT INTO raw.weather_forecast VALUES
('load_001', 'fc_001', 'Paris',     'FR', 48.8534, 2.3488, 2),
('load_001', 'fc_002', 'Lyon',      'FR', 45.7640, 4.8357, 2),
('load_001', 'fc_003', 'Marseille', 'FR', 43.2965, 5.3698, 2);

-- Forecast entries (child table)
CREATE TABLE IF NOT EXISTS raw.weather_forecast__list (
    _dlt_parent_id text,
    _dlt_list_idx bigint,
    _dlt_id text,
    dt bigint,
    main__temp double precision,
    main__feels_like double precision,
    main__humidity bigint,
    main__pressure bigint,
    wind__speed double precision,
    weather__0__main text,
    weather__0__description text,
    clouds__all bigint,
    pop double precision
);

INSERT INTO raw.weather_forecast__list VALUES
('fc_001', 0, 'fl_001', 1709838000, 11.2, 9.5,  75, 1014, 4.5, 'Rain',   'light rain', 80, 0.6),
('fc_001', 1, 'fl_002', 1709848800, 10.8, 8.9,  78, 1013, 5.1, 'Rain',   'moderate rain', 95, 0.8),
('fc_002', 0, 'fl_003', 1709838000, 13.5, 11.8, 62, 1012, 3.2, 'Clear',  'clear sky', 5, 0.0),
('fc_002', 1, 'fl_004', 1709848800, 12.1, 10.5, 67, 1011, 3.8, 'Clouds', 'scattered clouds', 40, 0.1),
('fc_003', 0, 'fl_005', 1709838000, 15.8, 14.2, 55, 1011, 5.8, 'Clear',  'clear sky', 0, 0.0),
('fc_003', 1, 'fl_006', 1709848800, 14.5, 12.9, 60, 1010, 6.2, 'Clouds', 'few clouds', 20, 0.05);
