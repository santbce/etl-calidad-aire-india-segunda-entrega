CREATE SCHEMA IF NOT EXISTS india_dw;
CREATE TABLE IF NOT EXISTS india_dw.dim_city (
 city_id INTEGER PRIMARY KEY, city_name TEXT NOT NULL UNIQUE,
 latitude DOUBLE PRECISION NOT NULL CHECK(latitude BETWEEN -90 AND 90),
 longitude DOUBLE PRECISION NOT NULL CHECK(longitude BETWEEN -180 AND 180), source_url TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS india_dw.dim_date (
 date DATE PRIMARY KEY, year INTEGER NOT NULL, month INTEGER NOT NULL,
 day INTEGER NOT NULL, quarter INTEGER NOT NULL, day_of_week TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS india_dw.dim_aqi (aqi_id INTEGER PRIMARY KEY, aqi_bucket TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS india_dw.fact_air_quality (
 measurement_id INTEGER PRIMARY KEY,
 city_id INTEGER NOT NULL REFERENCES india_dw.dim_city(city_id),
 date DATE NOT NULL REFERENCES india_dw.dim_date(date),
 aqi_id INTEGER NOT NULL REFERENCES india_dw.dim_aqi(aqi_id),
 pm25 DOUBLE PRECISION NOT NULL CHECK(pm25>=0), pm10 DOUBLE PRECISION NOT NULL CHECK(pm10>=0),
 no_value DOUBLE PRECISION NOT NULL CHECK(no_value>=0), no2 DOUBLE PRECISION NOT NULL CHECK(no2>=0),
 nox DOUBLE PRECISION NOT NULL CHECK(nox>=0), nh3 DOUBLE PRECISION NOT NULL CHECK(nh3>=0),
 co DOUBLE PRECISION NOT NULL CHECK(co>=0), so2 DOUBLE PRECISION NOT NULL CHECK(so2>=0),
 o3 DOUBLE PRECISION NOT NULL CHECK(o3>=0), aqi DOUBLE PRECISION NOT NULL CHECK(aqi>=0),
 temperature_2m_mean DOUBLE PRECISION NOT NULL,
 precipitation_sum DOUBLE PRECISION NOT NULL CHECK(precipitation_sum>=0),
 wind_speed_10m_max DOUBLE PRECISION NOT NULL CHECK(wind_speed_10m_max>=0),
 grid_latitude DOUBLE PRECISION NOT NULL, grid_longitude DOUBLE PRECISION NOT NULL,
 weather_model TEXT NOT NULL, run_id TEXT NOT NULL, UNIQUE(city_id,date)
);
CREATE TABLE IF NOT EXISTS india_dw.etl_runs (
 run_id TEXT PRIMARY KEY, loaded_at TIMESTAMPTZ NOT NULL, rows INTEGER NOT NULL, quality_result JSONB NOT NULL
);
CREATE OR REPLACE VIEW india_dw.air_quality_enriched AS
 SELECT f.*,c.city_name,c.latitude,c.longitude,c.source_url,a.aqi_bucket,
 d.year,d.month,d.day,d.quarter,d.day_of_week
 FROM india_dw.fact_air_quality f
 JOIN india_dw.dim_city c USING(city_id)
 JOIN india_dw.dim_date d USING(date)
 JOIN india_dw.dim_aqi a USING(aqi_id);
