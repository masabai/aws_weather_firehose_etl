-- Silver Table: Cleaned & Validated Data
-- This table points to the 'silver/' prefix where the Validator Lambda
-- drops records that have passed all schema and null-value checks.
CREATE EXTERNAL TABLE IF NOT EXISTS steam_weather.weather_silver (
    `timestamp` string,
    location string,
    state string,
    temp_current_f double,
    humidity int,
    aqi int,
    cold_flu_index double,
    migraine_index double
)
-- Using JsonSerDe to read the validated NDJSON files
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
-- Points to the dedicated silver zone in your ETL bucket
LOCATION 's3://weather-stream-data-etl/silver/';
