-- Athena Table Definition for Streaming Weather Data
-- Source: Lambda Producer -> Kinesis Firehose -> S3
-- Schema: Unified OpenWeather & AccuWeather metrics
CREATE EXTERNAL TABLE IF NOT EXISTS steam_weather.weather_data (
    timestamp string,
    location string,
    state string,
    temp_current_f double,
    temp_min_f double,
    temp_max_f double,
    feels_like_f double,
    humidity int,
    wind_speed double,
    weather_description string,
    aqi int,
    pm2_5 double,
    pm10 double,
    co double,
    no2 double,
    o3 double,
    cold_flu_index double,
    migraine_index double
)
-- Partitioned by Date and Hour to optimize query costs and performance
PARTITIONED BY (
    dt string,
    hour string
)
-- Use OpenX SerDe for robust JSON parsing
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json'='true'
)
LOCATION 's3://weather-stream-data-etl/'
-- Partition Projection: Automates partition discovery without running MSCK REPAIR TABLE
TBLPROPERTIES (
    'projection.enabled'='true',
    -- Date projection configuration
    'projection.dt.type'='date',
    'projection.dt.range'='2026/01/01,NOW',
    'projection.dt.format'='yyyy/MM/dd',
    -- Hour projection configuration (padded to 2 digits)
    'projection.hour.type'='integer',
    'projection.hour.range'='0,23',
    'projection.hour.digits'='2',
    -- Template matching Firehose's default S3 destination path
    'storage.location.template'='s3://weather-stream-data-etl/${dt}/${hour}/'
);
