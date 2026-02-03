-- Bronze Table: Initial Landing Zone for Raw Firehose Data
-- Handles schema-on-read for NDJSON files stored in S3
CREATE EXTERNAL TABLE IF NOT EXISTS steam_weather.weather_bronze (
    `timestamp` string,
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
-- Partitions align with Firehose's default /yyyy/MM/dd/HH S3 pathing
PARTITIONED BY (dt string, hour string)
-- JsonSerDe allows Athena to parse the raw JSON records
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json'='true'
    -- Note: Only use 'mapping' if your JSON key differs from the column name
    -- 'mapping.timestamp' = 'source_timestamp'
)
LOCATION 's3://weather-stream-data-etl/'
-- Partition Projection: Eliminates the need for manual 'MSCK REPAIR TABLE'
-- Dynamically calculates S3 paths for better query performance
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.range'='2026/01/01,NOW',
    'projection.dt.format'='yyyy/MM/dd',
    'projection.hour.type'='integer',
    'projection.hour.range'='0,23',
    'projection.hour.digits'='2',
    'storage.location.template'='s3://weather-stream-data-etl/${dt}/${hour}/'
);
