CREATE EXTERNAL TABLE IF NOT EXISTS steam_weather.weather_data (
    timestamp string,
    location string,
    state string,
    temp_current_f double,
    wind_speed double,
    humidity int,
    aqi int,
    cold_flu_index double,
    migraine_index double
)
PARTITIONED BY (dt string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION 's3://weather-stream-data-etl/raw/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.range'='2026/01/01,NOW',
    'projection.dt.format'='yyyy/MM/dd',
    'projection.hour.type'='integer',
    'projection.hour.range'='0,23',
    'projection.hour.digits'='2',
    'storage.location.template'='s3://weather-stream-data-etl/raw/${dt}/${hour}/'
)
