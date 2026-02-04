-- verify recent load
SELECT
  location,
  timestamp,
  date_diff('minute', from_iso8601_timestamp(timestamp), now()) as minutes_ago
FROM weather_silver
ORDER BY timestamp DESC
LIMIT 10;

-- verify tables and view (gold)
select * from weather_data limit 5
select * from weather_bronze limit 5
select * from weather_silver limit 5
select * from weather_gold limit 5

-- Calculates average temperature per location and flu risk level
SELECT location,
ROUND(AVG(temp_current_f),2) as avg_temp,
cold_flu_index
FROM weather_data
GROUP BY location, cold_flu_index
ORDER BY cold_flu_index DESC

