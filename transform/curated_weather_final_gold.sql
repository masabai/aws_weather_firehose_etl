-- Gold View: Production Analytics Layer
-- Final presentation layer for Streamlit and QuickSight.
-- Features:
-- 1. Regional Classification: Maps states to HHS Region 9.
-- 2. Risk Categorization: Logic-based alerts (High/Moderate/Low).
-- 3. Deduplication: Uses Window Functions to select only the 'Latest' record per location.
CREATE OR REPLACE VIEW steam_weather.weather_gold AS
SELECT
    `timestamp`,
    location,
    state,
    temp_current_f,
    humidity,
    cold_flu_index,
    cdc_hhs_region,
    flu_risk_category
FROM (
    SELECT
        `timestamp`,
        location,
        state,
        temp_current_f,
        humidity,
        cold_flu_index,
        -- HHS Regional Mapping for CDC-aligned tracking
        CASE
            WHEN state IN ('CA', 'NV', 'HI') THEN 'HHS Region 9'
            ELSE 'Other Region'
        END as cdc_hhs_region,
        -- Risk Category Logic based on AccuWeather Lifestyle Index
        CASE
            WHEN cold_flu_index >= 4.0 THEN 'High Risk'
            WHEN cold_flu_index >= 2.0 THEN 'Moderate Risk'
            ELSE 'Low Risk'
        END as flu_risk_category,
        -- Deduplication logic: Selects the most recent record (rn=1) for each city
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY `timestamp` DESC) as rn
    FROM steam_weather.weather_silver
)
-- Only pass the freshest data to the dashboard
WHERE rn = 1;
