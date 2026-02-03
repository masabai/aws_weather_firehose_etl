"""
AWS Lambda Producer: Weather & Health Data Pipeline
--------------------------------------------------
Fetches real-time weather, air quality (AQI), and health indices (Cold/Flu, Migraine)
from OpenWeather and AccuWeather APIs. Data is unified and pushed to an
Amazon Kinesis Data Firehose delivery stream for downstream ETL and Athena querying.

Date: 2026-02-02
"""

import json
import boto3
import urllib3
import os
from datetime import datetime

# CONFIG
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
ACCUWEATHER_API_KEY = os.environ.get("ACCUWEATHER_API_KEY")
STREAM_NAME = "PUT-S3-PRNex"
AWS_REGION = "us-west-2"

LOCATIONS = [
    {"name": "Honolulu", "state": "HI", "lat": 21.3045, "lon": -157.8556, "accu_key": "585346"},
    {"name": "Las Vegas", "state": "NV", "lat": 36.1674, "lon": -115.1484, "accu_key": "558374"},
    {"name": "Petaluma", "state": "CA", "lat": 38.2331, "lon": -122.6336, "accu_key": "552789"},
    {"name": "Richmond", "state": "VA", "lat": 37.5407, "lon": -77.4360, "accu_key": "331252"},
    {"name": "New York", "state": "NY", "lat": 40.7128, "lon": -74.0060, "accu_key": "349727"},
    {"name": "Chicago", "state": "IL", "lat": 41.8781, "lon": -87.6298, "accu_key": "348308"},
    {"name": "Denver", "state": "CO", "lat": 39.7392, "lon": -104.9903, "accu_key": "347810"},
    {"name": "Miami", "state": "FL", "lat": 25.7617, "lon": -80.1918, "accu_key": "347936"},
    {"name": "Houston", "state": "TX", "lat": 29.7604, "lon": -95.3698, "accu_key": "351197"}
]

http = urllib3.PoolManager()
firehose = boto3.client("firehose", region_name=AWS_REGION)


def get_lifestyle_index(data, name):
    """
    Parses the AccuWeather Indices response to extract a specific value.

    Args:
        data (list): The JSON response from AccuWeather containing index groups.
        name (str): The name of the forecast index (e.g., 'Common Cold Forecast').

    Returns:
        float/int/None: The index value if found, otherwise None.
    """
    if not isinstance(data, list): return None
    for idx in data:
        if idx.get("Name") == name: return idx.get("Value")
    return None


def lambda_handler(event, context):
    """
    Main entry point for the Lambda function.
    Iterates through LOCATIONS, gathers API data, and sends records to Firehose.

    Args:
        event: AWS Lambda uses this to pass in event data.
        context: AWS Lambda uses this to pass in runtime information.

    Returns:
        dict: A summary containing the processing status and count of records sent.
    """
    count = 0
    for loc in LOCATIONS:
        try:
            # Weather - Fetch current temperature and humidity
            w_url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={loc['lat']}&lon={loc['lon']}"
                f"&appid={OPENWEATHER_API_KEY}&units=imperial")
            w_data = json.loads(http.request('GET', w_url).data.decode('utf-8'))

            # AQI - Fetch current air pollution index
            aq_url = (
                f"https://api.openweathermap.org/data/2.5/air_pollution"
                f"?lat={loc['lat']}&lon={loc['lon']}"
                f"&appid={OPENWEATHER_API_KEY}")
            aq_data = json.loads(http.request('GET', aq_url).data.decode('utf-8'))

            # AccuWeather - Fetch health-related lifestyle indices
            acc_url = (
                f"https://dataservice.accuweather.com/indices/v1/daily/1day/"
                f"{loc['accu_key']}/groups/1"
                f"?apikey={ACCUWEATHER_API_KEY}")
            acc_data = json.loads(http.request('GET', acc_url).data.decode('utf-8'))

            # Record Structure matching downstream Data Quality requirements
            record = {
                "timestamp": datetime.utcnow().isoformat(),
                "location": loc["name"],
                "state": loc["state"],
                "temp_current_f": w_data.get("main", {}).get("temp"),
                "humidity": w_data.get("main", {}).get("humidity"),
                "aqi": aq_data.get("list", [{}])[0].get("main", {}).get("aqi"),
                "cold_flu_index": get_lifestyle_index(acc_data, "Common Cold Forecast"),
                "migraine_index": get_lifestyle_index(acc_data, "Migraine Headache Forecast")
            }

            # Put Record to Kinesis Firehose
            firehose.put_record(
                DeliveryStreamName=STREAM_NAME,
                Record={"Data": json.dumps(record) + "\n"} #+ "\n" -> convert NDJSON
            )
            count += 1
            print(f"Success: {loc['name']}")

        except Exception as e:
            print(f"Failed {loc['name']}: {str(e)}")

    return {"status": "complete", "records_sent": count}
