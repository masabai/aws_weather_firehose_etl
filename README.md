*** WORK IN PROGRESS ***

# aws_weather_firehose_etl: Serverless Weather Stream
This project demonstrates a fully functional Streaming Data Lake that ingests multi-city weather and AQI
data using a Schema-on-Read architecture. It is designed to monitor regional health risks—specifically Flu 
and Migraine indices—with comprehensive coverage across all major US regions (West, Mountain, South Central,
Midwest, Northeast, and Southeast).

## The Architecture
Producer: Python (Boto3) local application streaming ndjson to the cloud. The engine fetches data from OpenWeather and AccuWeather APIs and pushes to AWS Firehose. It includes custom error handling for IAM role assumption and API timeouts.
Ingestion: Amazon Data Firehose provides serverless transport and buffering for delivery to S3.
Processing: AWS Lambda (Event-Driven) is triggered by S3 raw/ uploads to validate data and prevent recursive loops through strict prefix filtering.
Storage: Amazon S3 Partitioned Data Lake organized via a Medallion architecture (Raw, Silver, and Gold tiers).
Analytics: Amazon Athena (SQL-based analytics) utilizes Window Functions to provide a "Latest Status" snapshot of regional health risks.
Dashboard: Streamlit application providing real-time CDC-style health alerts and risk visualizations.

## Repository Contents
producer/: extract_produce_weather.py – Core data ingestion script connecting local producers to AWS Firehose.
validation/: lambda_validator.py – S3-triggered Lambda function for data cleansing and folder-prefix enforcement.
database/: athena_setup.sql – DDL scripts to transform raw S3 JSON into queryable tables and managed views.
dashboard/: weather_watch_app.py – Streamlit application utilizing Altair for color-coded risk visualization and PyAthena for data retrieval.

## Engineering Solutions
Recursive Loop Prevention: Implemented S3 prefix filtering on event notifications to ensure Lambda executions only trigger on the raw/ prefix, preventing infinite loops during the write-back process to the silver/ prefix.
Data Deduplication: The Athena Gold View leverages ROW_NUMBER() over PARTITION BY location to ensure the dashboard reflects only the most recent health indices per city, filtering out historical batch noise.
Serverless Scaling: The architecture utilizes fully managed AWS services to eliminate infrastructure management while scaling automatically based on data volume.
