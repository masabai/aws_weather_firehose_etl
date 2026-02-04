# AWS Serverless Weather Stream

This project demonstrates a fully functional Streaming Data Lake that ingests multi-city weather and AQI data using a Schema-on-Read architecture. It is designed to monitor regional health risks—specifically Flu and Migraine indices—with comprehensive coverage across all major US regions (West, Mountain, South Central, Midwest, Northeast, and Southeast).

### End-to-End Flow ###
`EventBridge (Schedule) → Producer Lambda → Kinesis Data Firehose → S3 Raw → Validator Lambda → Silver Zone → Quarantine Zone → Athena → Streamlit → GitHub Actions`

## The Architecture

##### *Producer: Python Application Streaming NDJSON to Firehose* #####

Python (Boto3) local application streaming NDJSON to the cloud. The engine fetches data from OpenWeather and AccuWeather APIs and pushes to AWS Firehose, which provides serverless transport and buffering for delivery to S3.  
This scheduled execution model enables fully automated, time-based ingestion without reliance on always-on services or local cron jobs.

---

##### *Amazon EventBridge rule triggering the producer Lambda on a fixed schedule for continuous weather data ingestion* #####

![EventBridge Schedule for Weather Ingestion](screenshots/weather_event_schedule.png)

---

##### *CloudWatch logs showing the Producer Lambda successfully streaming weather data to Firehose and S3 in real time.* #####

![CloudWatch Producer Logs](screenshots/cloud_watch_petaluma.png)

---

##### *AWS Lambda ETL Validator: Raw → Silver Transformation* #####

AWS Lambda is triggered by S3 `ObjectCreated` events in the `raw/` prefix to perform automated data validation.  
It inspects each record for required weather and health fields, filters out malformed or incomplete entries, and routes clean data to the `silver/` zone while sending errors to a `quarantine/` zone for investigation.  
Strict prefix filtering and application-level execution gating prevent recursive loops when writing back to S3.  
This design ensures only high-quality, analytics-ready data is available for downstream processing with Athena, QuickSight, or other tools.  

![Lambda Trigger Screenshot](https://raw.githubusercontent.com/masabai/aws_weather_firehose_etl/main/screenshots/weather_lambda_trigger_s3.png)

---

##### *Storage: Amazon S3 Partitioned Data Lake (Medallion Architecture)* #####

Amazon S3 is used to implement a partitioned Data Lake with Raw, Silver, and Gold zones.  
S3 Lifecycle Policies automatically recycle `athena-results/` and `raw/` data after a defined retention period to maintain a zero-cost storage strategy.  

![S3 Firehose Storage Screenshot](https://raw.githubusercontent.com/masabai/aws_weather_firehose_etl/main/screenshots/weather_s3_firehose.png)

---

##### *Analytics: Amazon Athena SQL Queries for Latest Status Snapshots* #####

Amazon Athena provides SQL-based analytics directly on S3 using a Schema-on-Read approach.  
This enables real-time queries for regional health risks without duplicating data or incurring extra ETL overhead.  

![Athena Analytics Results](screenshots/weather_athena_results.png)

---

##### *Dashboard: Streamlit Application for Weather & Flu Alerts Visualization* #####

The Streamlit dashboard retrieves data via an API to avoid hardcoding AWS Access Keys or Secret Keys.  
It supports manual refresh and 60-second caching to prevent unnecessary Athena scan costs while providing real-time weather and flu alert visualization.  

![Weather Streamlit Dashboard](screenshots/weather_streamlit.png)

---

## CI/CD & Automated Deployment

The project uses GitHub Actions to automate the deployment and validation of the AWS serverless weather pipeline.  
On each push to the main branch, the workflow provisions and deploys cloud resources, validates configuration changes, and ensures the pipeline remains reproducible and consistent across environments.  
This approach enables continuous delivery without manual AWS console intervention, reducing deployment risk and configuration drift.

![Weather Pipeline Deployment](screenshots/deploy_weather_pipeline.png)

![GitHub Actions Workflow](screenshots/ga_workflow.png)
