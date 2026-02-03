"""
AWS Lambda ETL Validator: Raw to Silver Transformation
------------------------------------------------------
Triggered by S3 'ObjectCreated' events in the 'raw/' prefix. This function
performs schema validation, filters out malformed records, and routes
data to either the 'silver/' zone (clean) or 'quarantine/' zone (errors).

Workflow:
1. Triggered by S3 Put in 'raw/'.
2. Validates presence of required weather/health fields.
3. Decouples clean data for downstream analytics (Athena).
"""

import boto3
import json

s3 = boto3.client("s3")
DEST_BUCKET = "weather-stream-data-etl"


def validate_record(record):
    """
    Checks for the presence of mandatory fields and non-null values.

    Args:
        record (dict): The weather data record to validate.

    Returns:
        tuple: (bool, str) indicating (is_valid, missing_field_name).
    """
    required = ["timestamp", "location", "temp_current_f", "humidity", "aqi"]
    for field in required:
        if field not in record or record[field] is None:
            return False, field
    return True, None


def lambda_handler(event, context):
    """
    Standard AWS Lambda Handler for S3 Events.
    Processes incoming 'raw/' JSON files and performs line-by-line validation.

    Args:
        event (dict): S3 Event notification containing bucket and key info.
        context: Runtime information.

    Returns:
        dict: Processing status.
    """
    # Extract source details from the S3 event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    # SAFETY: Early exit if triggered by a prefix other than 'raw/'
    # (Prevents recursive triggers if the same bucket is used for output)
    if not source_key.startswith("raw/"):
        return {"status": "skipped", "reason": "not_in_raw_prefix"}

    # Fetch the raw file content
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    body = response['Body'].read().decode('utf-8')

    valid_records, invalid_records = [], []

    # Process records line-by-line (NDJSON format)
    for line in body.splitlines():
        if not line.strip(): continue
        try:
            record = json.loads(line)
            ok, missing = validate_record(record)
            if ok:
                valid_records.append(json.dumps(record))
            else:
                invalid_records.append({"error": f"Missing: {missing}", "data": record})
        except Exception as e:
            invalid_records.append({"error": str(e), "raw": line})

    # Generate output filename (maintaining the original name)
    filename = source_key.split('/')[-1]

    # Write validated data to Silver Zone
    if valid_records:
        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=f"silver/{filename}",
            Body="\n".join(valid_records)
        )

    # Write failed records to Quarantine for investigation
    if invalid_records:
        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=f"quarantine/{filename}",
            Body=json.dumps(invalid_records)
        )

    return {
        "status": "success",
        "processed_file": source_key,
        "clean_count": len(valid_records),
        "error_count": len(invalid_records)
    }
