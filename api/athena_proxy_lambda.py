import boto3
import json
import time

# Initialize Athena client
athena = boto3.client('athena')
print(boto3.client('sts').get_caller_identity())


def lambda_handler(event, context):
    """
    VIP API Proxy: Executes the Gold View query and returns JSON.
    This allows Streamlit to be 'Keyless' and secure.
    """

    # Dashboard view
    query = "SELECT * FROM steam_weather.weather_gold"
    database = "steam_weather"

    # S3 Athena path
    output_s3 = "s3://weather-stream-data-etl/athena-results/"

    try:
        # Trigger the Query
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_s3}
        )
        query_execution_id = response['QueryExecutionId']

        # Polling: Wait for the query to finish
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status'][
                'State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)

        if status != 'SUCCEEDED':
            return {
                'statusCode': 500,
                'body': json.dumps(f"Athena Query Failed with status: {status}")
            }

        # Fetch the results
        results = athena.get_query_results(QueryExecutionId=query_execution_id)

        # Transform Athena's complex row format into clean JSON
        rows = results['ResultSet']['Rows']
        columns = [col.get('VarCharValue', '') for col in rows[0]['Data']]

        clean_data = []
        for row in rows[1:]:
            values = [val.get('VarCharValue', '') for val in row['Data']]
            clean_data.append(dict(zip(columns, values)))

        # Return JSON to API Gateway (and then to Streamlit)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # Allows Streamlit Cloud to call this
            },
            'body': json.dumps(clean_data)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Internal Server Error: {str(e)}")
        }

