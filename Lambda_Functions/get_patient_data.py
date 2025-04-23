import http.client
import json
import gzip
import boto3
from urllib.parse import urlparse
from datetime import datetime
from botocore.exceptions import ClientError

def invoke_authorization_lambda():
    # Create a Lambda client
    client = boto3.client('lambda')

    # Invoke the authorization Lambda function
    response = client.invoke(
        FunctionName='authorization',  
        InvocationType='RequestResponse'
    )
    
    # Parse the response payload
    response_payload = json.loads(response['Payload'].read())
    if response_payload['statusCode'] == 200:
        return response_payload['body']
    else:
        raise Exception("Failed to retrieve access token from authorization Lambda.")

def process_fhir_export(url, type, access_token):
    parsed_url = urlparse(url)

    try:
        # Make a GET request to initiate bulk FHIR export
        conn = http.client.HTTPSConnection(parsed_url.netloc)
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/fhir+ndjson',
        }
        conn.request('GET', parsed_url.path, headers=headers)
        response = conn.getresponse()

        # Check if the response is a redirect
        if response.status == 307:
            # Follow the redirect
            location = response.getheader('Location')
            parsed_location = urlparse(location)
            conn = http.client.HTTPSConnection(parsed_location.netloc)
            conn.request('GET', parsed_location.path + "?" + parsed_location.query, headers={'Content-Type': 'application/fhir+ndjson'})
            response = conn.getresponse()

        # Check if the response is gzip-encoded
        if response.getheader('Content-Encoding') == 'gzip':
            body_bytes = gzip.decompress(response.read())
            body = body_bytes.decode('utf-8')
        else:
            body = response.read().decode('utf-8')

        jsonobjects = body.split('\n')
        if jsonobjects[-1] == '':
            jsonobjects = jsonobjects[:-1]
        
        # Initialize S3 client
        s3 = boto3.client('s3')
        todaydate = datetime.now().strftime('%Y-%m-%d')

        # Define the S3 key (filename) where the JSON object will be saved
        key = f"HealthLakeOutput/{type}_{todaydate}.ndjson"

        # Upload the JSON object as a file to the specified S3 bucket
        s3.put_object(Body=body, Bucket='myheathlakeimportbucket', Key=key)

    except ClientError as e:
        print(f"Error with S3 upload: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def lambda_handler(event, context):
    try:
        # Invoke authorization Lambda to get a new access token
        access_token = invoke_authorization_lambda()

        # Get output from input event
        get_job_status = event.get('GetJobStatus')
        output = get_job_status.get('ResponseBody', {}).get('output', [])

        # Process each URL in the output
        for item in output:
            process_fhir_export(item.get('url'), item.get('type'), access_token)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Processing complete for all URLs'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
