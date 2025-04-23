import json
import boto3
import http.client
import base64
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Extract secretName and tenantID from event
        connection_url = event['connection_url']
        authorization_url = event['authorization_url'] 
        secret_name = event['secret_name']
        
        # Retrieve secrets from AWS Secrets Manager
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name='us-west-1')
        
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(get_secret_value_response['SecretString'])
        
        client_id = secrets['client_id']
        client_secret = secrets['client_secret']

        # Authenticate with Cerner's FHIR API and retrieve access token
        conn = http.client.HTTPSConnection(connection_url)
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = 'grant_type=client_credentials&scope= system/Observation.read system/Practitioner.read system/Location.read system/Encounter.read'
        
        credentials = f'{client_id}:{client_secret}'
        auth_header = f'Basic {base64.b64encode(credentials.encode("utf-8")).decode("utf-8")}'
        headers['Authorization'] = auth_header

        conn.request('POST', authorization_url, payload, headers)
        response = conn.getresponse()

        # Log HTTP status code and response data for debugging
        print(f"HTTP Status Code: {response.status}")
        data = response.read()
        print(f"Raw response data: {data}")

        # Handle potential empty response
        if not data:
            raise Exception("Empty response received from the server.")

        # Decode JSON response
        token_data = json.loads(data)
        if 'access_token' not in token_data:
            raise Exception("Access token not found in response.")

        access_token = token_data['access_token']

        return {
            'statusCode': 200,
            'body': json.dumps({'access_token': access_token})
        }

    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error retrieving secret: {e}"})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
