import http.client
import json
import base64
import boto3

def lambda_handler(event, context):


    client = boto3.client('lambda')
    try:
        provider = client.invoke(
            FunctionName='get_healthcare_provider',  # Call getSecretValue Lambda function
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'provider_id': event.get('provider_id')
            })  
        )
        provider_response_payload = json.loads(provider['Payload'].read())
        print(provider_response_payload)
        provider_data = json.loads(provider_response_payload['body'])['provider']
        secret_name = provider_data.get('secret_name')
        print(secret_name)
        tenant_id = provider_data.get('tenant_id')
        bulk_fhir_url= provider_data.get('bulk_fhir_url')
        authorization_url = provider_data.get('authorization_url')
        connection_url = provider_data.get('connection_url')
        bulk_fhir_url = provider_data.get('bulk_fhir_url')
        is_tenant_id_required = provider_data.get('is_tenant_id_required')

        if is_tenant_id_required:
            authorization_url = authorization_url.replace(tenantID, tenant_id)

        # Invoke the getAuthorizationToken Lambda function
        response = client.invoke(
            FunctionName='get_authorization_token',  # Call Authorization Function
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'secret_name': secret_name,
                "connection_url": connection_url,
                "authorization_url":authorization_url
            })
        )
        # Parse the response
        response_payload = json.loads(response['Payload'].read())
        access_token = json.loads(response_payload['body'])['access_token']
        
        # Validate response
        if response_payload.get('statusCode') != 200:
            raise Exception(response_payload.get('body', 'Unknown error in response'))
        print('aaaa')
        # Group ID for the bulk FHIR export request
        conn = http.client.HTTPSConnection(connection_url)
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/fhir+json',
            'Prefer': 'respond-async'
        }
        print(bulk_fhir_url)
        since_timestamp = "2024-07-01T15:00:00Z"
        conn.request('GET', bulk_fhir_url+'?_type=Location', headers=headers)
        export_response = conn.getresponse()
        print(export_response)
        data = export_response.read()
        print(data)
        export_url = export_response.getheader('Content-Location')
        
        return export_url
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
