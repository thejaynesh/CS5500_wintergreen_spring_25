import http.client
import json
from urllib.parse import urlparse

def lambda_handler(event, context):
    """
    Lambda function that checks the status of a FHIR bulk export by polling 
    the Content-Location URL received from the initial export request.
    
    Designed to be used with Step Functions for polling management.
    
    Input:
        export_url: The complete Content-Location URL from the initial FHIR export request
        access_token: Bearer token for authorization
    
    Output:
        If complete (HTTP 200): The list of output file URLs and status "complete"
        If pending (HTTP 202): Status "pending" for step function to retry later
        If error: Error details and HTTP 500
    """
    try:
        # Extract parameters from the event
        export_url = event.get('export_url')
        access_token = event.get('access_token')
        
        if not export_url:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: export_url'})
            }
            
        if not access_token:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: access_token'})
            }
        
        # Parse the URL to get the host and path
        parsed_url = urlparse(export_url)
        host = parsed_url.netloc
        path = parsed_url.path
        
        # Set up headers for polling request
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        print(f"Checking export status: {export_url}")
        
        # Create connection and make request
        conn = http.client.HTTPSConnection(host)
        conn.request('GET', path, headers=headers)
        response = conn.getresponse()
        status = response.status
        
        print(f"Response status: {status}")
        
        if status == 200:
            # Export is complete, return the output files
            response_data = json.loads(response.read().decode('utf-8'))
            print("Export complete!")
            return {
                'status': 'complete',
                'statusCode': 200,
                'output': response_data,
                'export_url': export_url
            }
        elif status == 202:
            # Export is still in progress
            print("Export still in progress")
            
            # Get retry-after header if available
            retry_after = response.getheader('Retry-After')
            
            return {
                'status': 'pending',
                'statusCode': 202,
                'message': 'Export still in progress',
                'export_url': export_url,
                'retry_after': retry_after if retry_after else 10
            }
        else:
            # Unexpected status code
            error_data = response.read().decode('utf-8')
            print(f"Unexpected status code: {status}, Response: {error_data}")
            return {
                'status': 'error',
                'statusCode': status,
                'message': f'Received unexpected status code: {status}',
                'details': error_data,
                'export_url': export_url
            }
            
    except Exception as e:
        print(f"Error checking export status: {str(e)}")
        return {
            'status': 'error',
            'statusCode': 500,
            'message': f'Error checking export status: {str(e)}',
            'export_url': export_url if 'export_url' in locals() else 'unknown'
        }