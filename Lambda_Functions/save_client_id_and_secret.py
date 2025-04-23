import boto3
import json
import uuid
from datetime import datetime
from botocore.config import Config
from botocore.exceptions import ClientError

def lambda_handler(event,context):
    """
    Stores provider credentials in AWS Secrets Manager.
    
    Args:
        provider_name: Name of the healthcare provider
        client_id: Provider's client ID
        client_secret: Provider's client secret
        
    Returns:
        dict: Response containing status and ARN if successful
    """
    try:
        # Parse the incoming JSON payload, handling different event structures
        if isinstance(event, dict) and 'body' in event:
            # API Gateway integration pattern
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']  # Body might already be parsed
        else:
            # Direct Lambda invocation pattern
            body = event
        
        print("Event payload parsed, processing credentials...")

        # Extract sensitive credentials that should go to Secrets Manager
        client_id = body.get('client_id')
        client_secret = body.get('client_secret')
        
        # Store credentials in Secrets Manager if provided
        secrets_manager_arn = None
        if client_id and client_secret:
            try:
                # Generate a safe name for the secret based on provider name
                provider_name = body.get('provider_name', 'Unknown')
                safe_name = provider_name.replace(' ', '-').lower()
                secret_name = f"healthcare-provider/{safe_name}-{str(uuid.uuid4())[:8]}"
                
                # Create the secret value
                secret_value = json.dumps({
                    'client_id': client_id,
                    'client_secret': client_secret
                })
                
                print("Attempting to store credentials in Secrets Manager...")
                # Store in Secrets Manager
                session = boto3.session.Session()
                secrets_client = session.client(service_name='secretsmanager',region_name='us-west-1')
                print("cli initiated")
                response = secrets_client.create_secret(
                    Name=secret_name,
                    Description=f"API credentials for healthcare provider: {provider_name}",
                    SecretString=secret_value,
                )
                secrets_manager_arn = response['ARN']
                return {
                    'status': 'success',
                    'arn': secrets_manager_arn,
                    'secret_name': secret_name
                }
                print(f"Credentials stored in Secrets Manager with ARN: {secrets_manager_arn}")
            except Exception as e:
                print(f"Error storing credentials in Secrets Manager: {str(e)}")
                # Continue without storing credentials - just log the error
                # We don't want to block provider creation if Secrets Manager fails
                print("Proceeding without storing credentials.")
                pass
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"AWS Error: {error_code} - {error_message}")
        
        if error_code == 'AccessDeniedException':
            print("Lambda lacks permissions to access Secrets Manager")
        elif error_code == 'ResourceNotFoundException':
            print("The requested secret or resource was not found")
        elif error_code == 'InvalidRequestException':
            print("The request was invalid due to: " + error_message)
        elif error_code == 'LimitExceededException':
            print("Service limit exceeded")
        
        return {
            'status': 'error',
            'error': error_code,
            'message': error_message
        }
        
    except Exception as e:
        print(f"Unexpected error storing credentials: {str(e)}")
        return {
            'status': 'error',
            'error': 'UnexpectedException',
            'message': str(e)
        }