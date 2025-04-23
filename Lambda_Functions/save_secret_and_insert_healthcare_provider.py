import json
import boto3

def lambda_handler(event, context):
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
        data = body
        print("Event payload parsed, processing credentials...")

        # Extract sensitive credentials that should go to Secrets Manager
        client_id = body.get('client_id')
        client_secret = body.get('client_secret')
        
        # Store credentials in Secrets Manager if provided
        secret_name = None
        client = boto3.client('lambda')
        if client_id and client_secret:
            try:
                # Generate a safe name for the secret based on provider name
                provider_name = body.get('provider_name', 'Unknown')
                
                # Create the secret value
                secret_value = json.dumps({
                    'provider_name': provider_name,
                    'client_id': client_id,
                    'client_secret': client_secret
                })
                
                print("Attempting to store credentials in Secrets Manager...")
                # Store in Secrets Manager
                
                response = client.invoke(
                    FunctionName = 'store_ehr_client_id_and_secret',
                    InvocationType = 'RequestResponse',
                    Payload = secret_value
                )
                print("Response from Lambda function:", response)
                response_payload = json.loads(response['Payload'].read())
                print("Response payload:", response_payload)
                secret_name =response_payload.get('secret_name')
                print(f"Credentials stored in Secrets Manager with secret_name: {secret_name}")
            except Exception as e:
                print(f"Error storing credentials in Secrets Manager: {str(e)}")
                # Continue without storing credentials - just log the error
                # We don't want to block provider creation if Secrets Manager fails
                print("Proceeding without storing credentials.")
                pass
        
        try:
            healthcare_provider = json.dumps({
                'provider_name': body.get('provider_name'),
                'provider_type': body.get('provider_type'),
                'contact_email':body.get('contact_email'),
                'contact_phone':body.get('contact_phone'),
                'address':body.get('address'),
                'ehr_id': body.get('ehr_id'),
                'bulk_fhir_url':body.get('bulk_fhir_url'),
                'tenant_id':body.get('tenant_id'),
                'status':body.get('status'),
                'note':body.get('note'),
                'secret_name': secret_name
            })
            response_healthcare_provider = client.invoke(
                    FunctionName = 'insert_healthcare_provider',
                    InvocationType = 'RequestResponse',
                    Payload = healthcare_provider
                )
            print("Response from insert Lambda function:", response_healthcare_provider)
        except Exception as e:
                print(f"Error inserting: {str(e)}")
                # Continue without storing credentials - just log the error
                # We don't want to block provider creation if Secrets Manager fails
                print("Proceeding without storing credentials.")
                pass
        return {
            'statusCode': 201,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Healthcare provider added successfully',
                'provider': data.get('provider_name')
            })
        }
    except Exception as e:
        error_code = e.args[0]
        
        if error_code == 1062:  # Duplicate entry
            return {
                'statusCode': 409,
                'body': json.dumps({
                    'error': 'A provider with this ID already exists',
                    'details': "error_message"
                })
            }
        elif error_code == 1452:  # Foreign key constraint failure
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid reference to EHR system',
                    'details': "error_message"
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Database error occurred',
                    'details':" error_message"+error_code
                })
            }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to add healthcare provider',
                'details': str(e)
            })
        }