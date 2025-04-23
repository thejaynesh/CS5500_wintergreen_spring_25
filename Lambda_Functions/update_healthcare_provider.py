import os
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that updates an existing healthcare provider record.
    
    Input:
        provider_id: ID of the provider to update (required)
        Other fields to update (optional): provider_name, provider_type, contact_email,
        contact_phone, address, ehr_id, bulk_fhir_url, tenant_id, secret_name, 
        status, notes
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
        
        print("Event payload parsed, processing update request...")

        # Extract the provider_id - required field
        provider_id = body.get('provider_id')
        if not provider_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field',
                    'details': 'provider_id is required for updates'
                })
            }
        
        # Connect to the database
        db_config = {
            'host': os.environ['HOST'],
            'user': os.environ['USER_NAME'],
            'password': os.environ['PASSWORD'],
            'database': os.environ['DB_NAME'],
            'port': int(3306),
            'cursorclass': pymysql.cursors.DictCursor
        }

        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        print(f"Database connection established. Updating provider: {provider_id}")

        # First, check if the provider exists
        check_query = "SELECT * FROM healthcare_providers WHERE provider_id = %s"
        cursor.execute(check_query, (provider_id,))
        existing_provider = cursor.fetchone()
        
        if not existing_provider:
            cursor.close()
            conn.close()
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Provider not found',
                    'provider_id': provider_id
                })
            }
        
        # Fields that can be updated
        updatable_fields = [
            'provider_name', 'provider_type', 'contact_email', 'contact_phone', 
            'address', 'ehr_id', 'bulk_fhir_url', 'tenant_id', 'secret_name', 
            'status', 'notes', 'last_data_fetch'
        ]
        
        # Build the update query dynamically
        update_fields = []
        update_values = []
        
        for field in updatable_fields:
            if field in body:
                update_fields.append(f"{field} = %s")
                update_values.append(body[field])
        
        # If nothing to update, return early
        if not update_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No valid fields to update',
                    'details': 'Request must include at least one updatable field'
                })
            }
        
        # Add provider_id for the WHERE clause
        update_values.append(provider_id)
        
        # Construct and execute the update query
        update_query = f"UPDATE healthcare_providers SET {', '.join(update_fields)} WHERE provider_id = %s"
        
        print(f"Executing update query: {update_query}")
        print(f"With values: {update_values}")
        
        cursor.execute(update_query, update_values)
        conn.commit()
        
        # Check if any rows were affected
        rows_affected = cursor.rowcount
        print(f"Rows affected: {rows_affected}")
        
        # Get the updated record
        cursor.execute(check_query, (provider_id,))
        updated_provider = cursor.fetchone()
        
        cursor.close()
        conn.close()
        print("Database connection closed.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Healthcare provider updated successfully',
                'provider': json.loads(json.dumps(updated_provider, default=str))
            })
        }

    except pymysql.MySQLError as e:
        error_code = e.args[0]
        error_message = e.args[1]
        
        print(f"MySQL Error {error_code}: {error_message}")
        
        if error_code == 1452:  # Foreign key constraint failure
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid reference to EHR system',
                    'details': error_message
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Database error occurred',
                    'details': error_message
                })
            }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to update healthcare provider',
                'details': str(e)
            })
        }