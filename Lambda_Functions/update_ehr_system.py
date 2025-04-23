import os
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that updates an existing EHR system record.
    
    Input:
        ehr_id: ID of the EHR system to update (required)
        Other fields to update (optional): ehr_name, documentation_link, 
        authorization_url, connection_url, description, is_supported,
        is_tenant_id_required
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

        # Extract the ehr_id - required field
        ehr_id = body.get('ehr_id')
        if not ehr_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field',
                    'details': 'ehr_id is required for updates'
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
        print(f"Database connection established. Updating EHR system: {ehr_id}")

        # First, check if the EHR system exists
        check_query = "SELECT * FROM ehr_systems WHERE ehr_id = %s"
        cursor.execute(check_query, (ehr_id,))
        existing_ehr = cursor.fetchone()
        
        if not existing_ehr:
            cursor.close()
            conn.close()
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'EHR system not found',
                    'ehr_id': ehr_id
                })
            }
        
        # Fields that can be updated
        updatable_fields = [
            'ehr_name', 'documentation_link', 'authorization_url', 
            'connection_url', 'description', 'is_supported',
            'is_tenant_id_required'
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
        
        # Add ehr_id for the WHERE clause
        update_values.append(ehr_id)
        
        # Construct and execute the update query
        update_query = f"UPDATE ehr_systems SET {', '.join(update_fields)} WHERE ehr_id = %s"
        
        print(f"Executing update query: {update_query}")
        print(f"With values: {update_values}")
        
        cursor.execute(update_query, update_values)
        conn.commit()
        
        # Check if any rows were affected
        rows_affected = cursor.rowcount
        print(f"Rows affected: {rows_affected}")
        
        # Get the updated record
        cursor.execute(check_query, (ehr_id,))
        updated_ehr = cursor.fetchone()

        # Get count of providers using this EHR system
        count_query = "SELECT COUNT(*) as provider_count FROM healthcare_providers WHERE ehr_id = %s"
        cursor.execute(count_query, (ehr_id,))
        count_result = cursor.fetchone()
        provider_count = count_result['provider_count'] if count_result else 0
        
        cursor.close()
        conn.close()
        print("Database connection closed.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EHR system updated successfully',
                'ehr_system': json.loads(json.dumps(updated_ehr, default=str)),
                'provider_count': provider_count
            })
        }

    except pymysql.MySQLError as e:
        error_code = e.args[0]
        error_message = e.args[1]
        
        print(f"MySQL Error {error_code}: {error_message}")
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
                'error': 'Failed to update EHR system',
                'details': str(e)
            })
        }