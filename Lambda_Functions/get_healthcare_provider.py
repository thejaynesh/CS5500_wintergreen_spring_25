import os
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that retrieves a healthcare provider by ID,
    including all associated EHR system data as a single flat JSON object.
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
        
        print("Event payload parsed, processing provider data...")

        # Extract the fields from the body
        provider_id = body.get('provider_id')

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
        print("Database connection established.")

        # Validate required fields
        if not provider_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field',
                    'missing_field': 'provider_id'
                })
            }

        # Join healthcare_providers with ehr_systems to get all data in one query
        # Rename EHR fields to avoid column name collisions
        join_query = """
            SELECT 
                p.*,
                e.ehr_name,
                e.documentation_link,
                e.authorization_url,
                e.connection_url,
                e.description AS ehr_description,
                e.is_supported,
                e.is_tenant_id_required
            FROM 
                healthcare_providers p
            LEFT JOIN 
                ehr_systems e ON p.ehr_id = e.ehr_id
            WHERE 
                p.provider_id = %s
        """
        cursor.execute(join_query, (provider_id,))
        combined_data = cursor.fetchone()
        
        # Handle case where provider doesn't exist
        if not combined_data:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Provider not found',
                    'provider_id': provider_id
                })
            }
        
        cursor.close()
        conn.close()
        print(f"Provider data retrieved successfully for ID: {provider_id}")

        # Convert data to JSON-compatible format
        response_data = json.loads(json.dumps(combined_data, default=str))

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'provider': response_data
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
                'error': 'Failed to retrieve provider data',
                'details': str(e)
            })
        }