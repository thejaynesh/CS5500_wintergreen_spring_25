import os
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that receives JSON payload with data fetch details
    and inserts it into the data_fetch_history table.
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
        
        print("Event payload parsed:", body)

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

        # Validate required field
        provider_id = body.get('provider_id')
        if not provider_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field: provider_id'
                })
            }

        # SQL query to insert data into the data_fetch_history table
        insert_query = """
            INSERT INTO data_fetch_history (
                provider_id, group_id, status, s3_location, error_details
            ) VALUES (%s, %s, %s, %s, %s);
        """

        # Get current datetime for fetch_time (will use database default)
        values = (
            provider_id,
            body.get('group_id'),  # Added group_id
            body.get('status', 'Success'),  # Default to Success if not provided
            body.get('s3_location'),
            body.get('error_details')
        )

        cursor.execute(insert_query, values)
        conn.commit()
        
        # Get the inserted record
        select_query = "SELECT * FROM data_fetch_history WHERE fetch_id = LAST_INSERT_ID()"
        cursor.execute(select_query)
        new_fetch_record = cursor.fetchone()
        
        # Also retrieve the provider information for context
        provider_query = "SELECT provider_name, provider_type FROM healthcare_providers WHERE provider_id = %s"
        cursor.execute(provider_query, (provider_id,))
        provider_info = cursor.fetchone()
        
        cursor.close()
        conn.close()
        print("Data fetch history record added successfully")

        # Combine the data for the response
        response_data = {
            'data_fetch': json.loads(json.dumps(new_fetch_record, default=str))
        }
        
        if provider_info:
            response_data['provider'] = provider_info

        return {
            'statusCode': 201,  # Created
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Data fetch history record added successfully',
                'data': response_data
            })
        }

    except pymysql.MySQLError as e:
        error_code = e.args[0]
        error_message = e.args[1]
        
        print(f"MySQL Error {error_code}: {error_message}")
        
        if error_code == 1452:  # Foreign key constraint failure
            return {
                'statusCode': 400,  # Bad request
                'body': json.dumps({
                    'error': 'Invalid provider_id. The provider does not exist.',
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
                'error': 'Failed to add data fetch history record',
                'details': str(e)
            })
        }