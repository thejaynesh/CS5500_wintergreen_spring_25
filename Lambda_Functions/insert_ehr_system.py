import os
import json
import pymysql
import uuid
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that receives JSON payload with EHR system data
    and inserts it into the ehr_systems table.
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

        # Validate required fields
        required_fields = ['ehr_name']
        missing_fields = [field for field in required_fields if not body.get(field)]
        
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required fields',
                    'missing_fields': missing_fields
                })
            }

        # SQL query to insert data into the ehr_systems table - updated for new schema
        insert_query = """
            INSERT INTO ehr_systems (
                ehr_name, documentation_link, authorization_url, connection_url,
                description, is_supported
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """

        values = (
            body.get('ehr_name'),
            body.get('documentation_link'),
            body.get('authorization_url'),
            body.get('connection_url'),
            body.get('description'),
            body.get('is_supported', False)  # Default to False if not provided
        )

        cursor.execute(insert_query, values)
        conn.commit()
        
        # Get the inserted record
        select_query = "SELECT * FROM ehr_systems WHERE ehr_id = LAST_INSERT_ID()"
        cursor.execute(select_query)
        new_ehr = cursor.fetchone()
        
        cursor.close()
        conn.close()
        print("EHR system added successfully")

        return {
            'statusCode': 201,  # Created
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'EHR system added successfully',
                'ehr_system': json.loads(json.dumps(new_ehr, default=str))  # Convert dates to strings
            })
        }

    except pymysql.MySQLError as e:
        error_code = e.args[0]
        error_message = e.args[1]
        
        print(f"MySQL Error {error_code}: {error_message}")
        
        if error_code == 1062:  # Duplicate entry
            return {
                'statusCode': 409,  # Conflict
                'body': json.dumps({
                    'error': 'An EHR system with this ID already exists',
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
                'error': 'Failed to add EHR system',
                'details': str(e)
            })
        }