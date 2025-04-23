import os
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that receives a JSON payload with healthcare provider data
    and inserts it into the healthcare_providers table.
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
        provider_name = body.get('provider_name')
        provider_type = body.get('provider_type')
        contact_email = body.get('contact_email')
        contact_phone = body.get('contact_phone')
        address = body.get('address')
        ehr_id = body.get('ehr_id')
        bulk_fhir_url = body.get('bulk_fhir_url')
        tenant_id = body.get('tenant_id')
        status = body.get('status')  
        notes = body.get('note')       
        secret_name = body.get('secret_name')   # Secret name passed directly
        
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
        required_fields = ['provider_name', 'provider_type', 'contact_email', 'contact_phone']
        missing_fields = []
        
        if not provider_name:
            missing_fields.append('provider_name')
        if not provider_type:
            missing_fields.append('provider_type')
        if not contact_email:
            missing_fields.append('contact_email')
        if not contact_phone:
            missing_fields.append('contact_phone')
        
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required fields',
                    'missing_fields': missing_fields
                })
            }

        # SQL query to insert data into the healthcare_providers table
        insert_query = """
            INSERT INTO healthcare_providers (
                provider_name, provider_type, contact_email, contact_phone, address,
                ehr_id, bulk_fhir_url, tenant_id, secret_name,
                status, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values = (
            provider_name,
            provider_type,
            contact_email,
            contact_phone,
            address,
            ehr_id,
            bulk_fhir_url,
            tenant_id,
            secret_name,
            status,
            notes
        )

        cursor.execute(insert_query, values)
        conn.commit()
        
        # Get the last inserted ID
        provider_id = cursor.lastrowid
        
        if not provider_id:
            # If lastrowid is not available, try to find the new record by other means
            print("Warning: Could not get lastrowid, trying alternative method")
            find_query = """
                SELECT provider_id FROM healthcare_providers 
                WHERE provider_name = %s AND contact_email = %s
                ORDER BY onboarded_date DESC LIMIT 1
            """
            cursor.execute(find_query, (provider_name, contact_email))
            result = cursor.fetchone()
            if result:
                provider_id = result['provider_id']
                print(f"Found provider using alternative method: {provider_id}")
            else:
                print("Warning: Could not find newly inserted provider")
                provider_id = "unknown"  # Fallback
        
        # Get the inserted record with explicit selection of provider_id
        select_query = "SELECT provider_id, provider_name, provider_type, contact_email, contact_phone, address, ehr_id, bulk_fhir_url, tenant_id, secret_name, status, notes, onboarded_date, last_data_fetch FROM healthcare_providers WHERE provider_id = %s"
        cursor.execute(select_query, (provider_id,))
        new_provider = cursor.fetchone()
        
        # Handle case where we couldn't retrieve the newly inserted record
        if not new_provider:
            print(f"Warning: Could not retrieve provider details for ID: {provider_id}")
            # Create a minimal provider record so we can still return something
            new_provider = {
                'provider_id': provider_id,
                'provider_name': provider_name,
                'provider_type': provider_type,
                'contact_email': contact_email,
                'contact_phone': contact_phone
            }
        
        cursor.close()
        conn.close()
        print(f"Provider added successfully with ID: {provider_id}")

        # Make sure provider_id is explicitly included
        response_provider = json.loads(json.dumps(new_provider, default=str))
        
        # Double-check that provider_id is in the response
        if 'provider_id' not in response_provider and provider_id:
            response_provider['provider_id'] = str(provider_id)

        return {
            'statusCode': 201,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Healthcare provider added successfully',
                'provider': response_provider
            })
        }

    except pymysql.MySQLError as e:
        error_code = e.args[0]
        error_message = e.args[1]
        
        print(f"MySQL Error {error_code}: {error_message}")
        
        if error_code == 1062:  # Duplicate entry
            return {
                'statusCode': 409,
                'body': json.dumps({
                    'error': 'A provider with this ID already exists',
                    'details': error_message
                })
            }
        elif error_code == 1452:  # Foreign key constraint failure
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
                'error': 'Failed to add healthcare provider',
                'details': str(e)
            })
        }