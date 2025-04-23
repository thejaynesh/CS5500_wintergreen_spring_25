import os
import json
import pymysql

def lambda_handler(event, context):
    """
    Lambda function that retrieves and returns EHR systems 
    from the ehr_systems table.
    """
    try:
        # Database connection settings from environment variables
        db_config = {
            'host': os.environ['HOST'],
            'user': os.environ['USER_NAME'],
            'password': os.environ['PASSWORD'],
            'database': os.environ['DB_NAME'],
            'port': int(3306),
            'cursorclass': pymysql.cursors.DictCursor
        }

        # Connect to the database
        print("Connecting to the database...")
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        print("Database connection established.")

        # Check if a specific ehr_id was provided in the query parameters
        ehr_id = None
        if 'queryStringParameters' in event and event['queryStringParameters']:
            ehr_id = event['queryStringParameters'].get('ehr_id')

        if ehr_id:
            # Retrieve a specific EHR system
            print(f"Retrieving EHR system with ID: {ehr_id}")
            query = "SELECT * FROM ehr_systems WHERE ehr_id = %s"
            cursor.execute(query, (ehr_id,))
            ehr_system = cursor.fetchone()
            
            if not ehr_system:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'error': 'EHR system not found',
                        'ehr_id': ehr_id
                    })
                }
                
            # Print EHR system details for logging
            print(f"EHR system details: {json.dumps(ehr_system, default=str)}")
            
            # Format the response
            response = {
                'ehr_system': json.loads(json.dumps(ehr_system, default=str))
            }
        else:
            # Retrieve all EHR systems
            print("Retrieving all EHR systems")
            query = "SELECT * FROM ehr_systems"
            cursor.execute(query)
            ehr_systems = cursor.fetchall()
            
            # Print number of EHR systems found
            print(f"Found {len(ehr_systems)} EHR systems")
            
            # Print each EHR system for logging
            for system in ehr_systems:
                print(f"EHR system: {json.dumps(system, default=str)}")
            
            # Format the response
            response = {
                'count': len(ehr_systems),
                'ehr_systems': json.loads(json.dumps(ehr_systems, default=str))
            }

        # Optional: Get provider count for each EHR system
        if 'include_provider_count' in event.get('queryStringParameters', {}) and event['queryStringParameters']['include_provider_count'] == 'true':
            if ehr_id:
                # For a specific EHR system
                count_query = "SELECT COUNT(*) as provider_count FROM healthcare_providers WHERE ehr_id = %s"
                cursor.execute(count_query, (ehr_id,))
                count_result = cursor.fetchone()
                response['ehr_system']['provider_count'] = count_result['provider_count']
            else:
                # For all EHR systems
                for system in response['ehr_systems']:
                    count_query = "SELECT COUNT(*) as provider_count FROM healthcare_providers WHERE ehr_id = %s"
                    cursor.execute(count_query, (system['ehr_id'],))
                    count_result = cursor.fetchone()
                    system['provider_count'] = count_result['provider_count']

        cursor.close()
        conn.close()
        print("Database connection closed.")

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Failed to retrieve EHR systems',
                'details': str(e)
            })
        }